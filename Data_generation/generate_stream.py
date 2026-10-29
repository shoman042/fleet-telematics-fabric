"""
generate_stream.py - streaming fleet generator (P1-5, P1-6, P1-10, P1-11, P1-12).

What it does
  1. Reads reference/ (trips, routes, checkpoints, cargo types, vehicles + change files) and config.yaml.
  2. PLANS every trip first: natural short stops plus the injected cases
       cold-chain breach, cold-chain blip (negative), excessive idling, route deviation, GPS wander (negative),
       sensor fault (null readings), stuck sensor (flat readings), unit error (temperature in F),
       slow trip (late arrival), near-miss (arrives late but inside the grace period, negative),
       offline window (P1-12, only when offline.enabled is true).
  3. Data faults (P1-10) are applied to single messages after they are built: dropped, duplicate, near duplicate,
     malformed, missing field, wrong type, invalid GPS, impossible value, bad timestamp, unknown vehicle,
     schema drift, temperature spike, plus the optional poison batch.
     A dry pass runs the whole simulation once without sending. It collects the data faults, the offline windows and
     the real arrival time of every trip, so the complete ground truth is written to
     evidence/injected_events_log.csv and evidence/run_manifest.json BEFORE anything is sent.
  4. Simulates each truck (state machine on the simulated clock) and sends gps / engine / fuel / temperature
     (and device_status) messages, keyed by vehicle_id, to the Eventstream Kafka endpoints (or to a local file).
  5. At the end writes evidence/trip_outcomes.csv and evidence/run_summary.json.

Late arrival (P1-11): a late trip paces itself to a chosen arrival time (planned end + grace + 10..60 min for a slow
trip, planned end + 30..80 percent of the grace for a near-miss). The ground truth comes from the real arrival times
of the dry pass: every trip that really arrives later than planned end + grace is logged as LATE_ARRIVAL.

Offline windows (P1-12): the truck keeps measuring but cannot send. Its readings are stored with their true
event_time and consecutive sequence numbers. On reconnect the device sends a device_status report (RECONNECTED), then
the stored readings: as a burst on the stream (STREAM_BURST) or as a bulk file <out>/landing/offline/<vehicle_id>_<upload_id>.jsonl
(BULK_FILE). In an overflow window the buffer holds only buffer_capacity_messages per signal; the oldest readings are lost.

Not implemented here: reference-file faults (handled by generate_reference_data.py).

Examples
  python generate_stream.py --sink jsonl --fast          # no Kafka, as fast as possible, writes evidence/stream_messages.jsonl
  python generate_stream.py                              # smoke profile, paced by the acceleration factor, sends to Kafka
  python generate_stream.py --mode clean                 # baseline: no injected anomalies, no data faults, no late trips, no offline
  python generate_stream.py --mode live_demo --accel 5

.env variables (same as test_all.py): KAFKA_<SIGNAL>_BOOTSTRAP / _CONN_STR / _TOPIC for GPS, ENGINE, FUEL, TEMPERATURE
(and DEVICE_STATUS when offline.enabled is true).
"""
import argparse
import bisect
import csv
import glob
import heapq
import json
import math
import os
import random
import sys
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from functools import reduce

from config import load_config

FMT = "%Y-%m-%d %H:%M:%S"
GENERATOR_VERSION = "1.3"
SIGNALS = ["gps", "engine", "fuel", "temperature"]
ALL_SIGNALS = SIGNALS + ["device_status"]
SETPOINTS = {"FOOD_CHILLED": 4.0, "MEDICINE_COLD": 4.0, "FROZEN_FOOD": -21.0}
ACCEL_KMH_S = 1.5        # speeding up
DECEL_KMH_S = 3.0        # braking
IDLE_BURN_L_PER_H = 3.0
DRIVE_BURN_L_PER_KM = 0.28
MARGIN_MIN = 10.0        # minimum gap between two planned windows of one trip
NATURAL_SPEED_SHARE = 0.94        # a truck cruises at about 94 percent of the limit on average
LATE_MIN_SPEED_FRACTION = 0.6     # a slow trip never needs to go slower than 60 percent of its natural speed

INJECTION_COLS = ["injection_id", "run_id", "injection_type", "vehicle_id", "trip_id", "signal_type", "event_id",
                  "start_time", "end_time", "details", "seed"]

# Data faults (P1-10): (injection_type, key in config rates, signals it applies to). Order matters (cumulative draw).
FAULT_SPECS = [
    ("DROPPED_MESSAGE", "dropped_message_rate", SIGNALS),
    ("DUPLICATE", "duplicate_rate", SIGNALS),
    ("NEAR_DUPLICATE", "near_duplicate_rate", SIGNALS),
    ("MALFORMED", "malformed_payload", SIGNALS),
    ("MISSING_FIELD", "missing_field_rate", SIGNALS),
    ("WRONG_TYPE", "wrong_type_rate", SIGNALS),
    ("INVALID_GPS", "invalid_gps_rate", ["gps"]),
    ("IMPOSSIBLE_VALUE", "impossible_value_rate", ["gps", "engine"]),
    ("BAD_TIMESTAMP", "bad_timestamp_rate", SIGNALS),
    ("UNKNOWN_VEHICLE", "unknown_vehicle_rate", SIGNALS),
    ("SCHEMA_DRIFT", "schema_drift_rate", SIGNALS),
    ("TEMPERATURE_SPIKE", "temperature_spike_rate", ["temperature"]),
]
NUMERIC_FIELDS = {
    "gps": ["latitude", "longitude", "speed_kmh", "gps_accuracy_meters"],
    "engine": ["engine_rpm", "engine_temperature_c", "vehicle_speed_kmh", "battery_voltage_v"],
    "fuel": ["fuel_level_liters", "trip_fuel_used_liters", "odometer_km"],
    "temperature": ["temperature_value", "setpoint_temperature_c"],
}


# ----------------------------------------------------------------------------- helpers
def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(s):
    return datetime.strptime(s.strip()[:19].replace("T", " "), FMT)


def read_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def haversine_km(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


class RouteGeo:
    """Polyline of a route, built from its checkpoints."""

    def __init__(self, points):
        self.pts = points
        self.cum = [0.0]
        for a, b in zip(points, points[1:]):
            self.cum.append(self.cum[-1] + haversine_km(a, b))
        self.total = self.cum[-1]

    def at(self, frac):
        d = min(max(frac, 0.0), 1.0) * self.total
        i = min(bisect.bisect_right(self.cum, d) - 1, len(self.pts) - 2)
        seg = self.cum[i + 1] - self.cum[i]
        f = 0.0 if seg == 0 else (d - self.cum[i]) / seg
        (la1, lo1), (la2, lo2) = self.pts[i], self.pts[i + 1]
        lat, lon = la1 + (la2 - la1) * f, lo1 + (lo2 - lo1) * f
        bearing = math.degrees(math.atan2((lo2 - lo1) * math.cos(math.radians(lat)), la2 - la1)) % 360
        return lat, lon, bearing


def trapezoid(t, ramp_up, hold, ramp_down, peak):
    """0 -> peak over ramp_up, flat for hold, back to 0 over ramp_down (t in the same unit)."""
    if t < 0 or t >= ramp_up + hold + ramp_down:
        return 0.0
    if t < ramp_up:
        return peak * t / ramp_up
    if t < ramp_up + hold:
        return peak
    return peak * (ramp_up + hold + ramp_down - t) / ramp_down


# ----------------------------------------------------------------------------- planning
class Plan:
    def __init__(self, trip, route, cargo, start):
        self.trip, self.route, self.cargo, self.start = trip, route, cargo, start
        self.cold = cargo["temperature_min_c"].strip() != ""
        self.setpoint = None
        self.stops = []         # (start_s, end_s) seconds since departure
        self.excursions = []    # (start_min, ramp_up, hold, ramp_down, peak_c)
        self.deviations = []    # (start_min, ramp, hold, amplitude_km, sign)
        self.sensor_faults = []  # (start_min, length_min): temperature null, sensor_status FAULT
        self.stuck = []          # (start_min, length_min): temperature frozen at one value
        self.arrival_offset_min = None   # P1-11: target arrival, minutes after the planned end (None = natural)
        self.late_kind = None            # P1-11: "late" (slow trip) or "within_grace" (near-miss)
        self.offline = None              # P1-12: dict(start_s, len_s, overflow, method, upload_id)
        self.truth = []         # ground-truth rows (without id / run_id)


def plan_trip(ctx, trip, route, cargo, start):
    vid, tid = trip["vehicle_id"], trip["trip_id"]
    rng = random.Random(f"{ctx['seed']}:{vid}:plan:{tid}")
    dur = float(route["expected_duration_minutes"])
    plan = Plan(trip, route, cargo, start)
    forced = ctx["forced"].get(tid, set())
    zero = ctx["zero_rates"]
    rb, ri, rd = ctx["rates"]["breach"], ctx["rates"]["idle"], ctx["rates"]["dev"]
    u_breach, u_idle, u_dev, u_wander = rng.random(), rng.random(), rng.random(), rng.random()
    busy = []

    def free(s, e):
        return all(e + MARGIN_MIN <= bs or s >= be + MARGIN_MIN for bs, be in busy)

    def place(length, lo, hi, r=None):
        r = r or rng
        lo_m, hi_m = lo * dur, hi * dur - length
        if hi_m < lo_m:
            return None
        for _ in range(40):
            s = r.uniform(lo_m, hi_m)
            if free(s, s + length):
                busy.append((s, s + length))
                return s
        return None

    def log(kind, signal, s_min, e_min, details):
        plan.truth.append(dict(injection_type=kind, vehicle_id=vid, trip_id=tid, signal_type=signal, event_id="",
                               start_time=start + timedelta(minutes=s_min), end_time=start + timedelta(minutes=e_min),
                               details=details))

    if plan.cold:
        tmin, tmax = float(cargo["temperature_min_c"]), float(cargo["temperature_max_c"])
        plan.setpoint = SETPOINTS.get(cargo["cargo_type_id"], round(tmin + (tmax - tmin) / 3, 1))
        theta = tmax - plan.setpoint
        hold_alert = float(cargo["alert_hold_minutes"])
        want_breach = "breach" in forced or (not zero and u_breach < rb * 2 / 3)
        want_blip = "blip" in forced or (not zero and rb * 2 / 3 <= u_breach < rb)

        if want_breach and not want_blip:
            m = max(rng.uniform(5, 12), theta + 1.0)
            r1, r2, h = rng.uniform(5, 10), rng.uniform(5, 10), rng.uniform(10, 45)
            h = min(h, 0.6 * dur - r1 - r2)
            s = place(r1 + h + r2, 0.15, 0.85) if h >= 10 else None
            if s is not None:
                plan.excursions.append((s, r1, h, r2, m))
                ts, te = s + r1 * theta / m, s + r1 + h + r2 - r2 * theta / m
                log("COLD_CHAIN_BREACH", "temperature", ts, te, dict(
                    negative_case=False, expected_alert=True, cargo_type_id=cargo["cargo_type_id"],
                    setpoint_c=plan.setpoint, cargo_max_c=tmax, peak_c=round(plan.setpoint + m, 2),
                    ramp_up_min=round(r1, 2), hold_min=round(h, 2), ramp_down_min=round(r2, 2),
                    out_of_range_minutes=round(te - ts, 2), alert_hold_minutes=hold_alert))
        elif want_blip:
            overshoot = rng.uniform(1.0, 2.5)
            m = theta + overshoot
            r1 = r2 = 0.5
            target = hold_alert * rng.uniform(0.4, 0.7)   # clearly below the alert hold, even with sampling
            h = max(0.0, target - (r1 + r2) * (1 - theta / m))
            s = place(r1 + h + r2, 0.15, 0.85)
            if s is not None:
                plan.excursions.append((s, r1, h, r2, m))
                ts, te = s + r1 * theta / m, s + r1 + h + r2 - r2 * theta / m
                log("COLD_CHAIN_BLIP", "temperature", ts, te, dict(
                    negative_case=True, expected_alert=False, cargo_type_id=cargo["cargo_type_id"],
                    setpoint_c=plan.setpoint, cargo_max_c=tmax, peak_c=round(plan.setpoint + m, 2),
                    out_of_range_minutes=round(te - ts, 2), alert_hold_minutes=hold_alert))

    if "idle" in forced or (not zero and u_idle < ri):
        length = min(rng.uniform(15, 60), 0.4 * dur)
        s = place(length, 0.10, 0.85) if length >= 15 else None
        if s is not None:
            plan.stops.append((s * 60, (s + length) * 60))
            log("EXCESSIVE_IDLING", "engine", s, s + length, dict(
                negative_case=False, expected_alert=True, duration_minutes=round(length, 2),
                idle_minutes_threshold=ctx["idle_minutes_threshold"]))

    allowed_km = float(route["allowed_deviation_meters"]) / 1000.0
    if "deviation" in forced or (not zero and u_dev < rd):
        amp, hold, ramp = rng.uniform(1.0, 5.0), rng.uniform(5, 20), 1.0
        sign = rng.choice([-1, 1])
        s = place(hold + 2 * ramp, 0.15, 0.85)
        if s is not None:
            plan.deviations.append((s, ramp, hold, amp, sign))
            frac = allowed_km / amp
            log("ROUTE_DEVIATION", "gps", s + ramp * frac, s + ramp + hold + ramp * (1 - frac), dict(
                negative_case=False, expected_alert=True, peak_offset_m=round(amp * 1000),
                allowed_deviation_meters=round(allowed_km * 1000), side="left" if sign < 0 else "right"))
    if "wander" in forced or (not zero and u_wander < rd):
        amp, hold, ramp = rng.uniform(0.10, 0.35), rng.uniform(3, 15), 1.0
        sign = rng.choice([-1, 1])
        s = place(hold + 2 * ramp, 0.15, 0.85)
        if s is not None:
            plan.deviations.append((s, ramp, hold, amp, sign))
            log("GPS_WANDER", "gps", s, s + hold + 2 * ramp, dict(
                negative_case=True, expected_alert=False, peak_offset_m=round(amp * 1000),
                allowed_deviation_meters=round(allowed_km * 1000)))

    for _ in range(int(round(dur / 60 * 0.7))):      # normal short stops (negative cases for idling)
        length = rng.uniform(1.0, 4.0)
        s = place(length, 0.05, 0.95)
        if s is not None:
            plan.stops.append((s * 60, (s + length) * 60))
            log("SHORT_STOP", "engine", s, s + length, dict(
                negative_case=True, expected_alert=False, duration_minutes=round(length, 2)))

    # P1-10 sensor faults. They use their own random stream and are placed last, so the anomalies above
    # are identical with or without them.
    if plan.cold:
        frng = random.Random(f"{ctx['seed']}:{vid}:faultplan:{tid}")
        u_sensor, u_stuck = frng.random(), frng.random()
        if "sensor_fault" in forced or (not zero and u_sensor < ctx["rates"]["sensor_fault"]):
            length = frng.uniform(5, 30)
            s = place(length, 0.10, 0.90, frng)
            if s is not None:
                plan.sensor_faults.append((s, length))
                log("SENSOR_FAULT", "temperature", s, s + length, dict(
                    negative_case=False, expected_alert=True, duration_minutes=round(length, 2),
                    handling="KEEP_FLAGGED", flag="SENSOR_NULL"))
        if "stuck" in forced or (not zero and u_stuck < ctx["rates"]["stuck"]):
            length = ctx["flatline_len_min"] + frng.uniform(0, 10)
            s = place(length, 0.10, 0.90, frng)
            if s is not None:
                plan.stuck.append((s, length))
                log("STUCK_SENSOR", "temperature", s, s + length, dict(
                    negative_case=False, expected_alert=False, duration_minutes=round(length, 2),
                    handling="KEEP_FLAGGED", flag="FLATLINE"))

    # P1-11 late arrival. Own random stream, planned last. The truck paces itself to a chosen arrival time.
    # The ground truth of late arrivals is written later from the real arrival times (see late_truth_rows).
    lrng = random.Random(f"{ctx['seed']}:{vid}:lateplan:{tid}")
    u_late = lrng.random()
    grace = float(route["arrival_grace_minutes"])
    r_late, r_within = ctx["rates"]["late"], ctx["rates"]["within"]
    want_late = "late" in forced or (not zero and u_late < r_late)
    want_near = (not want_late) and ("within_grace" in forced or (not zero and r_late <= u_late < r_late + r_within))
    if want_late or want_near:
        slack = ctx["slack"].get(tid, 1e9)       # minutes between the planned end and the next trip of the vehicle
        nat_min = (float(route["planned_distance_km"]) / (NATURAL_SPEED_SHARE * float(route["speed_limit_kmh"])) * 60.0
                   + sum(e - s for s, e in plan.stops) / 60.0)
        latest = min(nat_min / LATE_MIN_SPEED_FRACTION - dur, slack - 3.0)   # latest possible arrival after the planned end
        if want_late:
            lo, hi = grace + 10.0, min(grace + 60.0, latest)
            if hi >= lo:
                plan.arrival_offset_min = lrng.uniform(lo, hi)
                plan.late_kind = "late"
        else:
            offset = lrng.uniform(0.3, 0.8) * grace
            if offset <= latest:
                plan.arrival_offset_min = offset
                plan.late_kind = "within_grace"

    # P1-12 offline window. Own random stream, planned last, so everything above is identical with or without it.
    # The ground truth of the window (stored / lost counts) is written later, from the dry pass.
    ocfg = ctx["offline"]
    if ocfg["enabled"] and not zero:
        orng = random.Random(f"{ctx['seed']}:{vid}:offlineplan:{tid}")
        u_off, u_surv, u_method = orng.random(), orng.random(), orng.random()
        kinds = forced & {"offline_burst", "offline_file", "offline_overflow"}
        if kinds or u_off < ocfg["share"]:
            overflow = "offline_overflow" in kinds or (not kinds and u_surv >= ocfg["survive"])
            if overflow:
                length = ctx["overflow_need_min"] + orng.uniform(ocfg["extra_min"], ocfg["extra_max"])
            else:
                length = orng.uniform(ocfg["min_minutes"], ocfg["max_len_survive"])
            if "offline_burst" in kinds:
                method = "STREAM_BURST"
            elif kinds:
                method = "BULK_FILE"
            else:
                span = max(ocfg["max_minutes"], length)
                p_burst = 0.0 if ocfg["burst_share"] <= 0 else min(
                    0.95, max(0.05, 2 * ocfg["burst_share"] * (1 - length / span)))
                method = "STREAM_BURST" if u_method < p_burst else "BULK_FILE"
            s = place(length, 0.10, 0.85, orng) if length <= 0.7 * dur else None
            if s is not None:
                plan.offline = dict(start_s=int(round(s * 60)), len_s=int(round(length * 60)),
                                    overflow=overflow, method=method, upload_id=None)
    plan.stops.sort()
    return plan


def choose_forced(seed, infos, min_per_type, offline_need=None):
    """infos: sorted list of (trip_id, is_cold, expected_duration_min, slack_min, grace_min).
    offline_need: minutes an overflow window needs (None = no offline cases). Returns {trip_id: {kinds}}."""
    forced = defaultdict(set)
    rng = random.Random(f"{seed}:force")
    used = set()
    kinds = (("breach", True), ("idle", False), ("deviation", False), ("blip", True), ("wander", False),
             ("sensor_fault", True), ("stuck", True), ("late", False), ("within_grace", False))
    if offline_need is not None:
        kinds = kinds + (("offline_burst", False), ("offline_file", False), ("offline_overflow", False))
    for kind, needs_cold in kinds:
        elig = []
        for tid, cold, d, slack, grace in infos:
            if tid in used or d < 90 or (needs_cold and not cold):
                continue
            if kind == "late" and slack < grace + 15:
                continue
            if kind == "within_grace" and slack < grace + 3:
                continue
            if kind == "offline_overflow" and d < offline_need / 0.65:
                continue
            elig.append(tid)
        for tid in rng.sample(elig, min(min_per_type, len(elig))):
            forced[tid].add(kind)
            used.add(tid)
    return forced


def late_truth_rows(outcomes, plans_by_trip):
    """Ground truth for arrivals, from the real arrival times of the dry pass. Returns (rows, warnings)."""
    rows, warnings = [], []
    for o in sorted(outcomes, key=lambda r: r["trip_id"]):
        plan = plans_by_trip[o["trip_id"]]
        trip = plan.trip
        planned_end = parse_ts(trip["planned_end_time"])
        grace = float(plan.route["arrival_grace_minutes"])
        limit_end = planned_end + timedelta(minutes=grace)
        end = parse_ts(o["actual_end_time"])
        late_s = (end - limit_end).total_seconds()
        after_plan = (end - planned_end).total_seconds() / 60.0
        base = dict(vehicle_id=trip["vehicle_id"], trip_id=trip["trip_id"], signal_type="gps", event_id="")
        common = dict(grace_minutes=grace, planned_end_time=iso(planned_end), actual_end_time=iso(end),
                      minutes_after_planned_end=round(after_plan, 1),
                      minutes_late_beyond_grace=round(max(0.0, late_s / 60.0), 1),
                      target_offset_min=None if plan.arrival_offset_min is None else round(plan.arrival_offset_min, 1))
        if late_s > 0:
            rows.append(dict(base, injection_type="LATE_ARRIVAL", start_time=limit_end, end_time=end,
                             details=dict(common, negative_case=False, expected_alert=True,
                                          cause="INJECTED_SLOW_TRIP" if plan.late_kind == "late" else "SIDE_EFFECT")))
            if plan.late_kind == "within_grace":
                warnings.append(f"{trip['trip_id']}: planned as a near-miss but arrived {late_s / 60:.1f} min beyond the grace")
        elif plan.late_kind == "within_grace" and after_plan > 0:
            rows.append(dict(base, injection_type="LATE_WITHIN_GRACE", start_time=planned_end, end_time=end,
                             details=dict(common, negative_case=True, expected_alert=False, cause="INJECTED_NEAR_MISS")))
        elif plan.late_kind:
            warnings.append(f"{trip['trip_id']}: planned as '{plan.late_kind}' but arrived {after_plan:.1f} min after the planned end")
    return rows, warnings


# ----------------------------------------------------------------------------- data faults (P1-10)
def build_fault_specs(rates_cfg, poison_on):
    """{signal: [(fault name, rate), ...]} with only the faults that can happen."""
    specs = {s: [] for s in SIGNALS}
    for name, key, sigs in FAULT_SPECS:
        rate = float(rates_cfg.get(key, 0.0))
        if rate <= 0 and not (name == "MALFORMED" and poison_on):
            continue
        for s in sigs:
            specs[s].append((name, rate))
    return {s: v for s, v in specs.items() if v}


def emit_message(ctx, vs, sig, t, m):
    """Turns one built message into the (signal, key, payload) items really sent: none, one or two of them."""
    vid = m["vehicle_id"]
    plain = json.dumps(m, separators=(",", ":"))
    specs = ctx["fault_specs"].get(sig)
    if not specs:
        return [(sig, vid, plain)]
    frng = vs.frng
    u = frng.random()
    poison = ctx["poison"]
    in_poison = bool(poison) and poison[0] <= t < poison[1]
    chosen, acc = None, 0.0
    for name, rate in specs:
        if name == "MALFORMED" and in_poison:
            rate = poison[2]
        acc += rate
        if u < acc:
            chosen = name
            break
    if chosen is None:
        return [(sig, vid, plain)]

    details = {"negative_case": False, "expected_alert": False, "device_id": m["device_id"],
               "sequence_number": m["sequence_number"]}
    mm = dict(m)
    key = vid
    logged_id = m["event_id"]
    expect = None

    if chosen == "DROPPED_MESSAGE":
        details["handling"] = "NOT_SENT"
        payloads = []
    elif chosen == "DUPLICATE":
        details["handling"] = "DEDUP"
        payloads = [plain, plain]
    elif chosen == "NEAR_DUPLICATE":
        logged_id = str(uuid.UUID(int=frng.getrandbits(128), version=4))
        copy = dict(m, event_id=logged_id)
        details.update(handling="DEDUP", original_event_id=m["event_id"])
        payloads = [plain, json.dumps(copy, separators=(",", ":"))]
    elif chosen == "MALFORMED":
        cut = max(5, int(len(plain) * frng.uniform(0.2, 0.8)))
        details.update(handling="DLQ", expected_error_code="PARSE_ERROR", poison=in_poison)
        payloads = [plain[:cut]]
    else:
        if chosen == "MISSING_FIELD":
            field = frng.choice(["vehicle_id", "event_time", "trip_id"])
            mm.pop(field)
            expect = {field: "__missing__"}
            details.update(handling="DLQ", expected_error_code="MISSING_REQUIRED_FIELD", removed_field=field)
        elif chosen == "WRONG_TYPE":
            cands = [f for f in NUMERIC_FIELDS[sig] if m.get(f) is not None]
            if not cands:
                return [(sig, vid, plain)]
            field = frng.choice(cands)
            mm[field] = str(m[field])
            expect = {field: mm[field]}
            details.update(handling="DLQ", expected_error_code="TYPE_MISMATCH", field=field)
        elif chosen == "INVALID_GPS":
            if frng.random() < 0.5:
                mm["latitude"], mm["longitude"] = 0.0, 0.0
            else:
                mm["latitude"] = round(frng.uniform(91.0, 120.0), 6)
            expect = {"latitude": mm["latitude"], "longitude": mm["longitude"]}
            details.update(handling="DLQ", expected_error_code="INVALID_GPS")
        elif chosen == "IMPOSSIBLE_VALUE":
            if sig == "gps":
                mm["speed_kmh"] = 400.0
                expect = {"speed_kmh": 400.0}
            else:
                mm["engine_rpm"] = -int(frng.uniform(100, 1000))
                expect = {"engine_rpm": mm["engine_rpm"]}
            details.update(handling="DLQ", expected_error_code="OUT_OF_RANGE")
        elif chosen == "BAD_TIMESTAMP":
            kind = frng.choice(["future", "wrong_format", "no_timezone"])
            if kind == "future":
                mm["event_time"] = iso(t + timedelta(days=30))
            elif kind == "wrong_format":
                mm["event_time"] = t.strftime("%d/%m/%Y %H:%M")
            else:
                mm["event_time"] = t.strftime("%Y-%m-%dT%H:%M:%S")
            expect = {"event_time": mm["event_time"]}
            details.update(handling="DLQ", expected_error_code="INVALID_TIMESTAMP", timestamp_kind=kind)
        elif chosen == "UNKNOWN_VEHICLE":
            key = f"VUNK{frng.randint(1, 9999):04d}"
            mm["vehicle_id"] = key
            expect = {"vehicle_id": key}
            details.update(handling="DLQ", expected_error_code="UNKNOWN_VEHICLE")
        elif chosen == "SCHEMA_DRIFT":
            if frng.random() < 0.5:
                mm["firmware_version"] = "2.1"
                expect = {"firmware_version": "2.1"}
                details.update(handling="KEEP_FLAGGED", drift_kind="extra_field")
            else:
                mm["schema_version"] = 2
                expect = {"schema_version": 2}
                details.update(handling="DLQ", drift_kind="unknown_version")
        else:  # TEMPERATURE_SPIKE
            if m.get("temperature_value") is None:
                return [(sig, vid, plain)]
            delta = frng.uniform(10, 15) * (1.8 if m.get("temperature_unit") == "F" else 1.0)
            mm["temperature_value"] = round(m["temperature_value"] + delta, 2)
            expect = {"temperature_value": mm["temperature_value"]}
            details.update(handling="KEEP_FLAGGED", flag="SPIKE", spike_c=round(delta, 2))
        details["expect"] = expect
        payloads = [json.dumps(mm, separators=(",", ":"))]

    if ctx["collect_faults"]:
        ctx["fault_log"].append(dict(injection_type=chosen, vehicle_id=vid, trip_id=m["trip_id"], signal_type=sig,
                                     event_id=logged_id, start_time=t, end_time=t, details=details))
    return [(sig, key, p) for p in payloads]


# ----------------------------------------------------------------------------- simulation
class VehicleState:
    def __init__(self, ctx, vehicle):
        self.vehicle = vehicle
        self.rng = random.Random(f"{ctx['seed']}:{vehicle['vehicle_id']}:sim")
        self.frng = random.Random(f"{ctx['seed']}:{vehicle['vehicle_id']}:fault")     # data faults only
        self.orng = random.Random(f"{ctx['seed']}:{vehicle['vehicle_id']}:offline")   # device_status event ids only
        self.odometer = round(self.rng.uniform(20000, 250000), 1)
        self.seq = defaultdict(int)


def device_at(ctx, vid, t):
    dev = None
    for when, d in ctx["devices"][vid]:
        if when <= t:
            dev = d
    return dev or ctx["devices"][vid][0][1]


def run_trip(ctx, vs, plan):
    """Yields (send_time, (signal, key, payload)). send_time is the simulated time the item is sent; it equals the
    event time, except for the stored readings and the report of an offline window (sent at reconnect)."""
    trip, route = plan.trip, plan.route
    vid, tid = trip["vehicle_id"], trip["trip_id"]
    rng = vs.rng
    D = float(route["planned_distance_km"])
    limit = float(route["speed_limit_kmh"])
    geo = ctx["geo"][route["route_id"]]
    iv, dt = ctx["intervals"], ctx["dt"]
    tank = float(vs.vehicle["fuel_tank_capacity_liters"])
    level0 = min(tank, max(tank * rng.uniform(0.8, 0.98), D * DRIVE_BURN_L_PER_KM * 1.15 + 5))
    odo0 = vs.odometer
    t0 = plan.start
    elapsed, dist, speed, fuel_used, idle_s = 0, 0.0, 0.0, 0.0, 0.0
    cruise = limit * rng.uniform(0.88, 1.0)
    next_change = rng.uniform(300, 900)
    phase = rng.uniform(0, 2 * math.pi)
    max_steps = int(float(route["expected_duration_minutes"]) * 60 / dt * 6) + 2000
    n_msgs = 0
    steps = 0
    held = None                                              # frozen value of a stuck sensor
    fahr = plan.cold and vid in ctx["unit_error_vehicles"]   # device reports temperature in F for the whole trip
    target_end = None                                        # P1-11: the truck paces itself to arrive at this time
    if plan.arrival_offset_min is not None:
        target_end = parse_ts(trip["planned_end_time"]) + timedelta(minutes=plan.arrival_offset_min)

    # P1-12 offline window
    off = plan.offline
    buffer = {sg: [] for sg in SIGNALS}
    flushed = off is None
    off_start_t = off_end_t = None
    if off is not None:
        off_start_t = t0 + timedelta(seconds=off["start_s"])
        off_end_t = off_start_t + timedelta(seconds=off["len_s"])

    def common(sig, t, idrng=None):
        dev = device_at(ctx, vid, t)
        vs.seq[(dev, sig)] += 1
        return {"schema_version": 1, "signal_type": sig,
                "event_id": str(uuid.UUID(int=(idrng or rng).getrandbits(128), version=4)),
                "vehicle_id": vid, "trip_id": tid, "device_id": dev, "event_time": iso(t),
                "sequence_number": vs.seq[(dev, sig)]}

    def flush(t_send):
        """The device is back online: first the report, then the stored readings (burst or bulk file)."""
        cap = ctx["offline"]["capacity"]
        kept, lost = {}, {}
        for sg in SIGNALS:
            buf = buffer[sg]
            if off["overflow"] and len(buf) > cap:
                lost[sg], kept[sg] = buf[:len(buf) - cap], buf[len(buf) - cap:]     # the oldest are lost
            else:
                lost[sg], kept[sg] = [], buf
        stored = {sg: len(kept[sg]) for sg in SIGNALS}
        dropped_total = sum(len(lost[sg]) for sg in SIGNALS)
        rep = common("device_status", t_send, vs.orng)
        rep.update(event_type="RECONNECTED", offline_from=iso(off_start_t), reconnected_at=iso(t_send),
                   stored_gps=stored["gps"], stored_engine=stored["engine"], stored_fuel=stored["fuel"],
                   stored_temperature=stored["temperature"], dropped_messages=dropped_total,
                   upload_method=off["method"], upload_id=off["upload_id"])
        yield t_send, ("device_status", vid, json.dumps(rep, separators=(",", ":")))
        stored_msgs = sorted((m for sg in SIGNALS for m in kept[sg]),
                             key=lambda x: (x["event_time"], x["signal_type"], x["sequence_number"]))
        file_name = None
        if off["method"] == "STREAM_BURST":
            for m in stored_msgs:
                yield t_send, (m["signal_type"], vid, json.dumps(m, separators=(",", ":")))
        else:
            file_name = f"{vid}_{off['upload_id']}.jsonl"
            if ctx["landing_dir"] and not ctx["collect_faults"]:
                with open(os.path.join(ctx["landing_dir"], file_name), "w", encoding="utf-8") as f:
                    for m in stored_msgs:
                        f.write(json.dumps(m, separators=(",", ":")) + "\n")
        if ctx["collect_faults"]:
            groups = defaultdict(list)
            for sg in SIGNALS:
                for m in lost[sg]:
                    groups[(m["device_id"], sg)].append(m["sequence_number"])
            lost_ranges = [dict(device_id=dev, signal=sg, first_seq=min(q), last_seq=max(q), count=len(q))
                           for (dev, sg), q in sorted(groups.items())]
            base = dict(vehicle_id=vid, trip_id=tid, start_time=off_start_t, end_time=off_end_t)
            ctx["offline_log"].append(dict(base, injection_type="OFFLINE_WINDOW", signal_type="",
                                           event_id=rep["event_id"], details=dict(
                negative_case=False, expected_alert=True, expected_alert_type="DEVICE_OFFLINE",
                duration_minutes=round(off["len_s"] / 60.0, 2), upload_method=off["method"],
                upload_id=off["upload_id"], file_name=file_name, reconnected_at=iso(t_send),
                buffer_survived=dropped_total == 0, built_by_signal={sg: len(buffer[sg]) for sg in SIGNALS},
                stored_by_signal=stored, dropped_total=dropped_total,
                expected_episode_status="CLOSED" if dropped_total == 0 else "CLOSED_WITH_LOSS")))
            if dropped_total:
                ctx["offline_log"].append(dict(base, injection_type="BUFFER_OVERFLOW", signal_type="",
                                               event_id="", details=dict(
                    negative_case=False, expected_alert=True, expected_alert_type="DATA_GAP",
                    capacity_per_signal=cap, dropped_total=dropped_total, lost_ranges=lost_ranges)))

    while True:
        t = t0 + timedelta(seconds=elapsed)
        if not flushed and t >= off_end_t:
            flushed = True
            yield from flush(t)
        p = min(dist / D, 1.0)
        arrived = p >= 1.0
        for sig in SIGNALS:
            due = elapsed % iv[sig] == 0
            if sig == "gps" and arrived:
                due = True                      # arrival fix
            if not due or (sig == "temperature" and not plan.cold):
                continue
            m = common(sig, t)
            if sig == "gps":
                lat, lon, brg = geo.at(p)
                off_m = 0.0
                for (s, ramp, hold, amp, sign) in plan.deviations:
                    off_m += sign * trapezoid(elapsed / 60 - s, ramp, hold, ramp, amp) * 1000
                perp = math.radians(brg + 90)
                n_m = off_m * math.cos(perp) + rng.gauss(0, 4)
                e_m = off_m * math.sin(perp) + rng.gauss(0, 4)
                lat += n_m / 111320.0
                lon += e_m / (111320.0 * math.cos(math.radians(lat)))
                sp = max(0.0, speed + rng.gauss(0, 0.8)) if speed >= 0.5 else 0.0
                m.update(latitude=round(lat, 6), longitude=round(lon, 6), speed_kmh=round(sp, 1),
                         heading_degrees=round((brg + rng.gauss(0, 2)) % 360, 1) % 360,
                         gps_accuracy_meters=round(rng.uniform(3, 15), 1))
            elif sig == "engine":
                rpm = 750 + rng.gauss(0, 25) if speed < 2 else 900 + speed * 7.5 + rng.gauss(0, 40)
                sp = max(0.0, speed + rng.gauss(0, 0.5)) if speed >= 0.5 else 0.0
                m.update(engine_on=True, engine_rpm=int(min(max(rpm, 650), 2200)),
                         engine_temperature_c=round(90 - 65 * math.exp(-elapsed / 360) + rng.gauss(0, 0.8), 1),
                         vehicle_speed_kmh=round(sp, 1), battery_voltage_v=round(rng.uniform(13.5, 14.2), 2))
            elif sig == "fuel":
                m.update(fuel_level_liters=round(max(0.0, level0 - fuel_used), 2),
                         trip_fuel_used_liters=round(fuel_used, 2), odometer_km=round(odo0 + dist, 1),
                         is_refueling=False)
            else:
                mins = elapsed / 60
                exc = sum(trapezoid(mins - s, r1, h, r2, pk) for (s, r1, h, r2, pk) in plan.excursions)
                val = plan.setpoint + 0.2 * math.sin(2 * math.pi * elapsed / 2700 + phase) + rng.gauss(0, 0.12) + exc
                if any(s <= mins < s + ln for s, ln in plan.stuck):
                    if held is None:
                        held = val
                    val = held
                else:
                    held = None
                shown, status = val, "OK"
                if any(s <= mins < s + ln for s, ln in plan.sensor_faults):
                    shown, status = None, "FAULT"
                unit = "F" if fahr else "C"
                if fahr and shown is not None:
                    shown = shown * 9 / 5 + 32
                m.update(shipment_id=trip["shipment_id"], sensor_id=f"SEN_{vid}_1",
                         temperature_value=None if shown is None else round(shown, 2),
                         temperature_unit=unit, setpoint_temperature_c=plan.setpoint, sensor_status=status)
            n_msgs += 1
            if off is not None and off_start_t <= t < off_end_t:
                buffer[sig].append(m)           # the device is offline: it stores the reading
                continue
            for item in emit_message(ctx, vs, sig, t, m):
                yield t, item
        if arrived:
            break
        steps += 1
        if steps > max_steps:
            raise RuntimeError(f"trip {tid} did not finish (check route data)")
        stopped = any(s <= elapsed < e for s, e in plan.stops)
        if elapsed >= next_change:
            cruise = limit * rng.uniform(0.88, 1.0)
            next_change = elapsed + rng.uniform(300, 900)
        eff = cruise
        if target_end is not None:
            # pace: the speed needed to cover the rest of the route by the target time (minus the stops still to come)
            move_s = (target_end - t).total_seconds() - sum(max(0.0, e - max(s, elapsed)) for s, e in plan.stops)
            if move_s > 60:
                eff = min(cruise, max(0.25 * limit, (D - dist) / (move_s / 3600.0)))
        target = 0.0 if stopped else eff
        new = min(target, speed + ACCEL_KMH_S * dt) if target > speed else max(target, speed - DECEL_KMH_S * dt)
        avg = (speed + new) / 2
        d_step = min(avg * dt / 3600.0, D - dist)
        if avg < 2.0:
            fuel_used += IDLE_BURN_L_PER_H * dt / 3600.0
            idle_s += dt
        else:
            fuel_used += DRIVE_BURN_L_PER_KM * d_step
        dist += d_step
        speed = new
        elapsed += dt

    if not flushed:                              # the trip ended before the planned reconnect
        flushed = True
        yield from flush(t)

    vs.odometer = round(odo0 + dist, 1)
    end = t0 + timedelta(seconds=elapsed)
    planned_end = parse_ts(trip["planned_end_time"])
    limit_end = planned_end + timedelta(minutes=float(route["arrival_grace_minutes"]))
    ctx["outcomes"].append(dict(
        trip_id=tid, vehicle_id=vid, actual_start_time=iso(t0), actual_end_time=iso(end),
        planned_end_time=iso(planned_end), minutes_late_beyond_grace=round(max(0.0, (end - limit_end).total_seconds() / 60), 1),
        distance_km=round(dist, 2), fuel_used_liters=round(fuel_used, 2), idle_minutes_total=round(idle_s / 60, 1),
        messages=n_msgs))


def run_vehicle(ctx, vid, plans):
    vs = VehicleState(ctx, ctx["vehicles"][vid])
    for plan in sorted(plans, key=lambda p: p.start):
        yield from run_trip(ctx, vs, plan)


# ----------------------------------------------------------------------------- sinks
class NullSink:
    def send(self, sig, vid, payload):
        pass

    def poll(self):
        pass

    def finish(self):
        return {}


class JsonlSink(NullSink):
    def __init__(self, path):
        self.f = open(path, "w", encoding="utf-8")

    def send(self, sig, vid, payload):
        self.f.write(payload + "\n")

    def finish(self):
        self.f.close()
        return {}


class KafkaSink(NullSink):
    def __init__(self, needs_device_status=False):
        from confluent_kafka import Producer
        from dotenv import load_dotenv
        load_dotenv()
        self.delivered, self.errors = 0, 0
        self.producers, self.route = {}, {}
        for sig in ALL_SIGNALS:
            up = sig.upper()
            try:
                bs, conn, topic = (os.environ[f"KAFKA_{up}_BOOTSTRAP"], os.environ[f"KAFKA_{up}_CONN_STR"],
                                   os.environ[f"KAFKA_{up}_TOPIC"])
            except KeyError as e:
                if sig == "device_status" and not needs_device_status:
                    continue                    # only needed when offline windows are on (P1-12)
                sys.exit(f"Missing {e.args[0]} in .env" +
                         (" (offline windows send device_status reports)" if sig == "device_status" else ""))
            key = (bs, conn)
            if key not in self.producers:
                self.producers[key] = Producer({
                    "bootstrap.servers": bs, "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
                    "sasl.username": "$ConnectionString", "sasl.password": conn.strip(),
                    "queue.buffering.max.messages": 500000, "linger.ms": 20, "client.id": "fleet-generator"})
            self.route[sig] = (self.producers[key], topic)

    def _cb(self, err, msg):
        if err is not None:
            self.errors += 1
            if self.errors <= 5:
                print(f"[!] delivery failed: {err}")
        else:
            self.delivered += 1

    def send(self, sig, vid, payload):
        prod, topic = self.route[sig]
        while True:
            try:
                prod.produce(topic, key=vid.encode(), value=payload.encode(), callback=self._cb)
                break
            except BufferError:
                prod.poll(0.5)

    def poll(self):
        for p in self.producers.values():
            p.poll(0)

    def finish(self):
        for p in self.producers.values():
            p.flush(60)
        return {"delivered": self.delivered, "delivery_errors": self.errors}


# ----------------------------------------------------------------------------- main
def load_reference(ref):
    need = ["trips.csv", "routes.csv", "route_checkpoints.csv", "cargo_types.csv"]
    for n in need:
        if not os.path.exists(os.path.join(ref, n)):
            sys.exit(f"{ref}/{n} not found. Run generate_reference_data.py first.")
    vfile = os.path.join(ref, "vehicles_initial.csv")
    if not os.path.exists(vfile):
        vfile = os.path.join(ref, "vehicles.csv")
    if not os.path.exists(vfile):
        sys.exit(f"{ref}/vehicles_initial.csv not found. Run generate_reference_data.py first.")
    rows = read_csv(vfile)
    changes = []
    for name in sorted(os.listdir(ref)):
        if name.startswith("vehicles_changes_") and name.endswith(".csv"):
            changes += read_csv(os.path.join(ref, name))
    devices = defaultdict(list)
    for r in rows + changes:
        devices[r["vehicle_id"]].append((parse_ts(r["updated_at"]), r["device_id"]))
    for v in devices.values():
        v.sort()
    return {
        "vehicles": {r["vehicle_id"]: r for r in rows},
        "devices": devices,
        "trips": [t for t in read_csv(os.path.join(ref, "trips.csv")) if t["trip_status"].strip() == "SCHEDULED"],
        "routes": {r["route_id"]: r for r in read_csv(os.path.join(ref, "routes.csv"))},
        "checkpoints": read_csv(os.path.join(ref, "route_checkpoints.csv")),
        "cargo": {r["cargo_type_id"]: r for r in read_csv(os.path.join(ref, "cargo_types.csv"))},
    }


def write_csv(path, cols, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", help="smoke | live_demo | evidence | clean (default: mode in config.yaml)")
    ap.add_argument("--accel", type=float, help="override the acceleration factor")
    ap.add_argument("--sink", choices=["kafka", "jsonl", "none"], default="kafka")
    ap.add_argument("--fast", action="store_true", help="do not wait for the simulated clock (tests and file output)")
    ap.add_argument("--reference", default="reference")
    ap.add_argument("--out", default="evidence")
    args = ap.parse_args()

    cfg = load_config("config.yaml")
    mode = args.mode or cfg.get("mode", "smoke")
    profile = cfg["profiles"][mode]
    A = args.accel or profile.get("acceleration_factor", cfg.get("acceleration_factor", 1))
    seed = cfg.get("seed", 42)
    zero = bool(profile.get("zero_rates", False))
    rates_cfg = cfg.get("rates", {})
    intervals = {k: int(v) for k, v in profile["intervals_sec"].items()}
    dt = reduce(math.gcd, intervals.values())
    rule = {r["rule_name"]: r["rule_value"] for r in cfg.get("business_rules", [])}
    min_per = int(cfg.get("injection", {}).get("min_per_type", 0))

    poison = None
    if rates_cfg.get("poison_batch_mode") and not zero:
        pcfg = cfg.get("poison_batch", {})
        day0 = parse_ts(cfg["simulation"]["start"]).replace(hour=0, minute=0, second=0)
        p_start = day0 + timedelta(hours=float(pcfg.get("start_hour", 10)))
        poison = (p_start, p_start + timedelta(minutes=float(pcfg.get("duration_minutes", 5))),
                  float(pcfg.get("malformed_rate", 0.10)))

    # offline windows (P1-12)
    oraw = cfg.get("offline", {})
    cap = int(oraw.get("buffer_capacity_messages", 300))
    min_m = float(oraw.get("min_minutes", 3))
    max_m = float(oraw.get("max_minutes", 60))
    offline = {"enabled": bool(oraw.get("enabled", False)) and not zero,
               "share": float(oraw.get("trips_share", 0.08)), "min_minutes": min_m, "max_minutes": max_m,
               "max_len_survive": max(min_m, min(max_m, 0.95 * cap * intervals["gps"] / 60.0)),
               "survive": float(oraw.get("buffer_survives_share", 0.85)), "capacity": cap,
               "burst_share": float(oraw.get("burst_share", 0.5)),
               "extra_min": float(oraw.get("overflow_extra_min_minutes", 5)),
               "extra_max": float(oraw.get("overflow_extra_max_minutes", 20))}
    overflow_need = cap * intervals["gps"] / 60.0     # minutes the buffer of the fastest signal needs to fill up

    ref = load_reference(args.reference)
    geo = {}
    by_route = defaultdict(list)
    for c in ref["checkpoints"]:
        by_route[c["route_id"]].append(c)
    for rid, cps in by_route.items():
        cps.sort(key=lambda c: int(c["sequence_number"]))
        geo[rid] = RouteGeo([(float(c["latitude"]), float(c["longitude"])) for c in cps])

    # minutes between the planned end of a trip and the start of the next trip of the same vehicle:
    # a late trip must be over before the next one starts (else the messages of the vehicle would overlap in time)
    slack = {}
    by_vehicle = defaultdict(list)
    for t in ref["trips"]:
        by_vehicle[t["vehicle_id"]].append(t)
    for trips_of_vehicle in by_vehicle.values():
        trips_of_vehicle.sort(key=lambda x: parse_ts(x["planned_start_time"]))
        for a, b in zip(trips_of_vehicle, trips_of_vehicle[1:] + [None]):
            slack[a["trip_id"]] = 1e9 if b is None else (
                parse_ts(b["planned_start_time"]) - parse_ts(a["planned_end_time"])).total_seconds() / 60.0

    ctx = {"seed": seed, "zero_rates": zero, "intervals": intervals, "dt": dt, "geo": geo,
           "devices": ref["devices"], "vehicles": ref["vehicles"], "outcomes": [], "slack": slack,
           "rates": {"breach": rates_cfg.get("cold_chain_breach", 0.0), "idle": rates_cfg.get("idling", 0.0),
                     "dev": rates_cfg.get("route_deviation", 0.0),
                     "sensor_fault": rates_cfg.get("sensor_fault_trips", 0.0),
                     "stuck": rates_cfg.get("stuck_sensor_trips", 0.0),
                     "late": rates_cfg.get("late_arrival", 0.0),
                     "within": rates_cfg.get("late_within_grace", 0.0)},
           "idle_minutes_threshold": rule.get("idle_minutes_threshold", 5),
           "flatline_len_min": float(rule.get("flatline_readings", 20)) * intervals["temperature"] / 60.0 * 1.6,
           "fault_specs": {} if zero else build_fault_specs(rates_cfg, poison is not None),
           "poison": poison, "collect_faults": False, "fault_log": [], "unit_error_vehicles": set(),
           "offline": offline, "overflow_need_min": overflow_need, "landing_dir": None, "offline_log": []}

    infos = []
    for t in ref["trips"]:
        route = ref["routes"][t["route_id"]]
        infos.append((t["trip_id"], ref["cargo"][t["cargo_type_id"]]["temperature_min_c"].strip() != "",
                      float(route["expected_duration_minutes"]), slack.get(t["trip_id"], 1e9),
                      float(route["arrival_grace_minutes"])))
    infos.sort()
    ctx["forced"] = (choose_forced(seed, infos, min_per,
                                   (overflow_need + offline["extra_max"]) if offline["enabled"] else None)
                     if not zero else {})

    plans = defaultdict(list)
    for t in sorted(ref["trips"], key=lambda x: x["trip_id"]):
        plans[t["vehicle_id"]].append(plan_trip(
            ctx, t, ref["routes"][t["route_id"]], ref["cargo"][t["cargo_type_id"]], parse_ts(t["planned_start_time"])))
    all_plans = [p for v in plans.values() for p in v]
    plans_by_trip = {p.trip["trip_id"]: p for p in all_plans}
    first = min(p.start for p in all_plans)
    sim_origin = first.replace(second=0) - timedelta(minutes=5)
    n_slow = sum(1 for p in all_plans if p.late_kind == "late")
    n_near = sum(1 for p in all_plans if p.late_kind == "within_grace")
    print(f"[*] late-arrival plan: {n_slow} slow trips, {n_near} trips arriving late inside the grace")

    # offline windows: upload ids of the bulk files, numbered in the order of the outages
    file_plans = sorted((p for p in all_plans if p.offline and p.offline["method"] == "BULK_FILE"),
                        key=lambda p: (p.start + timedelta(seconds=p.offline["start_s"]), p.trip["vehicle_id"]))
    for i, p in enumerate(file_plans, start=1):
        p.offline["upload_id"] = f"UPL{i:05d}"
    if offline["enabled"]:
        n_burst = sum(1 for p in all_plans if p.offline and p.offline["method"] == "STREAM_BURST")
        n_over = sum(1 for p in all_plans if p.offline and p.offline["overflow"])
        print(f"[*] offline plan: {n_burst} burst flushes, {len(file_plans)} bulk files, {n_over} buffer overflows")

    # unit errors: some refrigerated vehicles report temperature in F for all their cold trips
    if not zero:
        cold_vehicles = sorted({p.trip["vehicle_id"] for p in all_plans if p.cold})
        share = float(rates_cfg.get("unit_error_vehicle_share", 0.0))
        urng = random.Random(f"{seed}:unit_error")
        chosen_units = {v for v in cold_vehicles if urng.random() < share}
        if min_per > 0:
            rest = [v for v in cold_vehicles if v not in chosen_units]
            for v in urng.sample(rest, min(max(0, min_per - len(chosen_units)), len(rest))):
                chosen_units.add(v)
        ctx["unit_error_vehicles"] = chosen_units
        for p in all_plans:
            if p.cold and p.trip["vehicle_id"] in chosen_units:
                p.truth.append(dict(
                    injection_type="UNIT_ERROR", vehicle_id=p.trip["vehicle_id"], trip_id=p.trip["trip_id"],
                    signal_type="temperature", event_id="", start_time=p.start,
                    end_time=parse_ts(p.trip["planned_end_time"]),
                    details=dict(negative_case=False, expected_alert=False, temperature_unit="F",
                                 handling="CONVERT_TO_C", flag="UNIT_CONVERTED")))

    # dry pass: runs every truck once without sending. It collects the per-message data faults, the offline windows
    # and the real arrival time of every trip (for the late-arrival ground truth).
    late_rows, late_warnings = [], []
    if not zero:
        ctx["collect_faults"] = True
        for vid in sorted(plans):
            for _ in run_vehicle(ctx, vid, plans[vid]):
                pass
        ctx["collect_faults"] = False
        late_rows, late_warnings = late_truth_rows(ctx["outcomes"], plans_by_trip)
        ctx["outcomes"] = []
    for w in late_warnings:
        print(f"[!] {w}")

    truth_rows = ([r for p in all_plans for r in p.truth] + ctx["fault_log"] + late_rows + ctx["offline_log"])
    if poison:
        truth_rows.append(dict(injection_type="POISON_BATCH", vehicle_id="", trip_id="", signal_type="", event_id="",
                               start_time=poison[0], end_time=poison[1],
                               details=dict(negative_case=False, expected_alert=True,
                                            expected_alert_type="HIGH_INVALID_RATE", malformed_rate=poison[2])))

    os.makedirs(args.out, exist_ok=True)
    landing_dir = None                     # bulk files of the offline windows: <out>/landing/offline/
    if args.sink != "none":
        landing_dir = os.path.join(args.out, "landing", "offline")
        for old in glob.glob(os.path.join(landing_dir, "*.jsonl")):
            os.remove(old)                 # files of an earlier run
        if offline["enabled"]:
            os.makedirs(landing_dir, exist_ok=True)
    ctx["landing_dir"] = landing_dir
    wall_start = datetime.now(timezone.utc)
    run_id = f"run-{wall_start.strftime('%Y%m%d-%H%M%S')}-{mode}"

    truth = sorted(truth_rows, key=lambda r: (r["start_time"], r["vehicle_id"], r["injection_type"]))
    rows = []
    for i, r in enumerate(truth, start=1):
        rows.append(dict(r, injection_id=f"INJ-{i:06d}", run_id=run_id, seed=seed,
                         start_time=iso(r["start_time"]), end_time=iso(r["end_time"]),
                         details=json.dumps(r["details"], sort_keys=True)))
    write_csv(os.path.join(args.out, "injected_events_log.csv"), INJECTION_COLS, rows)
    counts = defaultdict(int)
    for r in rows:
        counts[r["injection_type"]] += 1

    plain_rates = ("cold_chain_breach", "idling", "route_deviation", "late_arrival", "late_within_grace")
    manifest = {"run_id": run_id, "seed": seed, "profile": mode, "vehicle_count": len(plans),
                "trip_count": len(all_plans), "sim_start": iso(sim_origin),
                "wall_start": wall_start.strftime("%Y-%m-%dT%H:%M:%SZ"), "acceleration_factor": A,
                "intervals_sec": intervals, "generator_version": GENERATOR_VERSION, "sink": args.sink,
                "fast_mode": args.fast, "zero_rates": zero, "rates": ctx["rates"],
                "fault_rates": {} if zero else {k: v for k, v in rates_cfg.items() if k not in plain_rates},
                "poison_batch": None if not poison else {"start": iso(poison[0]), "end": iso(poison[1]),
                                                         "malformed_rate": poison[2]},
                "offline": offline,
                "reference_dir": args.reference, "planned_injections": dict(counts)}
    with open(os.path.join(args.out, "run_manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"[*] {run_id}: {len(plans)} vehicles, {len(all_plans)} trips, A={A}, step={dt}s")
    print(f"[*] planned injections: {dict(counts)}")
    if cfg.get("reference_file_faults"):
        print("[i] reference_file_faults is handled by generate_reference_data.py, not by this script")

    if args.sink == "kafka":
        sink = KafkaSink(offline["enabled"])
    elif args.sink == "jsonl":
        sink = JsonlSink(os.path.join(args.out, "stream_messages.jsonl"))
    else:
        sink = NullSink()

    heap, tie = [], 0
    for vid in sorted(plans):
        gen = run_vehicle(ctx, vid, plans[vid])
        nxt = next(gen, None)
        if nxt:
            heap.append((nxt[0], vid, tie, nxt[1], gen))
            tie += 1
    heapq.heapify(heap)

    sent = defaultdict(int)
    max_lag = 0.0
    wall0 = time.time()
    last_report = wall0
    total = 0
    while heap:
        t, vid, n, item, gen = heap[0]
        if not args.fast:
            target = wall0 + (t - sim_origin).total_seconds() / A
            now = time.time()
            if target > now:
                sink.poll()
                time.sleep(min(target - now, 0.05))
                continue
            max_lag = max(max_lag, now - target)
        heapq.heappop(heap)
        sig, key, payload = item
        sink.send(sig, key, payload)
        sent[sig] += 1
        total += 1
        nxt = next(gen, None)
        if nxt:
            heapq.heappush(heap, (nxt[0], vid, tie, nxt[1], gen))
            tie += 1
        if total % 2000 == 0:
            sink.poll()
        if time.time() - last_report >= 10:
            last_report = time.time()
            print(f"[{iso(t)}] sent={total} lag={max_lag:.1f}s active_trucks={len(heap)}")
            max_lag = 0.0

    result = sink.finish()
    write_csv(os.path.join(args.out, "trip_outcomes.csv"),
              ["trip_id", "vehicle_id", "actual_start_time", "actual_end_time", "planned_end_time",
               "minutes_late_beyond_grace", "distance_km", "fuel_used_liters", "idle_minutes_total", "messages"],
              sorted(ctx["outcomes"], key=lambda r: r["trip_id"]))
    n_drop = counts.get("DROPPED_MESSAGE", 0)
    ow = [r["details"] for r in truth if r["injection_type"] == "OFFLINE_WINDOW"]
    bulk_files = sum(1 for d in ow if d["upload_method"] == "BULK_FILE")
    bulk_rows = sum(sum(d["stored_by_signal"].values()) for d in ow if d["upload_method"] == "BULK_FILE")
    lost_rows = sum(d["dropped_total"] for d in ow)
    reports = sent.get("device_status", 0)
    summary = {"run_id": run_id, "messages_sent": total, "per_signal": dict(sent),
               "messages_dropped_on_device": n_drop,
               "messages_lost_buffer_overflow": lost_rows,
               "device_status_reports": reports,
               "bulk_files": bulk_files, "bulk_file_messages": bulk_rows,
               "messages_built": (total - reports + bulk_rows + lost_rows + n_drop
                                  - counts.get("DUPLICATE", 0) - counts.get("NEAR_DUPLICATE", 0)),
               "elapsed_wall_seconds": round(time.time() - wall0, 1), "sink_result": result}
    with open(os.path.join(args.out, "run_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"[SUCCESS] {total} messages sent {dict(sent)} {result}")
    if bulk_files:
        print(f"[*] {bulk_files} bulk files with {bulk_rows} messages written to {landing_dir} (upload them to Files/landing/offline/)")
    print(f"[*] evidence written to {args.out}/ (run_manifest.json, injected_events_log.csv, trip_outcomes.csv, run_summary.json)")


if __name__ == "__main__":
    main()