"""Checks that the generated messages contain what injected_events_log.csv says
(P1-6 anomalies, P1-10 data faults, P1-11 late arrivals, P1-12 offline windows)."""
import csv, json, math, os, sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

ev = sys.argv[1] if len(sys.argv) > 1 else "evidence"
ref = sys.argv[2] if len(sys.argv) > 2 else "reference"
P = lambda s: datetime.strptime(s[:19].replace("T", " "), "%Y-%m-%d %H:%M:%S")
rd = lambda p: list(csv.DictReader(open(p, encoding="utf-8-sig")))
MISSING = "__missing__"
SIG4 = ["gps", "engine", "fuel", "temperature"]
bad = []

# ------------------------------------------------------------------ load the stream and the bulk files
msgs, first_line, unparsed = [], {}, 0
for n, line in enumerate(open(f"{ev}/stream_messages.jsonl", encoding="utf-8")):
    line = line.strip()
    if not line:
        continue
    try:
        m = json.loads(line)
        if not isinstance(m, dict):
            raise ValueError("not an object")
        msgs.append(m)
        first_line.setdefault(m.get("event_id"), n)
    except ValueError:
        unparsed += 1

file_msgs, file_lines = [], defaultdict(list)
landing = f"{ev}/landing/offline"
if os.path.isdir(landing):
    for fn in sorted(os.listdir(landing)):
        if not fn.endswith(".jsonl"):
            continue
        for line in open(os.path.join(landing, fn), encoding="utf-8"):
            line = line.strip()
            if not line:
                continue
            try:
                m = json.loads(line)
                file_msgs.append(m)
                file_lines[fn].append(m)
            except ValueError:
                bad.append(("unreadable line in bulk file", fn))

vfile = f"{ref}/vehicles_initial.csv" if os.path.exists(f"{ref}/vehicles_initial.csv") else f"{ref}/vehicles.csv"
known_vehicles = {v["vehicle_id"] for v in rd(vfile)}
trips = {t["trip_id"]: t for t in rd(f"{ref}/trips.csv")}
routes = {r["route_id"]: r for r in rd(f"{ref}/routes.csv")}
cargo = {c["cargo_type_id"]: c for c in rd(f"{ref}/cargo_types.csv")}
rules = {r["rule_name"]: float(r["rule_value"]) for r in rd(f"{ref}/business_rules.csv")}
flatline = int(rules.get("flatline_readings", 20))
cps = defaultdict(list)
for c in rd(f"{ref}/route_checkpoints.csv"):
    cps[c["route_id"]].append(c)
truth = rd(f"{ev}/injected_events_log.csv")
for r in truth:
    r["d"] = json.loads(r["details"])

MSG_FAULTS = {"DROPPED_MESSAGE", "DUPLICATE", "NEAR_DUPLICATE", "MALFORMED", "MISSING_FIELD", "WRONG_TYPE",
              "INVALID_GPS", "IMPOSSIBLE_VALUE", "BAD_TIMESTAMP", "UNKNOWN_VEHICLE", "SCHEMA_DRIFT",
              "TEMPERATURE_SPIKE"}
LATE_KINDS = {"LATE_ARRIVAL", "LATE_WITHIN_GRACE"}
OFFLINE_KINDS = {"OFFLINE_WINDOW", "BUFFER_OVERFLOW"}
faulty_ids = set()
for r in truth:
    if r["injection_type"] in MSG_FAULTS and r["event_id"]:
        faulty_ids.add(r["event_id"])
        if r["d"].get("original_event_id"):
            faulty_ids.add(r["d"]["original_event_id"])
clean_stream = [m for m in msgs if m.get("event_id") not in faulty_ids]
clean = clean_stream + file_msgs          # file messages carry no data fault
print("messages parsed", len(msgs), "unparseable lines", unparsed, "bulk file rows", len(file_msgs),
      "clean (no data fault)", len(clean))

windows = defaultdict(list)
for r in truth:
    if r["injection_type"] == "OFFLINE_WINDOW":
        windows[r["trip_id"]].append((P(r["start_time"]), P(r["end_time"])))


def in_window(m):
    try:
        tt = P(m["event_time"])
    except (KeyError, ValueError):
        return False
    return any(s <= tt < e for s, e in windows.get(m.get("trip_id"), []))


# ------------------------------------------------------------------ 1 data faults (P1-10)
print("\ndata-fault checks")
by_id = defaultdict(list)
for m in msgs:
    by_id[m.get("event_id")].append(m)
stats = defaultdict(lambda: [0, 0])


def result(kind, ok, info=None):
    stats[kind][1] += 1
    if ok:
        stats[kind][0] += 1
    else:
        bad.append((kind, info))


n_malformed = sum(1 for r in truth if r["injection_type"] == "MALFORMED")
if n_malformed or unparsed:
    result("MALFORMED", unparsed == n_malformed, f"unparseable lines {unparsed} vs logged {n_malformed}")
for r in truth:
    kind, d, eid = r["injection_type"], r["d"], r["event_id"]
    if kind == "DROPPED_MESSAGE":
        result(kind, eid not in by_id, eid)
    elif kind == "DUPLICATE":
        ms = by_id.get(eid, [])
        result(kind, len(ms) == 2 and ms[0] == ms[1], eid)
    elif kind == "NEAR_DUPLICATE":
        a, b = by_id.get(eid, []), by_id.get(d["original_event_id"], [])
        ok = len(a) == 1 and len(b) == 1 and all(
            a[0].get(k) == b[0].get(k) for k in ("vehicle_id", "signal_type", "event_time", "sequence_number"))
        result(kind, ok, eid)
    elif "expect" in d:
        ms = by_id.get(eid, [])
        ok = len(ms) == 1 and all(ms[0].get(k, MISSING) == v for k, v in d["expect"].items())
        if kind == "UNKNOWN_VEHICLE":
            ok = ok and ms[0].get("vehicle_id") not in known_vehicles
        result(kind, ok, eid)

expected_dups = {r["event_id"] for r in truth if r["injection_type"] == "DUPLICATE"}
for eid, c in Counter(m.get("event_id") for m in msgs).items():
    if c > 1 and eid not in expected_dups:
        bad.append(("unexpected duplicate event_id", eid))

# sequence numbers: every gap must be explained by a dropped message, a malformed message or a buffer overflow
missing_log = defaultdict(set)
for r in truth:
    if r["injection_type"] in ("DROPPED_MESSAGE", "MALFORMED"):
        missing_log[(r["d"]["device_id"], r["signal_type"])].add(r["d"]["sequence_number"])
    elif r["injection_type"] == "BUFFER_OVERFLOW":
        for x in r["d"]["lost_ranges"]:
            missing_log[(x["device_id"], x["signal"])].update(range(x["first_seq"], x["last_seq"] + 1))
seen = defaultdict(set)
for m in msgs + file_msgs:
    seen[(m.get("device_id"), m.get("signal_type"))].add(m.get("sequence_number"))
for k in set(seen) | set(missing_log):
    top = max(seen[k] | missing_log[k])
    expected = set(range(1, top + 1)) - missing_log[k]
    if seen[k] != expected:
        bad.append(("sequence mismatch", k, sorted(expected ^ seen[k])[:5]))
print("devices/signals", len(seen))

# ------------------------------------------------------------------ 2 field checks and sensors (clean messages)
need = {"gps": ["latitude", "longitude", "speed_kmh", "heading_degrees", "gps_accuracy_meters"],
        "engine": ["engine_on", "engine_rpm", "engine_temperature_c", "vehicle_speed_kmh", "battery_voltage_v"],
        "fuel": ["fuel_level_liters", "trip_fuel_used_liters", "odometer_km", "is_refueling"],
        "temperature": ["shipment_id", "sensor_id", "temperature_value", "temperature_unit", "setpoint_temperature_c",
                        "sensor_status"],
        "device_status": ["event_type", "offline_from", "reconnected_at", "stored_gps", "stored_engine",
                          "stored_fuel", "stored_temperature", "dropped_messages", "upload_method", "upload_id"]}
common = ["schema_version", "signal_type", "event_id", "vehicle_id", "trip_id", "device_id", "event_time",
          "sequence_number"]
by_trip = defaultdict(lambda: defaultdict(list))
for m in clean:
    if m["trip_id"] not in trips:
        bad.append(("unknown trip", m["trip_id"]))
    for f in common + need[m["signal_type"]]:
        if f not in m:
            bad.append(("missing field", f, m["signal_type"]))
    if m["signal_type"] == "gps" and not (0 <= m["heading_degrees"] < 360):
        bad.append(("heading", m["heading_degrees"]))
    by_trip[m["trip_id"]][m["signal_type"]].append(m)
for tl in by_trip.values():
    for lst in tl.values():
        lst.sort(key=lambda x: x["event_time"])
order = defaultdict(lambda: "")
for m in clean_stream:          # stream order must follow event time, except the reports and the stored readings
    if m["signal_type"] == "device_status" or in_window(m):
        continue
    if m["event_time"] < order[m["vehicle_id"]]:
        bad.append(("time order", m["vehicle_id"]))
    order[m["vehicle_id"]] = m["event_time"]
for tid, t in trips.items():
    cold = cargo[t["cargo_type_id"]]["temperature_min_c"] != ""
    if cold != bool(by_trip[tid]["temperature"]):
        bad.append(("temperature presence", tid, cold))
    fu = [m["trip_fuel_used_liters"] for m in by_trip[tid]["fuel"]]
    if any(b < a for a, b in zip(fu, fu[1:])):
        bad.append(("fuel decreased", tid))
    if any(m["fuel_level_liters"] < 0 for m in by_trip[tid]["fuel"]):
        bad.append(("negative fuel", tid))


def tc(m):
    """cargo temperature in C (None when the sensor sent null); converts F"""
    v = m.get("temperature_value")
    if v is None:
        return None
    return (v - 32) * 5 / 9 if m.get("temperature_unit") == "F" else v


def runs(flags, times):
    """longest consecutive True run in minutes (first to last reading of the run)"""
    best, start, prev = 0.0, None, None
    for f, t in zip(flags, times):
        if f:
            start = start or t
            prev = t
            best = max(best, (prev - start).total_seconds() / 60)
        else:
            start = None
    return best


def dist_m(p, route):
    best = 1e18
    pts = [(float(c["latitude"]), float(c["longitude"])) for c in sorted(cps[route], key=lambda c: int(c["sequence_number"]))]
    for (a, b), (c, d) in zip(pts, pts[1:]):
        k = 111320.0
        cl = math.cos(math.radians(p[0]))
        ax, ay, bx, by, px, py = a * k, b * k * cl, c * k, d * k * cl, p[0] * k, p[1] * k * cl
        dx, dy = bx - ax, by - ay
        L = dx * dx + dy * dy
        u = 0 if L == 0 else max(0, min(1, ((px - ax) * dx + (py - ay) * dy) / L))
        best = min(best, math.hypot(px - ax - u * dx, py - ay - u * dy))
    return best


print("\ninjection checks")
for r in truth:
    kind, d, tid = r["injection_type"], r["d"], r["trip_id"]
    if kind in MSG_FAULTS or kind in LATE_KINDS or kind in OFFLINE_KINDS or kind == "POISON_BATCH":
        continue
    t = trips[tid]
    s, e = P(r["start_time"]), P(r["end_time"])
    if kind in ("COLD_CHAIN_BREACH", "COLD_CHAIN_BLIP"):
        tm = by_trip[tid]["temperature"]
        mx = float(cargo[t["cargo_type_id"]]["temperature_max_c"])
        flags = [tc(x) is not None and tc(x) > mx for x in tm]
        times = [P(x["event_time"]) for x in tm]
        longest = runs(flags, times)
        hold = d["alert_hold_minutes"]
        ok = (longest >= hold) if kind == "COLD_CHAIN_BREACH" else (longest < hold)
        total_out = sum(flags)
        print(f"  {kind:18} {tid} out_of_range={d['out_of_range_minutes']}min hold={hold} readings_above_max={total_out} longest_run={longest:.1f}min -> {'OK' if ok else 'CHECK'}")
        if not ok:
            bad.append((kind, tid))
        if kind == "COLD_CHAIN_BLIP" and total_out == 0:
            print("     note: blip fell between two readings")
    elif kind in ("EXCESSIVE_IDLING", "SHORT_STOP"):
        en = by_trip[tid]["engine"]
        flags = [x["engine_on"] and x["vehicle_speed_kmh"] < 2 for x in en]
        times = [P(x["event_time"]) for x in en]
        if kind == "EXCESSIVE_IDLING":
            run = runs([f and s <= tt <= e for f, tt in zip(flags, times)], times)
            ok = run >= 5 and run >= d["duration_minutes"] - 1.5
            print(f"  {kind:18} {tid} planned={d['duration_minutes']}min idle_run_in_window={run:.1f}min -> {'OK' if ok else 'CHECK'}")
            if not ok:
                bad.append((kind, tid, run))
        else:
            run = runs([f and (s - timedelta(minutes=1) <= tt <= e + timedelta(minutes=1)) for f, tt in zip(flags, times)], times)
            if run >= 5:
                bad.append(("short stop too long", tid, run))
    elif kind in ("ROUTE_DEVIATION", "GPS_WANDER"):
        g = by_trip[tid]["gps"]
        allow = d["allowed_deviation_meters"]
        dd = [dist_m((x["latitude"], x["longitude"]), t["route_id"]) for x in g]
        times = [P(x["event_time"]) for x in g]
        flags = [x > allow for x in dd]
        consec = best = 0
        for f in flags:
            consec = consec + 1 if f else 0
            best = max(best, consec)
        peak = max(dd[i] for i, tt in enumerate(times) if s <= tt <= e) if any(s <= tt <= e for tt in times) else 0
        ok = (best >= 3) if kind == "ROUTE_DEVIATION" else (best == 0)
        print(f"  {kind:18} {tid} peak_in_window={peak:.0f}m allowed={allow} longest_consecutive_off_route={best} -> {'OK' if ok else 'CHECK'}")
        if not ok:
            bad.append((kind, tid, best))
    elif kind in ("SENSOR_FAULT", "STUCK_SENSOR"):
        win = [x for x in by_trip[tid]["temperature"] if s + timedelta(seconds=1) <= P(x["event_time"]) < e]
        if kind == "SENSOR_FAULT":
            ok = len(win) > 0 and all(x["temperature_value"] is None and x["sensor_status"] == "FAULT" for x in win)
            result(kind, ok, tid)
        else:
            best = cur = 0
            prev = object()
            for x in win:
                v = x["temperature_value"]
                cur = cur + 1 if v == prev and v is not None else 1
                prev = v
                best = max(best, cur)
            result(kind, best >= flatline, (tid, best))
    elif kind == "UNIT_ERROR":
        tm = by_trip[tid]["temperature"]
        result(kind, len(tm) > 0 and all(x["temperature_unit"] == "F" for x in tm), tid)

# ------------------------------------------------------------------ 3 late arrivals (P1-11)
# The arrival is the last clean gps fix of the trip. A tolerance covers a dropped or damaged last fix.
print("\nlate-arrival checks")
TOL = timedelta(minutes=3)


def trip_end(tid):
    return max((P(x["event_time"]) for x in by_trip[tid]["gps"]), default=None)


late_logged = {r["trip_id"]: r for r in truth if r["injection_type"] == "LATE_ARRIVAL"}
for r in truth:
    if r["injection_type"] not in LATE_KINDS:
        continue
    kind, d, tid = r["injection_type"], r["d"], r["trip_id"]
    t = trips[tid]
    planned_end = P(t["planned_end_time"])
    limit_end = planned_end + timedelta(minutes=float(routes[t["route_id"]]["arrival_grace_minutes"]))
    end, logged_end = trip_end(tid), P(r["end_time"])
    ok = end is not None and abs(end - logged_end) <= TOL
    beyond = d["minutes_late_beyond_grace"]
    if kind == "LATE_ARRIVAL":
        if d["cause"] == "INJECTED_SLOW_TRIP":
            ok = ok and 3 <= beyond <= 75
        ok = ok and end is not None and end > limit_end - TOL
        print(f"  {kind:18} {tid} cause={d['cause']} late_beyond_grace={beyond}min after_planned_end={d['minutes_after_planned_end']}min -> {'OK' if ok else 'CHECK'}")
    else:
        ok = ok and end is not None and planned_end - TOL < end <= limit_end + TOL and tid not in late_logged
        print(f"  {kind:18} {tid} after_planned_end={d['minutes_after_planned_end']}min grace={d['grace_minutes']}min -> {'OK' if ok else 'CHECK'}")
    result(kind, ok, tid)
for tid, t in trips.items():
    limit_end = P(t["planned_end_time"]) + timedelta(minutes=float(routes[t["route_id"]]["arrival_grace_minutes"]))
    end = trip_end(tid)
    if end is not None and end > limit_end + TOL and tid not in late_logged:
        bad.append(("late arrival without a LATE_ARRIVAL row", tid))

# ------------------------------------------------------------------ 4 offline windows (P1-12)
print("\noffline checks")
logged_files = set()
for r in truth:
    if r["injection_type"] != "OFFLINE_WINDOW":
        continue
    d, tid = r["d"], r["trip_id"]
    s, e = P(r["start_time"]), P(r["end_time"])
    reps = by_id.get(r["event_id"], [])
    ok = len(reps) == 1 and reps[0].get("signal_type") == "device_status"
    if ok:
        rep = reps[0]
        ok = (rep.get("event_type") == "RECONNECTED" and rep.get("upload_method") == d["upload_method"]
              and rep.get("upload_id") == d["upload_id"] and rep.get("dropped_messages") == d["dropped_total"]
              and rep.get("offline_from") == r["start_time"] and rep.get("reconnected_at") == d["reconnected_at"]
              and all(rep.get(f"stored_{sg}") == d["stored_by_signal"][sg] for sg in SIG4))
    in_stream = [m for m in clean_stream if m.get("trip_id") == tid and m["signal_type"] in SIG4
                 and s <= P(m["event_time"]) < e]
    if d["upload_method"] == "STREAM_BURST":
        cnt = Counter(m["signal_type"] for m in in_stream)
        rep_line = first_line.get(r["event_id"], -1)
        ok = (ok and all(cnt[sg] == d["stored_by_signal"][sg] for sg in SIG4)
              and all(first_line.get(m["event_id"], -1) > rep_line for m in in_stream))
        where = f"burst of {len(in_stream)} readings after the report"
    else:
        fl = file_lines.get(d["file_name"], [])
        cnt = Counter(m["signal_type"] for m in fl)
        ok = (ok and not in_stream and len(fl) > 0 and all(cnt[sg] == d["stored_by_signal"][sg] for sg in SIG4)
              and all(m.get("trip_id") == tid and s <= P(m["event_time"]) < e for m in fl))
        logged_files.add(d["file_name"])
        where = f"file {d['file_name']} with {len(fl)} readings, none on the stream"
    print(f"  OFFLINE_WINDOW     {tid} {d['duration_minutes']}min {d['upload_method']} stored={sum(d['stored_by_signal'].values())} dropped={d['dropped_total']} ({where}) -> {'OK' if ok else 'CHECK'}")
    result("OFFLINE_WINDOW", ok, tid)
for r in truth:
    if r["injection_type"] != "BUFFER_OVERFLOW":
        continue
    d = r["d"]
    ok = d["dropped_total"] > 0 and d["dropped_total"] == sum(x["count"] for x in d["lost_ranges"])
    for x in d["lost_ranges"]:
        got = seen.get((x["device_id"], x["signal"]), set())
        ok = ok and x["last_seq"] - x["first_seq"] + 1 == x["count"] and not any(
            q in got for q in range(x["first_seq"], x["last_seq"] + 1))
    print(f"  BUFFER_OVERFLOW    {r['trip_id']} lost={d['dropped_total']} readings in {len(d['lost_ranges'])} ranges, none of them arrived -> {'OK' if ok else 'CHECK'}")
    result("BUFFER_OVERFLOW", ok, r["trip_id"])
for fn in file_lines:
    if fn not in logged_files:
        bad.append(("bulk file without an OFFLINE_WINDOW row", fn))

# ------------------------------------------------------------------ 5 unplanned episodes (false positives in the data itself)
planned = defaultdict(set)
for r in truth:
    planned[r["trip_id"]].add(r["injection_type"])
for tid, t in trips.items():
    g = by_trip[tid]["gps"]
    flags = [dist_m((x["latitude"], x["longitude"]), t["route_id"]) > float(routes[t["route_id"]]["allowed_deviation_meters"]) for x in g]
    consec = best = 0
    for f in flags:
        consec = consec + 1 if f else 0
        best = max(best, consec)
    if best >= 3 and "ROUTE_DEVIATION" not in planned[tid]:
        bad.append(("unplanned deviation", tid, best))
    en = by_trip[tid]["engine"]
    run = runs([x["engine_on"] and x["vehicle_speed_kmh"] < 2 for x in en], [P(x["event_time"]) for x in en])
    if run >= 5 and "EXCESSIVE_IDLING" not in planned[tid]:
        bad.append(("unplanned idling", tid, run))
    if cargo[t["cargo_type_id"]]["temperature_min_c"] != "":
        mx = float(cargo[t["cargo_type_id"]]["temperature_max_c"])
        mn = float(cargo[t["cargo_type_id"]]["temperature_min_c"])
        out = [tc(x) is not None and (tc(x) > mx or tc(x) < mn) for x in by_trip[tid]["temperature"]]
        if any(out) and not (planned[tid] & {"COLD_CHAIN_BREACH", "COLD_CHAIN_BLIP"}):
            bad.append(("unplanned temperature excursion", tid))

# ------------------------------------------------------------------ 6 summary
print("\nfault results (found as logged / logged)")
for kind in sorted(stats):
    ok, total = stats[kind]
    print(f"  {kind:18} {ok}/{total}")
for r in truth:
    if r["injection_type"] == "POISON_BATCH":
        n = sum(1 for x in truth if x["injection_type"] == "MALFORMED" and x["d"].get("poison"))
        print(f"  POISON_BATCH       window {r['start_time']} to {r['end_time']}: {n} malformed messages in the window")
out = rd(f"{ev}/trip_outcomes.csv")
late = [(o["trip_id"], o["minutes_late_beyond_grace"]) for o in out if float(o["minutes_late_beyond_grace"]) > 0]
print("\ntrips late beyond grace:", late)
for tid, mins in late:
    if tid not in late_logged:
        bad.append(("late trip without a LATE_ARRIVAL row", tid, mins))
print("\nPROBLEMS:", bad if bad else "none")