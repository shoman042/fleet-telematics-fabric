import csv
import glob
import math
import os
import random
from datetime import datetime, timedelta

from config import load_config
from rng import SimulationRNG

FMT = "%Y-%m-%d %H:%M:%S"

# Column order exactly as in section 3 of the schemas document
COLS = {
    "vehicles": ["vehicle_id", "registration_number", "vehicle_type", "fuel_type", "fuel_tank_capacity_liters",
                 "refrigeration_capable", "device_id", "model_year", "active_from", "active_to", "updated_at"],
    "drivers": ["driver_id", "driver_name", "license_category", "hire_date", "active_from", "active_to"],
    "routes": ["route_id", "route_name", "origin_name", "destination_name", "planned_distance_km",
               "allowed_deviation_meters", "expected_duration_minutes", "arrival_grace_minutes", "speed_limit_kmh"],
    "route_checkpoints": ["checkpoint_id", "route_id", "sequence_number", "checkpoint_name", "latitude", "longitude",
                          "planned_offset_minutes", "checkpoint_type"],
    "cargo_types": ["cargo_type_id", "cargo_name", "temperature_min_c", "temperature_max_c", "cargo_value_per_unit",
                    "currency_code", "max_excursion_minutes", "spoilage_loss_pct", "alert_hold_minutes"],
    "trips": ["trip_id", "vehicle_id", "driver_id", "route_id", "shipment_id", "cargo_type_id", "planned_start_time",
              "planned_end_time", "cargo_quantity", "trip_status"],
    "fuel_prices": ["price_date", "fuel_type", "price_per_liter", "currency_code"],
    "business_rules": ["rule_name", "rule_value", "unit", "description"],
}

CITIES = {
    "Cairo": (30.0444, 31.2357), "Alexandria": (31.2001, 29.9187), "Tanta": (30.7865, 31.0004),
    "Port Said": (31.2653, 32.3019), "Aswan": (24.0889, 32.8998), "Luxor": (25.6872, 32.6396),
    "Asyut": (27.1809, 31.1837), "Minya": (28.1099, 30.7503), "Beni Suef": (29.0661, 31.0994),
    "Suez": (29.9668, 32.5498), "Ismailia": (30.5965, 32.2715), "Mansoura": (31.0409, 31.3785),
    "Hurghada": (27.2579, 33.8116), "Damietta": (31.4165, 31.8133), "Zagazig": (30.5877, 31.5020),
}

# (origin, [intermediate anchors], destination). The first N are used, N = route_count of the profile.
ROUTE_DEFS = [
    ("Cairo", [], "Alexandria"),
    ("Cairo", ["Beni Suef", "Minya", "Asyut", "Luxor"], "Aswan"),
    ("Tanta", ["Mansoura"], "Port Said"),
    ("Cairo", ["Zagazig", "Ismailia"], "Port Said"),
    ("Cairo", [], "Suez"),
    ("Alexandria", [], "Tanta"),
    ("Cairo", ["Beni Suef", "Minya"], "Asyut"),
    ("Cairo", ["Zagazig"], "Mansoura"),
    ("Cairo", ["Suez"], "Hurghada"),
    ("Alexandria", ["Tanta", "Mansoura"], "Damietta"),
    ("Luxor", [], "Aswan"),
    ("Mansoura", [], "Damietta"),
]

ROAD_FACTOR = 1.2        # road distance = straight-line distance x 1.2
ROUTE_SPEED_LIMIT = 90.0
AVG_SPEED_SHARE = 0.9    # expected duration assumes 90 percent of the speed limit

CARGO = [
    # cold cargo: values from the guide defaults
    {"cargo_type_id": "FOOD_CHILLED", "cargo_name": "Chilled Food", "temperature_min_c": 2.0, "temperature_max_c": 8.0,
     "cargo_value_per_unit": 500.0, "currency_code": "EGP", "max_excursion_minutes": 30, "spoilage_loss_pct": 25.0,
     "alert_hold_minutes": 5},
    {"cargo_type_id": "MEDICINE_COLD", "cargo_name": "Cold Medicine", "temperature_min_c": 2.0, "temperature_max_c": 8.0,
     "cargo_value_per_unit": 2000.0, "currency_code": "EGP", "max_excursion_minutes": 15, "spoilage_loss_pct": 100.0,
     "alert_hold_minutes": 2},
    {"cargo_type_id": "FROZEN_FOOD", "cargo_name": "Frozen Food", "temperature_min_c": -25.0, "temperature_max_c": -18.0,
     "cargo_value_per_unit": 300.0, "currency_code": "EGP", "max_excursion_minutes": 45, "spoilage_loss_pct": 40.0,
     "alert_hold_minutes": 10},
    # ordinary cargo: NO temperature limits (a dry van sends no temperature messages)
    {"cargo_type_id": "DRY_GOODS", "cargo_name": "Dry Goods", "temperature_min_c": "", "temperature_max_c": "",
     "cargo_value_per_unit": 200.0, "currency_code": "EGP", "max_excursion_minutes": 0, "spoilage_loss_pct": 0.0,
     "alert_hold_minutes": 0},
]

DRIVER_NAMES = ["Ahmed Mohamed", "Mahmoud Ali", "Khaled Hassan", "Ibrahim Youssef", "Omar Farouk",
                "Hassan Mostafa", "Tarek Zaki", "Samir Nabil", "Adel Kamel", "Karim Said"]


def haversine_km(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(h))


def write_csv(path, name, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS[name])
        w.writeheader()
        w.writerows(rows)
    print(f"[+] Created: {path} ({len(rows)} rows)")


def build_route(idx, origin, via, dest, rng):
    """Returns (route_row, checkpoint_rows). 15-25 checkpoints interpolated along the anchors with a small curve."""
    route_id = f"R{idx:03d}"
    anchors = [CITIES[origin]] + [CITIES[c] for c in via] + [CITIES[dest]]
    seg = [haversine_km(anchors[k], anchors[k + 1]) for k in range(len(anchors) - 1)]
    distance = round(sum(seg) * ROAD_FACTOR, 1)
    duration = int(round(distance / (AVG_SPEED_SHARE * ROUTE_SPEED_LIMIT) * 60))
    grace = 15 if distance <= 150 else 30
    route = {
        "route_id": route_id, "route_name": f"{origin} to {dest}", "origin_name": origin, "destination_name": dest,
        "planned_distance_km": distance, "allowed_deviation_meters": 500, "expected_duration_minutes": duration,
        "arrival_grace_minutes": grace, "speed_limit_kmh": ROUTE_SPEED_LIMIT,
    }

    n = rng.random.randint(15, 25)
    total = sum(seg)
    cps = []
    for k in range(n):
        t = k / (n - 1)
        d = t * total
        acc = 0.0
        for s, length in enumerate(seg):
            if d <= acc + length or s == len(seg) - 1:
                f = 0.0 if length == 0 else min(1.0, (d - acc) / length)
                (la1, lo1), (la2, lo2) = anchors[s], anchors[s + 1]
                lat, lon = la1 + (la2 - la1) * f, lo1 + (lo2 - lo1) * f
                dlat, dlon = la2 - la1, lo2 - lo1
                break
            acc += length
        if 0 < k < n - 1:  # small curve so the path is not a straight line
            norm = math.hypot(dlat, dlon) or 1.0
            off = 0.012 * math.sin(2 * math.pi * t) + rng.random.uniform(-0.002, 0.002)
            lat += -dlon / norm * off
            lon += dlat / norm * off
        if k == 0:
            ctype, cname = "start", f"{origin} Start"
        elif k == n - 1:
            ctype, cname = "destination", f"{dest} End"
        else:
            ctype, cname = "waypoint", f"{route_id} waypoint {k + 1}"
        cps.append({
            "checkpoint_id": f"CP-{route_id}-{k + 1:02d}", "route_id": route_id, "sequence_number": k + 1,
            "checkpoint_name": cname, "latitude": round(lat, 6), "longitude": round(lon, 6),
            "planned_offset_minutes": int(round(t * duration)), "checkpoint_type": ctype,
        })
    return route, cps


def write_reference_faults(seed, trips, drivers, assigned, vehicles, retrofit_at, routes, cold_ids, shortest):
    """P1-10 reference-file faults. Written to reference_faults/ so reference/ stays valid for the stream generator.
    The faulty files are meant for testing the file-ingestion step (DLQ stage FILE_INGEST)."""
    frng = random.Random(f"{seed}:reference_faults")
    out_dir = "reference_faults"
    os.makedirs(out_dir, exist_ok=True)
    clean_trips = [{k: v for k, v in t.items() if not k.startswith("_")} for t in trips]
    extra_trips = []
    log = []

    def free_driver(start, end, fallback):
        for d_id in sorted(assigned):
            if all(start >= e or end <= s for s, e in assigned[d_id]):
                return d_id
        return fallback

    def next_trip_id(vid):
        return f"TRP-{vid}-{sum(1 for t in trips if t['vehicle_id'] == vid) + sum(1 for t in extra_trips if t['vehicle_id'] == vid) + 1:03d}"

    # 1. two overlapping trips for one vehicle
    base = frng.choice(sorted(trips, key=lambda t: t["trip_id"]))
    length = base["_end"] - base["_start"]
    start = base["_start"] + min(timedelta(minutes=20), length / 2)
    end = start + length
    new_id = next_trip_id(base["vehicle_id"])
    extra_trips.append({
        "trip_id": new_id, "vehicle_id": base["vehicle_id"], "driver_id": free_driver(start, end, base["driver_id"]),
        "route_id": base["route_id"], "shipment_id": "SHP-FAULT-01", "cargo_type_id": base["cargo_type_id"],
        "planned_start_time": start.strftime(FMT), "planned_end_time": end.strftime(FMT),
        "cargo_quantity": 100.0, "trip_status": "SCHEDULED"})
    log.append({"injection_type": "REF_OVERLAPPING_TRIPS", "file": "trips.csv", "record_key": new_id,
                "details": f"overlaps {base['trip_id']} of vehicle {base['vehicle_id']}"})

    # 2. cold cargo on a vehicle that cannot refrigerate (never retrofitted)
    noncap = sorted(v["vehicle_id"] for v in vehicles
                    if v["refrigeration_capable"] == "false" and v["vehicle_id"] not in retrofit_at)
    if noncap:
        vid = frng.choice(noncap)
        last_end = max(t["_end"] for t in trips if t["vehicle_id"] == vid)
        start = last_end + timedelta(minutes=60)
        end = start + timedelta(minutes=shortest["expected_duration_minutes"])
        cargo_id = frng.choice(cold_ids)
        new_id = next_trip_id(vid)
        extra_trips.append({
            "trip_id": new_id, "vehicle_id": vid, "driver_id": free_driver(start, end, base["driver_id"]),
            "route_id": shortest["route_id"], "shipment_id": "SHP-FAULT-02", "cargo_type_id": cargo_id,
            "planned_start_time": start.strftime(FMT), "planned_end_time": end.strftime(FMT),
            "cargo_quantity": 100.0, "trip_status": "SCHEDULED"})
        log.append({"injection_type": "REF_COLD_CARGO_NO_REFRIGERATION", "file": "trips.csv", "record_key": new_id,
                    "details": f"{cargo_id} on vehicle {vid}, which is not refrigeration-capable"})
    else:
        print("[!] reference_file_faults: no non-refrigerated vehicle found, cold-cargo fault skipped")

    path = os.path.join(out_dir, "trips.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS["trips"])
        w.writeheader()
        w.writerows(clean_trips + extra_trips)

    # 3. a cargo whose minimum temperature is above its maximum
    bad_cargo = dict(CARGO[0], cargo_type_id="BAD_RANGE", cargo_name="Bad Range",
                     temperature_min_c=10.0, temperature_max_c=5.0)
    with open(os.path.join(out_dir, "cargo_types.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS["cargo_types"])
        w.writeheader()
        w.writerows(CARGO + [bad_cargo])
    log.append({"injection_type": "REF_CARGO_MIN_ABOVE_MAX", "file": "cargo_types.csv", "record_key": "BAD_RANGE",
                "details": "temperature_min_c 10.0 is above temperature_max_c 5.0"})

    # 4. a CSV row with an extra delimiter (10 fields instead of 9)
    with open(os.path.join(out_dir, "routes.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS["routes"])
        w.writeheader()
        w.writerows(routes)
        row = dict(routes[0], route_id="R999", route_name="Faulty Route")
        f.write(",".join(str(row[c]) for c in COLS["routes"]) + ",EXTRA\n")
    log.append({"injection_type": "REF_EXTRA_DELIMITER", "file": "routes.csv", "record_key": "R999",
                "details": "the row has 10 fields, the header has 9"})

    # 5. a file missing a column
    cols = [c for c in COLS["drivers"] if c != "license_category"]
    with open(os.path.join(out_dir, "drivers.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(drivers)
    log.append({"injection_type": "REF_MISSING_COLUMN", "file": "drivers.csv", "record_key": "",
                "details": "column license_category is missing"})

    with open(os.path.join(out_dir, "reference_faults_log.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["injection_type", "file", "record_key", "details"])
        w.writeheader()
        w.writerows(log)
    print(f"[+] Reference-file faults written to {out_dir}/ ({len(log)} faults, see reference_faults_log.csv)")


def generate_data():
    config = load_config("config.yaml")
    seed = config.get("seed", 42)
    mode = config.get("mode", "smoke")
    profile = config.get("profiles", {}).get(mode, config["profiles"]["smoke"])
    vehicle_count = profile.get("vehicle_count", 20)
    route_count = min(profile.get("route_count", 3), len(ROUTE_DEFS))
    window_hours = profile.get("departure_window_hours", 12)

    sim = config.get("simulation", {})
    sim_start = datetime.strptime(sim.get("start", "2026-01-01 00:00:00"), FMT)
    window_start = sim_start + timedelta(hours=sim.get("departure_window_start_hour", 6))
    reefer_share = config.get("refrigerated_share", 0.6)
    driver_ratio = config.get("driver_ratio", 0.6)
    fuel_cfg = config.get("fuel_prices", {"diesel": 15.0})
    scd = config.get("scd", {})
    change_dt = sim_start + timedelta(hours=scd.get("change_hour", 12))
    bulk_dt = sim_start + timedelta(hours=scd.get("bulk_change_hour", 15))
    days = max(1, int(sim.get("days", 1)))   # simulated days of trips, one trip per vehicle per day

    rng = SimulationRNG(seed)
    output_dir = "reference"
    os.makedirs(output_dir, exist_ok=True)
    print(f"[*] Generating reference data for mode '{mode}': {vehicle_count} vehicles, {route_count} routes (seed {seed})")

    # -------- 3.3 / 3.4 routes and checkpoints --------
    routes, checkpoints = [], []
    for idx, (o, via, d) in enumerate(ROUTE_DEFS[:route_count], start=1):
        r, c = build_route(idx, o, via, d, rng)
        routes.append(r)
        checkpoints.extend(c)

    # -------- 3.1 vehicles (initial versions) --------
    fuel_types = sorted(fuel_cfg.keys())
    vehicles = []
    for i in range(1, vehicle_count + 1):
        v_id = f"V{i:03d}"
        reefer = rng.random.random() < reefer_share
        if v_id == "V002":
            reefer = False  # V002 is retrofitted at change time
        vehicles.append({
            "vehicle_id": v_id,
            "registration_number": "SIM-O007" if v_id == "V007" else f"SIM-{i:04d}",  # V007: letter O, corrected later
            "vehicle_type": "refrigerated_truck" if reefer else rng.random.choice(["standard_truck", "heavy_carrier"]),
            "fuel_type": rng.random.choice(fuel_types),
            "fuel_tank_capacity_liters": float(rng.random.choice([300.0, 450.0, 600.0])),
            "refrigeration_capable": str(reefer).lower(),
            "device_id": f"DEV_{v_id}",
            "model_year": int(rng.random.randint(2018, 2025)),
            "active_from": sim_start.strftime(FMT),
            "active_to": "",
            "updated_at": sim_start.strftime(FMT),
        })
    by_id = {v["vehicle_id"]: v for v in vehicles}

    # -------- vehicle changes (SCD, section 7.7) --------
    def new_version(v, when, **updates):
        row = dict(v)
        row.update(updates)
        row["updated_at"] = when.strftime(FMT)
        return row

    scripted, bulk = [], []
    retrofit_at = {}
    used = set()
    if "V002" in by_id:
        scripted.append(new_version(by_id["V002"], change_dt, vehicle_type="refrigerated_truck", refrigeration_capable="true"))
        retrofit_at["V002"] = change_dt
        used.add("V002")
    if "V007" in by_id:
        scripted.append(new_version(by_id["V007"], change_dt, registration_number="SIM-0007"))
        used.add("V007")
    if "V010" in by_id:
        scripted.append(new_version(by_id["V010"], change_dt, device_id="DEV_V010B"))
        used.add("V010")

    scale = vehicle_count / 5000.0
    bulk_cfg = scd.get("bulk_per_5000_vehicles", {})
    n_retro = int(round(bulk_cfg.get("retrofits", 0) * scale))
    n_swap = int(round(bulk_cfg.get("device_swaps", 0) * scale))
    n_corr = int(round(bulk_cfg.get("corrections", 0) * scale))

    cands = [v for v in vehicles if v["refrigeration_capable"] == "false" and v["vehicle_id"] not in used]
    for v in rng.random.sample(cands, min(n_retro, len(cands))):
        bulk.append(new_version(v, bulk_dt, vehicle_type="refrigerated_truck", refrigeration_capable="true"))
        retrofit_at[v["vehicle_id"]] = bulk_dt
        used.add(v["vehicle_id"])
    cands = [v for v in vehicles if v["vehicle_id"] not in used]
    for v in rng.random.sample(cands, min(n_swap, len(cands))):
        bulk.append(new_version(v, bulk_dt, device_id=v["device_id"] + "B"))
        used.add(v["vehicle_id"])
    cands = [v for v in vehicles if v["vehicle_id"] not in used and "0" in v["registration_number"][4:]]
    for v in rng.random.sample(cands, min(n_corr, len(cands))):
        good = v["registration_number"]
        v["registration_number"] = "SIM-" + good[4:].replace("0", "O", 1)  # initial file carries the typo
        bulk.append(new_version(v, bulk_dt, registration_number=good))
        used.add(v["vehicle_id"])

    def capable_at(v, when):
        if v["refrigeration_capable"] == "true":
            return True
        t = retrofit_at.get(v["vehicle_id"])
        return t is not None and when >= t

    # -------- 3.6 trips --------
    cold_ids = [c["cargo_type_id"] for c in CARGO if c["temperature_min_c"] != ""]
    dry_id = "DRY_GOODS"
    by_route = {r["route_id"]: r for r in routes}
    shortest = min(routes, key=lambda r: r["expected_duration_minutes"])
    per_vehicle = {}
    last_end = {}
    trips = []

    def fit_start(vid, start):
        """A vehicle never starts a trip before its previous trip has ended (plus 30 minutes)."""
        prev = last_end.get(vid)
        return max(start, prev + timedelta(minutes=30)) if prev else start

    def add_trip(v, start, route, cargo_id):
        per_vehicle[v["vehicle_id"]] = per_vehicle.get(v["vehicle_id"], 0) + 1
        end = start + timedelta(minutes=route["expected_duration_minutes"])
        last_end[v["vehicle_id"]] = end
        trips.append({
            "trip_id": f"TRP-{v['vehicle_id']}-{per_vehicle[v['vehicle_id']]:03d}",
            "vehicle_id": v["vehicle_id"], "driver_id": "", "route_id": route["route_id"],
            "shipment_id": f"SHP-{len(trips) + 1:05d}", "cargo_type_id": cargo_id,
            "planned_start_time": start.strftime(FMT), "planned_end_time": end.strftime(FMT),
            "cargo_quantity": 100.0, "trip_status": "SCHEDULED",
            "_start": start, "_end": end,
        })

    for day in range(days):
        day_off = timedelta(days=day)
        for v in vehicles:
            if day == 0 and v["vehicle_id"] == "V002":
                # ordinary cargo BEFORE the retrofit; its cold-cargo trip is added below, after the change
                start = window_start + timedelta(minutes=rng.random.randint(0, 30))
                if start + timedelta(minutes=shortest["expected_duration_minutes"]) >= change_dt:
                    raise RuntimeError("V002 first trip would not finish before the retrofit; move change_hour later.")
                add_trip(v, start, shortest, dry_id)
                continue
            route = rng.random.choice(routes)
            start = window_start + day_off + timedelta(minutes=rng.random.randint(0, window_hours * 60 - 1))
            start = fit_start(v["vehicle_id"], start)
            cargo = rng.random.choice(cold_ids) if capable_at(v, start) else dry_id
            add_trip(v, start, route, cargo)
        if day == 0 and "V002" in by_id:
            start = change_dt + timedelta(minutes=30 + rng.random.randint(0, 60))
            add_trip(by_id["V002"], start, rng.random.choice(routes), rng.random.choice(cold_ids))

    # -------- 3.2 drivers: greedy assignment, max 2 non-overlapping trips per driver --------
    n_drivers = max(10, int(round(vehicle_count * driver_ratio)))
    drivers = []

    def new_driver():
        i = len(drivers) + 1
        drivers.append({
            "driver_id": f"D{i:03d}",
            "driver_name": f"{DRIVER_NAMES[(i - 1) % len(DRIVER_NAMES)]} {i}",
            "license_category": "Heavy", "hire_date": "2024-01-15",
            "active_from": sim_start.strftime(FMT), "active_to": "",
        })
        return drivers[-1]["driver_id"]

    for _ in range(n_drivers):
        new_driver()
    assigned = {d["driver_id"]: [] for d in drivers}
    for t in sorted(trips, key=lambda x: (x["_start"], x["trip_id"])):
        free = [d for d, items in assigned.items()
                if sum(1 for s, _ in items if s.date() == t["_start"].date()) < 2
                and all(t["_start"] >= e or t["_end"] <= s for s, e in items)]
        if free:
            d_id = rng.random.choice(free)
        else:
            d_id = new_driver()
            assigned[d_id] = []
        assigned[d_id].append((t["_start"], t["_end"]))
        t["driver_id"] = d_id

    # -------- 3.7 fuel prices: one row per day per fuel type --------
    last_day = max(t["_end"] for t in trips).date()
    day = sim_start.date()
    fuel_rows = []
    while day <= last_day:
        for ft in fuel_types:
            fuel_rows.append({"price_date": day.isoformat(), "fuel_type": ft,
                              "price_per_liter": round(fuel_cfg[ft] + rng.random.uniform(-0.3, 0.3), 2),
                              "currency_code": "EGP"})
        day += timedelta(days=1)

    # -------- 3.8 business rules (from the same config) --------
    rules = [dict(r) for r in config.get("business_rules", [])]
    iv = profile.get("intervals_sec", {})
    for rule_name, key, desc in (
        ("expected_interval_gps_sec", "gps", "How often gps is sent; used by the generator and by silence detection"),
        ("expected_interval_engine_sec", "engine", "How often engine is sent"),
        ("expected_interval_fuel_sec", "fuel", "How often fuel is sent"),
        ("expected_interval_temp_sec", "temperature", "How often temperature is sent"),
    ):
        rules.append({"rule_name": rule_name, "rule_value": iv.get(key), "unit": "seconds", "description": desc})

    # -------- write files --------
    for old in glob.glob(os.path.join(output_dir, "vehicles_changes_*.csv")):
        os.remove(old)  # outputs of earlier runs
    if os.path.exists(os.path.join(output_dir, "vehicles.csv")):
        print("[!] Old reference/vehicles.csv found: delete it, the file is now vehicles_initial.csv")

    write_csv(os.path.join(output_dir, "vehicles_initial.csv"), "vehicles", vehicles)
    for when, rows in ((change_dt, scripted), (bulk_dt, bulk)):
        if rows:
            name = f"vehicles_changes_{when.strftime('%Y%m%d_%H%M')}.csv"
            write_csv(os.path.join(output_dir, name), "vehicles", rows)
    write_csv(os.path.join(output_dir, "drivers.csv"), "drivers", drivers)
    write_csv(os.path.join(output_dir, "routes.csv"), "routes", routes)
    write_csv(os.path.join(output_dir, "route_checkpoints.csv"), "route_checkpoints", checkpoints)
    write_csv(os.path.join(output_dir, "cargo_types.csv"), "cargo_types", CARGO)
    write_csv(os.path.join(output_dir, "trips.csv"), "trips",
              [{k: v for k, v in t.items() if not k.startswith("_")} for t in trips])
    write_csv(os.path.join(output_dir, "fuel_prices.csv"), "fuel_prices", fuel_rows)
    write_csv(os.path.join(output_dir, "business_rules.csv"), "business_rules", rules)

    # -------- optional reference-file faults (P1-10), written to a separate folder --------
    if config.get("reference_file_faults"):
        write_reference_faults(seed, trips, drivers, assigned, vehicles, retrofit_at, routes, cold_ids, shortest)

    print("\n[SUCCESS] All reference CSV files generated inside /reference")


if __name__ == "__main__":
    generate_data()