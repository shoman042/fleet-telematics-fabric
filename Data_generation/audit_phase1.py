"""
audit_phase1.py - checks your project folder against the Phase 1 plan (P1-1 .. P1-9)
and the handoff contract. Read-only: it never changes your files.

Run from anywhere:
    python audit_phase1.py --root D:\\Project_fabric
Optional:
    --ref  path\\to\\reference     (if auto-detect picks the wrong folder)

Output: a PASS / FAIL / WARN list on screen and audit_report.md in the current folder.
Paste the output back to get an exact fix list.
"""
import argparse
import csv
import json
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

SCHEMAS = {
    "vehicles": ["vehicle_id", "registration_number", "vehicle_type", "fuel_type", "fuel_tank_capacity_liters",
                 "refrigeration_capable", "device_id", "model_year", "active_from", "active_to", "updated_at"],
    "drivers": ["driver_id", "driver_name", "license_category", "hire_date", "active_from", "active_to"],
    "routes": ["route_id", "route_name", "origin_name", "destination_name", "planned_distance_km",
               "allowed_deviation_meters", "expected_duration_minutes", "arrival_grace_minutes", "speed_limit_kmh"],
    "route_checkpoints": ["checkpoint_id", "route_id", "sequence_number", "checkpoint_name", "latitude",
                          "longitude", "planned_offset_minutes", "checkpoint_type"],
    "cargo_types": ["cargo_type_id", "cargo_name", "temperature_min_c", "temperature_max_c", "cargo_value_per_unit",
                    "currency_code", "max_excursion_minutes", "spoilage_loss_pct", "alert_hold_minutes"],
    "trips": ["trip_id", "vehicle_id", "driver_id", "route_id", "shipment_id", "cargo_type_id", "planned_start_time",
              "planned_end_time", "cargo_quantity", "trip_status"],
    "fuel_prices": ["price_date", "fuel_type", "price_per_liter", "currency_code"],
    "business_rules": ["rule_name", "rule_value", "unit", "description"],
}
REQUIRED_RULES = [
    "idle_speed_threshold_kmh", "idle_minutes_threshold", "speeding_tolerance_kmh", "gps_accuracy_max_meters",
    "deviation_consecutive_readings", "gps_jump_max_kmh", "speed_max_kmh", "engine_rpm_max",
    "engine_temperature_max_c", "temperature_spike_c", "flatline_readings", "future_timestamp_tolerance_minutes",
    "expected_interval_gps_sec", "expected_interval_engine_sec", "expected_interval_fuel_sec",
    "expected_interval_temp_sec", "offline_missed_intervals", "late_data_watermark_minutes",
    "sequence_gap_grace_minutes", "dq_window_minutes", "invalid_rate_alert_pct", "quality_gate_max_invalid_pct",
]
REQUIRED_RULE_PREFIXES = ["cargo_temperature", "score_weight_speeding", "score_weight_idling",
                          "score_weight_deviation", "score_weight_lateness"]
MANIFEST_KEYS = ["run_id", "seed", "profile", "vehicle_count", "sim_start", "wall_start", "acceleration_factor"]
INJECTION_COLS = ["injection_id", "run_id", "injection_type", "vehicle_id", "trip_id", "signal_type", "event_id",
                  "start_time", "end_time", "details", "seed"]
SKIP_DIRS = {".git", "node_modules", "venv", ".venv", "__pycache__", ".idea", ".vscode"}

results = []


def add(status, area, msg):
    results.append((status, area, msg))


def rglob(root, pattern):
    return [p for p in root.rglob(pattern) if not (set(p.parts) & SKIP_DIRS)]


def ts(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if d.tzinfo:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)
    return d


def read_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))
    if not rows:
        return [], [], []
    header = [h.strip() for h in rows[0]]
    bad = [i + 2 for i, r in enumerate(rows[1:]) if len(r) != len(header)]
    data = [dict(zip(header, r)) for r in rows[1:] if len(r) == len(header)]
    return header, data, bad


def truthy(v):
    return str(v).strip().lower() in ("true", "1", "yes", "y")


# ---------------------------------------------------------------- reference data
def find_ref_dir(root, forced):
    if forced:
        return Path(forced)
    counts = defaultdict(int)
    for name in list(SCHEMAS) + ["vehicles_initial"]:
        for p in rglob(root, f"{name}.csv"):
            counts[p.parent] += 1
    return max(counts, key=counts.get) if counts else None


def check_reference(root, forced):
    ref = find_ref_dir(root, forced)
    if not ref:
        add("FAIL", "P1-4", "No reference CSV files found under the project folder. Pass --ref <folder>.")
        return
    add("INFO", "P1-4", f"Reference folder used: {ref}")
    tables = {}
    for name, cols in SCHEMAS.items():
        fname = "vehicles_initial.csv" if name == "vehicles" and (ref / "vehicles_initial.csv").exists() else f"{name}.csv"
        path = ref / fname
        if not path.exists():
            add("FAIL", "P1-4", f"Missing {fname}")
            continue
        if name == "vehicles" and fname == "vehicles.csv":
            add("WARN", "P1-4", "File is named vehicles.csv but the plan asks for vehicles_initial.csv (rename it, and change the generator + pipeline to match)")
        header, data, bad = read_csv(path)
        if bad:
            add("FAIL", "P1-4", f"{fname}: rows with wrong number of fields (extra delimiter?) at lines {bad[:5]}")
        if header != cols:
            missing = [c for c in cols if c not in header]
            extra = [c for c in header if c not in cols]
            order = "" if (missing or extra) else " (same columns, different order)"
            add("FAIL", "P1-4", f"{fname}: columns differ from section 3. Missing={missing} Extra={extra}{order}")
        else:
            add("PASS", "P1-4", f"{fname}: columns match section 3 ({len(data)} rows)")
        tables[name] = data

    # business rules
    br = tables.get("business_rules")
    if br is not None:
        names = {r["rule_name"].strip() for r in br}
        miss = [r for r in REQUIRED_RULES if r not in names]
        miss += [p for p in REQUIRED_RULE_PREFIXES if not any(n.startswith(p) for n in names)]
        if miss:
            add("FAIL", "P1-4", f"business_rules.csv has {len(names)} rules; missing {len(miss)}: {miss[:8]}{' ...' if len(miss) > 8 else ''}")
        else:
            add("PASS", "P1-4", f"business_rules.csv covers all required rules ({len(names)} rules)")

    # checkpoints per route
    routes = {r["route_id"] for r in tables.get("routes", [])}
    cps = defaultdict(int)
    for r in tables.get("route_checkpoints", []):
        cps[r["route_id"]] += 1
    if routes:
        bad_routes = {rid: cps.get(rid, 0) for rid in routes if not 15 <= cps.get(rid, 0) <= 25}
        if bad_routes:
            sample = dict(list(bad_routes.items())[:5])
            add("FAIL", "P1-4", f"{len(bad_routes)}/{len(routes)} routes do not have 15-25 checkpoints, e.g. {sample}")
        else:
            add("PASS", "P1-4", f"All {len(routes)} routes have 15-25 checkpoints")
        orphan = [r for r in cps if r not in routes]
        if orphan:
            add("FAIL", "P1-4", f"route_checkpoints refer to unknown routes: {orphan[:5]}")

    # cargo ranges
    for c in tables.get("cargo_types", []):
        try:
            if c["temperature_min_c"].strip() and float(c["temperature_min_c"]) >= float(c["temperature_max_c"]):
                add("FAIL", "P1-4", f"cargo_types: {c['cargo_type_id']} has min temperature >= max")
        except ValueError:
            add("FAIL", "P1-4", f"cargo_types: non numeric temperature in {c.get('cargo_type_id')}")

    # vehicle versions + change files (P1-7)
    vehicles = tables.get("vehicles", [])
    versions = defaultdict(list)
    for v in vehicles:
        versions[v["vehicle_id"]].append((ts(v.get("updated_at")) or datetime.min, v))
    changes = sorted(ref.glob("vehicles_changes_*.csv"))
    if not changes:
        add("FAIL", "P1-7", "No vehicles_changes_YYYYMMDD_HHMM.csv files in the reference folder")
    else:
        for cf in changes:
            header, data, bad = read_csv(cf)
            if header != SCHEMAS["vehicles"]:
                add("FAIL", "P1-7", f"{cf.name}: columns differ from vehicles.csv")
            for v in data:
                versions[v["vehicle_id"]].append((ts(v.get("updated_at")) or datetime.min, v))
        add("PASS", "P1-7", f"{len(changes)} change file(s) found")
        v2 = [v for _, v in versions.get("V002", [])]
        if len(v2) < 2 or not any(truthy(v["refrigeration_capable"]) for v in v2) or all(truthy(v["refrigeration_capable"]) for v in v2):
            add("FAIL", "P1-7", "V002 retrofit (not refrigerated -> refrigerated) not found")
        else:
            add("PASS", "P1-7", "V002 retrofit present")
        if not any(v["registration_number"].strip() == "SIM-0007" for _, v in versions.get("V007", [])):
            add("FAIL", "P1-7", "V007 registration correction to SIM-0007 not found")
        else:
            add("PASS", "P1-7", "V007 registration correction present")
        if not any(v["device_id"].strip() == "DEV_V010B" for _, v in versions.get("V010", [])):
            add("WARN", "P1-7", "V010 device swap to DEV_V010B not found (optional in the plan)")
        else:
            add("PASS", "P1-7", "V010 device swap present")
    for vid in versions:
        versions[vid].sort(key=lambda x: x[0])

    # trips
    trips = [t for t in tables.get("trips", []) if t["trip_status"].strip() != "CANCELLED"]
    if trips:
        vids = {v["vehicle_id"] for v in vehicles}
        dids = {d["driver_id"] for d in tables.get("drivers", [])}
        cids = {c["cargo_type_id"]: c for c in tables.get("cargo_types", [])}
        rmap = {r["route_id"]: r for r in tables.get("routes", [])}
        fk = [t["trip_id"] for t in trips if t["vehicle_id"] not in vids or t["driver_id"] not in dids
              or t["route_id"] not in rmap or t["cargo_type_id"] not in cids]
        add("FAIL" if fk else "PASS", "P1-4", f"trips: {len(fk)} rows reference unknown vehicle/driver/route/cargo" if fk else "trips: all ids exist in reference tables")
        no_trip = vids - {t["vehicle_id"] for t in trips}
        add("FAIL" if no_trip else "PASS", "P1-4", f"{len(no_trip)} vehicles have no trip (every vehicle must have at least one)" if no_trip else "Every vehicle has at least one trip")
        for key, label in (("vehicle_id", "vehicle"), ("driver_id", "driver")):
            groups = defaultdict(list)
            for t in trips:
                s, e = ts(t["planned_start_time"]), ts(t["planned_end_time"])
                if s and e:
                    groups[t[key]].append((s, e, t["trip_id"]))
            overlaps = []
            for k, items in groups.items():
                items.sort()
                for a, b in zip(items, items[1:]):
                    if b[0] < a[1]:
                        overlaps.append((k, a[2], b[2]))
            add("FAIL" if overlaps else "PASS", "P1-4", f"{len(overlaps)} overlapping trips per {label}, e.g. {overlaps[:3]}" if overlaps else f"No overlapping trips per {label}")
        dur_bad, cold_bad = [], []
        for t in trips:
            s, e = ts(t["planned_start_time"]), ts(t["planned_end_time"])
            r = rmap.get(t["route_id"])
            if s and e and r:
                try:
                    if abs((e - s) - timedelta(minutes=float(r["expected_duration_minutes"]))) > timedelta(minutes=1):
                        dur_bad.append(t["trip_id"])
                except ValueError:
                    pass
            c = cids.get(t["cargo_type_id"])
            if s and c and c["temperature_min_c"].strip():
                vers = versions.get(t["vehicle_id"], [])
                cur = None
                for when, v in vers:
                    if when <= s:
                        cur = v
                cur = cur or (vers[0][1] if vers else None)
                if cur is not None and not truthy(cur["refrigeration_capable"]):
                    cold_bad.append(t["trip_id"])
        add("FAIL" if dur_bad else "PASS", "P1-4", f"{len(dur_bad)} trips where planned_end != start + route duration, e.g. {dur_bad[:3]}" if dur_bad else "planned_end_time = start + expected duration for all trips")
        add("FAIL" if cold_bad else "PASS", "P1-4", f"{len(cold_bad)} trips carry cold cargo on a vehicle version that is not refrigeration-capable, e.g. {cold_bad[:3]}" if cold_bad else "Cold cargo only on refrigeration-capable vehicle versions")


# ---------------------------------------------------------------- project files
def check_project(root):
    git = root / ".git"
    add("PASS" if git.exists() else "WARN", "P1-1", "Git repo found" if git.exists() else "No .git folder in the project root (P1-1 asks for a Git repo)")
    gi = root / ".gitignore"
    if gi.exists() and ".env" in gi.read_text(errors="ignore"):
        add("PASS", "P1-1", ".env is listed in .gitignore")
    else:
        add("FAIL", "P1-1", ".env is NOT in .gitignore (keys could leak)")
    if git.exists() and (root / ".env").exists():
        try:
            r = subprocess.run(["git", "-C", str(root), "ls-files", "--error-unmatch", ".env"], capture_output=True)
            if r.returncode == 0:
                add("FAIL", "P1-1", ".env is already tracked by git: remove it from git and regenerate ALL keys")
        except OSError:
            pass
    cfg = (rglob(root, "config.yaml") + rglob(root, "config.yml") + [None])[0]
    if not cfg:
        add("FAIL", "P1-3", "config.yaml not found")
    else:
        text = cfg.read_text(errors="ignore").lower()
        want = {"seed": "seed", "acceleration factor": "accel", "smoke": "smoke", "live demo": "demo",
                "evidence": "evidence", "clean": "clean", "anomaly rates": "anomal", "fault rates": "fault",
                "offline settings": "offline"}
        miss = [k for k, w in want.items() if w not in text]
        add("FAIL" if miss else "PASS", "P1-3", f"{cfg.name}: missing keys/modes: {miss}" if miss else f"{cfg.name}: has seed, A, four modes, anomaly/fault/offline settings")
    gens = [p for p in rglob(root, "*.py") if "generat" in p.name.lower()]
    add("PASS" if gens else "FAIL", "P1-5", f"Generator scripts: {[p.name for p in gens][:6]}" if gens else "No generator script found")
    readme = (rglob(root, "README.md") + [None])[0]
    add("PASS" if readme else "FAIL", "P1-9", "README.md found" if readme else "README.md missing (how to run each mode, config keys, outputs, limits)")
    whl = rglob(root, "*.whl")
    add("PASS" if whl else "FAIL", "P1-8", f"Wheel found: {whl[0].name}" if whl else "No .whl file (shared library v1.0). Also confirm it is installed in the Fabric Environment")
    find = rglob(root, "day1_findings.md") + rglob(root, "phase1_foundation.md")
    add("PASS" if find else "WARN", "P1-2", "Day-1 findings note found" if find else "Day-1 findings note not found in the project folder")


def check_evidence(root, forced_ref):
    man = rglob(root, "run_manifest.json")
    if not man:
        add("FAIL", "P1-6", "run_manifest.json not found")
    else:
        try:
            data = json.loads(man[0].read_text(encoding="utf-8"))
            miss = [k for k in MANIFEST_KEYS if k not in data]
            add("FAIL" if miss else "PASS", "P1-6", f"run_manifest.json missing keys {miss}" if miss else "run_manifest.json has all required keys")
        except ValueError:
            add("FAIL", "P1-6", "run_manifest.json is not valid JSON")
    logs = rglob(root, "injected_events_log*")
    if not logs:
        add("FAIL", "P1-6", "injected_events_log not found (ground truth)")
    else:
        p = logs[0]
        cols = []
        try:
            if p.suffix == ".csv":
                cols = read_csv(p)[0]
            else:
                line = next(l for l in p.read_text(encoding="utf-8").splitlines() if l.strip())
                obj = json.loads(line)
                cols = list(obj[0].keys()) if isinstance(obj, list) else list(obj.keys())
        except (StopIteration, ValueError, IndexError):
            pass
        miss = [c for c in INJECTION_COLS if c not in cols]
        add("FAIL" if miss else "PASS", "P1-6", f"{p.name} missing columns {miss}" if miss else f"{p.name} has all required columns")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--ref", default=None)
    a = ap.parse_args()
    root = Path(a.root).resolve()
    add("INFO", "-", f"Project root: {root}")
    check_project(root)
    check_reference(root, a.ref)
    check_evidence(root, a.ref)

    order = {"FAIL": 0, "WARN": 1, "PASS": 2, "INFO": 3}
    lines = []
    for st in ("FAIL", "WARN", "PASS", "INFO"):
        for s, area, msg in results:
            if s == st:
                lines.append(f"[{s:4}] {area:5} {msg}")
    n = {k: sum(1 for r in results if r[0] == k) for k in order}
    summary = f"\nFAIL={n['FAIL']}  WARN={n['WARN']}  PASS={n['PASS']}"
    out = "\n".join(lines) + summary
    print(out)
    Path("audit_report.md").write_text("# Phase 1 audit\n\n```\n" + out + "\n```\n", encoding="utf-8")
    print("\nSaved audit_report.md")
    print("Not checked here (look in Fabric): Eventstream destination, bronze.telemetry_raw columns, Airflow Job, Environment install, member access.")


if __name__ == "__main__":
    main()
