"""
load_test.py - Day-1 throughput test for the Eventstream (guide section 6.2 / 13).

What it does
  1. Sends synthetic gps / engine / fuel / temperature messages to the Eventstream
     custom endpoints at increasing target rates (steps).
  2. Measures per step: achieved delivered msgs/sec (confirmed by delivery
     callbacks), errors, queue-full events, delivery latency p50/p95.
  3. A step PASSES if delivered >= 95% of target, 0 errors, p95 latency <= --max-p95.
  4. R = highest delivered rate among passing steps.
  5. Computes the max acceleration factor:
        A <= 0.5 * R / (peak_trucks * msgs_per_truck_per_sim_second)

Run
  pip install confluent-kafka python-dotenv
  python load_test.py --rates 200,500,1000,2000,3000 --step-seconds 30 --procs 2

If R stops growing while CPU on your laptop is high, raise --procs first:
the limit may be your machine, not Fabric.

Notes
  - Messages use trip_id = LOADTEST so they are easy to ignore later.
  - R measured here is producer -> Eventstream ingest only. The Spark streaming
    job is not written yet; measure it later and record it separately.
"""
import argparse
import json
import multiprocessing as mp
import os
import random
import statistics
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# .env variable names (same as test_all.py): KAFKA_<SIGNAL>_BOOTSTRAP / _CONN_STR / _TOPIC
SIGNALS = ["gps", "engine", "fuel", "temperature"]


def env(sig, suffix):
    key = f"KAFKA_{sig.upper()}_{suffix}"
    v = os.environ.get(key)
    if not v:
        sys.exit(f"Missing {key} in .env")
    return v

S = {"delivered": 0, "errors": 0, "lat": []}  # per-process step stats


def make_producer(cs, sig):
    topic = env(sig, "TOPIC")
    p = Producer(
        {
            "bootstrap.servers": env(sig, "BOOTSTRAP"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": "$ConnectionString",
            "sasl.password": cs,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.kbytes": 262144,
            "linger.ms": 20,
            "batch.num.messages": 5000,
            "socket.keepalive.enable": True,
        }
    )
    return p, topic


def on_delivery(err, msg):
    if err:
        S["errors"] += 1
    else:
        S["delivered"] += 1
        lat = msg.latency()
        if lat is not None and len(S["lat"]) < 20000:
            S["lat"].append(lat)


def build(sig, seq):
    vid = f"V{random.randint(1, 5000):04d}"
    m = {
        "schema_version": 1,
        "signal_type": sig,
        "event_id": str(uuid.uuid4()),
        "vehicle_id": vid,
        "trip_id": "LOADTEST",
        "device_id": f"DEV_{vid}",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "sequence_number": seq,
    }
    if sig == "gps":
        m.update(latitude=round(random.uniform(29.5, 31.5), 6), longitude=round(random.uniform(29.5, 32.5), 6),
                 speed_kmh=round(random.uniform(60, 95), 1), heading_degrees=round(random.uniform(0, 359), 1),
                 gps_accuracy_meters=round(random.uniform(3, 20), 1))
    elif sig == "engine":
        m.update(engine_on=True, engine_rpm=random.randint(1400, 1800),
                 engine_temperature_c=round(random.uniform(85, 95), 1),
                 vehicle_speed_kmh=round(random.uniform(60, 95), 1),
                 battery_voltage_v=round(random.uniform(13.5, 14.2), 2))
    elif sig == "fuel":
        m.update(fuel_level_liters=round(random.uniform(100, 300), 1),
                 trip_fuel_used_liters=round(random.uniform(0, 60), 1),
                 odometer_km=round(random.uniform(10000, 200000), 1), is_refueling=False)
    else:
        m.update(shipment_id=f"SHP{random.randint(1, 9999):05d}", sensor_id=f"SNS_{vid}",
                 temperature_value=round(random.uniform(3.5, 4.5), 2), temperature_unit="C",
                 setpoint_temperature_c=4.0, sensor_status="OK")
    return vid, json.dumps(m)


def worker(pid, nprocs, rates, step_s, conns, weights, start_at, q):
    random.seed(pid * 7919 + int(time.time()))
    prods, topics = {}, {}
    for sig in SIGNALS:
        prods[sig], topics[sig] = make_producer(conns[sig], sig)
    while time.time() < start_at:
        time.sleep(0.01)
    seq = 0
    for i, rate in enumerate(rates):
        target = rate / nprocs
        S.update(delivered=0, errors=0, lat=[])
        produced = qfull = 0
        t0 = time.time()
        while True:
            el = time.time() - t0
            if el >= step_s:
                break
            due = int(target * el) - produced
            if due <= 0:
                for p in prods.values():
                    p.poll(0)
                time.sleep(0.002)
                continue
            for _ in range(min(due, 2000)):
                sig = random.choices(SIGNALS, weights)[0]
                seq += 1
                vid, payload = build(sig, seq)
                try:
                    prods[sig].produce(topics[sig], key=vid, value=payload, on_delivery=on_delivery)
                    produced += 1
                except BufferError:
                    qfull += 1
                    prods[sig].poll(0.05)
                    break
            for p in prods.values():
                p.poll(0)
        grace_end = time.time() + 2.0  # let outstanding delivery reports arrive
        while time.time() < grace_end and (S["delivered"] + S["errors"]) < produced:
            for p in prods.values():
                p.poll(0.05)
        q.put((i, produced, S["delivered"], S["errors"], qfull, list(S["lat"])))
    for p in prods.values():
        p.flush(15)


def pct(vals, p):
    if not vals:
        return float("nan")
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(len(vals) * p))]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rates", default="200,500,1000,2000,3000", help="target msgs/sec per step")
    ap.add_argument("--step-seconds", type=int, default=30)
    ap.add_argument("--procs", type=int, default=2)
    ap.add_argument("--max-p95", type=float, default=2.0, help="max p95 delivery latency (s) to pass")
    ap.add_argument("--peak-trucks", type=int, default=2500)
    ap.add_argument("--intervals", default="30,30,120,60", help="gps,engine,fuel,temperature sec")
    ap.add_argument("--refrigerated-share", type=float, default=0.6)
    a = ap.parse_args()

    rates = [int(x) for x in a.rates.split(",")]
    g, e, f, t = [float(x) for x in a.intervals.split(",")]
    weights = [1 / g, 1 / e, 1 / f, a.refrigerated_share / t]
    msgs_per_truck = sum(weights)

    conns = {sig: env(sig, "CONN_STR") for sig in SIGNALS}

    q = mp.Queue()
    start_at = time.time() + 5
    procs = [mp.Process(target=worker, args=(i, a.procs, rates, a.step_seconds, conns, weights, start_at, q))
             for i in range(a.procs)]
    for p in procs:
        p.start()

    steps = {i: [] for i in range(len(rates))}
    total = a.procs * len(rates)
    timeout = a.step_seconds * len(rates) + 180
    for _ in range(total):
        i, prod, dlv, err, qf, lat = q.get(timeout=timeout)
        steps[i].append((prod, dlv, err, qf, lat))
    for p in procs:
        p.join()

    print(f"\n{'target':>8} {'produced/s':>11} {'delivered/s':>12} {'errors':>7} {'qfull':>6} {'p50 s':>7} {'p95 s':>7}  result")
    results, R = [], None
    for i, rate in enumerate(rates):
        prod = sum(x[0] for x in steps[i])
        dlv = sum(x[1] for x in steps[i])
        err = sum(x[2] for x in steps[i])
        qf = sum(x[3] for x in steps[i])
        lat = [l for x in steps[i] for l in x[4]]
        drate = dlv / a.step_seconds
        p50 = statistics.median(lat) if lat else float("nan")
        p95 = pct(lat, 0.95)
        ok = drate >= 0.95 * rate and err == 0 and p95 <= a.max_p95
        if ok:
            R = max(R or 0, drate)
        results.append(dict(target=rate, produced_per_s=prod / a.step_seconds, delivered_per_s=drate,
                            errors=err, queue_full=qf, p50_s=p50, p95_s=p95, passed=ok))
        print(f"{rate:>8} {prod / a.step_seconds:>11.0f} {drate:>12.0f} {err:>7} {qf:>6} {p50:>7.3f} {p95:>7.3f}  {'PASS' if ok else 'FAIL'}")

    print(f"\nMessages per truck per simulated second = {msgs_per_truck:.4f}")
    out = dict(R=R, msgs_per_truck_per_sim_sec=msgs_per_truck, peak_trucks=a.peak_trucks, steps=results, procs=a.procs)
    if R:
        a_max = 0.5 * R / (a.peak_trucks * msgs_per_truck)
        out["A_max"] = a_max
        print(f"R (highest sustained rate) = {R:.0f} msgs/sec")
        print(f"A <= 0.5 x {R:.0f} / ({a.peak_trucks} x {msgs_per_truck:.4f}) = {a_max:.2f}")
        print(f"Suggested A (evidence profile): {min(10, max(1, int(a_max)))}")
    else:
        print("No step passed. Lower --rates, check the connection strings, or look at errors above.")
    with open("load_test_results.json", "w") as fh:
        json.dump(out, fh, indent=2)
    print("Saved load_test_results.json (copy it to Files/evidence).")


if __name__ == "__main__":
    main()
