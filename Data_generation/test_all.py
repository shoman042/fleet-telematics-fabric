"""
test_all.py - sends valid test messages to the Eventstream Kafka endpoints and prints OK / FAIL for each.

Usage
  python test_all.py                        one message to each of the 5 endpoints
  python test_all.py --only gps             only one endpoint (use this while you build the streams one by one)
  python test_all.py --only gps --count 3   send 3 messages (the Eventhouse wizard needs a few sample events)

Reads KAFKA_<SIGNAL>_BOOTSTRAP / _TOPIC / _CONN_STR from .env, with SIGNAL = GPS, ENGINE, FUEL, TEMPERATURE, DEVICE_STATUS.
Every test message has an event_id that starts with TEST-ALL-, so you can find it and clear it later.
"""
import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

SIGNALS = ["gps", "engine", "fuel", "temperature", "device_status"]


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def get_settings(signal):
    """Returns ([bootstrap, topic, conn_string], [names of missing .env keys])."""
    up = signal.upper()
    names = [f"KAFKA_{up}_BOOTSTRAP", f"KAFKA_{up}_TOPIC", f"KAFKA_{up}_CONN_STR"]
    values = [os.environ.get(n, "").strip().strip('"').strip("'") for n in names]
    missing = [n for n, v in zip(names, values) if not v]
    return values, missing


def check_settings(bootstrap, topic, conn):
    """Catches the usual copy and paste mistakes. Returns a list of warnings."""
    warnings = []
    if not bootstrap.endswith(":9093"):
        warnings.append("BOOTSTRAP should end with :9093")
    if not conn.startswith("Endpoint=sb://"):
        warnings.append("CONN_STR should be the whole connection string and start with Endpoint=sb://")
        return warnings
    host = conn.split("Endpoint=sb://", 1)[1].split("/", 1)[0]
    if bootstrap.split(":")[0] != host:
        warnings.append(f"BOOTSTRAP '{bootstrap}' does not match the host in CONN_STR ('{host}')")
    if "EntityPath=" in conn:
        entity = conn.split("EntityPath=", 1)[1].split(";", 1)[0]
        if entity != topic:
            warnings.append(f"TOPIC '{topic}' does not match the EntityPath in CONN_STR ('{entity}')")
    return warnings


def make_producer(bootstrap, conn):
    return Producer({
        "bootstrap.servers": bootstrap, "security.protocol": "SASL_SSL", "sasl.mechanisms": "PLAIN",
        "sasl.username": "$ConnectionString", "sasl.password": conn,
        "message.timeout.ms": 20000, "client.id": "fleet-test"})


def send_and_wait(producer, topic, key, payload):
    """Sends one message and waits for the delivery report. Returns {'partition', 'offset'} or {'error'}."""
    result = {}

    def cb(err, msg):
        if err is not None:
            result["error"] = str(err)
        else:
            result["partition"], result["offset"] = msg.partition(), msg.offset()

    producer.produce(topic, key=key.encode(), value=payload.encode(), callback=cb)
    producer.flush(30)
    return result or {"error": "no delivery report within 30 seconds"}


def build_message(signal, n):
    """A valid message of the given signal (schema v1) with sequence number n."""
    now = datetime.now(timezone.utc)
    m = {"schema_version": 1, "signal_type": signal, "event_id": f"TEST-ALL-{signal}-{uuid.uuid4().hex[:8]}",
         "vehicle_id": "V001", "trip_id": "TRP-V001-001", "device_id": "DEV_V001",
         "event_time": iso(now), "sequence_number": n}
    if signal == "gps":
        m.update(latitude=30.0444, longitude=31.2357, speed_kmh=60.0, heading_degrees=90.0,
                 gps_accuracy_meters=5.0)
    elif signal == "engine":
        m.update(engine_on=True, engine_rpm=1500, engine_temperature_c=90.0, vehicle_speed_kmh=60.0,
                 battery_voltage_v=13.8)
    elif signal == "fuel":
        m.update(fuel_level_liters=300.0, trip_fuel_used_liters=5.0, odometer_km=1000.0, is_refueling=False)
    elif signal == "temperature":
        m.update(shipment_id="SHP-TEST", sensor_id="SEN_V001_1", temperature_value=4.0, temperature_unit="C",
                 setpoint_temperature_c=4.0, sensor_status="OK")
    else:
        m.update(event_type="RECONNECTED", offline_from=iso(now - timedelta(minutes=10)), reconnected_at=iso(now),
                 stored_gps=0, stored_engine=0, stored_fuel=0, stored_temperature=0, dropped_messages=0,
                 upload_method="STREAM_BURST", upload_id=None)
    return m


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--only", choices=SIGNALS, help="test only this signal")
    ap.add_argument("--count", type=int, default=1, help="messages per signal (default 1)")
    args = ap.parse_args()
    load_dotenv()

    # each signal has its own Eventstream, so two signals must never share a connection string
    seen = {}
    for s in SIGNALS:
        values, missing = get_settings(s)
        if not missing:
            if values[2] in seen:
                print(f"[!] {s} and {seen[values[2]]} use the SAME connection string: each signal needs its own Eventstream")
            seen[values[2]] = s

    results = {}
    for s in ([args.only] if args.only else SIGNALS):
        values, missing = get_settings(s)
        if missing:
            print(f"[FAIL] {s}: missing in .env: {', '.join(missing)}")
            results[s] = False
            continue
        bootstrap, topic, conn = values
        for w in check_settings(bootstrap, topic, conn):
            print(f"[!] {s}: {w}")
        producer = make_producer(bootstrap, conn)
        ok = True
        for n in range(1, args.count + 1):
            msg = build_message(s, n)
            res = send_and_wait(producer, topic, msg["vehicle_id"], json.dumps(msg, separators=(",", ":")))
            if "error" in res:
                ok = False
                print(f"[FAIL] {s} message {n}: {res['error']}")
            else:
                print(f"[OK]   {s} message {n}: DELIVERED partition={res['partition']} offset={res['offset']} "
                      f"event_id={msg['event_id']}")
        results[s] = ok

    print("\nSummary")
    for s, ok in results.items():
        print(f"  {s:14} {'OK' if ok else 'FAIL'}")
    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    main()