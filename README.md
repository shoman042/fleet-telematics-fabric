# Fleet Telematics Lakehouse on Microsoft Fabric

SIC 8 / 2026 · Big Data Engineering · Project 3

An end-to-end data engineering project on Microsoft Fabric. A seeded Python simulator generates truck telemetry (GPS, engine, fuel, temperature and device status). The data is streamed through Eventstreams, stored raw, and refined through Bronze, Silver and Gold layers. A real-time layer raises alerts, and a serving layer feeds Power BI dashboards.

The simulator injects known anomalies, data faults, late arrivals and offline windows and records a ground truth for each, so every pipeline stage can be reconciled against what was injected.

## Architecture

```
Fleet generator (Python, seeded, outside Fabric)
        │  signals + device status
        ▼
Fabric Eventstreams (gps, engine, fuel, temperature, device_status)
        │
        ├─► Eventhouse eh_fleet (raw KQL tables, TXT direct ingestion)
        │         │  nb_p2_bronze_loader
        │         ▼
        │   Bronze (Delta, append-only)
        │         │  batch job 1: validate, dedup, MERGE, Gate 1  ──► DLQ (ops.dlq_events)
        │         ▼
        │   Silver (one Delta table per signal)
        │         │  batch job 2: dimensions, facts, dashboard tables
        │         ▼
        │   Gold (galaxy schema)
        │
        └─► Real-time layer ──► alert_events (KQL) ──► gold.fact_alerts

Serving layer (SQL endpoint) ──► Power BI dashboards
```

The architecture diagram in the repository shows orchestration as an Airflow Job. The implemented orchestration uses a Data Factory data pipeline (`pl_fleet_batch`), chosen because it is built into Fabric and supports retry, timeout, schedule and If Condition without DAG code.

## Fabric items

| Item | Type | Role |
|---|---|---|
| `SIC8-Fleet-Telematics` | Workspace | Contains all items |
| `eh_fleet` | Eventhouse + KQL database | Raw `bronze_*_raw` tables and `alert_events` |
| `es_gps`, `es_engine`, `es_fuel`, `es_temperature`, `es_device_status` | Eventstreams | One custom endpoint (Kafka protocol) per signal |
| `lh_fleet` | Lakehouse | Schemas `bronze`, `silver`, `gold`, `ref`, `ops`; Files folders `reference`, `landing/offline`, `evidence` |
| `env_fleet` | Environment | Holds the `fleet_shared` library |
| `pl_fleet_batch` | Data pipeline | Orchestrates the batch notebooks |

## Repository structure

| Folder | Content |
|---|---|
| `codes/` | Generator, configuration and test scripts |
| `notebooks/` | Fabric notebooks |
| `documentation/` | Project documentation |
| `screenshots/` | Evidence screenshots |

## Data generator

| File | Role |
|---|---|
| `config.yaml` | Seed, mode, intervals, acceleration factor, anomaly, fault and offline settings |
| `generate_reference_data.py` | Reference CSVs (mode read from `config.yaml` only) |
| `generate_stream.py` | Truck simulation (generator v1.3); sends to Eventstreams and writes evidence files |
| `verify_stream.py` | Checks generated messages against the ground truth (needs `--sink jsonl`) |
| `audit_phase1.py` | Checks the project folder against the Phase 1 plan |
| `config.py`, `rng.py` | Config loader and seeded random helper |
| `test_all.py`, `test_malformed.py`, `load_test.py` | Eventstream tests |

### Run modes

| Mode | Vehicles | Routes | Acceleration A | Use |
|---|---|---|---|---|
| `smoke` | 20 | 3 | 50 | End-to-end check (current mode) |
| `live_demo` | 200 | 8 | 5 | Show alerts live |
| `evidence` | 5,000 | 12 | 1 | Scale proof, about 5.4 million messages; run last |
| `clean` | 20 | 3 | 50 | Baseline with no anomalies, faults, late trips or offline windows |

One wall-clock second equals `A` simulated seconds.

### Injected cases

- **Anomalies:** cold-chain breach, excessive idling, route deviation, with negative cases (cold-chain blip, short stop, GPS wander).
- **Data faults (17 types):** dropped, duplicate and near-duplicate messages, malformed JSON, missing fields, wrong types, invalid GPS, impossible values, bad timestamps, unknown vehicles, schema drift, temperature spikes, unit errors, sensor faults, stuck sensors, and an optional poison batch.
- **Late arrivals:** slow trips and near-misses inside the grace period.
- **Offline windows (P1-12):** store-and-forward with burst or bulk-file upload and buffer overflow. Implemented and off by default.
- **SCD changes:** `vehicles_changes_YYYYMMDD_HHMM.csv` files (V002 retrofit, V007 registration correction, V010 device swap).

Ground truth is written to `evidence/injected_events_log.csv` and `evidence/run_manifest.json` before anything is sent.

## Layers

### Bronze
One raw table per signal in Eventhouse (single column `raw_value`, TXT format) so malformed messages are preserved. `nb_p2_bronze_loader` copies them incrementally to `bronze.bronze_{signal}_raw` (Delta, partitioned by `ingestion_date`) using a watermark and a 2-minute safety lag.

### Silver
`nb_p2_batch_job_1_clean` processes gps, engine, fuel and temperature:

1. Select the new Bronze window (watermark)
2. Validate with `fleet_shared.validate_message()`
3. Check the vehicle is active on the event date
4. Write rejected rows to `ops.dlq_events`
5. Parse, enrich (route, driver, cargo) and compute `lateness_sec`
6. Deduplicate by `event_id`
7. Add `SPIKE`, `FLATLINE` and `IMPOSSIBLE_JUMP` flags
8. `MERGE` into `silver.silver_<signal>`
9. Log counts and advance the watermark (last step)

**Gate 1** returns PASS or FAIL as the notebook exit value. It fails when the invalid share, overall or for any signal, exceeds 5 percent.

### Gold
`nb_p3_batch_job_2` builds a galaxy schema:

- **Dimensions:** `dim_vehicle` (SCD Type 2), `dim_driver`, `dim_route`, `dim_cargo`, `dim_date`
- **Facts:** `fact_trip_performance` (one row per trip), `fact_cold_chain` (one row per refrigerated trip), `fact_alerts` (one row per alert episode)

`nb_p4_fact_alerts_load` merges the latest state per `alert_key` from the KQL `alert_events` table into `gold.fact_alerts`.

## Orchestration

Pipeline `pl_fleet_batch`:

```
bronze_load → silver_setup → Wait (120 s) → batch_job_1 → If Condition (Gate 1)
            → Wait (120 s) → alerts_load → batch_job_2
```

- All retries are idempotent (Bronze retry guard, watermark as last step, MERGE for alerts, overwrite for Gold).
- The Wait activities work around Spark capacity failures at session start.
- If Gate 1 fails, Silver is already written and the pipeline stops before alerts and Gold.
- Schedule: daily at 12:00 PM and 12:00 AM (UTC+02:00 Cairo), from 2026-10-28 to 2027-08-28.
- The schedule identity needs Contributor on the workspace and Read on `eh_fleet`.

## Getting started

### Prerequisites

```bash
pip install pyyaml numpy python-dotenv confluent-kafka
```

Create a `.env` file with three keys per signal (GPS, ENGINE, FUEL, TEMPERATURE, DEVICE_STATUS), taken from the Event Hub tab of each custom endpoint:

```
KAFKA_GPS_BOOTSTRAP=<namespace>.servicebus.windows.net:9093
KAFKA_GPS_TOPIC=<topic>
KAFKA_GPS_CONN_STR=<connection string, one line>
```

Never commit `.env`. Regenerate any key that has been exposed. `--sink jsonl` and `--sink none` need no Kafka and no `.env`.

### Local run (smoke, no Fabric)

```bash
python generate_reference_data.py
python generate_stream.py --mode clean --sink jsonl --fast --out evidence_clean
python verify_stream.py evidence_clean reference
python generate_stream.py --sink jsonl --fast
python verify_stream.py evidence reference
python audit_phase1.py --root .
```

### Run into Fabric

1. Upload `reference/*` to `Files/reference` (delete old files first).
2. Clear the five raw KQL tables with `.clear table <name> data`.
3. Run `python generate_stream.py` (paced by `A`; lower it with `--accel` if lag grows).
4. Reconcile the raw table counts with `run_summary.json`. Do not run the generator twice against the same tables.
5. Upload `run_manifest.json`, `injected_events_log.csv`, `trip_outcomes.csv` and `run_summary.json` to `Files/evidence`, and bulk files to `Files/landing/offline/` when offline windows are on.

### Final evidence run

The 5,000-vehicle reference data is generated last, immediately before the final evidence run. Until then `config.yaml` stays on `mode: "smoke"`. Two rules apply:

1. The `mode` in `config.yaml` must match the reference data currently in Fabric.
2. If `simulation.days` changes, regenerate and re-upload the reference data before the next run.

## Verification (smoke run)

| Signal | Bronze | Silver | DLQ | Duplicates removed |
|---|---|---|---|---|
| gps | 30,895 | 30,472 | 275 | 148 |
| engine | 30,926 | 30,512 | 271 | 143 |
| fuel | 5,176 | 5,105 | 30 | 41 |
| temperature | 7,197 | 7,105 | 51 | 41 |
| **Total** | **74,194** | **73,194** | **627** | **373** |

For every signal, Bronze = Silver + DLQ + duplicates removed. The overall invalid share is 0.845 percent. Re-running the generator with the same seed produces identical output.

## Known limitations

- The Eventstreams must stay running; the pipeline reads only what KQL has already collected.
- `vehicle_key` in `gold.fact_alerts` is NULL on the first run because `alerts_load` runs before `batch_job_2`.
- `lateness_sec` depends on the `run_manifest.json` of a single run.
- Interactive Spark sessions compete with scheduled runs for capacity.
- `alerts_load` reads the whole `alert_events` table each run (no watermark).
- The evidence profile has 12 routes; the project guide mentions about 40.
- The final acceleration factor is not decided, and the throughput test (R = 500 messages/s) should be repeated on the current setup.
- P1-12 offline windows: local verification pending.

## Security

Connection strings and keys belong only in `.env`, which must be listed in `.gitignore`. Do not paste keys into notebooks or chats.