# Report Notes — FleetPulse Lambda Pipeline

Author: P.S. Dilshan (EG/2020/3898). These notes are updated phase by phase and feed into the written report.

---

## 1. Phase log

### Phase 1 — Skeleton, Kafka, Postgres, shared helpers
- Kafka 3.8.0 runs in **KRaft mode** (one node is both the broker and the controller), so ZooKeeper isn't needed and RAM use is lower.
- The `kafka-init` one-shot container creates the topics explicitly. Topic auto-creation is disabled, so a typo can't silently create a wrong topic.
  - `fleet.telemetry`: 3 partitions, keyed by `vehicle_id`. A vehicle's events always go to the same partition, so they stay in order, and up to 3 consumers can read in parallel.
  - `fleet.telemetry.dlq`: 1 partition, for rejected records (low volume).
- One Postgres 16 server has two databases: `fleet` for business data and `airflow` for Airflow metadata. They're separated so that resetting one never affects the other.
- `common/sim_clock.py`: 1 sim day = `SIM_DAY_SECONDS` (600 s), which is a 144x speedup. A shared anchor file (`data/sim_anchor.txt`) makes every container agree on the current sim day.
- `common/logging_config.py`: every service writes one JSON object per log line, with the fields `timestamp, level, service, stage, event, message` plus context fields.

### Phase 2 — Simulated sources
- **Telemetry producer** (`producers/telemetry_producer.py` + `fleet_simulator.py`):
  - Simulates 30 vehicles and 25 drivers across 5 Colombo zones. Each vehicle moves through the states idle → enroute → on_trip → idle.
  - Durations are picked in simulated minutes. Rush hours (07–09 and 17–19) have short idle waits and 20% surge pricing. Nights have long waits.
  - When a trip finishes, the producer emits one event with `fare > 0`. All other events have `fare = 0`. So trips = `count(fare > 0)` and earnings = `sum(fare)`.
  - Each event carries two clocks: the real `timestamp`, used for Spark windows and watermarks, and `sim_time`/`sim_date`, used for lake partitions and the daily batch.
  - Lazy vehicles (V007, V013, V021) idle for 150–300 sim-minutes, which is always over the 90-minute alert threshold. Normal vehicles are capped at 75 sim-minutes, so only the lazy ones trigger alerts.
  - Injected faults:
    - about 2% extra corrupted copies (null `vehicle_id`, impossible lat/lon, negative fare)
    - about 1% events sent twice with the same `event_id`, to prove de-duplication

    The corrupted records are sent *in addition* to the good events, so the clean stream stays complete.
  - Robustness:
    - waits for Kafka with exponential backoff (1, 2, 4 … up to 30 s)
    - `acks=all` and `enable.idempotence=true`
    - a delivery callback counts successes and failures
    - SIGINT/SIGTERM handlers flush the buffer before exit (checked: `undelivered: 0` after `docker stop`)
- **Expense producer** (`producers/expense_batch_producer.py`):
  - At the start of each sim day D, it writes `expenses_<D-1>.csv`, i.e. yesterday's costs.
  - Values are seeded by (vehicle, date), so rerunning gives identical files.
  - High-cost vehicles (V004, V011, V019, V027) have about 2x the fuel cost per km. Their maintenance grows 20% per sim day, so their profit keeps falling and they get the "trending unprofitable" flag.
  - Dirty data: 2 exact duplicate rows and 1 blank `fuel_cost`.
  - The file is written to `.expenses_<date>.csv.tmp` and then renamed with `os.replace`. The rename is atomic, so the Airflow FileSensor can never read a half-written file.
- **Verified:**
  - All 3 partitions receive data.
  - Every non-null key maps to exactly one partition, so per-vehicle order is kept. Null-key bad records are spread across partitions.
  - `events_failed_total = 0`.

### Phase 3 — Speed layer (Spark Structured Streaming)
- **Runtime:** one Spark 3.5.3 container in `local[2]` mode with 1 GB of driver memory and `spark.sql.shuffle.partitions=3`. The default of 200 would create hundreds of empty tasks per micro-batch.
- **Kafka connector:** the jars are baked into the image at build time.
- **Shared input:** Kafka → `from_json` with an explicit schema → an `error_reason` column. Native Spark expressions are used instead of UDFs, so the work stays in the JVM.
  - Null checks run first, because in SQL `NULL < 0` is neither true nor false and would let a bad record through.
- **Query `dlq`:** sends invalid records to `fleet.telemetry.dlq` as JSON containing `error_reason`, `raw_value`, the original partition and offset, and `rejected_at`.
- **Query `clean_events`:**
  - Applies a watermark of 2 minutes, then `dropDuplicatesWithinWatermark(event_id)`.
  - Writes through `foreachBatch`:
    1. Appends to the Parquet lake, partitioned by `sim_date`. This is the master dataset.
    2. Merges with stored state to compute each vehicle's latest status and `idle_since` (`merge_vehicle_status`, pure Python).
    3. Upserts `rt_vehicle_status`.
    4. Resolves alerts for vehicles that moved again, then opens alerts where `event_time - idle_since > IDLE_ALERT_SIM_MINUTES` (converted to real seconds), and logs a WARNING for each new alert.
- **Query `zone_metrics`:**
  - Groups by `window(event_time, 1 minute)` and zone, in `update` output mode, then upserts into `rt_zone_metrics`.
  - `trips_per_hour` is per *simulated* hour (1 real minute = 2.4 sim hours).
- **Fault tolerance:**
  - Each query has its own checkpoint folder (Kafka offsets plus state). If a micro-batch fails, the query stops and the container exits. Docker restarts it, and Spark resumes from the last committed batch.
  - `foreachBatch` is at-least-once, so a replayed batch must be harmless:
    - Postgres writes are upserts, so a replay overwrites rows instead of duplicating them.
    - The lake may get a duplicate file, but the batch layer de-duplicates by `event_id`.
- **Why the watermark matters:**
  - Without it, the window and dedup state would grow forever.
  - With 2 minutes, events up to 2 minutes late are still counted. Older ones are dropped. This is a trade-off between completeness and latency.

## 2. Final table schemas (from `sql/init.sql`)

| Table | Layer | Primary key | Purpose |
|---|---|---|---|
| `rt_zone_metrics` | Speed | (window_start, zone) | 1-min window KPIs per zone: active vehicles, idle ratio, trips/hour, earnings |
| `rt_vehicle_status` | Speed | vehicle_id | Latest state of each vehicle |
| `idle_alerts` | Speed | alert_id (+ unique open alert per vehicle) | Business alert: idle too long |
| `vehicle_expenses` | Batch | (vehicle_id, expense_date) | Cleaned daily expense CSV rows |
| `daily_vehicle_earnings` | Batch | (vehicle_id, sim_date) | Earnings recomputed from the Parquet lake |
| `daily_vehicle_profitability` | Batch | (vehicle_id, sim_date) | Profit, margin, cost/km, unprofitable + trending flags |
| `batch_run_log` | Batch | run_id | Audit trail of batch task runs / DQ checks |

Every table has a primary key, so every write can use `INSERT ... ON CONFLICT DO UPDATE`. That makes reruns idempotent: they don't create duplicates.

## 3. Assumptions & simplifications (running list)
- There's a single Kafka broker, so the replication factor is 1 (no fault tolerance). In production you'd use 3+ brokers with RF=3 and `min.insync.replicas=2`.
- Postgres stores both the speed views and the batch views. At scale, the speed views would move to a low-latency store (e.g. Cassandra or Druid) and the batch views to a warehouse.
- The simulated clock is anchored to a file on a shared bind mount. Across multiple machines, you'd use a config service, or put the sim time in each event.
- The anchor survives restarts, so the simulated timeline continues. After a long break (days), the sim date jumps far ahead. `make reset` deletes the anchor to start again at 2025-01-01.
- Expense distances are generated on their own and are not derived from the telemetry. A real fleet would get them from odometer or telematics data.
- A few drivers share two vehicles (30 vehicles, 25 drivers), modelled as shift work.

## 4. Screenshot checklist (filled in as phases complete)
- [ ] Kafka UI: topic `fleet.telemetry` showing 3 partitions (http://localhost:8085)
- [ ] `docker compose ps` showing all services healthy
- [ ] Kafka UI → `fleet.telemetry` → Messages: JSON events with key = vehicle_id and partition numbers
- [ ] Producer JSON logs (`docker compose logs telemetry-producer`) showing `producer_progress`
- [ ] A landing CSV file (`data/landing/expenses_*.csv`), showing the duplicate rows and the blank value
