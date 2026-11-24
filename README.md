# FleetPulse: Lambda-Architecture Fleet Monitoring

EC8202 Big Data Analytics mini project (University of Ruhuna), by P.S. Dilshan (EG/2020/3898).

FleetPulse monitors a simulated ride-hailing fleet of 30 vehicles in five Colombo zones. It answers:

> What is fleet utilisation and earnings by area and time of day right now, and which vehicles are
> becoming unprofitable once yesterday's fuel and maintenance costs are factored in?

| Layer | Technology | Output |
|---|---|---|
| Ingestion | Kafka (KRaft, 3 partitions, dead-letter topic) | `fleet.telemetry`, `fleet.telemetry.dlq` |
| Speed | Spark Structured Streaming | live zone KPIs, vehicle status, idle alerts, Parquet lake |
| Batch | Airflow (one run per simulated day) | exact daily profitability, trend flags, HTML/CSV report |
| Serving | PostgreSQL + FastAPI | REST API, including a merged per-vehicle view |
| Observability | Prometheus, Pushgateway, Grafana, JSON logs | 9 alert rules, 2 dashboards, `/health` |

One simulated day lasts 10 real minutes (`SIM_DAY_SECONDS=600`), so a few days of data build up in
half an hour.

## Run it

Requirements: Docker Desktop with about 6 GB of memory for its VM and about 8 GB of free disk.

```bash
cp .env.example .env          # then change the passwords
docker compose up -d --build  # first build takes several minutes
docker compose ps             # all services "running"; kafka-init and airflow-init "exited (0)"
```

| Service | URL | Login |
|---|---|---|
| API (Swagger) | http://localhost:8000/docs | none |
| API health | http://localhost:8000/health | none |
| Airflow | http://localhost:8080 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Grafana | http://localhost:3000 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` |
| Prometheus | http://localhost:9090 | none |
| Kafka UI | http://localhost:8085 | none |
| Spark UI | http://localhost:4540 | none |
| PostgreSQL | `localhost:5433`, database `fleet` | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

What to expect:
- After about a minute, live zone metrics appear in Grafana's **Fleet Operations** dashboard.
- The lazy vehicles (V007, V013, V021) raise idle alerts.
- About 10 minutes after the start, the first expense file lands and the Airflow DAG
  `daily_fleet_reconciliation` produces the first report in `reports/`.
- The high-cost vehicles (V004, V011, V019, V027) are flagged unprofitable every day, and as
  trending from the third simulated day.

**Failure test:** `docker compose stop telemetry-producer`. Within about 3 to 5 minutes, Prometheus fires
`NoDataReceived` and `/health` reports `degraded`. Then run `docker compose start telemetry-producer`.

**Start over:** `docker compose down -v`, then delete the generated files in `data/` and `reports/`
(keep the `.gitkeep` files). This also resets the simulated clock to `SIM_START_DATE`.

## Tests

```bash
python -m venv .venv
.venv/Scripts/pip install -r api/requirements.txt pytest httpx   # Linux/macOS: .venv/bin/pip
.venv/Scripts/python -m pytest -q
```

On Windows with Python 3.13, install `psycopg2-binary==2.9.10`, because 2.9.9 has no wheel for it.

## Repository layout

| Path | Contents |
|---|---|
| `producers/` | telemetry (Kafka) and daily expense CSV simulators |
| `streaming/` | Spark speed layer: validation, DLQ, windows, idle alerts, Postgres sinks |
| `airflow/dags/`, `batch/` | batch layer DAG and its pandas tasks |
| `api/` | FastAPI serving layer and health checks |
| `common/` | shared fleet definition, simulated clock, config, logging |
| `sql/init.sql` | database schema |
| `monitoring/` | Prometheus config and alert rules, Grafana provisioning and dashboards |
| `tests/` | pytest suite |
| `scripts/` | `take_screenshots.py` (UI screenshots), `record_demo.py` (demo video clips) |
