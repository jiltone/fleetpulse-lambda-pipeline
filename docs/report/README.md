# Final report (LaTeX)

## Compile on Overleaf
1. Zip this folder (`docs/report/`), including `figures/`.
2. Go to https://www.overleaf.com and choose **New Project → Upload Project**, then select the zip.
3. Open `main.tex` and press **Recompile**. The default pdfLaTeX compiler works.

## Adding the university logo
Save the University of Ruhuna logo as `figures/ruhuna_logo.png`. The title page shows it automatically.

## Adding screenshots
Each screenshot has a fixed file name. Save the image under that name in `figures/` and it replaces the grey placeholder box automatically:

| File | What to capture |
|---|---|
| `kafka_ui_partitions.png` | Kafka UI (http://localhost:8085) → Topics → `fleet.telemetry` → partitions/messages |
| `spark_ui.png` | Spark UI (http://localhost:4040) → Structured Streaming tab |
| `airflow_dag.png` | Airflow → `daily_fleet_reconciliation` → Graph view, all tasks green |
| `html_report.png` | `reports/profitability_<date>.html` opened in a browser |
| `swagger_vehicle_status.png` | http://localhost:8000/docs → `GET /vehicles/{vehicle_id}/status` → Execute |
| `grafana_fleet_ops.png` | Grafana → Fleet Operations dashboard |
| `grafana_pipeline_health.png` | Grafana → Pipeline Health dashboard |
| `alert_firing.png` | Prometheus Alerts page (or Grafana) with an alert firing after stopping the producer |

## Before you submit
Search `main.tex` for `\tocomplete` and `\tbd`. Every one must be replaced with real results. The orange boxes mark evidence that can only come from the finished, running system.
