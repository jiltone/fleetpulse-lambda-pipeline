"""Capture every report figure from the running stack into docs/report/figures/.

Requires:  pip install playwright   (uses the Microsoft Edge already installed on Windows,
           so no separate browser download is needed)
Usage:     python scripts/take_screenshots.py            # all figures
           python scripts/take_screenshots.py grafana    # only names containing "grafana"
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable
from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
FIGURES = ROOT / "docs" / "report" / "figures"


def env() -> dict[str, str]:
    """Read .env (credentials and ports) without extra libraries."""
    values = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


E = env()


def kafka_ui(page: Page) -> None:
    """Kafka UI: the telemetry topic overview with its 3 partitions."""
    page.goto(f"http://localhost:{E.get('KAFKA_UI_PORT', '8085')}/ui/clusters/fleetpulse/all-topics/fleet.telemetry")
    page.wait_for_timeout(4000)


def spark_ui(page: Page) -> None:
    """Spark UI: Structured Streaming tab listing the three queries."""
    page.goto(f"http://localhost:{E.get('SPARK_UI_PORT', '4540')}/StreamingQuery/")
    page.wait_for_timeout(2000)


def airflow_dag(page: Page) -> None:
    """Airflow: graph view of the DAG's latest SUCCESSFUL run (the newest run is
    usually still waiting for tomorrow's file, so its tasks have no colour yet)."""
    base = f"http://localhost:{E.get('AIRFLOW_WEB_PORT', '8080')}"
    page.goto(f"{base}/login/")
    page.fill("#username", E["AIRFLOW_ADMIN_USER"])
    page.fill("#password", E["AIRFLOW_ADMIN_PASSWORD"])
    page.click("input[type=submit], button[type=submit]")
    page.wait_for_timeout(2000)
    runs = page.request.get(
        f"{base}/api/v1/dags/daily_fleet_reconciliation/dagRuns"
        "?state=success&order_by=-execution_date&limit=1"
    ).json().get("dag_runs", [])
    run_param = f"&dag_run_id={quote(runs[0]['dag_run_id'])}" if runs else ""
    page.goto(f"{base}/dags/daily_fleet_reconciliation/grid?tab=graph{run_param}")
    page.wait_for_timeout(6000)


def html_report(page: Page) -> None:
    """The newest generated HTML profitability report."""
    reports = sorted((ROOT / "reports").glob("profitability_*.html"))
    if not reports:
        raise RuntimeError("no HTML report generated yet")
    page.goto(reports[-1].as_uri())
    page.wait_for_timeout(1000)


def swagger(page: Page) -> None:
    """Swagger UI: execute GET /vehicles/{vehicle_id}/status for a high-cost vehicle."""
    page.goto(f"http://localhost:{E.get('API_PORT', '8000')}/docs")
    page.wait_for_selector("#operations-merged_view-vehicle_status_vehicles__vehicle_id__status_get")
    block = page.locator("#operations-merged_view-vehicle_status_vehicles__vehicle_id__status_get")
    block.locator(".opblock-summary").click()
    block.locator("button.try-out__btn").click()
    block.locator("input[placeholder='vehicle_id']").fill("V004")
    block.locator("button.execute").click()
    block.locator(".responses-wrapper .live-responses-table").wait_for()
    page.wait_for_timeout(1000)
    block.locator(".highlight-code").first.scroll_into_view_if_needed()


def grafana(uid: str) -> Callable[[Page], None]:
    """Grafana dashboard in kiosk mode (no side menus)."""

    def capture(page: Page) -> None:
        base = f"http://localhost:{E.get('GRAFANA_PORT', '3000')}"
        page.goto(f"{base}/login")
        page.fill("input[name=user]", E["GRAFANA_ADMIN_USER"])
        page.fill("input[name=password]", E["GRAFANA_ADMIN_PASSWORD"])
        page.click("button[type=submit]")
        page.wait_for_timeout(2000)
        page.goto(f"{base}/d/{uid}?orgId=1&kiosk&from=now-30m&to=now")
        page.wait_for_timeout(9000)  # let every panel query and render

    return capture


def prometheus_alerts(page: Page) -> None:
    """Prometheus alerts page (take it while an alert is firing)."""
    page.goto(f"http://localhost:{E.get('PROMETHEUS_PORT', '9090')}/alerts?search=")
    page.wait_for_timeout(2500)


# figure file -> (capture function, viewport width, viewport height, full page?)
SHOTS: dict[str, tuple[Callable[[Page], None], int, int, bool]] = {
    "kafka_ui_partitions.png": (kafka_ui, 1440, 900, False),
    "spark_ui.png": (spark_ui, 1440, 900, True),
    "airflow_dag.png": (airflow_dag, 1440, 860, False),
    "html_report.png": (html_report, 1100, 1300, False),
    "swagger_vehicle_status.png": (swagger, 1300, 1000, False),
    "grafana_fleet_ops.png": (grafana("fleet-operations"), 1600, 1700, True),
    "grafana_pipeline_health.png": (grafana("pipeline-health"), 1600, 1700, True),
    "alert_firing.png": (prometheus_alerts, 1440, 1000, True),
}


def main() -> None:
    """Capture the selected figures; one failure doesn't stop the others."""
    wanted = sys.argv[1] if len(sys.argv) > 1 else ""
    FIGURES.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        for name, (capture, width, height, full) in SHOTS.items():
            if wanted not in name:
                continue
            page = browser.new_page(viewport={"width": width, "height": height}, device_scale_factor=1.5)
            try:
                capture(page)
                page.screenshot(path=str(FIGURES / name), full_page=full)
                print(f"saved {name}")
            except Exception as exc:
                print(f"FAILED {name}: {exc}")
            finally:
                page.close()
        browser.close()


if __name__ == "__main__":
    main()
