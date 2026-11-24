"""Record silent 1080p demo clips of the running stack for the presentation video.

Each clip follows one step of the demo slide, so you can narrate over it.
Clips are saved as MP4 in docs/presentation/clips/ (01_kafka_ui.mp4, ...).

Requires:  pip install playwright imageio-ffmpeg   (uses the installed Microsoft Edge)
           PLAYWRIGHT_BROWSERS_PATH=.venv/pw-browsers python -m playwright install ffmpeg
Usage:     python scripts/record_demo.py              # every clip except the failure test
           python scripts/record_demo.py failure      # only clips whose name contains "failure"
           python scripts/record_demo.py --all        # include the failure test (stops the
                                                      # telemetry producer for ~4 minutes)
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Callable
from urllib.parse import quote

import imageio_ffmpeg

# Playwright's own ffmpeg (needed for video) lives in the project's ignored .venv folder,
# installed once with:  PLAYWRIGHT_BROWSERS_PATH=.venv/pw-browsers playwright install ffmpeg
os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(Path(__file__).resolve().parents[1] / ".venv" / "pw-browsers"))

from playwright.sync_api import Page, sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parent))
from take_screenshots import E, ROOT  # noqa: E402  (shared .env reader)

OUT = ROOT / "docs" / "presentation" / "clips"
SIZE = {"width": 1920, "height": 1080}


def url(port_key: str, default: str, path: str = "") -> str:
    return f"http://localhost:{E.get(port_key, default)}{path}"


def slow_scroll(page: Page, total: int, steps: int = 6, pause_ms: int = 1200) -> None:
    """Scroll down gradually so the viewer can follow."""
    for _ in range(steps):
        page.mouse.wheel(0, total // steps)
        page.wait_for_timeout(pause_ms)


def kafka_ui(page: Page) -> None:
    page.goto(url("KAFKA_UI_PORT", "8085", "/ui/clusters/fleetpulse/all-topics"))
    page.wait_for_timeout(4000)
    page.goto(url("KAFKA_UI_PORT", "8085", "/ui/clusters/fleetpulse/all-topics/fleet.telemetry"))
    page.wait_for_timeout(6000)
    page.goto(url("KAFKA_UI_PORT", "8085", "/ui/clusters/fleetpulse/all-topics/fleet.telemetry/messages"))
    page.wait_for_timeout(8000)


def spark_ui(page: Page) -> None:
    page.goto(url("SPARK_UI_PORT", "4540", "/StreamingQuery/"))
    page.wait_for_timeout(6000)
    link = page.locator("a[href*='StreamingQuery/statistics']").first
    if link.count():
        link.click()
        page.wait_for_timeout(4000)
        slow_scroll(page, 1600)


def grafana_login(page: Page) -> str:
    base = url("GRAFANA_PORT", "3000")
    page.goto(f"{base}/login")
    page.fill("input[name=user]", E["GRAFANA_ADMIN_USER"])
    page.fill("input[name=password]", E["GRAFANA_ADMIN_PASSWORD"])
    page.click("button[type=submit]")
    page.wait_for_timeout(2500)
    return base


def grafana(page: Page) -> None:
    base = grafana_login(page)
    page.goto(f"{base}/d/fleet-operations?orgId=1&kiosk&from=now-30m&to=now&refresh=10s")
    page.wait_for_timeout(9000)
    slow_scroll(page, 1400)
    page.goto(f"{base}/d/pipeline-health?orgId=1&kiosk&from=now-30m&to=now&refresh=10s")
    page.wait_for_timeout(9000)
    slow_scroll(page, 1400)


def airflow(page: Page) -> None:
    base = url("AIRFLOW_WEB_PORT", "8080")
    page.goto(f"{base}/login/")
    page.fill("#username", E["AIRFLOW_ADMIN_USER"])
    page.fill("#password", E["AIRFLOW_ADMIN_PASSWORD"])
    page.click("input[type=submit], button[type=submit]")
    page.wait_for_timeout(2500)
    page.goto(f"{base}/dags/daily_fleet_reconciliation/grid")
    page.wait_for_timeout(6000)
    runs = page.request.get(  # latest successful run: the newest one is usually still waiting
        f"{base}/api/v1/dags/daily_fleet_reconciliation/dagRuns?state=success&order_by=-execution_date&limit=1"
    ).json().get("dag_runs", [])
    run_param = f"&dag_run_id={quote(runs[0]['dag_run_id'])}" if runs else ""
    page.goto(f"{base}/dags/daily_fleet_reconciliation/grid?tab=graph{run_param}")
    page.wait_for_timeout(8000)


def report_and_api(page: Page) -> None:
    reports = sorted((ROOT / "reports").glob("profitability_*.html"))
    if reports:
        page.goto(reports[-1].as_uri())
        page.wait_for_timeout(5000)
        slow_scroll(page, 1500, steps=5)
    page.goto(url("API_PORT", "8000", "/docs"))
    op = "#operations-merged_view-vehicle_status_vehicles__vehicle_id__status_get"
    page.wait_for_selector(op)
    block = page.locator(op)
    block.scroll_into_view_if_needed()
    page.wait_for_timeout(2000)
    block.locator(".opblock-summary").click()
    page.wait_for_timeout(1200)
    block.locator("button.try-out__btn").click()
    page.wait_for_timeout(800)
    block.locator("input[placeholder='vehicle_id']").type("V004", delay=150)
    page.wait_for_timeout(800)
    block.locator("button.execute").click()
    block.locator(".responses-wrapper .live-responses-table").wait_for()
    page.wait_for_timeout(1500)
    block.locator(".highlight-code").first.scroll_into_view_if_needed()
    page.wait_for_timeout(1500)
    slow_scroll(page, 900, steps=4)


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, capture_output=True)


def _firing() -> bool:
    """True while Prometheus reports NoDataReceived as firing."""
    with urllib.request.urlopen(url("PROMETHEUS_PORT", "9090", "/api/v1/alerts"), timeout=10) as r:
        alerts = json.load(r)["data"]["alerts"]
    return any(a["labels"]["alertname"] == "NoDataReceived" and a["state"] == "firing" for a in alerts)


def failure_before(page: Page) -> None:
    """Alerts all quiet, health 'healthy'."""
    page.goto(url("PROMETHEUS_PORT", "9090", "/alerts"))
    page.wait_for_timeout(5000)
    page.goto(url("API_PORT", "8000", "/health"))
    page.wait_for_timeout(5000)


def failure_after(page: Page) -> None:
    """Shown once NoDataReceived fires, after the producer was stopped off-camera."""
    page.goto(url("PROMETHEUS_PORT", "9090", "/alerts"))
    page.wait_for_timeout(4000)
    page.locator("text=NoDataReceived").first.click()
    page.wait_for_timeout(5000)
    page.goto(url("API_PORT", "8000", "/health"))
    page.wait_for_timeout(5000)


CLIPS: list[tuple[str, Callable[[Page], None]]] = [
    ("01_kafka_ui", kafka_ui),
    ("02_spark_ui", spark_ui),
    ("03_grafana", grafana),
    ("04_airflow", airflow),
    ("05_report_and_api", report_and_api),
    ("06a_failure_before", failure_before),
    ("06b_failure_after", failure_after),
]


def to_mp4(webm: Path, mp4: Path) -> None:
    ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    subprocess.run([ffmpeg, "-y", "-loglevel", "error", "-i", str(webm), "-c:v", "libx264",
                    "-pix_fmt", "yuv420p", "-crf", "20", "-r", "30", str(mp4)], check=True)


def record(browser, name: str, fn: Callable[[Page], None]) -> None:
    tmp = OUT / "_tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    context = browser.new_context(viewport=SIZE, record_video_dir=str(tmp), record_video_size=SIZE)
    page = context.new_page()
    try:
        fn(page)
    finally:
        video = page.video
        context.close()  # finalises the .webm
    to_mp4(Path(video.path()), OUT / f"{name}.mp4")
    shutil.rmtree(tmp, ignore_errors=True)
    print(f"saved {name}.mp4")


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    wanted = args[0] if args else ""
    include_failure = "--all" in sys.argv or "failure" in wanted
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        # A real (headed) window parked off-screen: headless video often misses repaints.
        browser = p.chromium.launch(channel="msedge", headless=False,
                                    args=["--window-position=-2600,0", "--window-size=1940,1150"])
        for name, fn in CLIPS:
            if wanted not in name or ("failure" in name and not include_failure):
                continue
            if name == "06b_failure_after":
                compose("stop", "telemetry-producer")
                print("telemetry producer stopped; waiting for NoDataReceived to fire ...")
                deadline = time.time() + 600
                while not _firing() and time.time() < deadline:
                    time.sleep(10)
            try:
                record(browser, name, fn)
            except Exception as exc:
                print(f"FAILED {name}: {exc}")
            finally:
                if name == "06b_failure_after":
                    compose("start", "telemetry-producer")
                    print("telemetry producer started again")
        browser.close()


if __name__ == "__main__":
    main()
