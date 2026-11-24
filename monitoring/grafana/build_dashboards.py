"""Generate the two Grafana dashboards as JSON files (run once after editing this file).

    python monitoring/grafana/build_dashboards.py

Writing dashboards as code keeps every panel's query visible in one place
instead of buried in a 2,000-line JSON export. Grafana loads the generated
files from monitoring/grafana/dashboards/ at start-up (see provisioning/).

  * Fleet Operations - business view, queries PostgreSQL (speed + batch tables)
  * Pipeline Health  - operations view, queries Prometheus (metrics + alerts)
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT_DIR = Path(__file__).parent / "dashboards"
PG = {"type": "grafana-postgresql-datasource", "uid": "fleet-postgres"}
PROM = {"type": "prometheus", "uid": "prometheus"}

# One fixed colour per zone (validated categorical palette, dark-theme steps):
# a zone keeps its colour in every panel, whatever else is shown.
ZONE_COLORS = {
    "Fort": "#3987e5", "Kollupitiya": "#d95926", "Bambalapitiya": "#199e70",
    "Dehiwala": "#c98500", "Nugegoda": "#d55181",
}
# Status colours are reserved for state (good / warning / critical), never for series.
GOOD, WARNING, CRITICAL = "#0ca30c", "#fab219", "#d03b3b"
LOSS, PROFIT = "#e66767", "#3987e5"   # diverging pair for profit vs loss

LATEST_WINDOWS = "(SELECT DISTINCT ON (zone) * FROM rt_zone_metrics ORDER BY zone, window_start DESC) z"
LATEST_DAY = "(SELECT MAX(sim_date) FROM daily_vehicle_profitability)"


class Layout:
    """Places panels left-to-right on Grafana's 24-column grid."""

    def __init__(self) -> None:
        self.x, self.y, self.row_h, self.next_id = 0, 0, 0, 1

    def pos(self, w: int, h: int) -> dict[str, int]:
        """Return a gridPos for a panel of width w and height h, wrapping rows."""
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        grid = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return grid

    def new_id(self) -> int:
        """Unique panel id."""
        self.next_id += 1
        return self.next_id


def thresholds(*steps: tuple[float | None, str]) -> dict:
    """Threshold steps: (value, colour); the first value must be None (the base)."""
    return {"mode": "absolute", "steps": [{"value": v, "color": c} for v, c in steps]}


def zone_overrides() -> list[dict]:
    """Pin each zone's series to its fixed colour."""
    return [
        {"matcher": {"id": "byName", "options": zone},
         "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]}
        for zone, color in ZONE_COLORS.items()
    ]


def pg_target(sql: str, fmt: str = "table") -> dict:
    """A raw-SQL query against the fleet database."""
    return {"refId": "A", "datasource": PG, "editorMode": "code", "rawQuery": True, "rawSql": sql, "format": fmt}


def prom_target(expr: str, legend: str = "", ref: str = "A", instant: bool = False) -> dict:
    """A PromQL query."""
    return {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": legend,
            "instant": instant, "range": not instant}


def stat(layout: Layout, title: str, target: dict, ds: dict, unit: str = "none",
         steps: dict | None = None, w: int = 4, desc: str = "", decimals: int | None = None) -> dict:
    """Single big number. Colour only from thresholds (status), with the value always shown."""
    defaults: dict[str, Any] = {"unit": unit, "color": {"mode": "thresholds"},
                                "thresholds": steps or thresholds((None, "text"))}
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "id": layout.new_id(), "type": "stat", "title": title, "description": desc, "datasource": ds,
        "gridPos": layout.pos(w, 4), "targets": [target],
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "center"},
    }


def timeseries(layout: Layout, title: str, targets: list[dict], ds: dict, unit: str = "none",
               w: int = 12, h: int = 8, overrides: list | None = None, desc: str = "",
               min_zero: bool = True) -> dict:
    """Line chart: thin 2px lines, light fill off, legend at the bottom with last value."""
    defaults: dict[str, Any] = {
        "unit": unit, "color": {"mode": "palette-classic"},
        "custom": {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never", "spanNulls": True},
    }
    if min_zero:
        defaults["min"] = 0
    return {
        "id": layout.new_id(), "type": "timeseries", "title": title, "description": desc, "datasource": ds,
        "gridPos": layout.pos(w, h), "targets": targets,
        "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "calcs": ["lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def table(layout: Layout, title: str, target: dict, ds: dict, w: int = 12, h: int = 9,
          overrides: list | None = None, desc: str = "") -> dict:
    """Plain table (also the accessible 'table view' of the charts above)."""
    return {
        "id": layout.new_id(), "type": "table", "title": title, "description": desc, "datasource": ds,
        "gridPos": layout.pos(w, h), "targets": [target],
        "fieldConfig": {"defaults": {"custom": {"align": "auto"}}, "overrides": overrides or []},
        "options": {"showHeader": True, "cellHeight": "sm"},
    }


def dashboard(uid: str, title: str, panels: list[dict], time_from: str, desc: str) -> dict:
    """Wrap panels into a dashboard that refreshes every 10 s."""
    return {
        "uid": uid, "title": title, "description": desc, "tags": ["fleetpulse"], "timezone": "browser",
        "schemaVersion": 39, "version": 1, "editable": True, "refresh": "10s",
        "time": {"from": time_from, "to": "now"}, "panels": panels,
        "templating": {"list": []}, "annotations": {"list": []},
    }


# =========================================================================== Fleet Operations
def fleet_operations() -> dict:
    """Business dashboard: what is the fleet doing now, and who is losing money?"""
    L = Layout()
    p = [
        stat(L, "Active vehicles", pg_target(f"SELECT SUM(active_vehicles) FROM {LATEST_WINDOWS}"), PG,
             desc="Distinct vehicles seen in the latest 1-minute window of each zone"),
        stat(L, "Fleet idle ratio", pg_target(
            f"SELECT SUM(idle_ratio * event_count) / NULLIF(SUM(event_count), 0) FROM {LATEST_WINDOWS}"), PG,
             unit="percentunit", decimals=1, desc="Share of pings that were idle (weighted by zone activity)"),
        stat(L, "Trips per sim-hour", pg_target(f"SELECT SUM(trips_per_hour) FROM {LATEST_WINDOWS}"), PG,
             decimals=1, desc="Completed trips in the latest window, scaled to one simulated hour"),
        stat(L, "Earnings, latest window (LKR)", pg_target(f"SELECT SUM(earnings) FROM {LATEST_WINDOWS}"), PG,
             unit="locale", decimals=0),
        stat(L, "Open idle alerts", pg_target("SELECT COUNT(*) FROM idle_alerts WHERE status = 'open'"), PG,
             steps=thresholds((None, GOOD), (1, WARNING)), desc="Vehicles idle longer than IDLE_ALERT_SIM_MINUTES"),
        stat(L, "Unprofitable vehicles (last batch day)", pg_target(
            f"SELECT COUNT(*) FILTER (WHERE is_unprofitable) FROM daily_vehicle_profitability "
            f"WHERE sim_date = {LATEST_DAY}"), PG,
             steps=thresholds((None, GOOD), (1, WARNING), (5, CRITICAL))),

        timeseries(L, "Earnings per 1-minute window, by zone (LKR)", [pg_target(
            "SELECT window_start AS time, zone AS metric, earnings AS value FROM rt_zone_metrics "
            "WHERE $__timeFilter(window_start) ORDER BY 1", fmt="time_series")], PG,
            unit="locale", overrides=zone_overrides(), desc="Speed layer: fares of trips completed in each window"),
        timeseries(L, "Idle ratio by zone", [pg_target(
            "SELECT window_start AS time, zone AS metric, idle_ratio AS value FROM rt_zone_metrics "
            "WHERE $__timeFilter(window_start) ORDER BY 1", fmt="time_series")], PG,
            unit="percentunit", overrides=zone_overrides()),
        timeseries(L, "Active vehicles by zone", [pg_target(
            "SELECT window_start AS time, zone AS metric, active_vehicles AS value FROM rt_zone_metrics "
            "WHERE $__timeFilter(window_start) ORDER BY 1", fmt="time_series")], PG,
            overrides=zone_overrides()),
        timeseries(L, "Trips per sim-hour by zone", [pg_target(
            "SELECT window_start AS time, zone AS metric, trips_per_hour AS value FROM rt_zone_metrics "
            "WHERE $__timeFilter(window_start) ORDER BY 1", fmt="time_series")], PG,
            overrides=zone_overrides()),

        table(L, "Open idle alerts", pg_target(
            "SELECT vehicle_id, driver_id, zone, ROUND(idle_sim_minutes) AS idle_sim_minutes, "
            "idle_since, detected_at FROM idle_alerts WHERE status = 'open' ORDER BY idle_sim_minutes DESC"), PG,
            desc="Business alert from the speed layer; resolves automatically when the vehicle moves"),
        table(L, "Latest profitability (batch layer)", pg_target(
            "SELECT vehicle_id, trips_completed AS trips, earnings, total_cost, profit, "
            "ROUND(profit_margin * 100, 1) AS margin_pct, "
            "CASE WHEN is_unprofitable THEN 'Unprofitable' ELSE 'OK' END AS status, "
            "CASE WHEN trending_unprofitable THEN 'Falling ' || consecutive_profit_drops || ' days' ELSE '' END AS trend "
            f"FROM daily_vehicle_profitability WHERE sim_date = {LATEST_DAY} "
            "ORDER BY is_unprofitable DESC, profit"), PG,
            overrides=[{"matcher": {"id": "byName", "options": "profit"}, "properties": [
                {"id": "custom.cellOptions", "value": {"type": "color-text"}},
                {"id": "thresholds", "value": thresholds((None, LOSS), (0, PROFIT))},
                {"id": "unit", "value": "locale"}]}],
            desc="Recomputed from the raw Parquet lake. Profit text: red = loss, blue = profit; "
                 "status column states it in words too."),
    ]
    bar = {
        "id": L.new_id(), "type": "barchart", "title": "Fleet earnings vs costs per simulated day (LKR)",
        "description": "Batch layer, last 10 simulated days. One axis: both measures are LKR.",
        "datasource": PG, "gridPos": L.pos(24, 8),
        "targets": [pg_target(
            "SELECT day, earnings, costs FROM (SELECT sim_date::text AS day, SUM(earnings) AS earnings, "
            "SUM(total_cost) AS costs, sim_date FROM daily_vehicle_profitability GROUP BY sim_date "
            "ORDER BY sim_date DESC LIMIT 10) t ORDER BY sim_date")],
        "fieldConfig": {"defaults": {"unit": "locale", "color": {"mode": "palette-classic"},
                                     "custom": {"lineWidth": 0, "fillOpacity": 90}},
                        "overrides": [
                            {"matcher": {"id": "byName", "options": "earnings"},
                             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": PROFIT}}]},
                            {"matcher": {"id": "byName", "options": "costs"},
                             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "#d95926"}}]}]},
        "options": {"xField": "day", "orientation": "vertical", "groupWidth": 0.6, "barWidth": 0.9,
                    "showValue": "never", "legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi"}},
    }
    p.append(bar)
    return dashboard("fleet-operations", "Fleet Operations", p, "now-30m",
                     "Live fleet KPIs (speed layer) and daily profitability (batch layer)")


# =========================================================================== Pipeline Health
DLQ_RATIO = ('sum(rate(stream_records_dlq_total[2m])) / (sum(rate(stream_records_dlq_total[2m])) + '
             'sum(rate(stream_records_processed_total{query="clean_events"}[2m])))')


def pipeline_health() -> dict:
    """Operations dashboard: is data flowing, is it clean, did the batch run, are alerts firing?"""
    L = Layout()
    up_mappings = [{"type": "value", "options": {
        "1": {"text": "UP", "color": GOOD}, "0": {"text": "DOWN", "color": CRITICAL}}}]
    services = {
        "id": L.new_id(), "type": "stat", "title": "Service status", "datasource": PROM,
        "description": "Prometheus scrape status of every instrumented service (text + colour)",
        "gridPos": L.pos(24, 4),
        "targets": [prom_target("up", "{{job}}", instant=True)],
        "fieldConfig": {"defaults": {"mappings": up_mappings, "color": {"mode": "thresholds"},
                                     "thresholds": thresholds((None, CRITICAL), (1, GOOD))}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "background", "graphMode": "none", "textMode": "value_and_name",
                    "orientation": "vertical"},
    }
    p = [
        services,
        stat(L, "Seconds since last event", prom_target("time() - last_event_timestamp", instant=True), PROM,
             unit="s", decimals=0, w=6, steps=thresholds((None, GOOD), (60, WARNING), (120, CRITICAL)),
             desc="Freshness of the speed layer; alert NoDataReceived fires above 120 s"),
        stat(L, "DLQ rate (2 min)", prom_target(DLQ_RATIO, instant=True), PROM, unit="percentunit", decimals=1,
             w=6, steps=thresholds((None, GOOD), (0.05, CRITICAL)),
             desc="Share of records rejected; ~2% is the injected baseline, alert above 5%"),
        stat(L, "Since last successful batch", prom_target("time() - batch_last_success_timestamp", instant=True),
             PROM, unit="s", decimals=0, w=6, steps=thresholds((None, GOOD), (900, WARNING), (1500, CRITICAL))),
        stat(L, "Alerts firing", prom_target('count(ALERTS{alertstate="firing"}) or vector(0)', instant=True),
             PROM, w=6, steps=thresholds((None, GOOD), (1, CRITICAL))),

        timeseries(L, "Throughput (records/s)", [
            prom_target("sum(rate(events_produced_total[1m]))", "produced to Kafka", "A"),
            prom_target('sum(rate(stream_records_processed_total{query="clean_events"}[1m]))', "processed (clean)", "B"),
            prom_target("sum(rate(stream_records_dlq_total[1m]))", "sent to DLQ", "C"),
        ], PROM, unit="reqps"),
        timeseries(L, "DLQ records by reason (/s)", [
            prom_target("sum by (reason) (rate(stream_records_dlq_total[2m]))", "{{reason}}")], PROM, unit="reqps"),
        timeseries(L, "Micro-batch duration p95 by query", [
            prom_target("histogram_quantile(0.95, sum by (le, query) (rate(stream_batch_duration_seconds_bucket[5m])))",
                        "{{query}}")], PROM, unit="s",
            desc="Must stay below the 10 s trigger interval or the speed layer falls behind"),
        timeseries(L, "Batch layer runs", [
            prom_target("batch_runs_total", "{{status}} runs", "A"),
            prom_target("batch_duration_seconds", "last run duration (s)", "B")], PROM),
        timeseries(L, "API requests (/s) by endpoint", [
            prom_target("sum by (path) (rate(api_requests_total[1m]))", "{{path}}")], PROM, unit="reqps"),
        timeseries(L, "API latency p95 by endpoint", [
            prom_target("histogram_quantile(0.95, sum by (le, path) (rate(api_request_duration_seconds_bucket[5m])))",
                        "{{path}}")], PROM, unit="s"),
    ]
    alerts = table(L, "Firing alerts", prom_target('ALERTS{alertstate="firing"}', instant=True), PROM, w=24, h=7,
                   desc="All Prometheus alert rules currently firing")
    alerts["targets"][0]["format"] = "table"
    alerts["transformations"] = [{"id": "organize", "options": {
        "excludeByName": {"Time": True, "Value": True, "__name__": True, "alertstate": True, "instance": True}}}]
    p.append(alerts)
    return dashboard("pipeline-health", "Pipeline Health", p, "now-30m",
                     "Throughput, data quality, freshness, batch status and alerts of the FleetPulse pipeline")


def main() -> None:
    """Write both dashboards to monitoring/grafana/dashboards/."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for name, board in (("fleet_operations.json", fleet_operations()), ("pipeline_health.json", pipeline_health())):
        (OUT_DIR / name).write_text(json.dumps(board, indent=2), encoding="utf-8")
        print(f"wrote {OUT_DIR / name} ({len(board['panels'])} panels)")


if __name__ == "__main__":
    main()
