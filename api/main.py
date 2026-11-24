"""SERVING LAYER: FastAPI service that merges the speed-layer and batch-layer views.

    speed layer tables (rt_*)    -> "what is happening right now"
    batch layer tables (daily_*) -> "what happened yesterday, exactly"

GET /vehicles/{vehicle_id}/status is the clearest example of the Lambda
serving layer: it answers one question using both views at once.

Run locally:  uvicorn api.main:app --reload   ->  http://localhost:8000/docs
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from api import db
from api.health import run_health_checks
from api.models import (
    AlertsResponse, FleetSummary, HealthResponse, IdleAlert, ProfitabilityReport, ProfitabilityRow,
    ProfitabilitySummary, RealtimeResponse, VehicleRealtime, VehicleStatusResponse, ZoneDetailResponse,
    ZoneMetrics,
)
from common import sim_clock
from common.config import get_float, get_str
from common.fleet import ZONES
from common.logging_config import get_logger

log = get_logger("fleet-api", stage="serving")

app = FastAPI(
    title="FleetPulse API",
    description="Serving layer of the FleetPulse Lambda pipeline: live fleet KPIs (speed layer) merged "
                "with daily profitability (batch layer). Author: P.S. Dilshan (EG/2020/3898).",
    version="1.0.0",
)

# ---------------- Prometheus metrics ----------------
REQUESTS = Counter("api_requests_total", "HTTP requests served", ["method", "path", "status"])
LATENCY = Histogram(
    "api_request_duration_seconds", "HTTP request latency", ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)


@app.middleware("http")
async def observe_requests(request: Request, call_next: Callable) -> Response:
    """Count, time and log every request.

    The metric label is the route TEMPLATE (/vehicles/{vehicle_id}/status),
    not the real URL, so each vehicle doesn't create a new time series.
    """
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        log.exception("Unhandled error", event="request_failed", path=request.url.path)
        response = JSONResponse({"detail": "internal error"}, status_code=500)
    route = request.scope.get("route")
    path = getattr(route, "path", "unmatched")
    elapsed = time.perf_counter() - start
    REQUESTS.labels(request.method, path, str(response.status_code)).inc()
    LATENCY.labels(request.method, path).observe(elapsed)
    if path != "/metrics":  # Prometheus scrapes every few seconds; don't flood the logs
        log.info("Request served", event="http_request", method=request.method, path=request.url.path,
                 status=response.status_code, duration_ms=round(elapsed * 1000, 1))
    return response


def _db_call(fn: Callable, *args: object) -> object:
    """Run a DB query; turn a database outage into a clear 503 instead of a 500."""
    try:
        return fn(*args)
    except Exception as exc:
        log.error("Database query failed", event="db_error", error=str(exc), query=fn.__name__)
        raise HTTPException(status_code=503, detail="database unavailable") from exc


def _canonical_zone(zone: str) -> str:
    """Accept any capitalisation (e.g. 'fort') and return the configured name, or 404."""
    for name in ZONES:
        if name.lower() == zone.lower():
            return name
    raise HTTPException(status_code=404, detail=f"Unknown zone '{zone}'. Zones: {', '.join(ZONES)}")


def _with_idle_minutes(row: dict) -> VehicleRealtime:
    """Add the length of the current idle period (event time, in simulated minutes)."""
    minutes = None
    if row.get("status") == "idle" and row.get("idle_since"):
        seconds = (row["event_time"] - row["idle_since"]).total_seconds()
        minutes = round(sim_clock.real_seconds_to_sim_minutes(seconds), 1)
    return VehicleRealtime(**row, idle_sim_minutes=minutes)


# ======================================================================= endpoints

@app.get("/health", response_model=HealthResponse, tags=["operations"])
def health(response: Response) -> HealthResponse:
    """Check Postgres, Kafka and data freshness. Returns HTTP 503 when unhealthy."""
    result = run_health_checks()
    if result.status == "unhealthy":
        response.status_code = 503
    return result


@app.get("/metrics", tags=["operations"], include_in_schema=False)
def metrics() -> Response:
    """Prometheus metrics in the text exposition format."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/fleet/realtime", response_model=RealtimeResponse, tags=["speed layer"])
def fleet_realtime() -> RealtimeResponse:
    """Latest window per zone (active vehicles, idle ratio, trips/hour, earnings) plus fleet totals."""
    zones = [ZoneMetrics(**r) for r in _db_call(db.latest_zone_metrics)]
    counts = _db_call(db.vehicle_status_counts)
    events = sum(z.event_count for z in zones)
    summary = FleetSummary(
        active_vehicles=sum(z.active_vehicles for z in zones),
        # weight each zone's idle ratio by its number of pings
        idle_ratio=round(sum(z.idle_ratio * z.event_count for z in zones) / events, 4) if events else 0.0,
        trips_per_hour=round(sum(z.trips_per_hour for z in zones), 2),
        earnings=round(sum(z.earnings for z in zones), 2),
        vehicles_by_status=counts,
    )
    return RealtimeResponse(
        generated_at=datetime.now(timezone.utc), sim_time=sim_clock.sim_now(), summary=summary, zones=zones
    )


@app.get("/fleet/realtime/zones/{zone}", response_model=ZoneDetailResponse, tags=["speed layer"])
def zone_detail(zone: str, windows: int = Query(30, ge=1, le=500, description="How many recent windows")) -> ZoneDetailResponse:
    """Recent windows for one zone and the vehicles currently in it."""
    name = _canonical_zone(zone)
    return ZoneDetailResponse(
        zone=name,
        windows=[ZoneMetrics(**r) for r in _db_call(db.zone_windows, name, windows)],
        vehicles=[_with_idle_minutes(r) for r in _db_call(db.vehicles_in_zone, name)],
    )


@app.get("/alerts/idle", response_model=AlertsResponse, tags=["speed layer"])
def idle_alerts(
    status: str = Query("open", pattern="^(open|resolved|all)$", description="open, resolved or all"),
    limit: int = Query(100, ge=1, le=1000),
) -> AlertsResponse:
    """Idle alerts raised by the speed layer (open ones by default)."""
    rows = _db_call(db.idle_alerts, None if status == "all" else status, limit)
    return AlertsResponse(count=len(rows), alerts=[IdleAlert(**r) for r in rows])


@app.get("/reports/profitability/latest", response_model=ProfitabilityReport, tags=["batch layer"])
def latest_profitability(unprofitable_only: bool = False) -> ProfitabilityReport:
    """The most recent daily profitability result computed by the batch layer."""
    sim_date = _db_call(db.latest_profitability_date)
    if sim_date is None:
        raise HTTPException(status_code=404, detail="No profitability report yet - the first DAG run "
                                                    "happens after simulated day 0 ends")
    rows = [ProfitabilityRow(**r) for r in _db_call(db.profitability_for, sim_date)]
    summary = ProfitabilitySummary(
        vehicles=len(rows),
        earnings=round(sum(r.earnings for r in rows), 2),
        total_cost=round(sum(r.total_cost for r in rows), 2),
        profit=round(sum(r.profit for r in rows), 2),
        unprofitable_vehicles=sorted(r.vehicle_id for r in rows if r.is_unprofitable),
        trending_unprofitable_vehicles=sorted(r.vehicle_id for r in rows if r.trending_unprofitable),
    )
    html = Path(get_str("REPORTS_DIR", "reports")) / f"profitability_{sim_date.isoformat()}.html"
    if unprofitable_only:
        rows = [r for r in rows if r.is_unprofitable]
    return ProfitabilityReport(sim_date=sim_date, summary=summary, vehicles=rows,
                               report_html=str(html) if html.exists() else None)


@app.get("/vehicles/{vehicle_id}/status", response_model=VehicleStatusResponse, tags=["merged view"])
def vehicle_status(vehicle_id: str, history_days: int = Query(5, ge=1, le=60)) -> VehicleStatusResponse:
    """MERGED VIEW: a vehicle's live state (speed layer) together with its recent profitability (batch layer)."""
    vid = vehicle_id.strip().upper()
    realtime_row = _db_call(db.vehicle_realtime, vid)
    alert_row = _db_call(db.open_alert_for, vid)
    history = [ProfitabilityRow(**r) for r in _db_call(db.profitability_history, vid, history_days)]
    if realtime_row is None and not history:
        raise HTTPException(status_code=404, detail=f"Unknown vehicle '{vid}'")

    realtime = _with_idle_minutes(realtime_row) if realtime_row else None
    latest = history[0] if history else None
    return VehicleStatusResponse(
        vehicle_id=vid,
        realtime=realtime,
        open_idle_alert=IdleAlert(**alert_row) if alert_row else None,
        latest_profitability=latest,
        profitability_history=history,
        assessment=assess(realtime, alert_row is not None, latest),
    )


def assess(realtime: VehicleRealtime | None, has_alert: bool, latest: ProfitabilityRow | None) -> list[str]:
    """Turn both views into short plain-language findings (the point of a merged view)."""
    notes = []
    threshold = get_float("IDLE_ALERT_SIM_MINUTES", 90)
    if realtime is None:
        notes.append("No live telemetry stored for this vehicle.")
    elif realtime.status == "idle" and realtime.idle_sim_minutes is not None:
        notes.append(f"Idle for {realtime.idle_sim_minutes:.0f} sim-minutes in {realtime.zone}"
                     + (" (idle alert open)." if has_alert else
                        f" (alert threshold {threshold:.0f})."))
    else:
        notes.append(f"Currently {realtime.status.replace('_', ' ')} in {realtime.zone}.")
    if latest is None:
        notes.append("No batch profitability result yet.")
    elif latest.is_unprofitable:
        notes.append(f"Unprofitable on {latest.sim_date}: profit {latest.profit:,.0f} LKR"
                     + (f", margin {latest.profit_margin:.0%}." if latest.profit_margin is not None else "."))
    else:
        notes.append(f"Profitable on {latest.sim_date}: profit {latest.profit:,.0f} LKR.")
    if latest is not None and latest.trending_unprofitable:
        notes.append(f"Profit has fallen {latest.consecutive_profit_drops} days in a row - review costs.")
    if latest is not None and latest.is_unprofitable and has_alert:
        notes.append("Unprofitable AND idle now: candidate for reassignment to a busier zone.")
    return notes
