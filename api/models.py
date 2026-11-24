"""Pydantic response models: they validate our output and document it in Swagger (/docs)."""
from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------- speed layer
class ZoneMetrics(BaseModel):
    """KPIs of one zone for one 1-minute window (speed layer)."""

    zone: str
    window_start: datetime
    window_end: datetime
    active_vehicles: int
    idle_vehicles: int
    idle_ratio: float = Field(description="Share of pings that were idle (0..1)")
    trips_completed: int
    trips_per_hour: float = Field(description="Completed trips per SIMULATED hour")
    earnings: float = Field(description="Fares of trips completed in the window (LKR)")
    event_count: int


class FleetSummary(BaseModel):
    """Fleet-wide totals over the latest window of every zone."""

    active_vehicles: int
    idle_ratio: float
    trips_per_hour: float
    earnings: float
    vehicles_by_status: dict[str, int]


class RealtimeResponse(BaseModel):
    """Response of GET /fleet/realtime."""

    generated_at: datetime
    sim_time: datetime
    summary: FleetSummary
    zones: list[ZoneMetrics]


class VehicleRealtime(BaseModel):
    """Latest known state of one vehicle (speed layer)."""

    vehicle_id: str
    driver_id: str | None
    trip_id: str | None
    zone: str | None
    lat: float | None
    lon: float | None
    speed: float | None
    status: str
    idle_since: datetime | None
    idle_sim_minutes: float | None = Field(None, description="Length of the current idle period (sim minutes)")
    event_time: datetime
    sim_date: date | None


class ZoneDetailResponse(BaseModel):
    """Response of GET /fleet/realtime/zones/{zone}."""

    zone: str
    windows: list[ZoneMetrics]
    vehicles: list[VehicleRealtime]


class IdleAlert(BaseModel):
    """One idle alert."""

    alert_id: int
    vehicle_id: str
    driver_id: str | None
    zone: str | None
    idle_since: datetime
    idle_sim_minutes: float
    status: Literal["open", "resolved"]
    detected_at: datetime
    resolved_at: datetime | None


class AlertsResponse(BaseModel):
    """Response of GET /alerts/idle."""

    count: int
    alerts: list[IdleAlert]


# ---------------------------------------------------------------- batch layer
class ProfitabilityRow(BaseModel):
    """One vehicle's profitability for one simulated day (batch layer)."""

    vehicle_id: str
    sim_date: date
    trips_completed: int
    earnings: float
    fuel_cost: float
    maintenance_cost: float
    total_cost: float
    profit: float
    profit_margin: float | None
    distance_covered: float
    cost_per_km: float | None
    is_unprofitable: bool
    trending_unprofitable: bool
    consecutive_profit_drops: int


class ProfitabilitySummary(BaseModel):
    """Fleet totals of one batch report."""

    vehicles: int
    earnings: float
    total_cost: float
    profit: float
    unprofitable_vehicles: list[str]
    trending_unprofitable_vehicles: list[str]


class ProfitabilityReport(BaseModel):
    """Response of GET /reports/profitability/latest."""

    sim_date: date
    summary: ProfitabilitySummary
    vehicles: list[ProfitabilityRow]
    report_html: str | None = Field(None, description="Path of the generated HTML report, if present")


# ---------------------------------------------------------------- merged view
class VehicleStatusResponse(BaseModel):
    """Response of GET /vehicles/{vehicle_id}/status: speed + batch views combined."""

    vehicle_id: str
    realtime: VehicleRealtime | None = Field(description="Speed layer: what the vehicle is doing now")
    open_idle_alert: IdleAlert | None
    latest_profitability: ProfitabilityRow | None = Field(description="Batch layer: last completed day")
    profitability_history: list[ProfitabilityRow] = Field(description="Last few days, newest first")
    assessment: list[str] = Field(description="Plain-language findings from combining both layers")


# ---------------------------------------------------------------- health
class CheckResult(BaseModel):
    """Result of one dependency check."""

    status: Literal["ok", "warn", "fail"]
    detail: str


class HealthResponse(BaseModel):
    """Response of GET /health."""

    status: Literal["healthy", "degraded", "unhealthy"]
    timestamp: datetime
    checks: dict[str, CheckResult]
