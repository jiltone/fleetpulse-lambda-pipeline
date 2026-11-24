"""API endpoint tests. The database layer (api.db) is replaced with sample rows,
so these run without Postgres and check routing, validation, the merged view
and error handling."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from api import db, health
from api.main import app
from api.models import CheckResult

NOW = datetime(2026, 11, 17, 10, 0, tzinfo=timezone.utc)

ZONE_ROW = {
    "zone": "Fort", "window_start": NOW - timedelta(minutes=1), "window_end": NOW,
    "active_vehicles": 6, "idle_vehicles": 2, "idle_ratio": 0.25, "trips_completed": 3,
    "trips_per_hour": 1.25, "earnings": 3600.0, "event_count": 40, "updated_at": NOW,
}
VEHICLE_ROW = {
    "vehicle_id": "V007", "driver_id": "D007", "trip_id": None, "zone": "Fort", "lat": 6.935, "lon": 79.845,
    "speed": 0.0, "status": "idle", "idle_since": NOW - timedelta(seconds=50), "event_time": NOW,
    "sim_date": date(2025, 1, 3), "updated_at": NOW,
}
ALERT_ROW = {
    "alert_id": 1, "vehicle_id": "V007", "driver_id": "D007", "zone": "Fort",
    "idle_since": NOW - timedelta(seconds=50), "idle_sim_minutes": 120.0, "status": "open",
    "detected_at": NOW, "resolved_at": None,
}


def profit_row(vid: str, profit: float, unprofitable: bool, trending: bool = False, drops: int = 0) -> dict:
    """Build one daily_vehicle_profitability row."""
    return {
        "vehicle_id": vid, "sim_date": date(2025, 1, 2), "trips_completed": 6, "earnings": 6400.0,
        "fuel_cost": 4000.0, "maintenance_cost": 6400.0 - 4000.0 - profit, "total_cost": 6400.0 - profit,
        "profit": profit, "profit_margin": profit / 6400.0, "distance_covered": 80.0, "cost_per_km": 50.0,
        "is_unprofitable": unprofitable, "trending_unprofitable": trending, "consecutive_profit_drops": drops,
        "computed_at": NOW,
    }


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """A test client with every database function replaced by sample data."""
    monkeypatch.setattr(db, "latest_zone_metrics", lambda: [ZONE_ROW])
    monkeypatch.setattr(db, "vehicle_status_counts", lambda: {"idle": 10, "enroute": 5, "on_trip": 15})
    monkeypatch.setattr(db, "zone_windows", lambda zone, limit: [ZONE_ROW])
    monkeypatch.setattr(db, "vehicles_in_zone", lambda zone: [VEHICLE_ROW])
    monkeypatch.setattr(db, "vehicle_realtime", lambda vid: VEHICLE_ROW if vid == "V007" else None)
    monkeypatch.setattr(db, "open_alert_for", lambda vid: ALERT_ROW if vid == "V007" else None)
    monkeypatch.setattr(db, "idle_alerts", lambda status, limit: [ALERT_ROW])
    monkeypatch.setattr(db, "latest_profitability_date", lambda: date(2025, 1, 2))
    monkeypatch.setattr(db, "profitability_for", lambda d: [
        profit_row("V004", -9000.0, True, True, 2), profit_row("V001", 15000.0, False)])
    monkeypatch.setattr(db, "profitability_history",
                        lambda vid, days: [profit_row(vid, 500.0, True)] if vid == "V007" else [])
    return TestClient(app)


def test_fleet_realtime_returns_zones_and_summary(client: TestClient) -> None:
    body = client.get("/fleet/realtime").json()
    assert body["zones"][0]["zone"] == "Fort"
    assert body["summary"]["active_vehicles"] == 6
    assert body["summary"]["vehicles_by_status"]["on_trip"] == 15


def test_zone_lookup_is_case_insensitive(client: TestClient) -> None:
    response = client.get("/fleet/realtime/zones/fort")
    assert response.status_code == 200
    assert response.json()["zone"] == "Fort"
    # 50 real seconds x 144 / 60 = 120 simulated minutes idle
    assert response.json()["vehicles"][0]["idle_sim_minutes"] == pytest.approx(120.0)


def test_unknown_zone_is_404(client: TestClient) -> None:
    assert client.get("/fleet/realtime/zones/Kandy").status_code == 404


def test_idle_alerts_and_status_validation(client: TestClient) -> None:
    assert client.get("/alerts/idle").json()["count"] == 1
    assert client.get("/alerts/idle?status=bogus").status_code == 422  # rejected by validation


def test_latest_profitability_summary(client: TestClient) -> None:
    body = client.get("/reports/profitability/latest").json()
    assert body["sim_date"] == "2025-01-02"
    assert body["summary"]["unprofitable_vehicles"] == ["V004"]
    assert body["summary"]["trending_unprofitable_vehicles"] == ["V004"]
    only_bad = client.get("/reports/profitability/latest?unprofitable_only=true").json()
    assert [v["vehicle_id"] for v in only_bad["vehicles"]] == ["V004"]


def test_no_report_yet_is_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(db, "latest_profitability_date", lambda: None)
    assert client.get("/reports/profitability/latest").status_code == 404


def test_vehicle_status_merges_both_layers(client: TestClient) -> None:
    body = client.get("/vehicles/v007/status").json()   # lower case is normalised
    assert body["vehicle_id"] == "V007"
    assert body["realtime"]["status"] == "idle"               # speed layer
    assert body["open_idle_alert"]["alert_id"] == 1           # speed layer alert
    assert body["latest_profitability"]["is_unprofitable"]    # batch layer
    assert any("candidate for reassignment" in note for note in body["assessment"])


def test_unknown_vehicle_is_404(client: TestClient) -> None:
    assert client.get("/vehicles/V999/status").status_code == 404


def test_database_outage_returns_503(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken() -> list:
        raise ConnectionError("db down")

    monkeypatch.setattr(db, "latest_zone_metrics", broken)
    assert client.get("/fleet/realtime").status_code == 503


def test_metrics_endpoint_counts_requests(client: TestClient) -> None:
    client.get("/fleet/realtime")
    text = client.get("/metrics").text
    assert 'api_requests_total{method="GET",path="/fleet/realtime",status="200"}' in text
    assert "api_request_duration_seconds_bucket" in text


@pytest.mark.parametrize(
    ("postgres", "kafka", "fresh", "expected", "http"),
    [
        ("ok", "ok", "ok", "healthy", 200),
        ("ok", "fail", "ok", "degraded", 200),
        ("ok", "ok", "warn", "degraded", 200),
        ("fail", "ok", "ok", "unhealthy", 503),
    ],
)
def test_health_status_combination(client: TestClient, monkeypatch: pytest.MonkeyPatch,
                                   postgres: str, kafka: str, fresh: str, expected: str, http: int) -> None:
    monkeypatch.setattr(health, "check_postgres", lambda: CheckResult(status=postgres, detail="x"))
    monkeypatch.setattr(health, "check_kafka", lambda: CheckResult(status=kafka, detail="x"))
    monkeypatch.setattr(health, "check_stream_freshness", lambda: CheckResult(status=fresh, detail="x"))
    monkeypatch.setattr(health, "check_batch_freshness", lambda: CheckResult(status="ok", detail="x"))
    response = client.get("/health")
    assert response.status_code == http
    assert response.json()["status"] == expected
