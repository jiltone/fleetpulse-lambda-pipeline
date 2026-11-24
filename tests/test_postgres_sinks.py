"""Speed-layer merge logic (pure Python, no database needed)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from streaming.postgres_sinks import merge_vehicle_status

T0 = datetime(2027, 1, 1, 12, 0, 0)  # naive, as Spark's collect() returns it (UTC)


def event(vid: str, status: str, seconds: int) -> dict:
    return {"vehicle_id": vid, "status": status, "event_time": T0 + timedelta(seconds=seconds)}


def test_naive_spark_times_merge_with_aware_postgres_state():
    """Regression: stored TIMESTAMPTZ values are aware, batch values are naive."""
    stored = {"V001": {"status": "idle", "idle_since": T0.replace(tzinfo=timezone.utc),
                       "event_time": (T0 + timedelta(seconds=5)).replace(tzinfo=timezone.utc)}}
    merged = merge_vehicle_status(stored, [event("V001", "idle", 10)])
    assert merged[0]["idle_since"] == T0.replace(tzinfo=timezone.utc)
    assert merged[0]["event_time"].tzinfo is not None


def test_out_of_order_event_is_ignored():
    stored = {"V002": {"status": "moving", "idle_since": None,
                       "event_time": (T0 + timedelta(seconds=30)).replace(tzinfo=timezone.utc)}}
    assert merge_vehicle_status(stored, [event("V002", "idle", 10)]) == []


def test_non_idle_event_resets_idle_since():
    merged = merge_vehicle_status({}, [event("V003", "idle", 0), event("V003", "moving", 5)])
    assert merged[0]["idle_since"] is None
