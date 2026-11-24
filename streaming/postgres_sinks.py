"""Postgres writes for the speed layer (plain Python, no Spark).

The Spark job collects each small micro-batch result to the driver and
calls these functions. Keeping them Spark-free makes them easy to unit test.

All writes are UPSERTS (INSERT ... ON CONFLICT DO UPDATE): if Spark replays
a micro-batch after a crash, rows are overwritten, never duplicated.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from common.config import postgres_dsn


def connect() -> PgConnection:
    """Open a new connection to the `fleet` database.

    A fresh connection per micro-batch is simple and thread-safe (Spark runs
    the callbacks of different queries in different threads); at one batch
    every 10 s the cost is negligible.

    Retries with backoff: on a busy host a single slow handshake should delay
    the batch, not kill the streaming query (which forces a full restart).
    """
    attempts = 4
    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(postgres_dsn(), connect_timeout=15)
        except psycopg2.OperationalError:
            if attempt == attempts:
                raise
            time.sleep(2 * attempt)
    raise AssertionError("unreachable")


# ------------------------------------------------------------------ zone metrics

ZONE_UPSERT_SQL = """
INSERT INTO rt_zone_metrics
    (window_start, window_end, zone, active_vehicles, idle_vehicles, idle_ratio,
     trips_completed, trips_per_hour, earnings, event_count, updated_at)
VALUES %s
ON CONFLICT (window_start, zone) DO UPDATE SET
    window_end      = EXCLUDED.window_end,
    active_vehicles = EXCLUDED.active_vehicles,
    idle_vehicles   = EXCLUDED.idle_vehicles,
    idle_ratio      = EXCLUDED.idle_ratio,
    trips_completed = EXCLUDED.trips_completed,
    trips_per_hour  = EXCLUDED.trips_per_hour,
    earnings        = EXCLUDED.earnings,
    event_count     = EXCLUDED.event_count,
    updated_at      = now()
"""


def upsert_zone_metrics(conn: PgConnection, rows: list[dict[str, Any]], window_sim_hours: float) -> int:
    """Upsert per-zone window aggregates; returns the number of rows written.

    Spark's "update" output mode re-emits a window every time new events
    arrive for it, so the same (window_start, zone) is overwritten until the
    watermark passes and the window is final.
    """
    values = [
        (
            r["window_start"],
            r["window_end"],
            r["zone"],
            r["active_vehicles"],
            r["idle_vehicles"],
            round(r["idle_ratio"] or 0.0, 4),
            r["trips_completed"],
            round(r["trips_completed"] / window_sim_hours, 2),  # per SIMULATED hour
            round(r["earnings"] or 0.0, 2),
            r["event_count"],
        )
        for r in rows
    ]
    if not values:
        return 0
    with conn.cursor() as cur:
        # The template fills updated_at with the server's now().
        execute_values(cur, ZONE_UPSERT_SQL, values,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())")
    return len(values)


# ------------------------------------------------------------------ vehicle status

def fetch_current_status(conn: PgConnection) -> dict[str, dict[str, Any]]:
    """Read the latest stored state of every vehicle (only ~30 rows)."""
    with conn.cursor() as cur:
        cur.execute("SELECT vehicle_id, status, idle_since, event_time FROM rt_vehicle_status")
        return {
            vid: {"status": status, "idle_since": idle_since, "event_time": event_time}
            for vid, status, idle_since, event_time in cur.fetchall()
        }


def as_utc(value: datetime | None) -> datetime | None:
    """Treat a naive datetime as UTC; leave aware ones (and None) unchanged."""
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def merge_vehicle_status(
    current: dict[str, dict[str, Any]], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Combine stored state with a micro-batch of events -> new latest state per vehicle.

    The tricky part is `idle_since` (when the current idle period began):
      * a non-idle event resets it to None,
      * the first idle event after a non-idle one sets it,
      * idle events that continue an idle period keep the original start,
        even if that period began in an earlier micro-batch.
    Events older than what is already stored are ignored (out-of-order safety).
    """
    by_vehicle: dict[str, list[dict[str, Any]]] = {}
    for e in events:
        # Spark's collect() yields naive datetimes (session time zone = UTC) while
        # Postgres TIMESTAMPTZ columns come back aware; make both aware UTC.
        e = {**e, "event_time": as_utc(e["event_time"])}
        by_vehicle.setdefault(e["vehicle_id"], []).append(e)

    merged = []
    for vid, vehicle_events in by_vehicle.items():
        prev = current.get(vid)
        idle_since: datetime | None = as_utc(prev["idle_since"]) if prev and prev["status"] == "idle" else None
        last_time: datetime | None = as_utc(prev["event_time"]) if prev else None
        latest = None
        for e in sorted(vehicle_events, key=lambda ev: ev["event_time"]):
            if last_time is not None and e["event_time"] < last_time:
                continue
            idle_since = (idle_since or e["event_time"]) if e["status"] == "idle" else None
            latest, last_time = e, e["event_time"]
        if latest is not None:
            merged.append({**latest, "idle_since": idle_since})
    return merged


STATUS_UPSERT_SQL = """
INSERT INTO rt_vehicle_status
    (vehicle_id, driver_id, trip_id, zone, lat, lon, speed, status, idle_since, event_time, sim_date, updated_at)
VALUES %s
ON CONFLICT (vehicle_id) DO UPDATE SET
    driver_id = EXCLUDED.driver_id, trip_id = EXCLUDED.trip_id, zone = EXCLUDED.zone,
    lat = EXCLUDED.lat, lon = EXCLUDED.lon, speed = EXCLUDED.speed, status = EXCLUDED.status,
    idle_since = EXCLUDED.idle_since, event_time = EXCLUDED.event_time,
    sim_date = EXCLUDED.sim_date, updated_at = now()
"""


def upsert_vehicle_status(conn: PgConnection, statuses: list[dict[str, Any]]) -> int:
    """Write the latest state of each vehicle (one row per vehicle)."""
    values = [
        (s["vehicle_id"], s["driver_id"], s["trip_id"], s["zone"], s["lat"], s["lon"], s["speed"],
         s["status"], s["idle_since"], s["event_time"], s["sim_date"])
        for s in statuses
    ]
    if not values:
        return 0
    with conn.cursor() as cur:
        execute_values(cur, STATUS_UPSERT_SQL, values,
                       template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())")
    return len(values)


# ------------------------------------------------------------------ idle alerts

RESOLVE_ALERTS_SQL = """
UPDATE idle_alerts a
SET status = 'resolved', resolved_at = s.event_time
FROM rt_vehicle_status s
WHERE a.vehicle_id = s.vehicle_id AND a.status = 'open' AND s.status <> 'idle'
RETURNING a.vehicle_id
"""

# Open (or refresh) an alert for every vehicle idle longer than the threshold.
# Idle time is measured in event time (event_time - idle_since), not wall-clock.
# `xmax = 0` is a Postgres idiom: it is true only for rows that were just
# INSERTED (not updated), so we can tell new alerts apart from refreshed ones.
OPEN_ALERTS_SQL = """
INSERT INTO idle_alerts (vehicle_id, driver_id, zone, idle_since, idle_sim_minutes)
SELECT vehicle_id, driver_id, zone, idle_since,
       EXTRACT(EPOCH FROM (event_time - idle_since)) * %(speedup)s / 60.0
FROM rt_vehicle_status
WHERE status = 'idle'
  AND idle_since IS NOT NULL
  AND event_time - idle_since > make_interval(secs => %(threshold_s)s)
ON CONFLICT (vehicle_id) WHERE status = 'open'
DO UPDATE SET idle_sim_minutes = EXCLUDED.idle_sim_minutes, zone = EXCLUDED.zone
RETURNING vehicle_id, driver_id, zone, idle_sim_minutes, (xmax = 0) AS is_new
"""


def count_open_alerts(conn: PgConnection) -> int:
    """Number of idle alerts currently open (exported as a Prometheus gauge)."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM idle_alerts WHERE status = 'open'")
        return int(cur.fetchone()[0])


def refresh_idle_alerts(conn: PgConnection, threshold_real_s: float, speedup: float) -> tuple[list[dict], list[str]]:
    """Resolve alerts of vehicles that moved again, then open alerts for long-idle vehicles.

    Returns (new_alerts, resolved_vehicle_ids) so the caller can log them.
    """
    with conn.cursor() as cur:
        cur.execute(RESOLVE_ALERTS_SQL)
        resolved = [row[0] for row in cur.fetchall()]
        cur.execute(OPEN_ALERTS_SQL, {"speedup": speedup, "threshold_s": threshold_real_s})
        new_alerts = [
            {"vehicle_id": vid, "driver_id": did, "zone": zone, "idle_sim_minutes": float(mins)}
            for vid, did, zone, mins, is_new in cur.fetchall()
            if is_new
        ]
    return new_alerts, resolved
