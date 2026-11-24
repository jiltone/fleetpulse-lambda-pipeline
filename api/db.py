"""Read-only queries used by the API.

A small connection pool is created on first use and shared by all requests
(FastAPI runs our sync endpoints in a thread pool, so the pool must be
thread-safe -> ThreadedConnectionPool). Every function returns plain dicts,
which keeps the endpoints simple and lets tests replace these functions.
"""
from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import date
from typing import Any, Iterator

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from common.config import postgres_dsn

_pool: ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> ThreadedConnectionPool:
    """Create the pool once (lazily, so the API can start before Postgres is ready)."""
    global _pool
    with _pool_lock:
        if _pool is None:
            _pool = ThreadedConnectionPool(minconn=1, maxconn=5, dsn=postgres_dsn(), connect_timeout=3)
        return _pool


@contextmanager
def cursor() -> Iterator[RealDictCursor]:
    """Borrow a connection from the pool and yield a cursor returning dict rows."""
    pool = _get_pool()
    conn = pool.getconn()
    broken = False
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.rollback()  # read-only: end the transaction so we never hold locks
    except psycopg2.OperationalError:
        broken = True    # e.g. Postgres restarted: throw this connection away
        raise
    finally:
        pool.putconn(conn, close=broken)


def _all(sql: str, params: dict | tuple = ()) -> list[dict[str, Any]]:
    """Run a query and return every row as a dict."""
    with cursor() as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _one(sql: str, params: dict | tuple = ()) -> dict[str, Any] | None:
    """Run a query and return the first row (or None)."""
    rows = _all(sql, params)
    return rows[0] if rows else None


# ---------------------------------------------------------------- speed layer

def latest_zone_metrics() -> list[dict]:
    """Most recent 1-minute window for each zone."""
    return _all("SELECT DISTINCT ON (zone) * FROM rt_zone_metrics ORDER BY zone, window_start DESC")


def zone_windows(zone: str, limit: int) -> list[dict]:
    """Recent windows of one zone, newest first."""
    return _all("SELECT * FROM rt_zone_metrics WHERE zone = %s ORDER BY window_start DESC LIMIT %s", (zone, limit))


def vehicle_status_counts() -> dict[str, int]:
    """How many vehicles are idle / enroute / on_trip right now."""
    return {r["status"]: r["n"] for r in _all("SELECT status, COUNT(*) AS n FROM rt_vehicle_status GROUP BY status")}


def vehicles_in_zone(zone: str) -> list[dict]:
    """Latest state of every vehicle currently in a zone."""
    return _all("SELECT * FROM rt_vehicle_status WHERE zone = %s ORDER BY vehicle_id", (zone,))


def vehicle_realtime(vehicle_id: str) -> dict | None:
    """Latest real-time state of one vehicle."""
    return _one("SELECT * FROM rt_vehicle_status WHERE vehicle_id = %s", (vehicle_id,))


def idle_alerts(status: str | None, limit: int) -> list[dict]:
    """Idle alerts, newest first; status=None returns open and resolved."""
    return _all(
        """SELECT * FROM idle_alerts
           WHERE %(status)s::text IS NULL OR status = %(status)s
           ORDER BY detected_at DESC LIMIT %(limit)s""",
        {"status": status, "limit": limit},
    )


def open_alert_for(vehicle_id: str) -> dict | None:
    """The open idle alert of a vehicle, if any."""
    return _one("SELECT * FROM idle_alerts WHERE vehicle_id = %s AND status = 'open'", (vehicle_id,))


def last_event_time() -> Any:
    """Event time of the newest event the speed layer has stored (freshness check)."""
    row = _one("SELECT MAX(event_time) AS t FROM rt_vehicle_status")
    return row["t"] if row else None


# ---------------------------------------------------------------- batch layer

def latest_profitability_date() -> date | None:
    """Most recent simulated day the batch layer has finished."""
    row = _one("SELECT MAX(sim_date) AS d FROM daily_vehicle_profitability")
    return row["d"] if row else None


def profitability_for(sim_date: date) -> list[dict]:
    """All vehicles' profitability rows for one simulated day."""
    return _all("SELECT * FROM daily_vehicle_profitability WHERE sim_date = %s ORDER BY profit", (sim_date,))


def profitability_history(vehicle_id: str, days: int) -> list[dict]:
    """A vehicle's most recent daily profitability rows, newest first."""
    return _all(
        "SELECT * FROM daily_vehicle_profitability WHERE vehicle_id = %s ORDER BY sim_date DESC LIMIT %s",
        (vehicle_id, days),
    )


def ping() -> None:
    """Raise if the database cannot answer a trivial query."""
    _one("SELECT 1 AS ok")
