"""Postgres reads and writes for the batch layer.

Tasks hand data to each other through these tables (not through Airflow
XCom), so every task can be re-run on its own. All writes are upserts on
the table's primary key, so a rerun replaces a day's rows instead of duplicating them.
"""
from __future__ import annotations

import json
from contextlib import closing
from datetime import date, datetime, timezone
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from common.config import postgres_dsn


def connect() -> PgConnection:
    """Open a connection to the `fleet` database."""
    return psycopg2.connect(postgres_dsn(), connect_timeout=5)


def _none_if_nan(value: Any) -> Any:
    """Postgres needs NULL, not NaN, for missing numbers."""
    return None if value is None or (isinstance(value, float) and pd.isna(value)) else value


def _query_df(conn: PgConnection, sql: str, params: tuple) -> pd.DataFrame:
    """Run a SELECT and return the rows as a DataFrame."""
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


# ------------------------------------------------------------------ writes

def upsert_expenses(conn: PgConnection, df: pd.DataFrame, source_file: str) -> int:
    """Insert or replace cleaned expense rows."""
    rows = [
        (r.vehicle_id, r.expense_date, r.fuel_cost, r.maintenance_cost, r.distance_covered,
         bool(r.service_flag), source_file)
        for r in df.itertuples()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO vehicle_expenses
                   (vehicle_id, expense_date, fuel_cost, maintenance_cost, distance_covered, service_flag, source_file)
               VALUES %s
               ON CONFLICT (vehicle_id, expense_date) DO UPDATE SET
                   fuel_cost = EXCLUDED.fuel_cost, maintenance_cost = EXCLUDED.maintenance_cost,
                   distance_covered = EXCLUDED.distance_covered, service_flag = EXCLUDED.service_flag,
                   source_file = EXCLUDED.source_file, loaded_at = now()""",
            rows,
        )
    return len(rows)


def upsert_earnings(conn: PgConnection, df: pd.DataFrame, sim_date: date) -> int:
    """Insert or replace recomputed daily earnings."""
    rows = [(r.vehicle_id, sim_date, int(r.trips_completed), float(r.total_earnings), int(r.event_count))
            for r in df.itertuples()]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO daily_vehicle_earnings (vehicle_id, sim_date, trips_completed, total_earnings, event_count)
               VALUES %s
               ON CONFLICT (vehicle_id, sim_date) DO UPDATE SET
                   trips_completed = EXCLUDED.trips_completed, total_earnings = EXCLUDED.total_earnings,
                   event_count = EXCLUDED.event_count, computed_at = now()""",
            rows,
        )
    return len(rows)


def upsert_profitability(conn: PgConnection, df: pd.DataFrame, sim_date: date) -> int:
    """Insert or replace the day's profitability rows."""
    rows = [
        (r.vehicle_id, sim_date, int(r.trips_completed), r.earnings, r.fuel_cost, r.maintenance_cost,
         r.total_cost, r.profit, _none_if_nan(r.profit_margin), r.distance_covered,
         _none_if_nan(r.cost_per_km), bool(r.is_unprofitable), bool(r.trending_unprofitable),
         int(r.consecutive_profit_drops))
        for r in df.itertuples()
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO daily_vehicle_profitability
                   (vehicle_id, sim_date, trips_completed, earnings, fuel_cost, maintenance_cost, total_cost,
                    profit, profit_margin, distance_covered, cost_per_km, is_unprofitable,
                    trending_unprofitable, consecutive_profit_drops)
               VALUES %s
               ON CONFLICT (vehicle_id, sim_date) DO UPDATE SET
                   trips_completed = EXCLUDED.trips_completed, earnings = EXCLUDED.earnings,
                   fuel_cost = EXCLUDED.fuel_cost, maintenance_cost = EXCLUDED.maintenance_cost,
                   total_cost = EXCLUDED.total_cost, profit = EXCLUDED.profit,
                   profit_margin = EXCLUDED.profit_margin, distance_covered = EXCLUDED.distance_covered,
                   cost_per_km = EXCLUDED.cost_per_km, is_unprofitable = EXCLUDED.is_unprofitable,
                   trending_unprofitable = EXCLUDED.trending_unprofitable,
                   consecutive_profit_drops = EXCLUDED.consecutive_profit_drops, computed_at = now()""",
            rows,
        )
    return len(rows)


def log_task_run(
    dag_run_id: str | None, sim_date: date, task: str, status: str,
    record_count: int | None, message: dict | str | None, started_at: datetime,
) -> None:
    """Append one row to the batch_run_log audit table (own connection, own commit).

    Using a separate connection means a failure is recorded even when the
    task's main transaction was rolled back.
    """
    text = json.dumps(message, default=str) if isinstance(message, dict) else message
    # closing() closes the connection; `with conn` commits the transaction.
    with closing(connect()) as conn, conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO batch_run_log (dag_run_id, sim_date, task, status, record_count, message, started_at, finished_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (dag_run_id, sim_date, task, status, record_count, text, started_at, datetime.now(timezone.utc)),
        )


# ------------------------------------------------------------------ reads

def fetch_earnings(conn: PgConnection, sim_date: date) -> pd.DataFrame:
    """Recomputed earnings of one day."""
    return _query_df(
        conn,
        """SELECT vehicle_id, trips_completed, total_earnings::float8 AS total_earnings
           FROM daily_vehicle_earnings WHERE sim_date = %s""",
        (sim_date,),
    )


def fetch_expenses(conn: PgConnection, sim_date: date) -> pd.DataFrame:
    """Cleaned expenses of one day."""
    return _query_df(
        conn,
        """SELECT vehicle_id, fuel_cost::float8 AS fuel_cost, maintenance_cost::float8 AS maintenance_cost,
                  distance_covered::float8 AS distance_covered
           FROM vehicle_expenses WHERE expense_date = %s""",
        (sim_date,),
    )


def fetch_profitability(conn: PgConnection, sim_date: date) -> pd.DataFrame:
    """Profitability rows of one day (empty DataFrame if the day was never computed)."""
    return _query_df(
        conn,
        """SELECT vehicle_id, trips_completed, earnings::float8 AS earnings, fuel_cost::float8 AS fuel_cost,
                  maintenance_cost::float8 AS maintenance_cost, total_cost::float8 AS total_cost,
                  profit::float8 AS profit, profit_margin::float8 AS profit_margin,
                  distance_covered::float8 AS distance_covered, cost_per_km::float8 AS cost_per_km,
                  is_unprofitable, trending_unprofitable, consecutive_profit_drops
           FROM daily_vehicle_profitability WHERE sim_date = %s ORDER BY vehicle_id""",
        (sim_date,),
    )


def latest_task_message(conn: PgConnection, sim_date: date, task: str) -> dict:
    """The JSON message of the most recent successful run of a task for a day."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT message FROM batch_run_log
               WHERE sim_date = %s AND task = %s AND status = 'success'
               ORDER BY finished_at DESC LIMIT 1""",
            (sim_date, task),
        )
        row = cur.fetchone()
    try:
        return json.loads(row[0]) if row and row[0] else {}
    except (TypeError, ValueError):
        return {}


def speed_layer_earnings(conn: PgConnection, start: datetime, end: datetime) -> float:
    """Total earnings the SPEED layer reported for the windows inside [start, end)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(earnings), 0)::float8 FROM rt_zone_metrics WHERE window_start >= %s AND window_start < %s",
            (start, end),
        )
        return float(cur.fetchone()[0])


def batch_run_totals(conn: PgConnection) -> dict[str, Any]:
    """Cumulative totals for Prometheus, read from the durable audit table."""
    with conn.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM batch_run_log WHERE task = 'dag_run' GROUP BY status")
        runs = dict(cur.fetchall())
        cur.execute(
            """SELECT COALESCE(SUM(record_count), 0) FROM batch_run_log
               WHERE task = 'validate_and_load_expenses' AND status = 'success'"""
        )
        loaded = int(cur.fetchone()[0])
    return {"runs": runs, "records_loaded": loaded}
