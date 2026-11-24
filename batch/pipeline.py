"""Task functions called by the Airflow DAG `daily_fleet_reconciliation`.

The DAG file only wires tasks together; the work lives here so it can be read
and tested without Airflow. Each task:
  * receives the simulated date it should process (`sim_date`, YYYY-MM-DD),
  * reads its inputs from files / Postgres and writes its outputs to Postgres,
  * records success or failure in batch_run_log (decorator `logged_task`).
"""
from __future__ import annotations

import functools
import time
from contextlib import closing
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from batch import db
from batch.earnings import compute_daily_earnings, read_lake_partition
from batch.expenses import load_and_clean_expenses
from batch.profitability import compute_profitability
from batch.report import generate_report
from common import sim_clock
from common.config import get_float, get_str
from common.fleet import build_fleet
from common.logging_config import get_logger

log = get_logger("airflow-batch", stage="processing")

LANDING_DIR = Path(get_str("LANDING_DIR", "data/landing"))
LAKE_PATH = Path(get_str("LAKE_PATH", "data/lake/raw/telemetry"))
REPORTS_DIR = Path(get_str("REPORTS_DIR", "reports"))
MARGIN_THRESHOLD = get_float("UNPROFITABLE_MARGIN_THRESHOLD", 0.10)
TRENDING_MARGIN_THRESHOLD = get_float("TRENDING_MARGIN_THRESHOLD", 0.30)


def expense_file(sim_date: date) -> Path:
    """Path of the landing CSV for a simulated day."""
    return LANDING_DIR / f"expenses_{sim_date.isoformat()}.csv"


def logged_task(task_name: str) -> Callable:
    """Decorator: parse sim_date, time the task, log JSON and write batch_run_log.

    The wrapped function returns (record_count, details_dict).
    """

    def decorator(fn: Callable[[date], tuple[int, dict]]) -> Callable[..., int]:
        @functools.wraps(fn)
        def wrapper(sim_date: str, **context: Any) -> int:
            day = date.fromisoformat(sim_date)
            run_id = context.get("run_id")
            started = datetime.now(timezone.utc)
            log.info("Task started", event="task_started", task=task_name, sim_date=day, batch_id=run_id)
            try:
                count, details = fn(day)
            except Exception as exc:
                db.log_task_run(run_id, day, task_name, "failed", None, str(exc)[:1000], started)
                log.exception("Task failed", event="task_failed", task=task_name, sim_date=day, batch_id=run_id)
                raise  # Airflow marks the task failed and retries it (retries=2)
            db.log_task_run(run_id, day, task_name, "success", count, details, started)
            log.info(
                "Task finished", event="task_succeeded", task=task_name, sim_date=day, batch_id=run_id,
                record_count=count, duration_s=round((datetime.now(timezone.utc) - started).total_seconds(), 2),
                **{k: v for k, v in details.items() if isinstance(v, (int, float, str))},
            )
            return count

        return wrapper

    return decorator


# ---------------------------------------------------------------- task 2
@logged_task("validate_and_load_expenses")
def validate_and_load_expenses(sim_date: date) -> tuple[int, dict]:
    """Clean the day's expense CSV and upsert it into vehicle_expenses."""
    path = expense_file(sim_date)
    df, report = load_and_clean_expenses(path, sim_date)
    if df.empty:
        raise ValueError(f"No valid expense rows in {path.name}")
    with closing(db.connect()) as conn, conn:
        loaded = db.upsert_expenses(conn, df, path.name)
    return loaded, report.as_dict()


# ---------------------------------------------------------------- task 3
@logged_task("compute_daily_earnings")
def compute_earnings(sim_date: date) -> tuple[int, dict]:
    """Recompute earnings per vehicle from the raw Parquet lake (NOT from the speed layer)."""
    events = read_lake_partition(LAKE_PATH, sim_date)
    vehicle_ids = [v.vehicle_id for v in build_fleet()]
    earnings, stats = compute_daily_earnings(events, vehicle_ids)
    with closing(db.connect()) as conn, conn:
        written = db.upsert_earnings(conn, earnings, sim_date)
    stats["total_earnings"] = round(float(earnings["total_earnings"].sum()), 2)
    return written, stats


# ---------------------------------------------------------------- task 4
@logged_task("reconcile_profitability")
def reconcile_profitability(sim_date: date) -> tuple[int, dict]:
    """Join earnings with expenses and yesterday's profit -> daily_vehicle_profitability."""
    with closing(db.connect()) as conn, conn:
        result = compute_profitability(
            earnings=db.fetch_earnings(conn, sim_date),
            expenses=db.fetch_expenses(conn, sim_date),
            previous=db.fetch_profitability(conn, sim_date - timedelta(days=1)),
            margin_threshold=MARGIN_THRESHOLD,
            trending_margin_threshold=TRENDING_MARGIN_THRESHOLD,
        )
        written = db.upsert_profitability(conn, result, sim_date)

    for row in result[result["is_unprofitable"] | result["trending_unprofitable"]].itertuples():
        log.warning(
            "Vehicle flagged by profitability check", event="vehicle_unprofitable", stage="processing",
            vehicle_id=row.vehicle_id, sim_date=sim_date, profit=row.profit,
            unprofitable=bool(row.is_unprofitable), trending=bool(row.trending_unprofitable),
        )
    return written, {
        "unprofitable": int(result["is_unprofitable"].sum()),
        "trending_unprofitable": int(result["trending_unprofitable"].sum()),
        "fleet_profit": round(float(result["profit"].sum()), 2),
    }


# ---------------------------------------------------------------- task 5
@logged_task("generate_report")
def make_report(sim_date: date) -> tuple[int, dict]:
    """Render the HTML + CSV report from the stored profitability rows."""
    start, end = sim_clock.real_window_for_sim_date(sim_date)
    with closing(db.connect()) as conn:
        df = db.fetch_profitability(conn, sim_date)
        cleaning = db.latest_task_message(conn, sim_date, "validate_and_load_expenses")
        earnings_stats = db.latest_task_message(conn, sim_date, "compute_daily_earnings")
        speed_total = db.speed_layer_earnings(
            conn, datetime.fromtimestamp(start, timezone.utc), datetime.fromtimestamp(end, timezone.utc)
        )
    if df.empty:
        raise ValueError(f"No profitability rows for {sim_date}")

    batch_total = float(df["earnings"].sum())
    quality = {
        "Expense rows read": cleaning.get("rows_read", "-"),
        "Duplicate rows removed": cleaning.get("exact_duplicates_removed", "-"),
        "Missing fuel costs imputed": cleaning.get("fuel_cost_imputed", "-"),
        "Rows rejected": cleaning.get("rows_rejected", "-"),
        "Lake events read": earnings_stats.get("events_read", "-"),
        "Duplicate events removed": earnings_stats.get("duplicates_removed", "-"),
        "Speed-layer earnings (LKR)": f"{speed_total:,.0f}",
        "Batch-layer earnings (LKR)": f"{batch_total:,.0f}",
    }
    html_path, csv_path = generate_report(df, sim_date, REPORTS_DIR, quality, MARGIN_THRESHOLD)
    return 2, {"html": str(html_path), "csv": str(csv_path),
               "speed_layer_earnings": round(speed_total, 2), "batch_layer_earnings": round(batch_total, 2)}


# ---------------------------------------------------------------- task 6
@logged_task("data_quality_checks")
def data_quality_checks(sim_date: date) -> tuple[int, dict]:
    """Check the day's outputs. Critical failures fail the task (and the DAG run)."""
    fleet_size = len(build_fleet())
    with closing(db.connect()) as conn:
        profitability = db.fetch_profitability(conn, sim_date)
        expenses = db.fetch_expenses(conn, sim_date)
        earnings_stats = db.latest_task_message(conn, sim_date, "compute_daily_earnings")
        report_info = db.latest_task_message(conn, sim_date, "generate_report")

    checks = {
        # name: (passed, critical)
        "lake_has_events": (earnings_stats.get("events_used", 0) > 0, True),
        "one_row_per_vehicle": (len(profitability) == fleet_size, True),
        "no_null_profit": (bool(profitability["profit"].notna().all()), True),
        "earnings_not_negative": (bool((profitability["earnings"] >= 0).all()), True),
        "report_files_exist": (all(Path(report_info.get(k, "")).is_file() for k in ("html", "csv")), True),
        "expenses_for_every_vehicle": (len(expenses) == fleet_size, False),
    }
    results = {name: passed for name, (passed, _) in checks.items()}
    failed_critical = [name for name, (passed, critical) in checks.items() if critical and not passed]
    failed_warning = [name for name, (passed, critical) in checks.items() if not critical and not passed]
    for name in failed_warning:
        log.warning("Data quality warning", event="dq_warning", check=name, sim_date=sim_date)

    # Lambda correction: how far the speed layer's live earnings were from the exact batch figure.
    speed, batch = report_info.get("speed_layer_earnings"), report_info.get("batch_layer_earnings")
    if speed is not None and batch:
        results["speed_vs_batch_earnings_diff_pct"] = round((speed - batch) / batch * 100, 2)

    if failed_critical:
        raise ValueError(f"Critical data quality checks failed: {failed_critical}")
    return len(checks), results


# ---------------------------------------------------------------- DAG callbacks
def push_run_metrics(status: str, sim_date: date, run_id: str | None, duration_s: float) -> None:
    """Record the DAG run outcome and push batch metrics to the Prometheus Pushgateway.

    Airflow tasks are short-lived, so Prometheus cannot scrape them; instead
    we PUSH. Cumulative totals are read from batch_run_log so they survive
    restarts. A Pushgateway problem is logged but never fails the pipeline.
    """
    db.log_task_run(run_id, sim_date, "dag_run", status, None, {"duration_s": duration_s},
                    datetime.now(timezone.utc) - timedelta(seconds=duration_s))
    with closing(db.connect()) as conn:
        totals = db.batch_run_totals(conn)

    registry = CollectorRegistry()
    runs = Gauge("batch_runs_total", "DAG runs by final status", ["status"], registry=registry)
    for s in ("success", "failed"):
        runs.labels(status=s).set(totals["runs"].get(s, 0))
    Gauge("batch_records_loaded_total", "Expense rows loaded by all runs", registry=registry).set(
        totals["records_loaded"])
    Gauge("batch_duration_seconds", "Duration of the last DAG run", registry=registry).set(duration_s)
    Gauge("batch_last_run_timestamp", "Unix time of the last DAG run", registry=registry).set(time.time())
    if status == "success":
        Gauge("batch_last_success_timestamp", "Unix time of the last successful run",
              registry=registry).set(time.time())
        Gauge("batch_last_success_sim_day", "Sim-day index of the last successful run",
              registry=registry).set(sim_clock.sim_day_index(sim_date))
    try:
        push_to_gateway(get_str("PUSHGATEWAY_URL", "pushgateway:9091"), job="fleet_batch", registry=registry)
    except Exception as exc:
        log.warning("Could not push metrics", event="pushgateway_unavailable", error=str(exc))
    log.info("DAG run finished", event="dag_run_finished", status=status, sim_date=sim_date,
             batch_id=run_id, duration_s=round(duration_s, 1))
