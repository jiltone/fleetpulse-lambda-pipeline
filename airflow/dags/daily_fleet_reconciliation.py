"""BATCH LAYER: Airflow DAG `daily_fleet_reconciliation`.

Runs once per simulated day (every SIM_DAY_SECONDS real seconds):

    wait_for_expense_file -> validate_and_load_expenses -> compute_daily_earnings
        -> reconcile_profitability -> generate_report -> data_quality_checks

Which simulated day does a run process?
    The DAG's start_date is the sim-clock anchor (simulated day 0, 00:00) and
    the interval is exactly one simulated day. So each run's data interval
    [data_interval_start, data_interval_end) IS one simulated day, and the run
    starts right after that day ends - when its expense file has just landed.
    A manual run can choose a day with conf {"sim_date": "2025-01-03"}
    (useful to re-run a day after a corrected file arrives).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

from batch import pipeline
from common import sim_clock
from common.config import get_int, get_str

SIM_DAY_SECONDS = get_int("SIM_DAY_SECONDS", 600)
LANDING_DIR = get_str("LANDING_DIR", "data/landing")


def target_sim_date(dag_run: Any, data_interval_start: datetime) -> str:
    """Return the simulated date (YYYY-MM-DD) this DAG run should process.

    Used as a Jinja macro so every task of a run gets the SAME date, even if a
    task is retried after the simulated clock has moved on.
    """
    conf = (dag_run.conf or {}) if dag_run is not None else {}
    if conf.get("sim_date"):
        return conf["sim_date"]
    # +1 s guards against floating-point rounding right at simulated midnight.
    return sim_clock.to_sim_datetime(data_interval_start.timestamp() + 1).date().isoformat()


def _run_sim_date(context: dict) -> date:
    """Sim date of a DAG run, for the callbacks below."""
    dag_run = context["dag_run"]
    return date.fromisoformat(target_sim_date(dag_run, dag_run.data_interval_start))


def _duration(context: dict) -> float:
    """Seconds since the DAG run started."""
    start = context["dag_run"].start_date or datetime.now(timezone.utc)
    return (datetime.now(timezone.utc) - start).total_seconds()


def on_dag_success(context: dict) -> None:
    """DAG-level callback: record the run and push metrics (status=success)."""
    pipeline.push_run_metrics("success", _run_sim_date(context), context["dag_run"].run_id, _duration(context))


def on_dag_failure(context: dict) -> None:
    """DAG-level callback: record the run and push metrics (status=failed) -> BatchJobFailed alert."""
    pipeline.push_run_metrics("failed", _run_sim_date(context), context["dag_run"].run_id, _duration(context))


SIM_DATE = "{{ target_sim_date(dag_run, data_interval_start) }}"

default_args = {
    "owner": "P.S. Dilshan",
    "retries": 2,                          # transient errors (e.g. DB restart) retry automatically
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="daily_fleet_reconciliation",
    description="Batch layer: reconcile lake earnings with daily expenses -> profitability report",
    # Day 0 of the simulation; one run per simulated day after that.
    start_date=datetime.fromtimestamp(sim_clock.anchor_epoch(), tz=timezone.utc),
    schedule=timedelta(seconds=SIM_DAY_SECONDS),
    catchup=False,                         # don't replay missed days when Airflow starts late
    max_active_runs=1,                     # days are processed in order (trend needs yesterday)
    default_args=default_args,
    user_defined_macros={"target_sim_date": target_sim_date},
    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
    tags=["fleetpulse", "batch-layer"],
) as dag:

    # Waits for the day's CSV. The producer writes it atomically (tmp file + rename),
    # so as soon as the file exists it is complete. If it never arrives within one
    # simulated day the sensor fails -> DAG failure -> "batch failed" alert.
    wait_for_expense_file = FileSensor(
        task_id="wait_for_expense_file",
        filepath=LANDING_DIR + "/expenses_" + SIM_DATE + ".csv",
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=SIM_DAY_SECONDS,
        mode="reschedule",                 # free the worker slot between checks
    )

    def _task(task_id: str, fn: Any) -> PythonOperator:
        """Create a PythonOperator that passes the run's sim_date to `fn`."""
        return PythonOperator(task_id=task_id, python_callable=fn, op_kwargs={"sim_date": SIM_DATE})

    validate_and_load = _task("validate_and_load_expenses", pipeline.validate_and_load_expenses)
    compute_earnings = _task("compute_daily_earnings", pipeline.compute_earnings)
    reconcile = _task("reconcile_profitability", pipeline.reconcile_profitability)
    report = _task("generate_report", pipeline.make_report)
    quality = _task("data_quality_checks", pipeline.data_quality_checks)

    wait_for_expense_file >> validate_and_load >> compute_earnings >> reconcile >> report >> quality
