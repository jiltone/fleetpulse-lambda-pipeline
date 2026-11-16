"""Daily-batch source: drops one expense CSV per simulated day.

At the start of each simulated day D, this writes `expenses_<D-1>.csv`
(yesterday's fuel and maintenance costs) into the landing folder. The
Airflow DAG waits for that file and reconciles it with yesterday's earnings.

Important details:
  * Atomic write: the CSV is written to a hidden `.tmp` file and then renamed.
    A rename is atomic, so the Airflow FileSensor can never see half a file.
  * Deterministic: values are seeded by (vehicle, date), so regenerating a
    day gives exactly the same numbers.
  * Dirty data on purpose: 2 duplicate rows and 1 missing value, so the batch
    layer's cleaning step has something to do.

Usage:
    python -m producers.expense_batch_producer                 # run forever
    python -m producers.expense_batch_producer --date 2025-01-03   # write one file and exit
"""
from __future__ import annotations

import argparse
import csv
import os
import random
import signal
import time
from datetime import date, timedelta
from pathlib import Path

from prometheus_client import Counter, start_http_server

from common import sim_clock
from common.config import get_float, get_int, get_str
from common.fleet import VehicleProfile, build_fleet
from common.logging_config import get_logger

log = get_logger("expense-producer", stage="ingestion")

FILES_WRITTEN = Counter("expense_files_written_total", "Expense CSV files written to landing")
ROWS_WRITTEN = Counter("expense_rows_written_total", "Expense rows written (including dirty rows)")
DIRTY_ROWS = Counter("expense_dirty_rows_injected_total", "Deliberately dirty rows", ["kind"])

COLUMNS = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag", "expense_date"]

_running = True


def handle_signal(signum: int, _frame: object) -> None:
    """Stop the main loop on Ctrl+C / docker stop."""
    global _running
    log.info("Shutdown signal received", event="shutdown_requested", signal=signum)
    _running = False


def expense_for_vehicle(profile: VehicleProfile, expense_date: date) -> dict:
    """Generate one vehicle's expenses for one day (all money in LKR).

    - normal vehicles: ~30 LKR/km fuel, small daily maintenance
    - lazy vehicles: drive less, so lower distance and fuel
    - high-cost vehicles: inefficient (~65 LKR/km) and maintenance that grows
      20% per simulated day, so their profit keeps falling ("trending unprofitable")
    """
    rng = random.Random(f"{profile.vehicle_id}-{expense_date.isoformat()}")
    day_index = max(sim_clock.sim_day_index(expense_date), 0)

    if profile.high_cost:
        distance = rng.uniform(160, 260)
        fuel = distance * rng.uniform(60, 75)
        maintenance = rng.uniform(5000, 9000) * (1 + 0.20 * day_index)
        service = rng.random() < 0.30
        if service:
            maintenance += rng.uniform(6000, 12000)
    else:
        distance = rng.uniform(50, 110) if profile.lazy else rng.uniform(140, 260)
        fuel = distance * rng.uniform(28, 36)
        maintenance = rng.uniform(300, 1500)
        service = rng.random() < 0.05
        if service:
            maintenance += rng.uniform(4000, 9000)

    return {
        "vehicle_id": profile.vehicle_id,
        "fuel_cost": f"{fuel:.2f}",
        "maintenance_cost": f"{maintenance:.2f}",
        "distance_covered": f"{distance:.2f}",
        "service_flag": "true" if service else "false",
        "expense_date": expense_date.isoformat(),
    }


def add_dirty_rows(rows: list[dict], rng: random.Random) -> list[dict]:
    """Return rows plus 2 exact duplicates and with 1 fuel_cost value blanked out."""
    dirty = [dict(r) for r in rows]
    for original in rng.sample(rows, 2):
        dirty.append(dict(original))
        DIRTY_ROWS.labels(kind="duplicate").inc()
    victim = rng.randrange(len(rows))
    dirty[victim]["fuel_cost"] = ""
    DIRTY_ROWS.labels(kind="missing_value").inc()
    rng.shuffle(dirty)
    return dirty


def write_csv_atomically(rows: list[dict], target: Path) -> None:
    """Write rows to `.<name>.tmp`, then rename to `target` in one atomic step."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp")
    with tmp.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, target)  # atomic on the same filesystem


def write_expense_file(expense_date: date, landing_dir: Path) -> Path:
    """Generate, dirty and write the expense CSV for one simulated day."""
    rows = [expense_for_vehicle(p, expense_date) for p in build_fleet()]
    rows = add_dirty_rows(rows, random.Random(f"dirty-{expense_date.isoformat()}"))
    target = landing_dir / f"expenses_{expense_date.isoformat()}.csv"
    write_csv_atomically(rows, target)
    FILES_WRITTEN.inc()
    ROWS_WRITTEN.inc(len(rows))
    log.info(
        "Expense file written",
        event="expense_file_written",
        sim_date=expense_date,
        record_count=len(rows),
        file=str(target),
    )
    return target


def run_forever(landing_dir: Path, poll_seconds: float) -> None:
    """Each poll, make sure yesterday's (simulated) expense file exists."""
    log.info("Expense producer started", event="producer_started", landing_dir=str(landing_dir))
    while _running:
        yesterday = sim_clock.current_sim_date() - timedelta(days=1)
        target = landing_dir / f"expenses_{yesterday.isoformat()}.csv"
        if yesterday >= sim_clock.sim_start_date() and not target.exists():
            write_expense_file(yesterday, landing_dir)
        # Sleep in 1 s steps so a shutdown signal is handled quickly.
        for _ in range(int(poll_seconds)):
            if not _running:
                break
            time.sleep(1)
    log.info("Expense producer stopped", event="producer_stopped")


def main() -> None:
    """Parse CLI arguments and either write one file or run continuously."""
    parser = argparse.ArgumentParser(description="Daily expense CSV producer")
    parser.add_argument("--date", type=date.fromisoformat, help="write the file for this date and exit")
    args = parser.parse_args()

    landing_dir = Path(get_str("LANDING_DIR", "data/landing"))
    if args.date:
        write_expense_file(args.date, landing_dir)
        return

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    start_http_server(get_int("EXPENSE_METRICS_PORT", 8002))
    run_forever(landing_dir, get_float("EXPENSE_POLL_SECONDS", 5))


if __name__ == "__main__":
    main()
