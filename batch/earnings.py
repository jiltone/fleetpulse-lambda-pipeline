"""Recompute daily earnings per vehicle from the raw Parquet lake.

LAMBDA PRINCIPLE: the lake is the master dataset and the batch layer is the
source of truth. The speed layer's numbers (rt_zone_metrics) are approximate:
  * foreachBatch is at-least-once, so a replayed micro-batch can append the
    same events to the lake twice -> we de-duplicate on event_id here;
  * the streaming de-duplication only remembers event_ids for the watermark
    period, so a duplicate arriving later slips through -> removed here too;
  * window aggregates use approx_count_distinct and update mode.
Recomputing from the raw events overwrites the speed layer's view with exact
figures, so any streaming error is corrected within one simulated day.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

LAKE_COLUMNS = ["event_id", "vehicle_id", "status", "fare", "zone", "event_time"]


def read_lake_partition(lake_path: Path, sim_date: date) -> pd.DataFrame:
    """Read every Parquet file of one sim_date partition (empty DataFrame if none).

    Spark writes partitions as folders named `sim_date=YYYY-MM-DD`. pyarrow
    skips hidden/underscore files (.crc checksums, _SUCCESS markers).
    """
    partition = lake_path / f"sim_date={sim_date.isoformat()}"
    if not partition.exists() or not any(partition.glob("*.parquet")):
        return pd.DataFrame(columns=LAKE_COLUMNS)
    return pd.read_parquet(partition, columns=LAKE_COLUMNS)


def compute_daily_earnings(events: pd.DataFrame, vehicle_ids: list[str]) -> tuple[pd.DataFrame, dict[str, int]]:
    """Aggregate de-duplicated events into trips and earnings per vehicle.

    Every vehicle in `vehicle_ids` gets a row, even with zero trips, so an idle
    vehicle still shows up (and its costs still count) in the profitability step.
    """
    raw_count = len(events)
    events = events.drop_duplicates(subset="event_id")
    stats = {"events_read": raw_count, "duplicates_removed": raw_count - len(events), "events_used": len(events)}

    completed = events[events["fare"] > 0]
    per_vehicle = pd.DataFrame(
        {
            "trips_completed": completed.groupby("vehicle_id")["fare"].size(),
            "total_earnings": completed.groupby("vehicle_id")["fare"].sum(),
            "event_count": events.groupby("vehicle_id")["event_id"].size(),
        }
    )
    all_ids = sorted(set(vehicle_ids) | set(per_vehicle.index))
    result = per_vehicle.reindex(all_ids).fillna(0)
    result["trips_completed"] = result["trips_completed"].astype(int)
    result["event_count"] = result["event_count"].astype(int)
    result["total_earnings"] = result["total_earnings"].round(2)
    result.index.name = "vehicle_id"
    stats["trips_completed"] = int(result["trips_completed"].sum())
    return result.reset_index(), stats
