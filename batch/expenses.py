"""Validate and clean one day's expense CSV (pure pandas, no database).

Cleaning steps, in order:
  1. check the file has every required column (otherwise fail the task)
  2. drop exact duplicate rows
  3. convert types; values that are not numbers become NaN instead of crashing
  4. reject rows that cannot be fixed (no vehicle_id, wrong date, negative or
     missing distance/maintenance)
  5. impute a missing fuel_cost from distance x the fleet's median fuel cost per km
  6. if a vehicle still appears twice, keep its last row
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag", "expense_date",
]
NUMERIC_COLUMNS = ["fuel_cost", "maintenance_cost", "distance_covered"]


@dataclass
class CleaningReport:
    """What the cleaning step did; stored in batch_run_log and shown in the report."""

    rows_read: int = 0
    exact_duplicates_removed: int = 0
    rows_rejected: int = 0
    fuel_cost_imputed: int = 0
    duplicate_vehicles_removed: int = 0
    rows_loaded: int = 0

    def as_dict(self) -> dict[str, int]:
        """Return the report as a plain dict (JSON-serialisable)."""
        return asdict(self)


def clean_expenses(raw: pd.DataFrame, expected_date: date) -> tuple[pd.DataFrame, CleaningReport]:
    """Apply the cleaning rules to a raw expense DataFrame (all columns as text)."""
    missing = set(REQUIRED_COLUMNS) - set(raw.columns)
    if missing:
        raise ValueError(f"Expense file is missing columns: {sorted(missing)}")

    report = CleaningReport(rows_read=len(raw))
    df = raw[REQUIRED_COLUMNS].drop_duplicates()
    report.exact_duplicates_removed = report.rows_read - len(df)

    # Types. errors="coerce" turns bad values into NaN so we can decide what to do with them.
    df = df.copy()
    df["vehicle_id"] = df["vehicle_id"].str.strip().replace("", pd.NA)
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    flag_map = {"true": True, "false": False, "1": True, "0": False, "yes": True, "no": False}
    df["service_flag"] = df["service_flag"].astype(str).str.strip().str.lower().map(flag_map).fillna(False)
    df["expense_date"] = pd.to_datetime(df["expense_date"], errors="coerce").dt.date

    # Rows we cannot repair are rejected (kept out of the warehouse, counted in the report).
    negative = (df[NUMERIC_COLUMNS] < 0).any(axis=1)
    unfixable = (
        df["vehicle_id"].isna()
        | (df["expense_date"] != expected_date)
        | df["distance_covered"].isna()
        | df["maintenance_cost"].isna()
        | negative
    )
    report.rows_rejected = int(unfixable.sum())
    df = df[~unfixable]

    # Impute missing fuel cost: distance x median(fuel_cost / distance) of the complete rows.
    missing_fuel = df["fuel_cost"].isna()
    if missing_fuel.any():
        complete = df[~missing_fuel & (df["distance_covered"] > 0)]
        rate = (complete["fuel_cost"] / complete["distance_covered"]).median()
        df.loc[missing_fuel, "fuel_cost"] = (df.loc[missing_fuel, "distance_covered"] * rate).round(2)
        report.fuel_cost_imputed = int(missing_fuel.sum())

    # Same vehicle twice with different values: keep the last row in the file.
    before = len(df)
    df = df.drop_duplicates(subset=["vehicle_id", "expense_date"], keep="last")
    report.duplicate_vehicles_removed = before - len(df)

    report.rows_loaded = len(df)
    return df.reset_index(drop=True), report


def load_and_clean_expenses(path: Path, expected_date: date) -> tuple[pd.DataFrame, CleaningReport]:
    """Read a landing CSV as text (so nothing is silently converted) and clean it."""
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    return clean_expenses(raw, expected_date)
