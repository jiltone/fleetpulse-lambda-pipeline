"""Join earnings with expenses and decide which vehicles are unprofitable (pure pandas).

    total_cost  = fuel_cost + maintenance_cost
    profit      = earnings - total_cost
    margin      = profit / earnings            (None when earnings = 0)
    cost_per_km = total_cost / distance        (None when distance = 0)

    unprofitable          : profit < 0  OR  margin < threshold
    trending_unprofitable : profit fell on 2+ consecutive sim days AND the
                            vehicle is already in the danger zone (margin below
                            the trending threshold, default 30%, or a loss).
                            The danger-zone condition stops a healthy vehicle
                            with two random slower days from being flagged.
"""
from __future__ import annotations

import pandas as pd

OUTPUT_COLUMNS = [
    "vehicle_id", "trips_completed", "earnings", "fuel_cost", "maintenance_cost", "total_cost",
    "profit", "profit_margin", "distance_covered", "cost_per_km", "is_unprofitable",
    "trending_unprofitable", "consecutive_profit_drops",
]
TRENDING_MIN_DROPS = 2


def compute_profitability(
    earnings: pd.DataFrame,
    expenses: pd.DataFrame,
    previous: pd.DataFrame,
    margin_threshold: float,
    trending_margin_threshold: float = 0.30,
) -> pd.DataFrame:
    """Build one profitability row per vehicle.

    Args:
        earnings: vehicle_id, trips_completed, total_earnings (from the lake).
        expenses: vehicle_id, fuel_cost, maintenance_cost, distance_covered.
        previous: yesterday's rows: vehicle_id, profit, consecutive_profit_drops
            (empty on the first day).
        margin_threshold: e.g. 0.10 -> a margin below 10% counts as unprofitable.
        trending_margin_threshold: danger zone for the trend flag (e.g. 0.30).
    """
    df = earnings.rename(columns={"total_earnings": "earnings"}).merge(
        expenses[["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered"]],
        on="vehicle_id",
        how="outer",  # keep vehicles that have earnings but no expense row, and vice versa
    )
    numeric = ["trips_completed", "earnings", "fuel_cost", "maintenance_cost", "distance_covered"]
    df[numeric] = df[numeric].fillna(0)
    df["trips_completed"] = df["trips_completed"].astype(int)

    df["total_cost"] = df["fuel_cost"] + df["maintenance_cost"]
    df["profit"] = df["earnings"] - df["total_cost"]
    df["profit_margin"] = (df["profit"] / df["earnings"]).where(df["earnings"] > 0)
    df["cost_per_km"] = (df["total_cost"] / df["distance_covered"]).where(df["distance_covered"] > 0)
    # margin is NaN when earnings are 0; fillna avoids NaN comparisons (profit < 0 covers that case).
    margin = df["profit_margin"].fillna(0.0)
    df["is_unprofitable"] = (df["profit"] < 0) | (df["profit_margin"].notna() & (margin < margin_threshold))

    # Trend: compare with yesterday. A drop extends the streak; anything else resets it.
    prev = previous.rename(columns={"profit": "prev_profit", "consecutive_profit_drops": "prev_drops"})
    df = df.merge(prev[["vehicle_id", "prev_profit", "prev_drops"]], on="vehicle_id", how="left")
    # No yesterday row -> prev_profit is NaN -> "no drop": nothing is below minus infinity.
    dropped = df["profit"] < df["prev_profit"].fillna(float("-inf"))
    df["consecutive_profit_drops"] = (df["prev_drops"].fillna(0).astype(int) + 1).where(dropped, 0)
    in_danger_zone = (df["profit"] < 0) | (margin < trending_margin_threshold)
    df["trending_unprofitable"] = (df["consecutive_profit_drops"] >= TRENDING_MIN_DROPS) & in_danger_zone

    money = ["earnings", "fuel_cost", "maintenance_cost", "total_cost", "profit", "distance_covered", "cost_per_km"]
    df[money] = df[money].round(2)
    df["profit_margin"] = df["profit_margin"].round(4)
    return df[OUTPUT_COLUMNS].sort_values("vehicle_id").reset_index(drop=True)
