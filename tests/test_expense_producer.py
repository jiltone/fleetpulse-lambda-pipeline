"""Simulated expense data stays realistic however long the stack runs."""
from __future__ import annotations

from datetime import date, timedelta

from common.fleet import VehicleProfile
from producers.expense_batch_producer import MAINTENANCE_CYCLE_DAYS, expense_for_vehicle

HIGH_COST = VehicleProfile("V004", "D004", "Colombo-01", lazy=False, high_cost=True)
START = date(2025, 1, 1)  # SIM_START_DATE in tests/conftest.py


def maintenance(day: int) -> float:
    return float(expense_for_vehicle(HIGH_COST, START + timedelta(days=day))["maintenance_cost"])


def test_high_cost_maintenance_rises_within_a_cycle():
    costs = [maintenance(d) for d in range(MAINTENANCE_CYCLE_DAYS)]
    assert all(later > earlier for earlier, later in zip(costs, costs[1:]))


def test_high_cost_maintenance_is_bounded_after_years():
    """Regression: unbounded growth reached ~3.5M LKR/day after ~1,190 sim days."""
    ceiling = 10000 * (1 + 0.30 * (MAINTENANCE_CYCLE_DAYS - 1))
    assert max(maintenance(d) for d in range(1180, 1200)) <= ceiling
