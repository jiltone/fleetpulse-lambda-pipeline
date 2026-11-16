"""Shared simulated clock: 1 simulated day = SIM_DAY_SECONDS real seconds.

Why we need this: the batch layer runs "once per day", but nobody wants to
wait 24 hours in a demo. So we speed time up. With SIM_DAY_SECONDS=600 the
simulation runs 144x faster than reality (86400 / 600 = 144):
    10 real minutes  = 1 simulated day
    1 real minute    = 2.4 simulated hours
    25 real seconds  = 1 simulated hour

How all containers agree: the first component to start writes the current
real Unix time into SIM_ANCHOR_FILE (in the shared ./data folder). Every
component converts real time to simulated time with the same formula:

    sim_time = SIM_START_DATE 00:00 + (real_now - anchor) * speedup
"""
from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path

SECONDS_PER_DAY = 86_400


def sim_day_seconds() -> int:
    """Real seconds that make up one simulated day (default 600)."""
    return int(os.getenv("SIM_DAY_SECONDS", "600"))


def speedup() -> float:
    """How many times faster simulated time runs than real time (600 s/day -> 144x)."""
    return SECONDS_PER_DAY / sim_day_seconds()


def sim_start_date() -> date:
    """Simulated calendar date of day 0."""
    return date.fromisoformat(os.getenv("SIM_START_DATE", "2025-01-01"))


@lru_cache(maxsize=None)
def _read_or_create_anchor(path_str: str) -> float:
    """Read the anchor file, creating it with the current time if it does not exist.

    Opening with mode "x" is atomic: if two containers start at the same
    moment, only one succeeds in creating the file and the other just reads it.
    The result is cached so we do not hit the disk on every event.
    """
    path = Path(path_str)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as f:
            f.write(f"{time.time():.3f}")
    except FileExistsError:
        pass
    # Another process may have created the file a moment ago and still be
    # writing it, so retry briefly until it contains a number.
    for _ in range(50):
        text = path.read_text(encoding="utf-8").strip()
        if text:
            return float(text)
        time.sleep(0.1)
    raise RuntimeError(f"Sim clock anchor file {path} is empty")


def anchor_epoch() -> float:
    """Real Unix time at which the simulation started (simulated day 0, 00:00).

    SIM_ANCHOR_EPOCH (if set) wins over the file; this is handy for tests.
    """
    override = os.getenv("SIM_ANCHOR_EPOCH")
    if override:
        return float(override)
    return _read_or_create_anchor(os.getenv("SIM_ANCHOR_FILE", "data/sim_anchor.txt"))


def to_sim_datetime(real_epoch: float) -> datetime:
    """Convert a real Unix timestamp to a simulated UTC datetime."""
    elapsed_real = max(0.0, real_epoch - anchor_epoch())  # never before day 0
    start = datetime.combine(sim_start_date(), datetime.min.time(), tzinfo=timezone.utc)
    return start + timedelta(seconds=elapsed_real * speedup())


def sim_now() -> datetime:
    """Current simulated date and time."""
    return to_sim_datetime(time.time())


def current_sim_date() -> date:
    """Current simulated calendar date (the 'today' of the simulation)."""
    return sim_now().date()


def sim_day_index(sim_date: date | None = None) -> int:
    """Number of simulated days since SIM_START_DATE (day 0, 1, 2, ...)."""
    return ((sim_date or current_sim_date()) - sim_start_date()).days


def sim_hour() -> int:
    """Current simulated hour of day (0-23); used for rush-hour behaviour."""
    return sim_now().hour


def sim_minutes_to_real_seconds(sim_minutes: float) -> float:
    """Convert a duration in simulated minutes to real seconds (90 sim-min -> 37.5 s)."""
    return sim_minutes * 60 / speedup()


def real_seconds_to_sim_minutes(real_seconds: float) -> float:
    """Convert a real duration in seconds to simulated minutes."""
    return real_seconds * speedup() / 60


def real_window_for_sim_date(sim_date: date) -> tuple[float, float]:
    """Real Unix time range [start, end) that corresponds to one simulated day.

    The batch layer uses this to know which real-time events belong to a day.
    """
    start = anchor_epoch() + sim_day_index(sim_date) * sim_day_seconds()
    return start, start + sim_day_seconds()


def real_seconds_until_next_sim_day() -> float:
    """Real seconds left until the next simulated midnight."""
    _, end = real_window_for_sim_date(current_sim_date())
    return max(0.0, end - time.time())
