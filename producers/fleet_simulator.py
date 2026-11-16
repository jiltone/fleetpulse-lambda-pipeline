"""Vehicle state machine for the telemetry simulator (pure logic, no Kafka).

Each vehicle cycles through:

    idle  --(ride request)-->  enroute  --(passenger picked up)-->  on_trip
      ^                                                                 |
      +---------------------(trip completed, fare emitted)-------------+

Durations are chosen in SIMULATED minutes and converted to real seconds
with the shared sim clock, so behaviour scales with SIM_DAY_SECONDS.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from common import sim_clock
from common.fleet import ZONES, VehicleProfile, random_point_in_zone

# Morning and evening rush (simulated hours): more requests, shorter idle time.
RUSH_HOURS = {7, 8, 9, 17, 18, 19}
NIGHT_HOURS = set(range(0, 6))

FARE_BASE_LKR = 150.0     # flag-down fare
FARE_PER_KM_LKR = 95.0    # distance rate
RUSH_SURGE = 1.2          # 20% surge pricing at rush hour

# Normal vehicles never idle longer than this, so only "lazy" vehicles
# cross the idle-alert threshold (90 sim-minutes by default).
MAX_NORMAL_IDLE_SIM_MIN = 75.0


@dataclass
class VehicleState:
    """Mutable runtime state of one simulated vehicle."""

    profile: VehicleProfile
    status: str
    zone: str
    lat: float
    lon: float
    phase_ends: float                 # real epoch when the current status ends
    trip_id: str | None = None
    phase_started: float = 0.0
    start_lat: float = 0.0            # trip start point (for interpolation)
    start_lon: float = 0.0
    dest_zone: str | None = None
    dest_lat: float = 0.0
    dest_lon: float = 0.0
    trip_speed_kmh: float = 0.0
    trip_sim_minutes: float = 0.0


def idle_sim_minutes(hour: int, lazy: bool, rng: random.Random) -> float:
    """Pick how long a vehicle waits for its next ride (simulated minutes).

    Rush hour -> short waits, night -> long waits, lazy vehicles -> very long
    waits (150-300 sim-min) so they reliably trigger idle alerts.
    """
    if lazy:
        return rng.uniform(150, 300)
    if hour in RUSH_HOURS:
        mean = 10.0
    elif hour in NIGHT_HOURS:
        mean = 45.0
    else:
        mean = 25.0
    # Exponential waits are a classic model of random arrivals (ride requests).
    return min(rng.expovariate(1 / mean), MAX_NORMAL_IDLE_SIM_MIN)


def compute_fare(trip_km: float, rush: bool) -> float:
    """Fare in LKR = flag-down + per-km rate, with surge at rush hour."""
    fare = FARE_BASE_LKR + FARE_PER_KM_LKR * trip_km
    if rush:
        fare *= RUSH_SURGE
    return round(fare, 2)


def new_vehicle_state(profile: VehicleProfile, now: float, rng: random.Random) -> VehicleState:
    """Start a vehicle idle in its home zone with a random remaining idle time."""
    lat, lon = random_point_in_zone(profile.home_zone, rng)
    wait = idle_sim_minutes(sim_clock.to_sim_datetime(now).hour, profile.lazy, rng)
    return VehicleState(
        profile=profile,
        status="idle",
        zone=profile.home_zone,
        lat=lat,
        lon=lon,
        phase_started=now,
        # Stagger start-up so all vehicles don't get a ride at the same moment.
        phase_ends=now + sim_clock.sim_minutes_to_real_seconds(wait * rng.random()),
    )


def _start_phase(state: VehicleState, status: str, sim_minutes: float, now: float) -> None:
    """Switch a vehicle to a new status lasting `sim_minutes` simulated minutes."""
    state.status = status
    state.phase_started = now
    state.phase_ends = now + sim_clock.sim_minutes_to_real_seconds(sim_minutes)


def make_event(state: VehicleState, now: float, speed: float, fare: float = 0.0) -> dict:
    """Build one telemetry event (the JSON message sent to Kafka).

    `timestamp` is the real event time (used for Spark windows/watermarks);
    `sim_time`/`sim_date` are the simulated equivalents (used for the lake
    partitioning and the daily batch).
    """
    sim_dt = sim_clock.to_sim_datetime(now)
    return {
        "event_id": uuid.uuid4().hex,
        "trip_id": state.trip_id,
        "driver_id": state.profile.driver_id,
        "vehicle_id": state.profile.vehicle_id,
        "lat": round(state.lat, 6),
        "lon": round(state.lon, 6),
        "speed": round(speed, 1),
        "status": state.status,
        "fare": fare,
        "zone": state.zone,
        "timestamp": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(timespec="milliseconds"),
        "sim_time": sim_dt.isoformat(timespec="seconds"),
        "sim_date": sim_dt.date().isoformat(),
    }


def step(state: VehicleState, now: float, rng: random.Random) -> dict:
    """Advance one vehicle to time `now` and return the event it emits.

    A trip-completion event has status "on_trip" and fare > 0. Every other
    event has fare = 0. Downstream: trips = count(fare > 0), earnings = sum(fare).
    """
    hour = sim_clock.to_sim_datetime(now).hour
    rush = hour in RUSH_HOURS

    # ---- 1. State transitions when the current phase is over ----
    if now >= state.phase_ends:
        if state.status == "idle":
            # Ride request accepted: drive to the pickup point (5-15 sim-min).
            state.trip_id = f"T{uuid.uuid4().hex[:12]}"
            _start_phase(state, "enroute", rng.uniform(5, 15), now)

        elif state.status == "enroute":
            # Passenger picked up: choose a destination (60% stay in the same zone).
            state.dest_zone = state.zone if rng.random() < 0.6 else rng.choice(list(ZONES))
            state.dest_lat, state.dest_lon = random_point_in_zone(state.dest_zone, rng)
            state.start_lat, state.start_lon = state.lat, state.lon
            state.trip_speed_kmh = rng.uniform(18, 35)
            state.trip_sim_minutes = rng.uniform(10, 40)
            _start_phase(state, "on_trip", state.trip_sim_minutes, now)

        elif state.status == "on_trip":
            # Trip finished: emit the fare event (credited to the pickup zone),
            # then move the vehicle to the destination and make it idle.
            state.lat, state.lon = state.dest_lat, state.dest_lon
            trip_km = state.trip_speed_kmh * state.trip_sim_minutes / 60
            event = make_event(state, now, speed=0.0, fare=compute_fare(trip_km, rush))
            state.zone = state.dest_zone or state.zone
            state.trip_id = None
            _start_phase(state, "idle", idle_sim_minutes(hour, state.profile.lazy, rng), now)
            return event

    # ---- 2. Normal position ping for the current status ----
    if state.status == "idle":
        speed = 0.0
    elif state.status == "enroute":
        # Wander a little inside the zone while heading to the pickup.
        min_lat, max_lat, min_lon, max_lon = ZONES[state.zone]
        state.lat = min(max(state.lat + rng.uniform(-0.0005, 0.0005), min_lat), max_lat)
        state.lon = min(max(state.lon + rng.uniform(-0.0005, 0.0005), min_lon), max_lon)
        speed = rng.uniform(20, 35)
    else:  # on_trip: move along a straight line from start to destination
        total = max(state.phase_ends - state.phase_started, 1e-6)
        progress = min((now - state.phase_started) / total, 1.0)
        state.lat = state.start_lat + (state.dest_lat - state.start_lat) * progress
        state.lon = state.start_lon + (state.dest_lon - state.start_lon) * progress
        speed = max(0.0, rng.gauss(state.trip_speed_kmh, 5))
    return make_event(state, now, speed=speed)


BAD_RECORD_TYPES = ("null_vehicle_id", "impossible_coordinates", "negative_fare")


def make_bad_record(event: dict, rng: random.Random) -> tuple[dict, str]:
    """Return a corrupted COPY of an event plus the reason, to test validation/DLQ.

    It is sent in addition to the good event, so the clean data stays complete.
    """
    bad = dict(event, event_id=uuid.uuid4().hex)
    reason = rng.choice(BAD_RECORD_TYPES)
    if reason == "null_vehicle_id":
        bad["vehicle_id"] = None
    elif reason == "impossible_coordinates":
        bad["lat"], bad["lon"] = rng.choice([(999.0, 79.85), (6.9, -500.0), (0.0, 0.0)])
    else:
        bad["fare"] = -round(rng.uniform(100, 2000), 2)
    return bad, reason
