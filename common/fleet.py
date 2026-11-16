"""Static description of the simulated fleet, shared by producers and processors.

Keeping zones, vehicle IDs and vehicle "personalities" in one place means the
telemetry producer, the expense producer and the streaming validator all
agree on the same facts.
"""
from __future__ import annotations

import os
import random
from dataclasses import dataclass

# Five Colombo-style zones with approximate lat/lon bounding boxes:
# (min_lat, max_lat, min_lon, max_lon)
ZONES: dict[str, tuple[float, float, float, float]] = {
    "Fort":          (6.930, 6.940, 79.840, 79.852),
    "Kollupitiya":   (6.905, 6.920, 79.848, 79.858),
    "Bambalapitiya": (6.888, 6.905, 79.852, 79.862),
    "Dehiwala":      (6.840, 6.860, 79.860, 79.875),
    "Nugegoda":      (6.860, 6.880, 79.885, 79.905),
}

# Outer box around greater Colombo. Any coordinate outside it is "impossible"
# for this fleet, so the speed layer rejects it and sends it to the DLQ.
COLOMBO_BOUNDS = (6.80, 7.00, 79.80, 79.95)

VALID_STATUSES = ("idle", "enroute", "on_trip")


@dataclass(frozen=True)
class VehicleProfile:
    """Behaviour settings for one vehicle."""

    vehicle_id: str
    driver_id: str
    home_zone: str
    lazy: bool        # long idle periods -> triggers idle alerts
    high_cost: bool   # high fuel/maintenance -> becomes unprofitable


def _id_list(env_name: str, default: str) -> set[str]:
    """Read a comma-separated list of vehicle IDs from an env var."""
    return {v.strip() for v in os.getenv(env_name, default).split(",") if v.strip()}


def build_fleet() -> list[VehicleProfile]:
    """Create the list of vehicles from env settings.

    There are more vehicles (30) than drivers (25), so a few drivers work
    two vehicles in shifts: vehicle i is assigned driver (i mod NUM_DRIVERS).
    """
    num_vehicles = int(os.getenv("NUM_VEHICLES", "30"))
    num_drivers = int(os.getenv("NUM_DRIVERS", "25"))
    lazy = _id_list("LAZY_VEHICLES", "V007,V013,V021")
    high_cost = _id_list("HIGH_COST_VEHICLES", "V004,V011,V019,V027")
    zone_names = list(ZONES)

    fleet = []
    for i in range(num_vehicles):
        vid = f"V{i + 1:03d}"
        fleet.append(
            VehicleProfile(
                vehicle_id=vid,
                driver_id=f"D{(i % num_drivers) + 1:03d}",
                home_zone=zone_names[i % len(zone_names)],
                lazy=vid in lazy,
                high_cost=vid in high_cost,
            )
        )
    return fleet


def random_point_in_zone(zone: str, rng: random.Random) -> tuple[float, float]:
    """Return a random (lat, lon) inside a zone's bounding box."""
    min_lat, max_lat, min_lon, max_lon = ZONES[zone]
    return rng.uniform(min_lat, max_lat), rng.uniform(min_lon, max_lon)


def is_valid_coordinate(lat: float | None, lon: float | None) -> bool:
    """True if the coordinate lies inside greater Colombo."""
    if lat is None or lon is None:
        return False
    min_lat, max_lat, min_lon, max_lon = COLOMBO_BOUNDS
    return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
