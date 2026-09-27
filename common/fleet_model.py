"""Shared fleet reference data: zones, vehicle roster, fare model.

Both simulators (streaming telemetry + daily expenses) and the batch
reconciliation job import this module so vehicle IDs and zone names never
drift apart between the two data sources.
"""
from __future__ import annotations

from dataclasses import dataclass

from common.config import settings

ZONES: dict[str, dict[str, tuple[float, float]]] = {
    "downtown": {"lat": (6.925, 6.945), "lon": (79.845, 79.860)},
    "airport": {"lat": (7.170, 7.185), "lon": (79.880, 79.895)},
    "suburbs_north": {"lat": (6.960, 6.985), "lon": (79.895, 79.915)},
    "suburbs_south": {"lat": (6.820, 6.845), "lon": (79.860, 79.880)},
    "industrial_park": {"lat": (6.960, 6.975), "lon": (79.960, 79.980)},
}

ZONE_NAMES = list(ZONES.keys())

BASE_FARE = 3.5
RATE_PER_KM = 0.85


@dataclass(frozen=True)
class Vehicle:
    vehicle_id: str
    driver_id: str
    home_zone: str


def vehicle_roster(num_vehicles: int | None = None) -> list[Vehicle]:
    n = num_vehicles or settings.num_vehicles
    roster = []
    for i in range(1, n + 1):
        zone = ZONE_NAMES[i % len(ZONE_NAMES)]
        roster.append(
            Vehicle(
                vehicle_id=f"V{i:04d}",
                driver_id=f"D{i:04d}",
                home_zone=zone,
            )
        )
    return roster
