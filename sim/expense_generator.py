"""Daily-batch data source: one expense file per simulated day.

Drops ``expenses_day_{D}.csv`` into ``LANDING_PATH`` once per simulated day,
containing fuel/maintenance costs reported by garages and fuel partners for
day ``D`` (the day that just ended). Written atomically (temp file + rename)
so the Airflow sensor never picks up a partially-written file.

A couple of vehicles are randomly dropped from each day's file to exercise
the "vehicle missing from expense file" (``has_expenses = false``) path in
the reconciliation job -- real feeds from third-party garages are never
perfectly complete.
"""
from __future__ import annotations

import csv
import os
import random
import signal
import time

from common.config import settings
from common.db import get_connection, mark_heartbeat
from common.fleet_model import vehicle_roster
from common.logging import get_logger
from common.simclock import sim_day, seconds_until_next_day

log = get_logger("batch_sim")

FIELDNAMES = ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]


def generate_day_file(day: int, roster) -> str:
    os.makedirs(settings.landing_path, exist_ok=True)
    final_path = os.path.join(settings.landing_path, f"expenses_day_{day}.csv")
    tmp_path = final_path + ".tmp"

    reporting_vehicles = [v for v in roster if random.random() > 0.05]  # ~5% missing from feed

    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for vehicle in reporting_vehicles:
            distance = round(random.uniform(40, 320), 1)
            fuel_cost = round(distance * random.uniform(0.18, 0.30) + random.uniform(-3, 5), 2)
            service_flag = random.random() < 0.08
            maintenance_cost = round(random.uniform(40, 220), 2) if service_flag else round(random.uniform(0, 8), 2)
            writer.writerow(
                {
                    "vehicle_id": vehicle.vehicle_id,
                    "fuel_cost": max(0.0, fuel_cost),
                    "maintenance_cost": maintenance_cost,
                    "distance_covered": distance,
                    "service_flag": service_flag,
                }
            )

    os.replace(tmp_path, final_path)
    return final_path


def run() -> None:
    roster = vehicle_roster()
    stopping = False

    def _stop(*_args):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    last_written_day = None
    log.info("batch_sim_started", sim_day_seconds=settings.sim_day_seconds, num_vehicles=len(roster))

    while not stopping:
        current_day = sim_day()
        # Once the clock rolls past a day boundary, drop the file for the
        # day that just completed (day 0 never gets a file: there is no
        # "yesterday" yet).
        completed_day = current_day - 1
        if completed_day >= 0 and completed_day != last_written_day:
            path = generate_day_file(completed_day, roster)
            last_written_day = completed_day
            log.info("expense_file_written", sim_day=completed_day, path=path, vehicles=len(roster))
            try:
                with get_connection() as conn:
                    mark_heartbeat(conn, "batch_sim", detail=f"wrote day {completed_day}")
            except Exception:
                log.error("heartbeat_failed", exc_info=True)

        time.sleep(min(5.0, max(1.0, seconds_until_next_day())))

    log.info("batch_sim_stopping")


if __name__ == "__main__":
    run()
