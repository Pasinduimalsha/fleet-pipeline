"""Streaming data source: per-vehicle GPS/telemetry events on Kafka.

Simulates ``settings.num_vehicles`` ride-hailing vehicles, each independently
cycling through idle -> enroute -> on_trip -> idle. One event per vehicle is
published every ``EVENT_INTERVAL_SECONDS`` (default 3s). A small fraction of
events are deliberately corrupted so the streaming job's dead-letter path has
something to catch (Requirement: "robustness of simulated sources").

Fields emitted: trip_id, driver_id, vehicle_id, zone, lat, lon, speed,
status, fare, trip_completed, timestamp. ``zone`` and ``trip_completed`` are
additions to the fields suggested in the brief (explicitly permitted: "you
may adapt scope, e.g. add/change fields as required") -- ``zone`` avoids
reverse-geocoding lat/lon downstream, and ``trip_completed`` marks exactly
one event per finished trip carrying the settled fare, so the speed layer
can sum earnings without double counting.
"""
from __future__ import annotations

import json
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from common.config import settings
from common.fleet_model import BASE_FARE, RATE_PER_KM, ZONES, vehicle_roster
from common.logging import get_logger
from common.metrics import INVALID_RECORDS_TOTAL, RECORDS_TOTAL, start_metrics_server

log = get_logger("producer")

MIN_IDLE_SECONDS = 5
MAX_IDLE_SECONDS = 45
# A small share of idle episodes run long enough to trip the idle-vehicle
# alert, so the alerting path is actually demonstrable in a live session.
STUCK_VEHICLE_RATE = 0.06
STUCK_IDLE_SECONDS = (settings.idle_alert_minutes * 60) + 60

ENROUTE_SECONDS = (10, 30)
ON_TRIP_SECONDS = (20, 90)


class VehicleAgent:
    """Local state machine for one simulated vehicle."""

    def __init__(self, vehicle_id: str, driver_id: str, home_zone: str):
        self.vehicle_id = vehicle_id
        self.driver_id = driver_id
        self.zone = home_zone
        bounds = ZONES[home_zone]
        self.lat = random.uniform(*bounds["lat"])
        self.lon = random.uniform(*bounds["lon"])
        self.status = "idle"
        self.trip_id: str | None = None
        self.state_until = time.monotonic() + random.uniform(MIN_IDLE_SECONDS, MAX_IDLE_SECONDS)
        self.distance_this_trip = 0.0

    def _jitter_position(self) -> None:
        bounds = ZONES[self.zone]
        self.lat = min(max(self.lat + random.uniform(-0.002, 0.002), bounds["lat"][0]), bounds["lat"][1])
        self.lon = min(max(self.lon + random.uniform(-0.002, 0.002), bounds["lon"][0]), bounds["lon"][1])

    def _maybe_switch_zone(self) -> None:
        if random.random() < 0.15:
            self.zone = random.choice(list(ZONES.keys()))
            bounds = ZONES[self.zone]
            self.lat = random.uniform(*bounds["lat"])
            self.lon = random.uniform(*bounds["lon"])

    def tick(self) -> dict:
        now = time.monotonic()
        trip_completed = False
        fare = 0.0

        if now >= self.state_until:
            if self.status == "idle":
                self.status = "enroute"
                self.trip_id = str(uuid.uuid4())
                self.distance_this_trip = 0.0
                self._maybe_switch_zone()
                self.state_until = now + random.uniform(*ENROUTE_SECONDS)
            elif self.status == "enroute":
                self.status = "on_trip"
                self.state_until = now + random.uniform(*ON_TRIP_SECONDS)
            elif self.status == "on_trip":
                fare = round(BASE_FARE + RATE_PER_KM * self.distance_this_trip, 2)
                trip_completed = True
                self.status = "idle"
                self.trip_id = None
                if random.random() < STUCK_VEHICLE_RATE:
                    idle_for = STUCK_IDLE_SECONDS
                else:
                    idle_for = random.uniform(MIN_IDLE_SECONDS, MAX_IDLE_SECONDS)
                self.state_until = now + idle_for

        if self.status == "idle":
            speed = 0.0
        elif self.status == "enroute":
            speed = round(random.uniform(15, 45), 1)
            self._jitter_position()
        else:  # on_trip
            speed = round(random.uniform(0, 50), 1)
            self._jitter_position()
            self.distance_this_trip += speed * (settings.event_interval_seconds / 3600.0)

        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "zone": self.zone,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": speed,
            "status": self.status,
            "fare": fare,
            "trip_completed": trip_completed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
        retries=5,
        acks="all",
    )


def corrupt(event: dict) -> str:
    """Return a deliberately malformed payload to exercise the DLQ path."""
    choice = random.choice(["truncated_json", "missing_field", "wrong_type"])
    if choice == "truncated_json":
        raw = json.dumps(event)
        return raw[: len(raw) // 2]
    if choice == "missing_field":
        broken = dict(event)
        broken.pop("vehicle_id", None)
        return json.dumps(broken)
    broken = dict(event)
    broken["speed"] = "very_fast"  # wrong type
    return json.dumps(broken)


def run() -> None:
    start_metrics_server(settings.metrics_port_producer)
    producer = make_producer()
    vehicles = [VehicleAgent(v.vehicle_id, v.driver_id, v.home_zone) for v in vehicle_roster()]
    log.info("producer_started", num_vehicles=len(vehicles), topic=settings.kafka_topic)

    stopping = False

    def _stop(*_args):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while not stopping:
        loop_start = time.monotonic()
        for vehicle in vehicles:
            event = vehicle.tick()
            is_malformed = random.random() < settings.malformed_event_rate
            payload = corrupt(event) if is_malformed else json.dumps(event)
            try:
                producer.send(settings.kafka_topic, key=vehicle.vehicle_id, value=payload)
                if is_malformed:
                    INVALID_RECORDS_TOTAL.labels(component="producer").inc()
                else:
                    RECORDS_TOTAL.labels(component="producer").inc()
            except KafkaError:
                log.error("produce_failed", vehicle_id=vehicle.vehicle_id, exc_info=True)

        producer.flush()
        elapsed = time.monotonic() - loop_start
        time.sleep(max(0.0, settings.event_interval_seconds - elapsed))

    log.info("producer_stopping")
    producer.close()


if __name__ == "__main__":
    run()
