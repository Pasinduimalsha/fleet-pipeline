"""Prometheus metrics helpers.

Metric names here are a contract shared with ``observability/alert_rules.yml``
-- do not rename without updating the alert expressions too.
"""
from __future__ import annotations

import threading
import time

from prometheus_client import Counter, Gauge, start_http_server

RECORDS_TOTAL = Counter(
    "fleet_records_total", "Valid telemetry records processed", ["component"]
)
INVALID_RECORDS_TOTAL = Counter(
    "fleet_invalid_records_total", "Malformed/rejected telemetry records", ["component"]
)
SECONDS_SINCE_LAST_EVENT = Gauge(
    "fleet_seconds_since_last_event", "Seconds since the last event reached the pipeline"
)
SECONDS_SINCE_LAST_BATCH_SUCCESS = Gauge(
    "fleet_seconds_since_last_batch_success", "Seconds since the daily reconciliation job last succeeded"
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)


class HeartbeatGauge:
    """Keeps a Prometheus gauge ticking up in wall-clock time in the
    background, reset to zero whenever ``touch()`` is called.

    Used for the "no data received in N minutes" alert: the gauge climbs on
    its own between events instead of only updating on the (irregular)
    micro-batch cadence.
    """

    def __init__(self, gauge: Gauge, initial_seconds: float = 10_000.0):
        self._gauge = gauge
        self._last_touch = time.monotonic() - initial_seconds
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def touch(self) -> None:
        with self._lock:
            self._last_touch = time.monotonic()

    def _run(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                elapsed = time.monotonic() - self._last_touch
            self._gauge.set(elapsed)
            time.sleep(1)

    def stop(self) -> None:
        self._stop.set()
