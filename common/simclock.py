"""Simulated calendar shared by every component.

One "simulated day" compresses to ``settings.sim_day_seconds`` wall-clock
seconds. Every service derives sim_day/sim_hour from a fixed epoch plus the
current wall-clock time instead of passing a counter around, so producers,
the streaming job, the batch simulator and the reconciliation DAG always
agree on "today" without any coordination service.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

from common.config import settings


def _default_epoch() -> datetime:
    """Simulated day 0 starts at UTC midnight of the day the stack first
    imports this module. Every component (producer, streaming job, batch
    simulator, Airflow) does this independently within moments of each
    other at startup, so they agree on the epoch without any coordination,
    and a fresh `docker compose up` always starts a demo near sim_day 0
    instead of the huge day count a fixed historical epoch would produce.
    Override with SIM_EPOCH="YYYY-MM-DD" to pin it (e.g. across restarts).
    """
    override = os.environ.get("SIM_EPOCH")
    if override:
        return datetime.fromisoformat(override).replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


SIM_EPOCH = _default_epoch()


def sim_day(ts: datetime | None = None) -> int:
    ts = ts or datetime.now(timezone.utc)
    elapsed = (ts - SIM_EPOCH).total_seconds()
    return max(0, int(elapsed // settings.sim_day_seconds))


def sim_hour(ts: datetime | None = None) -> int:
    ts = ts or datetime.now(timezone.utc)
    elapsed = (ts - SIM_EPOCH).total_seconds()
    seconds_into_day = elapsed % settings.sim_day_seconds
    fraction_of_day = seconds_into_day / settings.sim_day_seconds
    return int(fraction_of_day * 24) % 24


def seconds_until_next_day(ts: datetime | None = None) -> float:
    ts = ts or datetime.now(timezone.utc)
    elapsed = (ts - SIM_EPOCH).total_seconds()
    remainder = elapsed % settings.sim_day_seconds
    return settings.sim_day_seconds - remainder
