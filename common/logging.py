"""Structured (JSON) logging shared across all pipeline components.

Every log line carries a ``component`` field (producer, streaming,
batch_reconcile, api, ...) so log aggregation can filter by pipeline stage,
matching the "structured logging across ingestion, processing and storage
stages" observability requirement.
"""
from __future__ import annotations

import logging
import sys

import structlog


def _configure_root() -> None:
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


_configure_root()


def get_logger(component: str) -> structlog.BoundLogger:
    """Return a structlog logger bound to a pipeline component name."""
    return structlog.get_logger().bind(component=component)
