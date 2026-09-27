"""Serving layer: read API + live dashboard over the Postgres store.

Answers the use case's business question directly:
  * ``/metrics/fleet`` and ``/metrics/zones``  -- "what is fleet utilization
    and earnings by area/time-of-day right now?"
  * ``/reports/profitability``                  -- "which vehicles are
    becoming unprofitable once yesterday's fuel/maintenance costs are
    factored in?"
  * ``/alerts``                                  -- active idle-vehicle alerts.
  * ``/health``                                  -- pipeline health-check
    rule required by the observability section (stale-component detection).
  * ``/metrics``                                  -- Prometheus exposition.
  * ``/`` (dashboard.html)                        -- a small auto-refreshing
    live dashboard, the "consolidated report or dashboard" deliverable.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily
from prometheus_client.registry import Collector, REGISTRY

from common.config import settings
from common.db import fetch_all, fetch_one, get_connection
from common.logging import get_logger

log = get_logger("api")

app = FastAPI(title="Fleet Operations Serving API")

TEMPLATES_DIR = Path(__file__).parent / "templates"

REQUESTS_TOTAL = Counter("fleet_api_requests_total", "API requests", ["path", "method", "status"])
REQUEST_LATENCY = Histogram("fleet_api_request_latency_seconds", "API request latency", ["path"])

# Component -> max age (seconds) before it's considered stale. Backs the
# "no data received in N minutes" health-check requirement.
HEALTH_THRESHOLDS = {
    "stream_sink": 60.0,
    "batch_sim": settings.sim_day_seconds * 2,
    "batch_reconcile": settings.sim_day_seconds * 3,
}


def _seconds_since(component: str) -> float:
    try:
        with get_connection() as conn:
            row = fetch_one(conn, "SELECT last_success_at FROM pipeline_status WHERE component = %s", (component,))
    except Exception:
        log.error("db_unreachable", exc_info=True)
        return 1e9
    if not row or not row["last_success_at"]:
        return 1e9
    return (datetime.now(timezone.utc) - row["last_success_at"]).total_seconds()


class BatchHeartbeatCollector(Collector):
    """Exposes fleet_seconds_since_last_batch_success, computed live from
    pipeline_status at scrape time (the batch job itself is not a
    long-running process that could hold its own Prometheus gauge)."""

    def collect(self):
        gauge = GaugeMetricFamily(
            "fleet_seconds_since_last_batch_success",
            "Seconds since the daily reconciliation job last succeeded",
        )
        gauge.add_metric([], _seconds_since("batch_reconcile"))
        yield gauge


REGISTRY.register(BatchHeartbeatCollector())


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    elapsed = time.monotonic() - start
    path = request.url.path
    REQUESTS_TOTAL.labels(path=path, method=request.method, status=response.status_code).inc()
    REQUEST_LATENCY.labels(path=path).observe(elapsed)
    return response


@app.get("/health")
def health():
    components = {}
    overall = "ok"
    for component, threshold in HEALTH_THRESHOLDS.items():
        age = _seconds_since(component)
        status = "healthy" if age < threshold else "stale"
        if status == "stale":
            overall = "degraded"
        components[component] = {"seconds_since_last_success": round(age, 1), "threshold_seconds": threshold, "status": status}
    return {"status": overall, "components": components, "checked_at": datetime.now(timezone.utc).isoformat()}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)


@app.get("/metrics/fleet")
def fleet_metrics():
    with get_connection() as conn:
        row = fetch_one(conn, "SELECT * FROM rt_fleet_metrics ORDER BY window_start DESC LIMIT 1")
    return row or JSONResponse({"detail": "no speed-layer data yet"}, status_code=404)


@app.get("/metrics/zones")
def zone_metrics():
    with get_connection() as conn:
        rows = fetch_all(
            conn,
            """
            SELECT DISTINCT ON (zone) *
            FROM rt_zone_metrics
            ORDER BY zone, window_start DESC
            """,
        )
    return {"zones": rows}


@app.get("/alerts")
def alerts(vehicle_id: str | None = None, limit: int = 50):
    if vehicle_id:
        with get_connection() as conn:
            rows = fetch_all(
                conn,
                "SELECT * FROM alerts WHERE vehicle_id = %s ORDER BY created_at DESC LIMIT %s",
                (vehicle_id, limit),
            )
    else:
        with get_connection() as conn:
            rows = fetch_all(conn, "SELECT * FROM alerts ORDER BY created_at DESC LIMIT %s", (limit,))
    return {"alerts": rows}


@app.get("/reports/profitability/days")
def profitability_days():
    with get_connection() as conn:
        rows = fetch_all(conn, "SELECT DISTINCT sim_day FROM daily_profitability ORDER BY sim_day DESC")
    return {"sim_days": [r["sim_day"] for r in rows]}


@app.get("/reports/profitability")
def profitability_report(sim_day: int | None = None, unprofitable_only: bool = False):
    with get_connection() as conn:
        if sim_day is None:
            latest = fetch_one(conn, "SELECT MAX(sim_day) AS sim_day FROM daily_profitability")
            sim_day = latest["sim_day"] if latest else None
        if sim_day is None:
            return JSONResponse({"detail": "no daily reconciliation has run yet"}, status_code=404)

        sql = "SELECT * FROM daily_profitability WHERE sim_day = %s"
        params: tuple = (sim_day,)
        if unprofitable_only:
            sql += " AND unprofitable = TRUE"
        sql += " ORDER BY profit ASC NULLS LAST"
        rows = fetch_all(conn, sql, params)
        consistency = fetch_one(conn, "SELECT * FROM consistency_checks WHERE sim_day = %s", (sim_day,))

    return {"sim_day": sim_day, "vehicles": rows, "consistency_check": consistency}


@app.get("/")
def dashboard():
    return FileResponse(TEMPLATES_DIR / "dashboard.html")
