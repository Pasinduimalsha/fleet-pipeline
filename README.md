# Fleet Operations Pipeline

EC8203 Data Engineering Mini-Project -- **Use Case 1: Ride-Hailing Fleet Operations**.

A Lambda-architecture pipeline that gives a ride-hailing operator live fleet
utilization/earnings by zone, and a daily per-vehicle profitability
reconciliation once the previous day's fuel/maintenance costs land.

## Architecture

```
                    ┌──────────────────┐
 sim/               │ stream-sim       │  GPS/telemetry events, every 3s/vehicle
 telemetry_producer  │ (Kafka producer) │  (~2% deliberately malformed)
                    └────────┬─────────┘
                             │ trips.telemetry
                             ▼
                    ┌──────────────────┐        ┌─────────────────┐
                    │  Kafka           │───────▶│  streaming       │  SPEED LAYER
                    │  (3 partitions)  │ dlq    │  (Spark          │  (Structured
                    └──────────────────┘◀───────│  Structured      │   Streaming,
                                                 │  Streaming)      │   4 queries)
                                                 └────────┬─────────┘
                                                          │
                             ┌────────────────────────────┼─────────────────────┐
                             ▼                             ▼                     ▼
                   rt_zone_metrics /              vehicle_state /        data/lake/raw_events
                   rt_fleet_metrics                  alerts              (Parquet, sim_day=N)
                   (5-min/1-min windows)         (idle-vehicle alert)     for batch replay
                             │                             │                     │
                             └─────────────┬───────────────┘                     │
                                            ▼                                    │
                                     ┌──────────────┐                            │
                                     │  Postgres    │◀───────────────────────────┘
                                     │  (serving DB)│         (read by Airflow)
                                     └──────┬───────┘
                                            │
                    ┌───────────────────────┼────────────────────────┐
                    ▼                                                 ▼
            ┌──────────────┐                                ┌─────────────────┐
            │  serving/api │  FastAPI + dashboard            │  Airflow DAG     │  BATCH LAYER
            │  :8000       │  (live utilization, alerts,     │  daily_fleet_    │  (reconcile
            └──────────────┘   daily profitability report)   │  reconciliation  │   expenses vs.
                                                               └────────┬─────────┘   trip totals)
                                                                        ▲
                                                               ┌────────┴─────────┐
                                                               │  batch-sim       │  one CSV/
                                                               │  (expense        │  simulated day
                                                               │  generator)      │
                                                               └──────────────────┘
```

## Why Lambda, not Kappa

The use case genuinely needs two different processing semantics operating on
two different arrival cadences:

- **Speed layer** (continuous, approximate, windowed): "what's happening
  right now" -- active/idle vehicles, trips/hour, earnings by zone. Low
  latency matters here; a few seconds of staleness is fine, exactness is not
  required (`approx_count_distinct`, 1-minute sliding windows).
- **Batch layer** (once/day, exact, replayable): per-vehicle profitability
  once fuel/maintenance costs land. This must reconcile against a
  *second, independently-arriving source* (the expense feed) that a pure
  streaming join can't wait indefinitely for, and it must be exactly
  reproducible/auditable (finance-adjacent numbers), which favors a
  recompute-from-source batch job over incremental streaming state.

A Kappa architecture (single streaming pipeline, batch = replay) was
considered and rejected: it would force the daily join against a
once-a-day file through the same streaming engine, which either means
holding an all-day streaming state waiting for a file that arrives once, or
re-deriving a second "batch-shaped" path anyway inside the stream job --
at that point it's a Lambda architecture with extra steps. Lambda's
speed/batch split maps directly onto the two data sources this use case
actually has (continuous telemetry vs. once-daily expense file), and the
`consistency_checks` table (speed-layer totals vs. batch-layer totals per
sim day) turns the classic Lambda "two codepaths can drift" risk into an
observable, queryable metric instead of a hidden assumption.

**Trade-off accepted:** duplicated aggregation logic between the speed
layer's windowed trip/earnings totals and the batch layer's exact recompute
from the Parquet lake. This is exactly the trade-off Lambda architectures
are known for; the `consistency_checks` table and `/reports/profitability`
endpoint's `consistency_check` field make the drift visible rather than
letting it hide.

## Technology stack

| Layer | Choice | Why |
|---|---|---|
| Ingestion | Apache Kafka (3 partitions, keyed by `vehicle_id`) | Required by the brief; partitioning by vehicle preserves per-vehicle event order, which the idle-alert state machine depends on. |
| Stream processing | Apache Spark Structured Streaming (`local[*]`, 4 concurrent queries) | Required by the brief; native windowed aggregation (`groupBy(window(...))`) and arbitrary stateful aggregation (`groupBy` + `max_by`) cover both the windowed-metrics and latest-vehicle-state needs without hand-rolled state management. `local[*]` is a deliberate scale choice -- see Limitations. |
| Batch orchestration | Apache Airflow (`schedule=timedelta(seconds=SIM_DAY_SECONDS)`) | Required by the brief; TaskFlow DAG with a `PythonSensor` waiting for the day's file, then load → compute → consistency-check → report as separate, independently retryable tasks. |
| Batch compute | pandas (not Spark) inside the Airflow task | One sim day's trip volume for a class-scale fleet is a few thousand rows -- a JVM/Spark cluster would add operational weight without a throughput benefit at this scale. Documented as a production-scale trade-off below. |
| Storage / serving | PostgreSQL | Both layers write structured, queryable rows (metrics, alerts, profitability); a relational store with upserts (`ON CONFLICT DO UPDATE`) gives idempotent writes for free under Spark micro-batch retries and Airflow task retries, and lets the API/Grafana query with plain SQL. |
| Raw event archive | Parquet on a local volume (`data/lake/raw_events/sim_day=N/`), partitioned by sim day | Gives the batch layer an authoritative, replayable source independent of the speed layer's windowed approximations -- the actual Lambda "batch reprocesses raw data" property. |
| Serving API | FastAPI + a small static dashboard | Lightweight, typed, auto-docs (`/docs`), easy to add Prometheus instrumentation to. |
| Observability | Prometheus + Grafana + structlog (JSON logs) | Required by the brief; `structlog` gives every log line a `component` field across ingestion/processing/storage stages; Prometheus alert rules cover the "no data in N minutes" and "error rate" requirements explicitly. |

## Observability

- **Structured logging**: every component (`producer`, `streaming`,
  `batch_sim`, `batch_reconcile`, `api`) logs JSON lines via `structlog`
  bound with a `component` field (`common/logging.py`).
- **Metrics** (`observability/prometheus.yml` scrapes 3 targets + itself):
  - `stream-sim:8001` -- `fleet_records_total`, `fleet_invalid_records_total`
    (events produced / deliberately malformed).
  - `streaming:8002` -- `fleet_records_total`, `fleet_invalid_records_total`
    (events actually consumed/rejected), `fleet_seconds_since_last_event`
    (climbs continuously between batches via a background thread, so it
    reflects true staleness, not just the last micro-batch tick).
  - `api:8000` -- `fleet_seconds_since_last_batch_success` (computed live
    from `pipeline_status` at scrape time), plus request count/latency.
- **Alert rules** (`observability/alert_rules.yml`, loaded by Prometheus):
  - `NoDataReceived`: `fleet_seconds_since_last_event > 60` for 15s.
  - `HighInvalidRecordRate`: invalid/total ratio > 5% for 30s.
  - `BatchReconciliationStale`: no successful daily reconciliation for 3
    simulated days.
- **Health-check endpoint**: `GET /health` on the API cross-checks
  `pipeline_status` against per-component staleness thresholds and reports
  `ok`/`degraded` -- the same signal Prometheus uses, exposed for a
  human/demo audience.
- **Business-level alerting**: the streaming job raises a `VEHICLE_IDLE`
  row in the `alerts` table once a vehicle has been idle past
  `IDLE_ALERT_MINUTES`, deduplicated per idle episode via a unique index
  on `(alert_type, vehicle_id, message)`.

## Simulated clock

One simulated day = `SIM_DAY_SECONDS` real seconds (default **300s = 5
minutes**). Every component derives `sim_day`/`sim_hour` independently from
`common/simclock.py`, using UTC midnight of the day the stack starts as
day 0 -- no coordination service needed, since every container computes
the same value within moments of each other at startup. Because 5 minutes
compresses a full day, `sim_day` climbs quickly (e.g. by ~90 within the
first 7.5 real hours after midnight UTC); for a crisp "day 0" demo start,
set `SIM_EPOCH` in `.env` to the current UTC timestamp right before you
bring the stack up:

```bash
echo "SIM_EPOCH=$(date -u +%Y-%m-%dT%H:%M:%S)" >> .env
```

## Running it

Requires Docker Desktop with **>= 6 GB** memory (Kafka + Spark + Airflow +
Postgres + Prometheus/Grafana all run locally).

```bash
make up        # creates .env from .env.example if missing, builds, starts everything
make logs      # tail all services
make topics    # show Kafka topic/partition layout
make psql      # psql shell on the serving DB
make test      # pure-python unit tests, no Docker required
make down      # stop, keep data
make reset     # stop + wipe volumes and generated data/reports
```

First `spark-submit` invocation downloads the `spark-sql-kafka` connector
jars via Maven (`--packages`, cached in the `spark-ivy` volume across
restarts) -- this needs internet access the first time only.

### URLs

| Service | URL | Notes |
|---|---|---|
| Fleet dashboard | http://localhost:8000/ | Live utilization + daily profitability, auto-refreshes every 5s |
| API docs | http://localhost:8000/docs | OpenAPI/Swagger |
| Airflow | http://localhost:8088 | admin/admin (from `.env`) |
| Prometheus | http://localhost:9090 | Targets: Status -> Targets |
| Grafana | http://localhost:3001 | admin/admin; Postgres + Prometheus datasources pre-provisioned |

### Key API endpoints

- `GET /metrics/fleet`, `GET /metrics/zones` -- live utilization (business
  question: "what is fleet utilization and earnings by area/time-of-day
  right now?").
- `GET /reports/profitability?sim_day=N` -- daily reconciliation report,
  including the Lambda consistency check (business question: "which
  vehicles are becoming unprofitable once yesterday's costs are
  factored in?").
- `GET /alerts` -- active idle-vehicle alerts.
- `GET /health` -- pipeline health-check rule.
- `GET /metrics` -- Prometheus exposition.

The same daily report is also written to `reports/daily_profitability_day_N.md`
by the Airflow DAG (the "scheduled report file" deliverable, independent of
the live dashboard).

## Repository layout

```
common/       shared config, structured logging, DB helpers, metrics, sim clock, fleet/zone model
sim/          simulated data sources: telemetry_producer.py (streaming), expense_generator.py (daily batch)
streaming/    Spark Structured Streaming speed layer (stream_job.py)
airflow/      dags/ (DAG definition) + jobs/reconcile.py (pure, testable batch logic)
serving/      FastAPI serving layer + dashboard.html
sql/          Postgres schema (idempotent upserts throughout)
observability/ Prometheus scrape/alert config, Grafana provisioning
docker/       per-service Dockerfiles
tests/        pytest unit tests for the pure-logic pieces (no Docker required)
```

## Limitations, trade-offs, and what would change at production scale

- **Spark runs in `local[*]` mode**, not a real cluster. Correct choice at
  this data volume (one class-scale fleet), but at production scale you'd
  run on YARN/Kubernetes with autoscaling executors, and would need to
  revisit the `foreachBatch`-collects-to-driver pattern used for the
  Postgres sinks (fine for a few hundred rows/micro-batch here; would need
  a partitioned JDBC write or a proper sink connector at higher volume).
- **Batch reconciliation uses pandas, not Spark**, deliberately (see stack
  table above). At real fleet scale (thousands of vehicles, millions of
  trips/day) this would move to the same Spark cluster as the speed layer.
- **Idle-vehicle alerting is per-micro-batch stateful logic in Python**
  (read-then-write against `vehicle_state`), not Spark's built-in
  `applyInPandasWithState`. Simpler to reason about and test at this
  scale; would not scale past a single-partition bottleneck on
  `vehicle_state` writes.
- **No exactly-once guarantee end-to-end.** Postgres upserts make the sinks
  idempotent under replay/retry, but a crash between Kafka commit and
  Postgres write can still double-process a micro-batch's *effects* being
  computed twice (harmless here since upserts overwrite, but would matter
  for e.g. billing).
- **Single-broker Kafka, single-node Postgres.** No replication; acceptable
  for a 2-week class project, not for production availability.
- **Vehicle roster and zone geography are synthetic** (`common/fleet_model.py`);
  a real deployment would source these from a fleet-management system.
- **Malformed-event injection is coarse** (~2% of events, three corruption
  modes). Good enough to exercise the DLQ path; not a substitute for a
  real schema-registry-enforced contract.

## Assumptions

- 1 simulated day = 300 real seconds by default (`SIM_DAY_SECONDS` in `.env`).
- The daily expense file for sim day *D* is expected once sim day *D+1*
  begins (i.e. "yesterday's" costs), matching the business question's
  "reconciling it daily against ... costs" framing.
- `zone` and `trip_completed` are additions to the fields suggested in the
  brief (explicitly permitted -- "you may adapt scope, e.g. add/change
  fields as required"): `zone` avoids needing reverse-geocoding downstream,
  and `trip_completed` marks exactly one event per finished trip carrying
  the settled fare, so the speed layer can sum earnings without
  double-counting a trip's in-progress events.
