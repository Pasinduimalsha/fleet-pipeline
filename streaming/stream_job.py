"""Speed layer: Kafka -> Spark Structured Streaming -> Postgres / Parquet lake.

Four independent streaming queries share one Kafka topic subscription
pattern (each gets its own consumer group and checkpoint):

1. ``archive_and_dlq``   -- splits valid/malformed records, archives valid
                             events to the Parquet lake for batch replay,
                             forwards malformed payloads to the DLQ topic.
2. ``zone_window``        -- 5-minute/1-minute sliding window aggregation of
                             utilization + earnings per zone -> rt_zone_metrics.
3. ``fleet_window``        -- same aggregation without the zone grouping ->
                             rt_fleet_metrics.
4. ``vehicle_state``       -- tracks each vehicle's latest status (arbitrary
                             stateful streaming aggregation via groupBy +
                             max_by) and raises an idle-duration alert once a
                             vehicle has been idle past the configured
                             threshold.

Consistent with a Lambda architecture: this is the always-on speed layer:
low-latency, approximate/windowed. The batch layer (Airflow DAG) reprocesses
the same raw events from the Parquet lake nightly for the authoritative
per-vehicle profitability numbers and cross-checks totals against this
speed-layer output (see ``consistency_checks``).
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from common.config import settings
from common.db import fetch_one, get_connection, mark_heartbeat, upsert_rows
from common.logging import get_logger
from common.metrics import (
    INVALID_RECORDS_TOTAL,
    RECORDS_TOTAL,
    SECONDS_SINCE_LAST_EVENT,
    HeartbeatGauge,
    start_metrics_server,
)
from common.simclock import sim_day, sim_hour

log = get_logger("streaming")

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType()),
        StructField("driver_id", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("zone", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("status", StringType()),
        StructField("fare", DoubleType()),
        StructField("trip_completed", BooleanType()),
        StructField("timestamp", TimestampType()),
        StructField("_corrupt_record", StringType()),
    ]
)

_heartbeat: HeartbeatGauge | None = None
_dlq_producer: KafkaProducer | None = None


def _valid_cond():
    return (
        F.col("_corrupt_record").isNull()
        & F.col("vehicle_id").isNotNull()
        & F.col("status").isNotNull()
        & F.col("zone").isNotNull()
        & F.col("timestamp").isNotNull()
    )


def read_parsed(spark: SparkSession) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return raw.select(
        F.col("value").cast("string").alias("raw_value"),
        F.from_json(
            F.col("value").cast("string"),
            EVENT_SCHEMA,
            {"columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("data"),
    ).select("raw_value", "data.*")


def _get_dlq_producer() -> KafkaProducer:
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = KafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    return _dlq_producer


def _to_utc(pandas_ts) -> datetime:
    py_dt = pandas_ts.to_pydatetime()
    return py_dt if py_dt.tzinfo else py_dt.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------- Query 1


def process_archive_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return

    valid_cond = _valid_cond()
    valid = batch_df.filter(valid_cond)
    invalid = batch_df.filter(~valid_cond)

    valid_rows = valid.select(
        "trip_id", "driver_id", "vehicle_id", "zone", "lat", "lon",
        "speed", "status", "fare", "trip_completed", "timestamp",
    ).toPandas()
    invalid_rows = [r["raw_value"] for r in invalid.select("raw_value").collect()]

    if len(valid_rows):
        if _heartbeat:
            _heartbeat.touch()
        RECORDS_TOTAL.labels(component="streaming").inc(len(valid_rows))
        valid_rows["sim_day"] = valid_rows["timestamp"].apply(lambda ts: sim_day(_to_utc(ts)))
        for day, group in valid_rows.groupby("sim_day"):
            day_dir = os.path.join(settings.lake_path, "raw_events", f"sim_day={int(day)}")
            os.makedirs(day_dir, exist_ok=True)
            out_path = os.path.join(day_dir, f"part-{batch_id}-{uuid.uuid4().hex[:8]}.parquet")
            group.drop(columns=["sim_day"]).to_parquet(out_path, index=False)

    if invalid_rows:
        INVALID_RECORDS_TOTAL.labels(component="streaming").inc(len(invalid_rows))
        producer = _get_dlq_producer()
        for raw in invalid_rows:
            producer.send(settings.kafka_dlq_topic, value=(raw or "").encode("utf-8"))
        producer.flush()

    with get_connection() as conn:
        mark_heartbeat(
            conn, "stream_sink",
            detail=f"batch {batch_id}: {len(valid_rows)} valid, {len(invalid_rows)} invalid",
        )
    log.info("batch_archived", batch_id=batch_id, valid=len(valid_rows), invalid=len(invalid_rows))


# ---------------------------------------------------------------- Queries 2 & 3


def _windowed_aggregate(spark: SparkSession, group_by_zone: bool) -> DataFrame:
    parsed = read_parsed(spark).filter(_valid_cond())
    group_cols = [F.window("timestamp", "5 minutes", "1 minute")]
    if group_by_zone:
        group_cols.append(F.col("zone"))
    return (
        parsed.withWatermark("timestamp", "2 minutes")
        .groupBy(*group_cols)
        .agg(
            F.approx_count_distinct(F.when(F.col("status") != "idle", F.col("vehicle_id"))).alias("active_vehicles"),
            F.approx_count_distinct(F.when(F.col("status") == "idle", F.col("vehicle_id"))).alias("idle_vehicles"),
            F.sum(F.when(F.col("trip_completed"), F.lit(1)).otherwise(F.lit(0))).alias("trips_completed"),
            F.sum(F.when(F.col("trip_completed"), F.col("fare")).otherwise(F.lit(0.0))).alias("earnings"),
        )
    )


def sink_zone_metrics(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    pdf = batch_df.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "zone", "active_vehicles", "idle_vehicles", "trips_completed", "earnings",
    ).toPandas()

    rows = []
    for _, r in pdf.iterrows():
        ws = _to_utc(r["window_start"])
        active, idle = int(r["active_vehicles"] or 0), int(r["idle_vehicles"] or 0)
        total = active + idle
        rows.append(
            {
                "window_start": ws,
                "window_end": _to_utc(r["window_end"]),
                "zone": r["zone"],
                "sim_day": sim_day(ws),
                "sim_hour": sim_hour(ws),
                "active_vehicles": active,
                "idle_vehicles": idle,
                "idle_ratio": (idle / total) if total else None,
                "trips_completed": int(r["trips_completed"] or 0),
                "earnings": float(r["earnings"] or 0.0),
            }
        )
    with get_connection() as conn:
        upsert_rows(conn, "rt_zone_metrics", rows, conflict_cols=["window_start", "zone"])


def sink_fleet_metrics(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    pdf = batch_df.select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "active_vehicles", "idle_vehicles", "trips_completed", "earnings",
    ).toPandas()

    rows = []
    for _, r in pdf.iterrows():
        ws = _to_utc(r["window_start"])
        active, idle = int(r["active_vehicles"] or 0), int(r["idle_vehicles"] or 0)
        total = active + idle
        rows.append(
            {
                "window_start": ws,
                "window_end": _to_utc(r["window_end"]),
                "sim_day": sim_day(ws),
                "sim_hour": sim_hour(ws),
                "active_vehicles": active,
                "idle_vehicles": idle,
                "idle_ratio": (idle / total) if total else None,
                "trips_completed": int(r["trips_completed"] or 0),
                "earnings": float(r["earnings"] or 0.0),
            }
        )
    with get_connection() as conn:
        upsert_rows(conn, "rt_fleet_metrics", rows, conflict_cols=["window_start"])


# ---------------------------------------------------------------- Query 4


def vehicle_latest_stream(spark: SparkSession) -> DataFrame:
    parsed = read_parsed(spark).filter(_valid_cond())
    return (
        parsed.withWatermark("timestamp", "2 minutes")
        .groupBy("vehicle_id")
        .agg(F.max_by(F.struct("status", "zone", "timestamp"), F.col("timestamp")).alias("latest"))
        .select("vehicle_id", "latest.status", "latest.zone", "latest.timestamp")
    )


def _insert_idle_alert(conn, vehicle_id: str, idle_since: datetime) -> None:
    message = f"Vehicle {vehicle_id} idle since {idle_since.isoformat()}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alerts (alert_type, vehicle_id, severity, message, sim_day)
            VALUES ('VEHICLE_IDLE', %s, 'warning', %s, %s)
            ON CONFLICT (alert_type, vehicle_id, message) DO NOTHING
            """,
            (vehicle_id, message, sim_day()),
        )


def sink_vehicle_state(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    pdf = batch_df.toPandas()

    with get_connection() as conn:
        for _, r in pdf.iterrows():
            vehicle_id, new_status, zone = r["vehicle_id"], r["status"], r["zone"]
            event_ts = _to_utc(r["timestamp"])

            prev = fetch_one(conn, "SELECT status, idle_since FROM vehicle_state WHERE vehicle_id = %s", (vehicle_id,))
            if new_status == "idle":
                idle_since = prev["idle_since"] if prev and prev["status"] == "idle" and prev["idle_since"] else event_ts
            else:
                idle_since = None

            upsert_rows(
                conn, "vehicle_state",
                [{"vehicle_id": vehicle_id, "status": new_status, "zone": zone, "last_event_ts": event_ts, "idle_since": idle_since}],
                conflict_cols=["vehicle_id"],
            )

            if idle_since is not None:
                idle_minutes = (datetime.now(timezone.utc) - idle_since).total_seconds() / 60.0
                if idle_minutes >= settings.idle_alert_minutes:
                    _insert_idle_alert(conn, vehicle_id, idle_since)

        mark_heartbeat(conn, "stream_sink", detail=f"vehicle_state batch {batch_id}: {len(pdf)} vehicles")


# ---------------------------------------------------------------- Entry point


def main() -> None:
    global _heartbeat

    spark = (
        SparkSession.builder.appName("fleet-speed-layer")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    start_metrics_server(settings.metrics_port_streaming)
    _heartbeat = HeartbeatGauge(SECONDS_SINCE_LAST_EVENT)
    _heartbeat.start()

    checkpoints = settings.checkpoints_path
    queries = [
        read_parsed(spark)
        .writeStream.foreachBatch(process_archive_batch)
        .option("checkpointLocation", os.path.join(checkpoints, "archive_dlq"))
        .trigger(processingTime="10 seconds")
        .start(),
        _windowed_aggregate(spark, group_by_zone=True)
        .writeStream.outputMode("update")
        .foreachBatch(sink_zone_metrics)
        .option("checkpointLocation", os.path.join(checkpoints, "zone_metrics"))
        .trigger(processingTime="15 seconds")
        .start(),
        _windowed_aggregate(spark, group_by_zone=False)
        .writeStream.outputMode("update")
        .foreachBatch(sink_fleet_metrics)
        .option("checkpointLocation", os.path.join(checkpoints, "fleet_metrics"))
        .trigger(processingTime="15 seconds")
        .start(),
        vehicle_latest_stream(spark)
        .writeStream.outputMode("update")
        .foreachBatch(sink_vehicle_state)
        .option("checkpointLocation", os.path.join(checkpoints, "vehicle_state"))
        .trigger(processingTime="10 seconds")
        .start(),
    ]

    log.info("streaming_job_started", queries=len(queries), topic=settings.kafka_topic)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
