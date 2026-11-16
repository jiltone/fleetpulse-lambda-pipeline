"""SPEED LAYER: Spark Structured Streaming job (Kafka -> Postgres + Parquet lake + DLQ).

Three streaming queries share one parsed + validated input:

  1. dlq           invalid records            -> Kafka topic fleet.telemetry.dlq
  2. clean_events  valid, de-duplicated events -> Parquet lake (master dataset)
                                               -> rt_vehicle_status + idle_alerts
  3. zone_metrics  1-minute tumbling windows per zone -> rt_zone_metrics

Fault tolerance: every query has its own checkpoint folder holding Kafka
offsets and aggregation state. If the container crashes, Docker restarts it
and Spark resumes from the last committed micro-batch. Writes are upserts
(Postgres) or de-duplicated later by event_id (lake), so a replayed batch is harmless.
"""
from __future__ import annotations

import time
from contextlib import closing
from typing import Callable

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import sim_clock
from common.config import get_float, get_int, get_str
from common.logging_config import get_logger
from streaming import postgres_sinks as pg
from streaming.validation import add_validation, parse_events

log = get_logger("spark-streaming", stage="processing")

# ---------------- configuration ----------------
KAFKA_BOOTSTRAP = get_str("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TELEMETRY_TOPIC = get_str("KAFKA_TELEMETRY_TOPIC", "fleet.telemetry")
DLQ_TOPIC = get_str("KAFKA_DLQ_TOPIC", "fleet.telemetry.dlq")
LAKE_PATH = get_str("LAKE_PATH", "data/lake/raw/telemetry")
CHECKPOINT_DIR = get_str("CHECKPOINT_DIR", "data/checkpoints")
TRIGGER = f"{get_int('SPARK_TRIGGER_SECONDS', 10)} seconds"
WATERMARK = get_str("STREAM_WATERMARK", "2 minutes")
WINDOW_SECONDS = get_int("STREAM_WINDOW_SECONDS", 60)
IDLE_ALERT_SIM_MINUTES = get_float("IDLE_ALERT_SIM_MINUTES", 90)

# ---------------- Prometheus metrics (served on :STREAMING_METRICS_PORT/metrics) ----------------
BATCHES = Counter("stream_batches_processed_total", "Micro-batches processed", ["query"])
RECORDS = Counter("stream_records_processed_total", "Valid records processed", ["query"])
DLQ_RECORDS = Counter("stream_records_dlq_total", "Invalid records sent to the DLQ", ["reason"])
BATCH_DURATION = Histogram(
    "stream_batch_duration_seconds", "Time to process one micro-batch", ["query"],
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60),
)
LAST_EVENT = Gauge("last_event_timestamp", "Unix time (event time) of the newest valid event seen")
IDLE_ALERTS_OPENED = Counter("idle_alerts_opened_total", "Idle alerts raised by the speed layer")


def timed_batch(query: str) -> Callable:
    """Decorator for foreachBatch functions: metrics, timing and error logging.

    If the function raises, we log it and re-raise. Spark then stops the
    query, the container exits and Docker restarts it from the checkpoint.
    """

    def wrap(fn: Callable[[DataFrame, int], None]) -> Callable[[DataFrame, int], None]:
        def inner(batch_df: DataFrame, batch_id: int) -> None:
            start = time.perf_counter()
            try:
                fn(batch_df, batch_id)
            except Exception:
                log.exception("Micro-batch failed", event="batch_failed", query=query, batch_id=batch_id)
                raise
            BATCHES.labels(query=query).inc()
            BATCH_DURATION.labels(query=query).observe(time.perf_counter() - start)

        return inner

    return wrap


# =====================================================================
# Query 1: invalid records -> dead-letter queue
# =====================================================================
@timed_batch("dlq")
def process_dlq_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Send rejected records, with the reason and original position, to the DLQ topic."""
    reasons = {r["error_reason"]: r["count"] for r in batch_df.groupBy("error_reason").count().collect()}
    if not reasons:
        return
    (
        batch_df.select(
            F.col("kafka_key").alias("key"),
            F.to_json(
                F.struct(
                    "error_reason", "raw_value", "kafka_partition", "kafka_offset",
                    F.current_timestamp().alias("rejected_at"),
                )
            ).alias("value"),
        )
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", DLQ_TOPIC)
        .save()
    )
    for reason, count in reasons.items():
        DLQ_RECORDS.labels(reason=reason).inc(count)
    log.warning(
        "Invalid records sent to DLQ", event="dlq_written", stage="storage",
        batch_id=batch_id, record_count=sum(reasons.values()), reasons=reasons,
    )


# =====================================================================
# Query 2: clean events -> Parquet lake + vehicle status + idle alerts
# =====================================================================
LAKE_COLUMNS = [
    "event_id", "trip_id", "driver_id", "vehicle_id", "lat", "lon", "speed", "status",
    "fare", "zone", "event_time", "sim_time", "sim_date", "kafka_partition", "kafka_offset",
]


@timed_batch("clean_events")
def process_clean_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Archive the batch to the lake, then update vehicle status and idle alerts."""
    batch_df = batch_df.persist()  # used three times below; don't recompute from Kafka
    try:
        count = batch_df.count()
        if count == 0:
            return

        # (a) Master dataset: append raw cleaned events, one folder per sim_date.
        #     coalesce(1) = one file per partition per batch (fewer small files).
        (
            batch_df.select(*LAKE_COLUMNS)
            .withColumn("ingested_at", F.current_timestamp())
            .coalesce(1)
            .write.mode("append")
            .partitionBy("sim_date")
            .parquet(LAKE_PATH)
        )

        # (b) Latest status per vehicle. A batch is ~150 rows, so collecting is cheap.
        events = [
            r.asDict()
            for r in batch_df.select(
                "vehicle_id", "driver_id", "trip_id", "zone", "lat", "lon", "speed",
                "status", "event_time", "sim_date",
            ).collect()
        ]
        threshold_s = sim_clock.sim_minutes_to_real_seconds(IDLE_ALERT_SIM_MINUTES)
        with closing(pg.connect()) as conn, conn:  # `with conn` = one transaction
            statuses = pg.merge_vehicle_status(pg.fetch_current_status(conn), events)
            pg.upsert_vehicle_status(conn, statuses)
            new_alerts, resolved = pg.refresh_idle_alerts(conn, threshold_s, sim_clock.speedup())

        # (c) Observability
        for alert in new_alerts:
            IDLE_ALERTS_OPENED.inc()
            log.warning(
                "Vehicle idle too long", event="idle_alert", **alert,
                threshold_sim_minutes=IDLE_ALERT_SIM_MINUTES,
            )
        for vid in resolved:
            log.info("Idle alert resolved", event="idle_alert_resolved", vehicle_id=vid)

        newest = max(e["event_time"] for e in events)
        LAST_EVENT.set(newest.timestamp())
        RECORDS.labels(query="clean_events").inc(count)
        log.info(
            "Clean batch stored", event="clean_batch_stored", stage="storage",
            batch_id=batch_id, record_count=count, vehicles_updated=len(statuses),
            sim_date=events[-1]["sim_date"],
        )
    finally:
        batch_df.unpersist()


# =====================================================================
# Query 3: 1-minute tumbling windows per zone -> rt_zone_metrics
# =====================================================================
@timed_batch("zone_metrics")
def process_zone_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Upsert the windows that changed in this micro-batch."""
    rows = [r.asDict() for r in batch_df.collect()]  # at most a few windows x 5 zones
    if not rows:
        return
    # 1 real minute = 2.4 simulated hours at 144x, used to express trips per sim hour.
    window_sim_hours = WINDOW_SECONDS * sim_clock.speedup() / 3600
    with closing(pg.connect()) as conn, conn:
        written = pg.upsert_zone_metrics(conn, rows, window_sim_hours)
    RECORDS.labels(query="zone_metrics").inc(sum(r["event_count"] for r in rows))
    log.info("Zone metrics upserted", event="zone_metrics_upserted", stage="storage",
             batch_id=batch_id, record_count=written)


def zone_window_aggregates(valid: DataFrame) -> DataFrame:
    """Per zone and 1-minute window: active vehicles, idle ratio, trips and earnings.

    - active_vehicles: distinct vehicles seen (approx_count_distinct is the
      streaming-friendly distinct count; exact for numbers this small)
    - idle_ratio: share of pings that were idle = share of time spent idle
    - trips/earnings: trip-completion events are the ones with fare > 0
    """
    return (
        valid.groupBy(F.window("event_time", f"{WINDOW_SECONDS} seconds"), "zone")
        .agg(
            F.approx_count_distinct("vehicle_id", 0.01).alias("active_vehicles"),
            F.approx_count_distinct(F.when(F.col("status") == "idle", F.col("vehicle_id")), 0.01).alias("idle_vehicles"),
            (F.sum(F.when(F.col("status") == "idle", 1).otherwise(0)) / F.count("*")).alias("idle_ratio"),
            F.sum(F.when(F.col("fare") > 0, 1).otherwise(0)).alias("trips_completed"),
            F.sum("fare").alias("earnings"),
            F.count("*").alias("event_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone", "active_vehicles", "idle_vehicles", "idle_ratio",
            "trips_completed", "earnings", "event_count",
        )
    )


# =====================================================================
# Wiring
# =====================================================================
def build_spark() -> SparkSession:
    """Create a small local Spark session suited to a laptop."""
    return (
        SparkSession.builder.appName("fleetpulse-speed-layer")
        # Default is 200 shuffle partitions -> far too many tiny tasks for our volume.
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )


def main() -> None:
    """Define the three streaming queries and run them until the process stops."""
    start_http_server(get_int("STREAMING_METRICS_PORT", 8003))
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TELEMETRY_TOPIC)
        .option("startingOffsets", get_str("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("maxOffsetsPerTrigger", 5000)   # back-pressure: cap work per micro-batch
        .option("failOnDataLoss", "false")      # survive topic retention deleting old data
        .load()
    )
    events = add_validation(parse_events(raw))

    invalid = events.filter(F.col("error_reason").isNotNull())
    valid = (
        events.filter(F.col("error_reason").isNull())
        .drop("error_reason")
        # Watermark: wait up to 2 minutes for late events, then finalise windows
        # and forget old state (otherwise state would grow forever).
        .withWatermark("event_time", WATERMARK)
        # Drop re-sent events (same event_id) that arrive within the watermark.
        .dropDuplicatesWithinWatermark(["event_id"])
    )

    def start(df: DataFrame, name: str, fn: Callable, mode: str = "append") -> None:
        df.writeStream.queryName(name).outputMode(mode).foreachBatch(fn).option(
            "checkpointLocation", f"{CHECKPOINT_DIR}/{name}"
        ).trigger(processingTime=TRIGGER).start()

    start(invalid, "dlq", process_dlq_batch)
    start(valid, "clean_events", process_clean_batch)
    # "update" mode: emit a window each time it changes, so dashboards see it
    # filling up live instead of only after the watermark closes it.
    start(zone_window_aggregates(valid), "zone_metrics", process_zone_batch, mode="update")

    log.info(
        "Speed layer started", event="stream_started", topic=TELEMETRY_TOPIC, trigger=TRIGGER,
        watermark=WATERMARK, window_seconds=WINDOW_SECONDS, lake_path=LAKE_PATH,
        idle_alert_sim_minutes=IDLE_ALERT_SIM_MINUTES,
    )
    spark.streams.awaitAnyTermination()  # raises if any query fails -> container restarts


if __name__ == "__main__":
    main()
