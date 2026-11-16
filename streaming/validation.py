"""Parsing and validation of raw Kafka telemetry messages (Spark expressions).

Every message gets an `error_reason` column:
    NULL         -> valid event, continues to the aggregations and the lake
    "<reason>"   -> invalid event, goes to the dead-letter queue (DLQ)

Using native Spark column expressions (not Python UDFs) keeps the work
inside the JVM, which is faster and lets Spark optimise the query.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common.fleet import COLOMBO_BOUNDS, VALID_STATUSES, ZONES

# Explicit schema: we never let Spark guess types from the data.
TELEMETRY_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("trip_id", T.StringType()),
        T.StructField("driver_id", T.StringType()),
        T.StructField("vehicle_id", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("speed", T.DoubleType()),
        T.StructField("status", T.StringType()),
        T.StructField("fare", T.DoubleType()),
        T.StructField("zone", T.StringType()),
        T.StructField("timestamp", T.TimestampType()),
        T.StructField("sim_time", T.TimestampType()),
        T.StructField("sim_date", T.StringType()),
    ]
)


def parse_events(kafka_df: DataFrame) -> DataFrame:
    """Turn raw Kafka rows (binary key/value) into typed event columns.

    The original JSON text and Kafka position are kept so that a rejected
    record can be sent to the DLQ exactly as it arrived.
    """
    return (
        kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            # Malformed JSON does not crash the job: from_json just returns NULLs.
            F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("e"),
        )
        .select("kafka_key", "raw_value", "kafka_partition", "kafka_offset", "e.*")
        .withColumnRenamed("timestamp", "event_time")
    )


def error_reason() -> Column:
    """Return the first rule an event breaks, or NULL if it is valid.

    NULL checks come first because comparisons with NULL are neither true nor
    false in SQL, so `NULL < 0` would silently let a bad record through.
    """
    min_lat, max_lat, min_lon, max_lon = COLOMBO_BOUNDS
    lat, lon = F.col("lat"), F.col("lon")
    return (
        F.when(F.col("event_id").isNull(), "malformed_json")
        .when(F.col("vehicle_id").isNull() | (F.trim("vehicle_id") == ""), "null_vehicle_id")
        .when(F.col("event_time").isNull(), "missing_timestamp")
        .when(
            lat.isNull() | lon.isNull() | ~lat.between(min_lat, max_lat) | ~lon.between(min_lon, max_lon),
            "impossible_coordinates",
        )
        .when(F.col("fare").isNull() | (F.col("fare") < 0), "negative_fare")
        .when(F.col("speed").isNull() | (F.col("speed") < 0), "invalid_speed")
        .when(F.col("status").isNull() | ~F.col("status").isin(*VALID_STATUSES), "invalid_status")
        .when(F.col("zone").isNull() | ~F.col("zone").isin(*ZONES), "unknown_zone")
        .otherwise(F.lit(None).cast("string"))
    )


def add_validation(df: DataFrame) -> DataFrame:
    """Add the `error_reason` column to a parsed events DataFrame."""
    return df.withColumn("error_reason", error_reason())
