"""StreamGuard Spark Structured Streaming job.

Two streaming queries share one parsed Kafka stream. Query A
aggregates per-(user, one-minute window) event counts to
output/metrics/ in append mode. Query B archives the parsed events
to output/events/ for the upcoming batch job to read.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, when, window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructType,
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Must match the installed pyspark version exactly. pyspark 4.1.1 is
# built against Scala 2.13, so the connector artifact is _2.13.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

METRICS_PATH = "output/metrics"
EVENTS_PATH = "output/events"
METRICS_CHECKPOINT = "output/checkpoint/metrics"
EVENTS_CHECKPOINT = "output/checkpoint/events"

WATERMARK = "30 seconds"
WINDOW_DURATION = "1 minute"


def build_session():
    return (
        SparkSession.builder
        .appName("streamguard")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def event_schema():
    return (
        StructType()
        .add("type", StringType())
        .add("key", StringType())
        .add("x", LongType())
        .add("y", LongType())
        .add("button", StringType())
        .add("pressed", BooleanType())
        .add("dx", LongType())
        .add("dy", LongType())
        .add("user", StringType())
        .add("ts", DoubleType())
    )


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_time", col("ts").cast("timestamp"))
        .withWatermark("event_time", WATERMARK)
    )

    is_kd = col("type") == "key_down"
    metrics = (
        parsed.groupBy(window(col("event_time"), WINDOW_DURATION), col("user"))
        .agg(
            count(when(is_kd, 1)).alias("keystrokes"),
            count(when(is_kd & (col("key") == " "), 1)).alias("words"),
            count(
                when(is_kd & col("key").isin("Key.backspace", "Key.delete"), 1)
            ).alias("corrections"),
            count(when(col("type") == "click", 1)).alias("clicks"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("user"),
            col("keystrokes"),
            col("words"),
            col("corrections"),
            col("clicks"),
        )
    )

    metrics_query = (
        metrics.writeStream
        .format("parquet")
        .option("path", METRICS_PATH)
        .option("checkpointLocation", METRICS_CHECKPOINT)
        .outputMode("append")
        .start()
    )

    events_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", EVENTS_PATH)
        .option("checkpointLocation", EVENTS_CHECKPOINT)
        .outputMode("append")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
