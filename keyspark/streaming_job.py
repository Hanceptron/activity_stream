"""KeySpark Spark Structured Streaming job.

Two streaming queries share one parsed Kafka stream. Query A
aggregates per-(user, one-minute window) event counts (keystrokes,
words, corrections, clicks) to output/metrics/ in append mode.
Query B archives the parsed events to output/events/ for the batch
job to read. The spatial heatmap is computed in the batch job (see
batch_job.py) so it can be filtered to user-selected time ranges.

Migration note when changing the metrics schema: Spark Structured
Streaming will refuse to recover from output/checkpoint/metrics
when the output column set changes. Delete that directory before
restart. The events checkpoint is unaffected.
"""

import logging
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructType,
)

from keyspark.aggregations import event_count_exprs

log = logging.getLogger("keyspark.streaming_job")

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Must match the installed pyspark version exactly. pyspark 4.1.1 is
# built against Scala 2.13, so the connector artifact is _2.13.
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

METRICS_PATH = "output/metrics"
EVENTS_PATH = "output/events"
METRICS_CHECKPOINT = "output/checkpoint/metrics"
EVENTS_CHECKPOINT = "output/checkpoint/events"

# 5-second watermark balances late-event tolerance against time-to-first-window-emit.
# Events flow over localhost from a process whose clock is sub-millisecond aligned
# with the streaming JVM's, so 5 s is generous. The trade-off matters for the
# live-dot smoke test: a 1-minute window with append-mode + N-second watermark
# cannot emit a window before window_size+N seconds of wall-clock have elapsed
# since the first event. With WATERMARK=30 s, that was ~90 s, which (a) put the
# dot through a flicker cycle each minute under sustained typing and (b) made
# cold-start-to-green and wake-to-green slower than the README's 2-minute
# smoke-test threshold. Header.jsx now ages off window_end, so dropping the
# watermark also leaves more headroom inside the 2-minute "live" window.
WATERMARK = "5 seconds"
WINDOW_DURATION = "1 minute"


def build_session():
    return (
        SparkSession.builder
        .appName("keyspark")
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        # Survive a wiped/recreated Kafka (e.g. host sleep-wake): when the
        # checkpoint's offset is past what the broker still has, reset to
        # the earliest available offset and continue instead of crashing
        # forever with "Some data may have been lost". Input events are
        # cheap to lose; a permanent crash loop is not.
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), event_schema()).alias("e"))
        .select("e.*")
        .withColumn("event_time", col("ts").cast("timestamp"))
        .withWatermark("event_time", WATERMARK)
    )

    metrics = (
        parsed.groupBy(window(col("event_time"), WINDOW_DURATION), col("user"))
        .agg(*event_count_exprs())
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

    # Triggers bound the file-creation rate. Without them the default
    # (process-as-fast-as-possible) writes a part file every micro-batch
    # - sub-second under input - which is what produced the 100k+ tiny
    # files. Metrics feeds the live dashboard (2-min freshness window) so
    # 30 s keeps the dot fresh; events only feed the 5-min batch, so 60 s
    # is plenty and halves their file count.
    metrics_query = (
        metrics.writeStream
        .format("parquet")
        .option("path", METRICS_PATH)
        .option("checkpointLocation", METRICS_CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )

    events_query = (
        parsed.writeStream
        .format("parquet")
        .option("path", EVENTS_PATH)
        .option("checkpointLocation", EVENTS_CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start()
    )

    queries = [metrics_query, events_query]
    try:
        # Liveness poll instead of a bare awaitAnyTermination() that
        # blocks forever. If a query fails - or the Spark RPC dies on a
        # host sleep, leaving the JVM up but unresponsive - the next poll
        # flips isActive or raises across the Py4J bridge, so this process
        # EXITS and the outer restart loop respawns a clean interpreter
        # instead of lingering as a zombie that produces nothing.
        while all(q.isActive for q in queries):
            time.sleep(10)
        for q in queries:
            exc = q.exception() if not q.isActive else None
            if exc is not None:
                log.error("streaming query failed: %s", exc)
        log.warning("a streaming query is no longer active; exiting for respawn")
    finally:
        for q in queries:
            try:
                q.stop()
            except Exception:
                log.exception("error stopping streaming query")
        spark.stop()


if __name__ == "__main__":
    main()
