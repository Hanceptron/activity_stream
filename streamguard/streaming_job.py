"""StreamGuard Spark Structured Streaming job.

Two streaming queries share one parsed Kafka stream. Query A
aggregates per-(user, one-minute window) event counts AND rhythm
metrics (flight-time stddev, long-pause count) to output/metrics/
in append mode. Query B archives the parsed events to output/events/
for the batch job to read. The spatial heatmap is computed in the
batch job (see batch_job.py) so it can be filtered to user-selected
time ranges.

Rhythm metrics note: Spark Structured Streaming does not support
lag() across micro-batches, so the batch job's per-event gap
calculation cannot be ported directly. Instead, we collect_list the
key_down timestamps within each (window, user) and apply a Python
UDF that walks the sorted list, computes consecutive gaps, and
emits both metrics as a struct. State-store overhead is bounded by
the watermark (collected lists are discarded ~90 s after the
window closes).

Migration note when changing the metrics schema: Spark Structured
Streaming will refuse to recover from output/checkpoint/metrics
when the output column set changes. Delete that directory before
restart. The events checkpoint is unaffected.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_list,
    count,
    from_json,
    udf,
    when,
    window,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
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

# Mirrors PAUSE_THRESHOLD_SECONDS in batch_job.py. Kept in this
# file rather than imported so the two jobs can evolve their
# thresholds independently if needed; values must match for the
# streaming and batch numbers to be comparable.
PAUSE_THRESHOLD_SECONDS = 2.0

# Activity floor for rhythm. A minute with fewer than this many
# key_down events is too sparse to produce a meaningful
# flight_time_std or long_pause_count — a couple of keystrokes
# scattered across a minute have one big gap that dominates both
# statistics. Sparse minutes still appear in the metrics output
# with their count columns; the two rhythm columns are nulled out.
# Mirrors MIN_KEYSTROKES_FOR_RHYTHM in batch_job.py and must stay
# in sync for the streaming and batch rhythm numbers to align.
MIN_KEYSTROKES_FOR_RHYTHM = 20

RHYTHM_RETURN_TYPE = StructType([
    StructField("flight_time_std", DoubleType(), nullable=True),
    StructField("long_pause_count", LongType(), nullable=True),
])


@udf(returnType=RHYTHM_RETURN_TYPE)
def rhythm_metrics(timestamps):
    """Per-window rhythm metrics from a list of key_down Unix-second
    timestamps. Mirrors batch_job.per_window_metrics semantics:

    - The first key_down in the window has no predecessor and so
      contributes no gap — same as batch, where the first event's
      lag() is null and both stddev() and the >2.0s count skip nulls.
    - Sample stddev (n-1 denominator), matching Spark's default
      stddev = stddev_samp used in batch_job.
    - Strict `> PAUSE_THRESHOLD_SECONDS`, matching
      `gap_seconds > PAUSE_THRESHOLD_SECONDS` in batch_job —
      a gap of exactly 2.0s is NOT counted as a long pause.
    - Below MIN_KEYSTROKES_FOR_RHYTHM keystrokes the window is
      treated as too sparse to measure: both fields return None
      rather than a noise-dominated number. Matches the activity
      filter applied in batch_job.per_window_metrics.

    Returns (None, None) when below the activity floor.
    Returns (None, long_pauses) when at or above the floor but
    only enough keystrokes for one gap (not enough for sample
    stddev) — defensive path for low N values; unreachable when
    MIN_KEYSTROKES_FOR_RHYTHM >= 3.
    """
    if not timestamps or len(timestamps) < MIN_KEYSTROKES_FOR_RHYTHM:
        return (None, None)
    sorted_ts = sorted(timestamps)
    gaps = [sorted_ts[i + 1] - sorted_ts[i] for i in range(len(sorted_ts) - 1)]
    long_pauses = sum(1 for g in gaps if g > PAUSE_THRESHOLD_SECONDS)
    if len(gaps) < 2:
        return (None, long_pauses)
    mean = sum(gaps) / len(gaps)
    variance = sum((g - mean) ** 2 for g in gaps) / (len(gaps) - 1)
    return (variance ** 0.5, long_pauses)


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
    metrics_agg = (
        parsed.groupBy(window(col("event_time"), WINDOW_DURATION), col("user"))
        .agg(
            count(when(is_kd, 1)).alias("keystrokes"),
            count(when(is_kd & col("key").isin(" ", "Key.space"), 1)).alias("words"),
            count(
                when(is_kd & col("key").isin("Key.backspace", "Key.delete"), 1)
            ).alias("corrections"),
            count(when(col("type") == "click", 1)).alias("clicks"),
            # The when(...) filter inside collect_list returns NULL for
            # non-key_down events; collect_list skips nulls, so we get
            # only the key_down timestamps without a separate filter
            # branch.
            collect_list(when(is_kd, col("ts"))).alias("kd_timestamps"),
        )
        .withColumn("rhythm", rhythm_metrics(col("kd_timestamps")))
    )

    metrics = metrics_agg.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("user"),
        col("keystrokes"),
        col("words"),
        col("corrections"),
        col("clicks"),
        col("rhythm.flight_time_std").alias("flight_time_std"),
        col("rhythm.long_pause_count").alias("long_pause_count"),
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
