"""StreamGuard Spark batch job.

Reads the raw event archive at output/events/, computes
order-dependent rhythm metrics with lag, sessionizes activity,
writes per-session fatigue summaries to output/sessions/, a
per-user baseline to output/baseline/, and one spatial heatmap per
preset time range under output/heatmaps/{1h,6h,1d,3d,1w}/.
Overwrites its output directories each run.
"""

from datetime import timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

EVENTS_PATH = "output/events"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"

WINDOW_SIZE = "1 minute"
SESSION_GAP_SECONDS = 5 * 60
PAUSE_THRESHOLD_SECONDS = 2.0
MIN_WINDOWS_FOR_FATIGUE = 5

CELL_SIZE = 40
HEATMAP_PRESETS = [
    ("1h", 1),
    ("6h", 6),
    ("1d", 24),
    ("3d", 72),
    ("1w", 168),
]


def build_session():
    return (
        SparkSession.builder
        .appName("streamguard-batch")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def per_window_metrics(events):
    """One row per (session_id, time_window, user) with all six metrics."""
    is_kd = F.col("type") == "key_down"

    # Sessionize manually. Walking each user's events in event-time
    # order, a new session starts whenever the gap from the previous
    # event exceeds SESSION_GAP_SECONDS. The running cumulative sum
    # of those "new session" flags is a stable integer session_id per
    # event. We do this instead of Spark's session_window because (a)
    # session_window cannot share a groupBy with window() and (b) its
    # materialized struct values are per-event, so they do not
    # partition correctly for Window.partitionBy in session_summary
    # below, which leaves window_idx stuck at 0 and silently breaks
    # every regr_slope.
    user_order = Window.partitionBy("user").orderBy("event_time")
    cumulative = user_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    events_s = (
        events
        .withColumn("_prev_ts", F.lag("event_time").over(user_order))
        .withColumn(
            "_new_session",
            F.when(
                F.col("_prev_ts").isNull()
                | (
                    F.col("event_time").cast("double")
                    - F.col("_prev_ts").cast("double")
                    > SESSION_GAP_SECONDS
                ),
                1,
            ).otherwise(0),
        )
        .withColumn("session_id", F.sum("_new_session").over(cumulative))
        .drop("_prev_ts", "_new_session")
    )

    counts = (
        events_s.groupBy(
            F.col("session_id"),
            F.window(F.col("event_time"), WINDOW_SIZE).alias("time_window"),
            F.col("user"),
        )
        .agg(
            F.count(F.when(is_kd, 1)).alias("keystrokes"),
            F.count(
                F.when(is_kd & F.col("key").isin(" ", "Key.space"), 1)
            ).alias("words"),
            F.count(
                F.when(is_kd & F.col("key").isin("Key.backspace", "Key.delete"), 1)
            ).alias("corrections"),
            F.count(F.when(F.col("type") == "click", 1)).alias("clicks"),
        )
    )

    # lag over key_down events partitioned by user and ordered by
    # event_time gives the previous keystroke's time. Subtracting
    # yields the gap in seconds for every keystroke except the first
    # one per user. The first per-user keystroke gets a null gap,
    # which stddev and conditional count both ignore.
    key_events_with_gap = (
        events_s.filter(is_kd)
        .withColumn(
            "gap_seconds",
            F.col("event_time").cast("double")
            - F.lag("event_time").over(user_order).cast("double"),
        )
    )

    rhythm = (
        key_events_with_gap.groupBy(
            F.col("session_id"),
            F.window(F.col("event_time"), WINDOW_SIZE).alias("time_window"),
            F.col("user"),
        )
        .agg(
            F.stddev(F.col("gap_seconds")).alias("flight_time_std"),
            F.count(
                F.when(F.col("gap_seconds") > PAUSE_THRESHOLD_SECONDS, 1)
            ).alias("long_pause_count"),
        )
    )

    return counts.join(rhythm, on=["session_id", "time_window", "user"], how="left")


def session_summary(per_window):
    """One row per (session_id, user) with totals and four fatigue slopes."""
    window_order = Window.partitionBy("session_id", "user").orderBy(
        F.col("time_window.start")
    )
    indexed = per_window.withColumn(
        "window_idx", F.row_number().over(window_order) - 1
    )

    return (
        indexed.groupBy("session_id", "user")
        .agg(
            F.min(F.col("time_window.start")).alias("session_start"),
            F.max(F.col("time_window.end")).alias("session_end"),
            F.count("*").alias("window_count"),
            F.sum("keystrokes").alias("keystrokes_total"),
            F.sum("words").alias("words_total"),
            F.sum("corrections").alias("corrections_total"),
            F.sum("clicks").alias("clicks_total"),
            F.regr_slope(F.col("keystrokes"), F.col("window_idx")).alias(
                "keystrokes_slope"
            ),
            F.regr_slope(F.col("corrections"), F.col("window_idx")).alias(
                "corrections_slope"
            ),
            F.regr_slope(F.col("flight_time_std"), F.col("window_idx")).alias(
                "rhythm_slope"
            ),
            F.regr_slope(F.col("long_pause_count"), F.col("window_idx")).alias(
                "pause_slope"
            ),
        )
        .withColumn(
            "fatigue_index",
            -F.col("keystrokes_slope")
            + F.col("corrections_slope")
            + F.col("rhythm_slope")
            + F.col("pause_slope"),
        )
        .withColumn(
            "fatigue_reliable",
            F.col("window_count") >= MIN_WINDOWS_FOR_FATIGUE,
        )
    )


def user_baseline(per_window):
    """One row per user with mean and stddev for each of the six metrics."""
    metrics = [
        "keystrokes",
        "words",
        "corrections",
        "clicks",
        "flight_time_std",
        "long_pause_count",
    ]
    agg_exprs = []
    for m in metrics:
        agg_exprs.append(F.mean(m).alias(f"{m}_mean"))
        agg_exprs.append(F.stddev(m).alias(f"{m}_std"))
    return (
        per_window.groupBy("user")
        .agg(*agg_exprs)
        .withColumn("computed_at", F.current_timestamp())
    )


def heatmap_for_range(events, hours, max_event_time):
    """Spatial heatmap counts for the last `hours` hours of mouse events.

    The cutoff is measured backwards from `max_event_time` (the
    newest event in the archive), not from wall-clock now, so an
    archive recorded days ago still produces a populated heatmap.
    """
    cutoff = max_event_time - timedelta(hours=hours)
    return (
        events
        .filter(F.col("event_time") >= F.lit(cutoff))
        .filter(F.col("type").isin("move", "click"))
        .withColumn("cell_x", (F.col("x") / CELL_SIZE).cast("int"))
        .withColumn("cell_y", (F.col("y") / CELL_SIZE).cast("int"))
        .groupBy("cell_x", "cell_y", "type", "user")
        .agg(F.count("*").alias("count"))
    )


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    events = spark.read.parquet(EVENTS_PATH).cache()
    per_window = per_window_metrics(events).cache()

    session_summary(per_window).write.mode("overwrite").parquet(SESSIONS_PATH)
    user_baseline(per_window).write.mode("overwrite").parquet(BASELINE_PATH)

    # One spatial heatmap per preset time range.
    max_event_time = events.agg(F.max("event_time").alias("m")).first()["m"]
    if max_event_time is not None:
        for name, hours in HEATMAP_PRESETS:
            (
                heatmap_for_range(events, hours, max_event_time)
                .write.mode("overwrite")
                .parquet(f"{HEATMAPS_PATH}/{name}")
            )

    spark.stop()


if __name__ == "__main__":
    main()
