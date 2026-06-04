"""KeySpark Spark batch job.

Reads the raw event archive at output/events/, sessionizes activity,
writes per-session summaries to output/sessions/, a per-user
baseline to output/baseline/, and one spatial heatmap per preset
time range under output/heatmaps/{1h,6h,1d,3d,1w}/. Overwrites its
output directories each run.
"""

import logging
from datetime import timedelta
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from keyspark.aggregations import event_count_exprs

log = logging.getLogger("keyspark.batch_job")

EVENTS_PATH = "output/events"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"
PER_WINDOW_PATH = "output/per_window"
DAY_MINUTE_METRICS_PATH = "output/day_minute_metrics"
HEATMAP_BY_DAY_PATH = "output/heatmap_by_day"

WINDOW_SIZE = "1 minute"
SESSION_GAP_SECONDS = 5 * 60

# Heatmap grid resolution in screen pixels per cell. Smaller = finer
# grid = sharper heatmap. 16px gives a ~240-wide grid on a 4K display,
# enough detail to avoid the blocky/low-res look while keeping the
# cell count renderable.
CELL_SIZE = 16
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
        .appName("keyspark-batch")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def per_window_metrics(events):
    """One row per (session_id, time_window, user) with the four count metrics."""

    # Sessionize manually. Walking each user's events in event-time
    # order, a new session starts whenever the gap from the previous
    # event exceeds SESSION_GAP_SECONDS. The running cumulative sum
    # of those "new session" flags is a stable integer session_id per
    # event. We do this instead of Spark's session_window because it
    # cannot share a groupBy with window() (per_window_metrics groups
    # by the 1-minute time_window).
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

    events_w = events_s.withColumn(
        "time_window", F.window(F.col("event_time"), WINDOW_SIZE)
    )

    return (
        events_w.groupBy("session_id", "time_window", "user")
        .agg(*event_count_exprs())
    )


def session_summary(per_window):
    """One row per (session_id, user) with session totals.

    Aggregates each session's per-minute windows into start/end, the
    window count, and the four count totals. (The previous fatigue
    trend index was removed.)
    """
    return (
        per_window.groupBy("session_id", "user")
        .agg(
            F.min(F.col("time_window.start")).alias("session_start"),
            F.max(F.col("time_window.end")).alias("session_end"),
            F.count("*").alias("window_count"),
            F.sum("keystrokes").alias("keystrokes_total"),
            F.sum("words").alias("words_total"),
            F.sum("corrections").alias("corrections_total"),
            F.sum("clicks").alias("clicks_total"),
        )
    )


def user_baseline(per_window):
    """One row per user with mean and stddev for each of the four metrics."""
    metrics = ["keystrokes", "words", "corrections", "clicks"]
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


def day_minute_metrics(events):
    """One row per (day, one-minute window, user) with the four count
    metrics. Same aggregation as per_window_metrics but keyed by the
    calendar day instead of a session, so the frontend can pull a
    single historical day's keystroke timeline. `day` is the local
    calendar day (host timezone) via date_format, matching the
    browser's localDayKey on a single-user machine.
    """
    ev = (
        events
        .withColumn("time_window", F.window(F.col("event_time"), WINDOW_SIZE))
        .withColumn("day", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
    )
    return (
        ev.groupBy("day", "time_window", "user")
        .agg(*event_count_exprs())
        .select(
            "day",
            F.col("time_window.start").alias("window_start"),
            F.col("time_window.end").alias("window_end"),
            "user",
            "keystrokes",
            "words",
            "corrections",
            "clicks",
        )
    )


def heatmap_by_day(events):
    """One row per (day, cell_x, cell_y, type, user) over the whole
    archive. Same spatial bucketing as heatmap_for_range but with no
    time cutoff and a `day` column, so the frontend can render the
    movement + click heatmap for any single calendar day.
    """
    return (
        events
        .filter(F.col("type").isin("move", "click"))
        .withColumn("day", F.date_format(F.col("event_time"), "yyyy-MM-dd"))
        .withColumn("cell_x", (F.col("x") / CELL_SIZE).cast("int"))
        .withColumn("cell_y", (F.col("y") / CELL_SIZE).cast("int"))
        .groupBy("day", "cell_x", "cell_y", "type", "user")
        .agg(F.count("*").alias("count"))
    )


def compute_all(spark):
    """Run one batch pass over the event archive: read events,
    compute per-session summaries, the per-user baseline, and a
    spatial heatmap for each preset range. Overwrites the output
    directories in place. Safe to call repeatedly on the same
    long-lived spark session.

    Called both from the CLI ``main()`` below and from the API's
    in-process scheduler in ``api.py``. The ``unpersist()`` calls in
    the ``finally`` blocks matter for the scheduled case - without
    them, cached DataFrames pile up in Spark memory across runs.

    Returns early with a warning when ``output/events/`` does not
    exist yet (first run, or after ``rm -rf output/``). Without this
    guard ``spark.read.parquet`` raises ``AnalysisException`` and the
    scheduler tick fails with a misleading traceback every 5 min.
    """
    if not Path(EVENTS_PATH).exists() or not any(Path(EVENTS_PATH).glob("*.parquet")):
        log.warning(
            "event archive at %s has no parquet files yet; skipping batch run",
            EVENTS_PATH,
        )
        return

    # Read the part files directly (glob) instead of the directory root.
    # Reading the root makes Spark honor the Structured Streaming commit
    # log (output/events/_spark_metadata), which only references files
    # written since the last streaming checkpoint reset - so a reset
    # (Kafka offset change, etc.) silently orphans every earlier part file
    # and the batch loses weeks of history. A plain glob always reads the
    # whole archive. (Trade-off: it lists every part file each run; if the
    # file count grows large, add a streaming trigger + periodic compaction.)
    events = spark.read.parquet(f"{EVENTS_PATH}/*.parquet").cache()
    try:
        per_window = per_window_metrics(events).cache()
        try:
            session_summary(per_window).write.mode("overwrite").parquet(SESSIONS_PATH)
            user_baseline(per_window).write.mode("overwrite").parquet(BASELINE_PATH)

            # Flatten the time_window struct so the ML pipeline can read
            # this dataset with vanilla pandas - it would otherwise need
            # to know the struct schema. Written every batch so the
            # training set always reflects the latest sessionization.
            (
                per_window
                .select(
                    "session_id",
                    F.col("time_window.start").alias("window_start"),
                    F.col("time_window.end").alias("window_end"),
                    "user",
                    "keystrokes",
                    "words",
                    "corrections",
                    "clicks",
                )
                .write.mode("overwrite")
                .parquet(PER_WINDOW_PATH)
            )

            # Per-day keystroke timeline and per-day spatial heatmap,
            # both for the calendar drill-down. Single clean parquet
            # dirs (unlike the raw event archive) so the pandas reader
            # in the API reads them without hitting empty-file errors.
            (
                day_minute_metrics(events)
                .write.mode("overwrite")
                .parquet(DAY_MINUTE_METRICS_PATH)
            )
            (
                heatmap_by_day(events)
                .write.mode("overwrite")
                .parquet(HEATMAP_BY_DAY_PATH)
            )

            # One spatial heatmap per preset time range.
            max_event_time = events.agg(F.max("event_time").alias("m")).first()["m"]
            if max_event_time is not None:
                for name, hours in HEATMAP_PRESETS:
                    (
                        heatmap_for_range(events, hours, max_event_time)
                        .write.mode("overwrite")
                        .parquet(f"{HEATMAPS_PATH}/{name}")
                    )
        finally:
            per_window.unpersist()
    finally:
        events.unpersist()


def main():
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        compute_all(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
