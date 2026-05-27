"""StreamGuard Spark batch job.

Reads the raw event archive at output/events/, sessionizes activity,
writes per-session fatigue summaries to output/sessions/, a per-user
baseline to output/baseline/, and one spatial heatmap per preset
time range under output/heatmaps/{1h,6h,1d,3d,1w}/. Overwrites its
output directories each run.
"""

import logging
from datetime import timedelta
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

log = logging.getLogger("streamguard.batch_job")

EVENTS_PATH = "output/events"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"

WINDOW_SIZE = "1 minute"
SESSION_GAP_SECONDS = 5 * 60

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
    """One row per (session_id, time_window, user) with the four count metrics."""
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

    events_w = events_s.withColumn(
        "time_window", F.window(F.col("event_time"), WINDOW_SIZE)
    )

    return (
        events_w.groupBy("session_id", "time_window", "user")
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


def session_summary(per_window):
    """One row per (session_id, user) with totals and the fatigue index.

    fatigue_index combines two within-session slopes:
      - keystrokes_slope: typing speed trend (negative = slowing down)
      - corrections_slope: error trend (positive = more mistakes)
    Both are computed against `window_idx`, the zero-based ordinal of
    each minute inside the session. Fatigue = slowing down AND making
    more errors, so we add the corrections slope and subtract the
    keystroke slope. (The previous rhythm/pause inputs were removed
    when the rhythm pipeline was retired.)
    """
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
        )
        .withColumn(
            "fatigue_index",
            -F.col("keystrokes_slope") + F.col("corrections_slope"),
        )
        .withColumn(
            # fatigue_index is null when either slope is null (Spark
            # addition propagates null). regr_slope is null when a
            # session has fewer than two windows. We require both the
            # window-count floor and a non-null fatigue_index so the
            # frontend can use this single flag as a safe "the number
            # below is renderable" guard.
            "fatigue_reliable",
            (F.col("window_count") >= MIN_WINDOWS_FOR_FATIGUE)
            & F.col("fatigue_index").isNotNull(),
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


def compute_all(spark):
    """Run one batch pass over the event archive: read events,
    compute per-session summaries, the per-user baseline, and a
    spatial heatmap for each preset range. Overwrites the output
    directories in place. Safe to call repeatedly on the same
    long-lived spark session.

    Called both from the CLI ``main()`` below and from the API's
    in-process scheduler in ``api.py``. The ``unpersist()`` calls in
    the ``finally`` blocks matter for the scheduled case — without
    them, cached DataFrames pile up in Spark memory across runs.

    Returns early with a warning when ``output/events/`` does not
    exist yet (first run, or after ``rm -rf output/``). Without this
    guard ``spark.read.parquet`` raises ``AnalysisException`` and the
    scheduler tick fails with a misleading traceback every 5 min.
    """
    if not Path(EVENTS_PATH).exists():
        log.warning(
            "event archive at %s does not exist yet; skipping batch run",
            EVENTS_PATH,
        )
        return

    events = spark.read.parquet(EVENTS_PATH).cache()
    try:
        per_window = per_window_metrics(events).cache()
        try:
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
