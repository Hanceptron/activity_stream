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

# Activity floor for rhythm. A minute with fewer than this many
# key_down events is too sparse to produce a meaningful
# flight_time_std or long_pause_count - a couple of keystrokes
# scattered across a minute have one big gap that dominates both
# statistics. Sparse minutes still appear in the per_window output
# with their count columns; the two rhythm columns are nulled out
# (see per_window_metrics). user_baseline and session_summary then
# automatically aggregate "active minutes only" because Spark's
# mean / stddev / regr_slope skip nulls. Mirrors
# MIN_KEYSTROKES_FOR_RHYTHM in streaming_job.py.
MIN_KEYSTROKES_FOR_RHYTHM = 20

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

    # Materialize the per-minute window once and reuse it for the
    # counts groupBy, the rhythm lag's partitionBy, and the rhythm
    # groupBy. F.window is deterministic — calling it three times
    # would produce identical structs — but pinning the column up
    # front keeps the three downstream uses provably consistent and
    # is what allows the lag below to be partitioned by the same
    # window value the rhythm aggregation groups on.
    events_w = events_s.withColumn(
        "time_window", F.window(F.col("event_time"), WINDOW_SIZE)
    )

    counts = (
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

    # Window-local lag. Partitioning by (user, time_window) means
    # the previous-event lookup only crosses keystrokes inside the
    # same minute — the first key_down of every minute gets a null
    # gap, matching the streaming UDF's per-window semantics.
    #
    # Why this matters: the previous partitioning (just by user)
    # let the first key_down of a freshly-resumed session take its
    # gap from the LAST key_down of the previous session, which
    # could be hours earlier. That single inter-session gap then
    # poisoned the minute's flight_time_std (max observed ~7900 s
    # for a 49-keystroke minute) and inflated its long_pause_count
    # by one. With window-local lag, idle gaps that span window
    # boundaries vanish from the per-minute stats — which is the
    # correct semantics for "how steady was the typing inside this
    # one minute."
    window_user_order = Window.partitionBy("user", "time_window").orderBy("event_time")
    key_events_with_gap = (
        events_w.filter(is_kd)
        .withColumn(
            "gap_seconds",
            F.col("event_time").cast("double")
            - F.lag("event_time").over(window_user_order).cast("double"),
        )
    )

    rhythm = (
        key_events_with_gap.groupBy("session_id", "time_window", "user")
        .agg(
            F.stddev(F.col("gap_seconds")).alias("flight_time_std"),
            F.count(
                F.when(F.col("gap_seconds") > PAUSE_THRESHOLD_SECONDS, 1)
            ).alias("long_pause_count"),
        )
    )

    # Null out the rhythm columns on minutes that don't clear the
    # activity floor. The count columns are kept as-is so sparse
    # minutes still show up with their keystrokes/words/clicks
    # contributions. Spark's downstream aggregates (mean, stddev,
    # regr_slope) skip nulls, so user_baseline and session_summary
    # automatically compute "active minutes only" rhythm without
    # any other code changes.
    return (
        counts.join(rhythm, on=["session_id", "time_window", "user"], how="left")
        .withColumn(
            "flight_time_std",
            F.when(
                F.col("keystrokes") >= MIN_KEYSTROKES_FOR_RHYTHM,
                F.col("flight_time_std"),
            ),
        )
        .withColumn(
            "long_pause_count",
            F.when(
                F.col("keystrokes") >= MIN_KEYSTROKES_FOR_RHYTHM,
                F.col("long_pause_count"),
            ),
        )
    )


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
            # fatigue_index is null when any of the four slopes is
            # null (Spark addition propagates null). After the N=20
            # threshold, a session whose active minutes are all
            # sparse can have null rhythm_slope and pause_slope even
            # though window_count >= MIN_WINDOWS_FOR_FATIGUE. We
            # therefore also require fatigue_index itself to be
            # non-null so the frontend can use this single flag as a
            # safe "the number below is renderable" guard.
            "fatigue_reliable",
            (F.col("window_count") >= MIN_WINDOWS_FOR_FATIGUE)
            & F.col("fatigue_index").isNotNull(),
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
    """
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
