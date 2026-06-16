"""KeySpark throughput / latency benchmark - the Big-Data performance numbers in
the paper and demo. Two sub-commands:

  batch     - time the Spark batch analytical pipeline (sessionize + per-window
              counts + per-session summaries + per-user baseline) over the full
              archive; reports events/sec, excluding JVM/session startup.
  streaming - replay the archive through Structured Streaming (parquet source,
              availableNow trigger, the same 1-minute windowed aggregation) and
              read Spark's StreamingQueryProgress for rows/sec and per-micro-batch
              latency.

  uv run python -m keyspark.benchmark batch
  uv run python -m keyspark.benchmark streaming

Both read only output/events/ and write nothing back into the live output dirs
(throwaway checkpoint + noop sink), so a benchmark never disturbs the pipeline.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import time

from pyspark.sql.functions import col, window
from pyspark.sql.types import TimestampType

from keyspark.aggregations import event_count_exprs
from keyspark.batch_job import (
    EVENTS_PATH,
    build_session,
    per_window_metrics,
    session_summary,
    user_baseline,
)
from keyspark.streaming_job import WATERMARK, WINDOW_DURATION, event_schema

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
# Throwaway checkpoint so the streaming benchmark reprocesses the whole archive
# every run instead of resuming from a previous benchmark.
BENCH_CHECKPOINT = "output/checkpoint/_benchmark"

# All physical event part files, ignoring the streaming sink's _spark_metadata
# (a bare directory read honours that log, which after a reset only references
# recent events; the glob benchmarks the full archive).
EVENTS_GLOB = f"{EVENTS_PATH}/part-*.parquet"

# Throwaway dir the streaming benchmark stages the archive into (a clean dir with
# no _spark_metadata forces ingestion of every recorded event).
STAGE_DIR = "output/_benchmark_events"


# --------------------------------------------------------------------------
# Helper
# --------------------------------------------------------------------------
def _as_dict(progress) -> dict:
    # PySpark returns recentProgress entries as dicts on some versions and as
    # StreamingQueryProgress objects (with .json()) on others. Normalise to dict.
    if isinstance(progress, dict):
        return progress
    return json.loads(progress.json())


# --------------------------------------------------------------------------
# Batch benchmark
# --------------------------------------------------------------------------
def benchmark_batch() -> dict:
    """Time the batch analytical core over the full event archive."""
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        events = spark.read.parquet(EVENTS_GLOB).cache()
        # Force the read+cache first so disk I/O isn't billed to compute.
        t0 = time.perf_counter()
        n_events = events.count()
        read_s = time.perf_counter() - t0

        # Time sessionization + per-window counts + per-session summaries + the
        # per-user baseline. count() forces execution; nothing is written to disk.
        t1 = time.perf_counter()
        per_window = per_window_metrics(events).cache()
        n_windows = per_window.count()
        n_sessions = session_summary(per_window).count()
        user_baseline(per_window).count()
        process_s = time.perf_counter() - t1

        return {
            "events": int(n_events),
            "windows": int(n_windows),
            "sessions": int(n_sessions),
            "read_seconds": round(read_s, 2),
            "process_seconds": round(process_s, 2),
            "throughput_events_per_sec": round(n_events / process_s)
            if process_s
            else None,
        }
    finally:
        spark.stop()


# --------------------------------------------------------------------------
# Streaming benchmark
# --------------------------------------------------------------------------
def benchmark_streaming() -> dict:
    """Replay the archive through Structured Streaming and read Spark's own
    progress metrics for throughput and per-micro-batch latency.
    """
    shutil.rmtree(BENCH_CHECKPOINT, ignore_errors=True)
    shutil.rmtree(STAGE_DIR, ignore_errors=True)
    os.makedirs(STAGE_DIR, exist_ok=True)
    for f in glob.glob(EVENTS_GLOB):
        shutil.copy2(f, STAGE_DIR)

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        # The archive parquet carries the parsed columns plus event_time.
        schema = event_schema().add("event_time", TimestampType())
        stream = (
            spark.readStream.schema(schema)
            # maxFilesPerTrigger chunks the staged archive into several
            # micro-batches so the per-batch latency distribution is meaningful.
            .option("maxFilesPerTrigger", 4)   # tune: files per micro-batch
            .parquet(STAGE_DIR)
            .withWatermark("event_time", WATERMARK)
        )
        agg = stream.groupBy(
            window(col("event_time"), WINDOW_DURATION), col("user")
        ).agg(*event_count_exprs())

        t0 = time.perf_counter()
        query = (
            agg.writeStream.format("noop")
            .option("checkpointLocation", BENCH_CHECKPOINT)
            .outputMode("append")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
        wall_s = time.perf_counter() - t0

        progress = [_as_dict(p) for p in query.recentProgress]
        total_rows = sum(p.get("numInputRows", 0) for p in progress)
        batch_ms = [
            p["durationMs"]["triggerExecution"]
            for p in progress
            if p.get("durationMs", {}).get("triggerExecution") is not None
        ]
        total_ms = sum(batch_ms)
        n_batches = len(batch_ms)

        return {
            "events": int(total_rows),
            "micro_batches": n_batches,
            "wall_seconds": round(wall_s, 2),
            "processing_seconds": round(total_ms / 1000, 2),
            "throughput_rows_per_sec": round(total_rows / (total_ms / 1000))
            if total_ms
            else None,
            "mean_batch_latency_ms": round(total_ms / n_batches)
            if n_batches
            else None,
            "max_batch_latency_ms": max(batch_ms) if batch_ms else None,
        }
    finally:
        query_stop = getattr(locals().get("query", None), "stop", None)
        if callable(query_stop):
            try:
                query_stop()
            except Exception:
                pass
        spark.stop()
        shutil.rmtree(BENCH_CHECKPOINT, ignore_errors=True)
        shutil.rmtree(STAGE_DIR, ignore_errors=True)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="KeySpark throughput/latency benchmark"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("batch", help="batch analytical pipeline throughput")
    sub.add_parser("streaming", help="Structured Streaming throughput + latency")
    args = parser.parse_args()

    if args.cmd == "batch":
        print(json.dumps(benchmark_batch(), indent=2))
    elif args.cmd == "streaming":
        print(json.dumps(benchmark_streaming(), indent=2))


if __name__ == "__main__":
    main()
