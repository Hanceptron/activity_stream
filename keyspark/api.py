"""KeySpark backend API.

Thin FastAPI service that reads the four parquet outputs from the
streaming and batch jobs and serves them as JSON for the React
dashboard. Pandas reads parquet directly on each request; no
caching, no SQL layer. Adequate for a single-user demo.

In addition to serving the parquet data, this process owns the
batch job. On startup, ``lifespan`` builds the Spark session in a
background thread so uvicorn accepts connections immediately (the
data endpoints only read parquet from disk, so the dashboard is
usable while the JVM cold-starts). Once Spark is ready an APScheduler
``BackgroundScheduler`` fires ``compute_all`` every
``REFRESH_INTERVAL_SEC`` seconds in a worker thread. A batch failure
does not take the API down: the session is probed, and the process
only hard-exits (for the bash restart loop to respawn a fresh JVM)
when the Spark RPC is actually dead. The batch loop state is exposed
via ``/api/batch_status`` and consumed by the frontend staleness chips.
"""

import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from keyspark.batch_job import CELL_SIZE, build_session, compute_all

log = logging.getLogger("keyspark.api")

METRICS_PATH = "output/metrics"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"
DAY_MINUTE_METRICS_PATH = "output/day_minute_metrics"
HEATMAP_BY_DAY_PATH = "output/heatmap_by_day"
ML_METRICS_PATH = "output/models/metrics.json"
LIVENESS_PATH = "output/liveness.parquet"
DISPLAY_PATH = "output/display.json"

# Fallback when the agent has not written output/display.json yet:
# MacBook 16" built-in at default scaling. Keeps the heatmap frame
# sane on a fresh checkout.
DEFAULT_DISPLAY = {"width": 1728, "height": 1117}

# The 8-week history calendar and the day drill-down are both built from
# /api/sessions, so this must span the whole calendar window, not just a
# recent handful. 2000 is months of typical use; the payload stays small
# (a dozen scalar fields per session).
SESSIONS_LIMIT = 2000
HEATMAP_RANGES = {"1h", "6h", "1d", "3d", "1w"}
REFRESH_INTERVAL_SEC = 300  # 5 min


@dataclass
class BatchState:
    """In-memory snapshot of the most recent batch run. Updated by
    ``_run_batch`` and read by the ``/api/batch_status`` endpoint.
    """

    last_run: Optional[datetime] = None
    last_status: str = "idle"  # "idle" | "running" | "ok" | "failed"
    last_error: Optional[str] = None


batch_state = BatchState()
# Guards every read/write of `batch_state`. The scheduler thread mutates
# it inside `_run_batch`; FastAPI request handlers read it from worker
# threads. Without the lock, a request could observe torn state (e.g.
# status="running" alongside a stale `last_run` from the previous tick).
batch_state_lock = threading.Lock()


def _spark_alive(spark, timeout_sec: float = 20.0) -> bool:
    """Probe whether the Spark session's RPC is still usable. Runs a
    trivial action in a worker thread so a hung RPC (the macOS sleep/wake
    failure mode) is treated as dead rather than blocking the scheduler
    forever. Returns False on any error or timeout. Used by ``_run_batch``
    to tell a genuinely dead session (respawn via os._exit) from a
    data/transient batch error (keep serving the last-good parquet).
    """
    import concurrent.futures

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        executor.submit(lambda: spark.range(1).count()).result(timeout=timeout_sec)
        return True
    except Exception:
        return False
    finally:
        # Never block on a hung probe: if the session is dead the caller is
        # about to os._exit, which tears the thread down with the process.
        executor.shutdown(wait=False)


def _run_batch(spark) -> None:
    """Scheduler entry point. Wraps ``compute_all`` so failures are
    recorded in ``batch_state``. ``last_run`` is set on both success and
    failure so the staleness chip always shows a real timestamp.

    On failure we probe the Spark session (``_spark_alive``) to decide:
      - session dead (macOS sleep/wake permanently breaks the local-mode
        RPC, and PySpark cannot rebuild a session in-process): os._exit so
        the outer bash restart loop respawns the API with a fresh JVM.
      - session still alive (a bad input file or a transient compute
        error): log it, mark the batch failed, keep the session, and keep
        serving the last-good parquet. The next tick retries.
    This keeps one bad event file or a transient hiccup from taking the
    whole HTTP API down, while preserving the sleep/wake recovery path.
    """
    with batch_state_lock:
        batch_state.last_status = "running"
    try:
        compute_all(spark)
        # Score liveness right after the batch refresh. Pure pandas/sklearn,
        # unrelated to the Spark RPC, so a failure here must not be treated
        # as a dead session - catch it and keep serving the last good flags.
        # Imported locally so a missing scikit-learn never breaks the batch.
        try:
            from keyspark.ml import write_liveness

            write_liveness()
        except Exception:
            log.exception("liveness scoring failed; serving previous flags")
        with batch_state_lock:
            batch_state.last_status = "ok"
            batch_state.last_error = None
            batch_state.last_run = datetime.now(timezone.utc)
    except Exception as exc:  # noqa: BLE001 - see docstring
        # Record the failure first so a request landing now sees real state.
        with batch_state_lock:
            batch_state.last_status = "failed"
            batch_state.last_error = str(exc)
            batch_state.last_run = datetime.now(timezone.utc)
        if _spark_alive(spark):
            # Data/transient error: keep the session and the last-good
            # parquet, retry next tick. Do NOT exit - the API stays up.
            log.exception(
                "batch run failed; Spark still alive - keeping session, "
                "serving last-good parquet, will retry next tick"
            )
            return
        # Spark session is dead: hard-exit so the bash loop respawns a fresh
        # JVM. os._exit bypasses uvicorn/APScheduler/asyncio teardown
        # intentionally - anything touching the dead RPC would hang.
        log.error("batch run failed and Spark session is dead - exiting for fresh respawn")
        os._exit(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Accept HTTP connections immediately and build Spark off the startup
    path in a background thread, then schedule the batch once it is ready.

    Building the Spark session (JVM cold-start) takes tens of seconds. Doing
    it before ``yield`` kept uvicorn from accepting connections for that
    whole window - so after every respawn (e.g. post-wake) the dashboard was
    unreachable and every frontend fetch failed. The data endpoints only
    read parquet from disk, so the API can serve the existing outputs the
    instant it starts; the scheduler fills in fresh data a few seconds after
    Spark finishes building (batch_status reads "idle" until the first run).
    """
    runtime = {"spark": None, "scheduler": None, "stopping": False}

    def _bring_up_spark() -> None:
        try:
            spark = build_session()
            spark.sparkContext.setLogLevel("WARN")
        except Exception:
            log.exception(
                "Spark session build failed; batch disabled, still serving existing parquet"
            )
            return
        if runtime["stopping"]:
            spark.stop()
            return
        runtime["spark"] = spark
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            _run_batch,
            "interval",
            seconds=REFRESH_INTERVAL_SEC,
            args=[spark],
            max_instances=1,  # skip tick if previous run is still in progress
            coalesce=True,    # collapse multiple missed ticks into one run
            # First run ~2 s after Spark is ready, not one full interval
            # later, so fresh data lands quickly.
            next_run_time=datetime.now() + timedelta(seconds=2),
        )
        scheduler.start()
        runtime["scheduler"] = scheduler

    threading.Thread(target=_bring_up_spark, name="spark-init", daemon=True).start()
    try:
        yield
    finally:
        # May run before the background thread finished building Spark; the
        # stopping flag makes that thread skip starting the scheduler, and we
        # tear down whatever already exists.
        runtime["stopping"] = True
        if runtime["scheduler"] is not None:
            runtime["scheduler"].shutdown(wait=False)
        if runtime["spark"] is not None:
            runtime["spark"].stop()


app = FastAPI(title="KeySpark API", lifespan=lifespan)
# allow_origins=["*"] is intentionally permissive for the single-user
# dev setup (Vite on 5173 + this API on 8000). Tighten before exposing
# the dashboard on a network.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _read_parquet(path: str) -> pd.DataFrame:
    # Return an empty DataFrame when the directory does not exist
    # yet. The batch job's outputs in particular are missing until
    # that job has been run at least once.
    #
    # Note: this reads parquet directories that Spark is appending to
    # in another process. Spark uses atomic file-level commits, so a
    # request lands on a self-consistent snapshot; the tiny race where
    # a request opens the directory between `_temporary` rename and
    # `_spark_metadata` commit can yield an empty DataFrame on rare
    # occasions. Acceptable for a single-user dashboard.
    if not Path(path).exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _to_records(df: pd.DataFrame) -> list:
    # Round-trip through pandas's JSON serializer so timestamps
    # become ISO strings and NaN values become JSON null in one
    # step. We parse the JSON back into Python lists and dicts that
    # FastAPI re-serializes for the response.
    if df.empty:
        return []
    return json.loads(df.to_json(orient="records", date_format="iso"))


@app.get("/api/metrics")
def get_metrics(minutes: int = 60) -> list:
    df = _read_parquet(METRICS_PATH)
    if df.empty:
        return []
    # Spark writes timestamps as UTC and pandas reads them as
    # tz-naive datetime64[ns] holding the raw UTC value. Compute
    # the threshold in UTC and strip the tz so the comparison is
    # naive-vs-naive at the same UTC reference.
    threshold = pd.Timestamp.now("UTC").tz_localize(None) - pd.Timedelta(minutes=minutes)
    df = df[df["window_start"] >= threshold].sort_values("window_start")
    return _to_records(df)


@app.get("/api/sessions")
def get_sessions() -> list:
    df = _read_parquet(SESSIONS_PATH)
    if df.empty:
        return []
    df = df.sort_values("session_start", ascending=False).head(SESSIONS_LIMIT)
    return _to_records(df)


@app.get("/api/baseline")
def get_baseline() -> list:
    return _to_records(_read_parquet(BASELINE_PATH))


@app.get("/api/heatmap")
def get_heatmap(range: str = "1h") -> list:
    # The batch job writes one parquet directory per preset range.
    # An unknown range yields an empty list rather than an error so
    # the dashboard never breaks on a typo in the URL.
    if range not in HEATMAP_RANGES:
        return []
    return _to_records(_read_parquet(f"{HEATMAPS_PATH}/{range}"))


@app.get("/api/day_metrics")
def get_day_metrics(day: str, user: str) -> list:
    """Per-minute keystroke/word/correction/click counts for a single
    calendar day, for the History drill-down's timeline graph. ``day``
    is a local-day key "YYYY-MM-DD"; the batch job's ``day`` column was
    written with the same host timezone, so this is a direct match.
    Empty list until the batch has produced output/day_minute_metrics.
    """
    df = _read_parquet(DAY_MINUTE_METRICS_PATH)
    if df.empty:
        return []
    df = df[(df["day"] == day) & (df["user"] == user)].sort_values("window_start")
    return _to_records(df)


@app.get("/api/heatmap_day")
def get_heatmap_day(day: str, user: str) -> list:
    """Spatial heatmap cells (move + click) for a single calendar day,
    for the History drill-down's heatmaps. Same day/user matching as
    /api/day_metrics. Empty list until the batch has produced
    output/heatmap_by_day.
    """
    df = _read_parquet(HEATMAP_BY_DAY_PATH)
    if df.empty:
        return []
    df = df[(df["day"] == day) & (df["user"] == user)]
    return _to_records(df)


@app.get("/api/display")
def get_display() -> dict:
    """Primary-screen grid bounds for framing the mouse heatmap. The
    agent writes output/display.json (screen size in points) at
    startup; we divide by the heatmap CELL_SIZE so the frontend gets
    bounds in the same cell units the heatmap data uses. Falls back to
    the MacBook 16" default when the file is absent.
    """
    try:
        disp = json.loads(Path(DISPLAY_PATH).read_text())
        width = int(disp["width"])
        height = int(disp["height"])
    except (OSError, ValueError, KeyError, TypeError):
        width = DEFAULT_DISPLAY["width"]
        height = DEFAULT_DISPLAY["height"]
    # ceil so a partial trailing cell is included rather than clipped.
    grid_w = -(-width // CELL_SIZE)
    grid_h = -(-height // CELL_SIZE)
    return {"grid_w": grid_w, "grid_h": grid_h}


@app.get("/api/batch_status")
def get_batch_status() -> dict:
    """Most recent batch-job run state. Source of truth for the
    staleness chips on the heatmap, hotspots, and sessions panels.
    """
    with batch_state_lock:
        last_run = batch_state.last_run
        status = batch_state.last_status
        error = batch_state.last_error
    return {
        "last_run": last_run.isoformat() if last_run else None,
        "status": status,
        "error": error,
    }


@app.get("/api/health")
def get_health() -> dict:
    """One-glance liveness/freshness snapshot for diagnosing flaky
    sleep/wake episodes. ``metrics_age_seconds`` is how long ago the
    freshest per-minute window ended (the capture + streaming path);
    ``streaming_fresh`` mirrors the dashboard's 2-minute live threshold.
    ``batch_*`` reflects the in-process batch scheduler.
    """
    now = pd.Timestamp.now("UTC").tz_localize(None)
    metrics_age = None
    df = _read_parquet(METRICS_PATH)
    if not df.empty and "window_end" in df.columns:
        newest = pd.to_datetime(df["window_end"]).max()
        if pd.notna(newest):
            metrics_age = float((now - newest).total_seconds())

    with batch_state_lock:
        last_run = batch_state.last_run
        status = batch_state.last_status
        error = batch_state.last_error
    batch_age = (
        (datetime.now(timezone.utc) - last_run).total_seconds()
        if last_run
        else None
    )

    return {
        "now": now.isoformat(),
        "metrics_age_seconds": metrics_age,
        "streaming_fresh": metrics_age is not None and metrics_age < 120,
        "batch_last_run": last_run.isoformat() if last_run else None,
        "batch_age_seconds": batch_age,
        "batch_status": status,
        "batch_error": error,
    }


@app.get("/api/ml/metrics")
def get_ml_metrics() -> dict:
    """Held-out evaluation metrics for the human-vs-non-human liveness classifier.

    Returns the contents of ``output/models/metrics.json`` written by
    ``keyspark.ml evaluate``. An absent file means the user has not
    trained or evaluated the model yet - we return ``{"available":
    false}`` rather than 404 so the dashboard can render a "not yet
    trained" state cleanly.
    """
    p = Path(ML_METRICS_PATH)
    if not p.exists():
        return {"available": False}
    return {"available": True, **json.loads(p.read_text())}


@app.get("/api/liveness")
def get_liveness(user: Optional[str] = None) -> list:
    """Per-(user, day) human-vs-non-human flags from output/liveness.parquet,
    written by the batch scheduler's liveness scoring. The calendar colors a
    day red where ``nonhuman`` is true. Empty list until the model has scored
    at least once. Optional ``user`` filter.
    """
    df = _read_parquet(LIVENESS_PATH)
    if df.empty:
        return []
    if user is not None:
        df = df[df["user"] == user]
    return _to_records(df)
