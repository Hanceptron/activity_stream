"""KeySpark backend API.

Thin FastAPI service that reads the four parquet outputs from the
streaming and batch jobs and serves them as JSON for the React
dashboard. Pandas reads parquet directly on each request; no
caching, no SQL layer. Adequate for a single-user demo.

In addition to serving the parquet data, this process now owns the
batch job. On startup, ``lifespan`` builds a Spark session and
synchronously runs ``compute_all`` once so the dashboard isn't
stale immediately after a restart - uvicorn does not accept
connections until that completes (~60-120 s on cold start). After
that, an APScheduler ``BackgroundScheduler`` fires ``compute_all``
every ``REFRESH_INTERVAL_SEC`` seconds in a worker thread so live
HTTP traffic is not blocked by Spark calls. The current state of
the batch loop is exposed via ``/api/batch_status`` and consumed by
the frontend staleness chips.
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

from streamguard.batch_job import CELL_SIZE, build_session, compute_all

log = logging.getLogger("streamguard.api")

METRICS_PATH = "output/metrics"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"
DAY_MINUTE_METRICS_PATH = "output/day_minute_metrics"
HEATMAP_BY_DAY_PATH = "output/heatmap_by_day"
ML_METRICS_PATH = "output/models/metrics.json"
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


def _run_batch(spark) -> None:
    """Scheduler entry point. Wraps ``compute_all`` so failures are
    recorded in ``batch_state``. ``last_run`` is set on both success
    and failure so the staleness chip always shows a real timestamp.

    On any failure we exit the whole process so the outer bash
    restart loop respawns the API with a fresh Spark session. macOS
    sleep/wake breaks the Spark RPC permanently for the surviving
    JVM, and PySpark cannot rebuild a session in-process after the
    JVM dies. Without an exit, every subsequent 5-minute tick would
    fail the same way and ``last_run`` would freeze - breaking the
    `/api/batch_status` freshness contract.
    """
    with batch_state_lock:
        batch_state.last_status = "running"
    try:
        compute_all(spark)
        with batch_state_lock:
            batch_state.last_status = "ok"
            batch_state.last_error = None
            batch_state.last_run = datetime.now(timezone.utc)
    except Exception as exc:  # noqa: BLE001 - see docstring
        # Record the failure before exiting so a request that lands
        # between the failure and process death sees the real state.
        log.exception("batch run failed - exiting for fresh Spark respawn")
        with batch_state_lock:
            batch_state.last_status = "failed"
            batch_state.last_error = str(exc)
            batch_state.last_run = datetime.now(timezone.utc)
        # os._exit bypasses uvicorn's signal handlers, the
        # APScheduler shutdown, and the asyncio loop. That is
        # intentional - the outer bash loop is the recovery
        # mechanism, and we want a hard, fast restart rather than a
        # tidy one. Anything that touches Spark here would also
        # hang on the same dead RPC.
        os._exit(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build a long-lived Spark session and schedule the batch every
    REFRESH_INTERVAL_SEC, kicking the first run off a couple of seconds
    AFTER startup rather than synchronously. Spark is torn down on
    shutdown.

    The first batch used to run synchronously here, which blocked
    uvicorn from accepting connections for the ~60-120 s it takes - so
    after every respawn (e.g. post-wake) the dashboard was unreachable
    for that whole window. Yielding first lets the API serve the
    existing parquet immediately; the scheduler fills in fresh data a
    few seconds later (batch_status reads "idle" until the first run
    completes).
    """
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _run_batch,
        "interval",
        seconds=REFRESH_INTERVAL_SEC,
        args=[spark],
        max_instances=1,  # skip tick if previous run is still in progress
        coalesce=True,    # collapse multiple missed ticks into one run
        # First run ~2 s after startup (off the uvicorn startup path), not
        # one full interval later, so fresh data lands quickly without
        # blocking the dashboard from coming up.
        next_run_time=datetime.now() + timedelta(seconds=2),
    )
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        spark.stop()


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
    """Cross-validated evaluation metrics for the fatigue classifier.

    Returns the contents of ``output/models/metrics.json`` written by
    ``streamguard.ml evaluate``. An absent file means the user has not
    trained or evaluated the model yet - we return ``{"available":
    false}`` rather than 404 so the dashboard can render a "not yet
    trained" state cleanly.
    """
    p = Path(ML_METRICS_PATH)
    if not p.exists():
        return {"available": False}
    return {"available": True, **json.loads(p.read_text())}
