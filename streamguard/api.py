"""StreamGuard backend API.

Thin FastAPI service that reads the four parquet outputs from the
streaming and batch jobs and serves them as JSON for the React
dashboard. Pandas reads parquet directly on each request; no
caching, no SQL layer. Adequate for a single-user demo.

In addition to serving the parquet data, this process now owns the
batch job. On startup, ``lifespan`` builds a Spark session and
synchronously runs ``compute_all`` once so the dashboard isn't
stale immediately after a restart — uvicorn does not accept
connections until that completes (~60-120 s on cold start). After
that, an APScheduler ``BackgroundScheduler`` fires ``compute_all``
every ``REFRESH_INTERVAL_SEC`` seconds in a worker thread so live
HTTP traffic is not blocked by Spark calls. The current state of
the batch loop is exposed via ``/api/batch_status`` and consumed by
the frontend staleness chips.
"""

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from streamguard.batch_job import build_session, compute_all

METRICS_PATH = "output/metrics"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"

SESSIONS_LIMIT = 50
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


def _run_batch(spark) -> None:
    """Scheduler entry point. Wraps ``compute_all`` so failures are
    recorded in ``batch_state`` rather than crashing the scheduler
    thread. ``last_run`` is set on both success and failure so the
    staleness chip always shows a real timestamp.
    """
    batch_state.last_status = "running"
    try:
        compute_all(spark)
        batch_state.last_status = "ok"
        batch_state.last_error = None
    except Exception as exc:  # noqa: BLE001 — see docstring
        batch_state.last_status = "failed"
        batch_state.last_error = str(exc)
    finally:
        batch_state.last_run = datetime.now(timezone.utc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build a long-lived Spark session, run the batch once
    synchronously, then schedule it every REFRESH_INTERVAL_SEC.
    Spark is torn down on shutdown.
    """
    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    # First run blocks until done so /api/batch_status returns a
    # real last_run on the first poll and the dashboard isn't stale.
    _run_batch(spark)

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        _run_batch,
        "interval",
        seconds=REFRESH_INTERVAL_SEC,
        args=[spark],
        max_instances=1,  # skip tick if previous run is still in progress
        coalesce=True,    # collapse multiple missed ticks into one run
        next_run_time=datetime.now() + timedelta(seconds=REFRESH_INTERVAL_SEC),
    )
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        spark.stop()


app = FastAPI(title="StreamGuard API", lifespan=lifespan)
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


@app.get("/api/batch_status")
def get_batch_status() -> dict:
    """Most recent batch-job run state. Source of truth for the
    staleness chips on the heatmap, hotspots, and sessions panels.
    """
    return {
        "last_run": batch_state.last_run.isoformat() if batch_state.last_run else None,
        "status": batch_state.last_status,
        "error": batch_state.last_error,
    }
