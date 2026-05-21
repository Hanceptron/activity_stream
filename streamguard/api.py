"""StreamGuard backend API.

Thin FastAPI service that reads the four parquet outputs from the
streaming and batch jobs and serves them as JSON for the React
dashboard. Pandas reads parquet directly on each request; no
caching, no SQL layer. Adequate for a single-user demo.
"""

import json
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

METRICS_PATH = "output/metrics"
SESSIONS_PATH = "output/sessions"
BASELINE_PATH = "output/baseline"
HEATMAPS_PATH = "output/heatmaps"

SESSIONS_LIMIT = 50
HEATMAP_RANGES = {"1h", "6h", "1d", "3d", "1w"}

app = FastAPI(title="StreamGuard API")
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
