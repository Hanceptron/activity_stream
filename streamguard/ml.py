"""StreamGuard next-window keystroke forecaster.

A one-step-ahead regression model: given the last ``LAGS`` one-minute
windows of a typing session, predict the *next* window's keystroke
count. Because we only ever use **past** windows (plus the wall-clock
hour and how many minutes into the session we are) to predict a
**future** value, the task is leakage-free by construction - there is
no circular label derived from the thing we are predicting.

The model is judged against two naive baselines on a chronological
hold-out (train on the earliest windows, test on the most recent),
which mirrors how it would really be used - fit on history, forecast
forward:

  - persistence : "the next minute looks like this minute"
                  (predict y_t = keystrokes_{t-1})
  - mean        : predict the training-set average every time

Reported metrics are RMSE, MAE, and R^2. Beating the persistence
baseline means the model has learned something past a single-step
carry-forward.

  uv run python -m streamguard.ml train       # fit on all windows, persist
  uv run python -m streamguard.ml evaluate    # chronological hold-out metrics
  uv run python -m streamguard.ml predict     # per-window forecasts, printed

The model reads only ``output/per_window`` (produced by the Spark batch
job). It has no dependency on the batch job's fatigue_index or session
summaries.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import asdict, dataclass, field
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

log = logging.getLogger("streamguard.ml")

PER_WINDOW_PATH = "output/per_window"
MODEL_DIR = Path("output/models")
MODEL_PATH = MODEL_DIR / "keystroke_forecaster.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"

# We forecast keystrokes. The other three per-window counts feed in only
# as lagged context features.
TARGET = "keystrokes"
COUNT_COLS = ["keystrokes", "words", "corrections", "clicks"]

# How many previous windows feed each prediction. 3 minutes of context
# captures short-term momentum without discarding too many session-start
# rows (the first LAGS windows of every session have no full history and
# are dropped).
LAGS = 3

# Fraction of the (chronologically last) data held out for evaluation.
TEST_FRACTION = 0.25

# Refuse to train/evaluate on a dataset too small to be meaningful.
MIN_ROWS = 30

# Lag features first (stable order), then two non-leaky context features:
# window_idx (how many minutes into the session we are - known in real
# time) and hour (wall-clock hour of day). We deliberately do NOT use
# session length / progress, which would peek at how long the session
# ultimately runs and leak the future into the prediction.
LAG_FEATURES = [f"{c}_lag{lag}" for lag in range(1, LAGS + 1) for c in COUNT_COLS]
FEATURES = LAG_FEATURES + ["window_idx", "hour"]


def _build_features() -> pd.DataFrame:
    """Load output/per_window and build the lag-based feature table.

    One row per (session_id, window). Within each session, in event-time
    order, every count column is shifted forward 1..LAGS windows to make
    "previous minute(s)" features. The first LAGS windows of each session
    have incomplete history and are dropped. Returns the feature columns
    plus the TARGET (the current window's keystrokes).
    """
    if not Path(PER_WINDOW_PATH).exists():
        raise FileNotFoundError(
            f"Missing {PER_WINDOW_PATH}. Run the batch job at least once "
            "(uv run python -m streamguard.batch_job) to populate it."
        )

    df = pd.read_parquet(PER_WINDOW_PATH)
    if df.empty:
        raise ValueError(
            f"{PER_WINDOW_PATH} is empty. Record some typing and re-run the "
            "batch job before training."
        )

    df["window_start"] = pd.to_datetime(df["window_start"])
    df = df.sort_values(["user", "session_id", "window_start"]).reset_index(drop=True)

    # Shift within each session so a lag never reaches across a session
    # boundary (the gap between sessions is >= 5 min of idle, not a real
    # "previous minute").
    grp = df.groupby(["session_id", "user"], sort=False)
    for lag in range(1, LAGS + 1):
        for c in COUNT_COLS:
            df[f"{c}_lag{lag}"] = grp[c].shift(lag)

    # Minutes elapsed in the session so far (0-based). Known in real time.
    df["window_idx"] = grp.cumcount()
    df["hour"] = df["window_start"].dt.hour

    # Drop the session-start rows that lack a full LAGS-window history.
    df = df.dropna(subset=LAG_FEATURES).reset_index(drop=True)
    return df


def _chronological_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split by time: the earliest (1 - TEST_FRACTION) of rows train, the
    most recent TEST_FRACTION test. Forecasting is judged on the future,
    so the test set must be strictly later than the training set.
    """
    df = df.sort_values("window_start").reset_index(drop=True)
    cut = int(len(df) * (1.0 - TEST_FRACTION))
    return df.iloc[:cut].copy(), df.iloc[cut:].copy()


def _make_model() -> RandomForestRegressor:
    return RandomForestRegressor(
        n_estimators=300,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )


def _regression_metrics(y_true, y_pred) -> dict:
    # RMSE via sqrt(MSE) rather than the squared= kwarg, which is
    # deprecated/removed across recent scikit-learn versions.
    return {
        "rmse": float(np.sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


@dataclass
class EvalReport:
    """Regression evaluation summary saved to metrics.json, used by the
    paper's results section and served by /api/ml/metrics.
    """

    task: str
    target: str
    lags: int
    n_samples: int
    n_train: int
    n_test: int
    split: str
    features: list = field(default_factory=list)
    model: dict = field(default_factory=dict)
    baseline_persistence: dict = field(default_factory=dict)
    baseline_mean: dict = field(default_factory=dict)
    rmse_improvement_over_persistence_pct: float = 0.0
    feature_importances: dict = field(default_factory=dict)


def train() -> dict:
    """Fit the forecaster on every available window and persist it. The
    persisted model trains on all data; honest metrics come from
    ``evaluate``, which fits only on the training split.
    """
    df = _build_features()
    if len(df) < MIN_ROWS:
        raise ValueError(
            f"need at least {MIN_ROWS} usable windows after lagging "
            f"(got {len(df)}). Record more typing and re-run the batch job."
        )

    model = _make_model()
    model.fit(df[FEATURES], df[TARGET])

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"model": model, "features": FEATURES, "target": TARGET, "lags": LAGS},
        MODEL_PATH,
    )
    log.info("trained on %d windows; saved model to %s", len(df), MODEL_PATH)
    return {
        "model_path": str(MODEL_PATH),
        "n_samples": int(len(df)),
        "n_features": len(FEATURES),
    }


def evaluate() -> EvalReport:
    """Chronological hold-out evaluation. Fits on the earliest windows,
    predicts the most recent, and compares against the persistence and
    mean baselines. Writes metrics.json.
    """
    df = _build_features()
    if len(df) < MIN_ROWS:
        raise ValueError(
            f"need at least {MIN_ROWS} usable windows after lagging "
            f"(got {len(df)})."
        )

    train_df, test_df = _chronological_split(df)

    model = _make_model()
    model.fit(train_df[FEATURES], train_df[TARGET])
    y_test = test_df[TARGET].to_numpy()
    y_pred = model.predict(test_df[FEATURES])

    # Both baselines scored on the SAME held-out rows.
    persistence_pred = test_df["keystrokes_lag1"].to_numpy()
    mean_pred = np.full(len(test_df), float(train_df[TARGET].mean()))

    m_model = _regression_metrics(y_test, y_pred)
    m_pers = _regression_metrics(y_test, persistence_pred)
    m_mean = _regression_metrics(y_test, mean_pred)
    impr = (
        100.0 * (m_pers["rmse"] - m_model["rmse"]) / m_pers["rmse"]
        if m_pers["rmse"]
        else 0.0
    )

    report = EvalReport(
        task="next-window keystroke count (1-step-ahead forecast)",
        target=TARGET,
        lags=LAGS,
        n_samples=int(len(df)),
        n_train=int(len(train_df)),
        n_test=int(len(test_df)),
        split=f"chronological {int((1 - TEST_FRACTION) * 100)}/"
        f"{int(TEST_FRACTION * 100)} by window_start",
        features=FEATURES,
        model=m_model,
        baseline_persistence=m_pers,
        baseline_mean=m_mean,
        rmse_improvement_over_persistence_pct=round(impr, 2),
        feature_importances={
            f: float(w) for f, w in zip(FEATURES, model.feature_importances_)
        },
    )

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(json.dumps(asdict(report), indent=2))
    log.info("saved metrics to %s", METRICS_PATH)
    return report


def predict() -> pd.DataFrame:
    """Run the persisted model over every usable window and return one row
    per (session_id, user, window_start) with the actual and predicted
    keystroke counts. Empty frame when no model has been trained yet.
    """
    if not MODEL_PATH.exists():
        log.info("no model at %s; run `train` first", MODEL_PATH)
        return pd.DataFrame()

    bundle = joblib.load(MODEL_PATH)
    model = bundle["model"]
    features = bundle["features"]

    df = _build_features()
    if df.empty:
        return pd.DataFrame()

    df["predicted_keystrokes"] = model.predict(df[features])
    return (
        df[["session_id", "user", "window_start", TARGET, "predicted_keystrokes"]]
        .rename(columns={TARGET: "actual_keystrokes"})
        .reset_index(drop=True)
    )


def _format_report(r: EvalReport) -> str:
    def row(name: str, m: dict) -> str:
        return (
            f"  {name:<14} rmse={m['rmse']:8.2f}  "
            f"mae={m['mae']:8.2f}  r2={m['r2']:7.3f}"
        )

    lines = [
        f"task:   {r.task}",
        f"data:   {r.n_samples} windows  ({r.n_train} train / {r.n_test} test)"
        f",  {r.lags} lags",
        f"split:  {r.split}",
        "",
        "metrics on the held-out (most recent) windows:",
        row("model (RF)", r.model),
        row("persistence", r.baseline_persistence),
        row("mean", r.baseline_mean),
        "",
        f"RMSE improvement over persistence: "
        f"{r.rmse_improvement_over_persistence_pct:.1f}%",
        "",
        "feature importances:",
    ]
    for f, w in sorted(r.feature_importances.items(), key=lambda x: -x[1]):
        lines.append(f"  {f:<20} {w:.3f} {'#' * int(w * 40)}")
    return "\n".join(lines)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="StreamGuard next-window keystroke forecaster"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("train", help="fit on all windows and persist the model")
    sub.add_parser("evaluate", help="chronological hold-out metrics -> metrics.json")
    sub.add_parser("predict", help="print per-window actual vs predicted keystrokes")
    args = parser.parse_args()

    if args.cmd == "train":
        print(json.dumps(train(), indent=2))
    elif args.cmd == "evaluate":
        print(_format_report(evaluate()))
    elif args.cmd == "predict":
        df = predict()
        if df.empty:
            print("no predictions: train the model first")
        else:
            print(df.head(20).to_string(index=False))
            print(f"... {len(df)} rows total")


if __name__ == "__main__":
    main()
