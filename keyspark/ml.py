"""KeySpark human vs non-human (liveness) classifier. Per one-minute window,
decides whether activity came from a real person or input automation (mouse
jiggler, auto-typer, auto-clicker, keep-awake). The signal is the natural
irregularity and variety of human input vs the regularity of a bot.

Two classes, both run through the SAME featurizer (no train/serve skew):
  - human (label 0): real recorded events in output/events.
  - non-human (label 1): synthetic bot events from keyspark.botgen.

Features are per-window, cross-modal (keyboard AND mouse), and shape-based
(regularity / diversity / input mix), not volume-based - see FEATURES below.
Model: scikit-learn RandomForestClassifier (class_weight balanced).

  uv run python -m keyspark.ml train       # fit on all windows, persist
  uv run python -m keyspark.ml evaluate    # held-out accuracy/.../ROC-AUC
  uv run python -m keyspark.ml predict     # per-window non-human probability
  uv run python -m keyspark.ml score       # write output/liveness.parquet

Reads only output/events and the synthetic generator; does not depend on the
batch job's session summaries.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split

from keyspark import botgen

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
EVENTS_PATH = "output/events"
MODEL_DIR = Path("output/models")
MODEL_PATH = MODEL_DIR / "liveness_classifier.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"
LIVENESS_PATH = "output/liveness.parquet"

# The per-window shape features, in a stable order:
#   key_diversity : distinct keys / keystrokes (low for an auto-typer)
#   ks_iei_cv     : CV of inter-keystroke intervals
#   move_iei_cv   : CV of inter-mouse-move intervals
#   iei_cv        : CV of all inter-event intervals (low for a fixed-cadence timer)
#   step_mean     : mean mouse-move step size (px)
#   step_cv       : CV of mouse-move step size (low for rigid micro-geometry)
#   mouse_fraction: mouse events / all events (jiggler ~1.0, typer ~0.0)
FEATURES = [
    "key_diversity",
    "ks_iei_cv",
    "move_iei_cv",
    "iei_cv",
    "step_mean",
    "step_cv",
    "mouse_fraction",
]

# A day is flagged non-human only on a SUSTAINED burst: >= MIN_FLAG_WINDOWS
# one-minute windows scoring >= WINDOW_THRESHOLD. Requiring >1 window stops one
# odd human minute from painting a day red.
WINDOW_THRESHOLD = 0.8   # tune: per-window non-human probability to count as flagged
MIN_FLAG_WINDOWS = 2     # tune: flagged windows in a day before the day flags

# A window needs at least this many events for its shape features to be
# meaningful: with <3 events every interval CV is 0 and the step features need
# >=2 moves, so a near-empty minute collapses to an all-zeros row that looks like
# a low-variability bot. Such windows are dropped - we abstain rather than accuse.
MIN_WINDOW_EVENTS = 5    # tune: min events for a window to be scored

MIN_ROWS = 30            # tune: refuse to train/evaluate below this many labeled windows

# Demo/test bot users and seeded demo days are automation we deliberately fed in,
# so they must NOT count as human ground truth when training. They ARE still
# scored/flagged at inference; only the human training class excludes them.
HUMAN_EXCLUDE_USERS = frozenset({"bot-test"})  # tune: users excluded from the human class
HUMAN_EXCLUDE_DAYS = frozenset({              # tune: seeded demo days excluded from the human class
    "2026-05-11", "2026-05-13", "2026-05-15"
})


# --------------------------------------------------------------------------
# Feature engineering (shared by training and scoring -> no feature skew)
# --------------------------------------------------------------------------
def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a raw-event frame (real or synthetic) to the columns the
    featurizer needs, deriving event_time from epoch ts when absent.
    """
    df = df.copy()
    if "event_time" not in df.columns:
        df["event_time"] = pd.to_datetime(df["ts"], unit="s")
    df["event_time"] = pd.to_datetime(df["event_time"])
    for c in ("type", "key", "x", "y", "user"):
        if c not in df.columns:
            df[c] = pd.NA
    return df[["user", "event_time", "type", "key", "x", "y"]]


def _cv(times: np.ndarray) -> float:
    """Coefficient of variation (std/mean) of the gaps between sorted timestamps.
    0.0 with too few events to form >=2 gaps. Low CV = robotic; high CV = human.
    """
    if times.size < 3:
        return 0.0
    gaps = np.diff(times)
    mean = gaps.mean()
    return float(gaps.std() / mean) if mean > 0 else 0.0


def _one_window(user, window_start, g: pd.DataFrame) -> dict:
    """Compute the cross-modal shape features for one (user, minute)."""
    typ = g["type"].to_numpy()
    t = g["t"].to_numpy()  # seconds, already ascending (frame is sorted)
    is_kd = typ == "key_down"
    is_mv = typ == "move"
    is_mouse = np.isin(typ, ("move", "click", "scroll"))
    n = len(g)

    keystrokes = int(is_kd.sum())
    keys = [k for k in g["key"].to_numpy()[is_kd].tolist() if k is not None and k == k]
    key_diversity = (len(set(keys)) / keystrokes) if keystrokes else 0.0

    xs = pd.to_numeric(g["x"].to_numpy()[is_mv], errors="coerce")
    ys = pd.to_numeric(g["y"].to_numpy()[is_mv], errors="coerce")
    valid = ~(np.isnan(xs) | np.isnan(ys))
    xs, ys = xs[valid], ys[valid]
    if xs.size >= 2:
        steps = np.sqrt(np.diff(xs) ** 2 + np.diff(ys) ** 2)
        step_mean = float(steps.mean())
        step_cv = float(steps.std() / steps.mean()) if steps.mean() > 0 else 0.0
    else:
        step_mean = step_cv = 0.0

    return {
        "user": user,
        "window_start": window_start,
        "n_events": n,
        "key_diversity": key_diversity,
        "ks_iei_cv": _cv(t[is_kd]),
        "move_iei_cv": _cv(t[is_mv]),
        "iei_cv": _cv(t),
        "step_mean": step_mean,
        "step_cv": step_cv,
        "mouse_fraction": float(is_mouse.sum() / n) if n else 0.0,
    }


def _window_features(df: pd.DataFrame) -> pd.DataFrame:
    """One row of FEATURES per (user, one-minute window), dropping near-empty
    windows (< MIN_WINDOW_EVENTS) whose shape features are degenerate.
    """
    df = _prepare(df).dropna(subset=["event_time"]).sort_values(
        ["user", "event_time"]
    )
    if df.empty:
        return pd.DataFrame(columns=["user", "window_start", "n_events", *FEATURES])
    df["window_start"] = df["event_time"].dt.floor("min")
    df["t"] = df["event_time"].astype("int64") / 1e9
    rows = [
        _one_window(user, ws, g)
        for (user, ws), g in df.groupby(["user", "window_start"], sort=False)
    ]
    out = pd.DataFrame(rows)
    return out[out["n_events"] >= MIN_WINDOW_EVENTS].reset_index(drop=True)


def _load_events() -> pd.DataFrame:
    """Read the full raw event archive (pandas/pyarrow reads the directory
    directly, ignoring Spark's _spark_metadata; a glob is the fallback).
    """
    if not Path(EVENTS_PATH).exists():
        raise FileNotFoundError(
            f"Missing {EVENTS_PATH}. Start the pipeline so the streaming job "
            "archives events before training."
        )
    try:
        return pd.read_parquet(EVENTS_PATH)
    except Exception:
        import glob

        files = glob.glob(f"{EVENTS_PATH}/part-*.parquet")
        if not files:
            raise
        return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)


def _labeled_dataset() -> pd.DataFrame:
    """Human windows (label 0) from the real archive + non-human windows (label 1)
    from synthetic bot events, both via _window_features. Demo/test bot users and
    seeded demo days are excluded from the human class so a live demo never
    poisons the ground truth.
    """
    events = _load_events()
    events = events[~events["user"].isin(HUMAN_EXCLUDE_USERS)]
    if HUMAN_EXCLUDE_DAYS:
        et = (events["event_time"] if "event_time" in events.columns
              else pd.to_datetime(events["ts"], unit="s"))
        events = events[~_local_day(et).isin(HUMAN_EXCLUDE_DAYS).to_numpy()]
    human = _window_features(events)
    human["label"] = 0
    synth = _window_features(botgen.synthetic_event_frame())
    synth["label"] = 1
    return pd.concat([human, synth], ignore_index=True)


# --------------------------------------------------------------------------
# Model
# --------------------------------------------------------------------------
def _make_model() -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=300,         # tune: number of trees
        min_samples_leaf=2,       # tune: min samples per leaf (higher = more regularized)
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )


# --------------------------------------------------------------------------
# Train / evaluate
# --------------------------------------------------------------------------
@dataclass
class EvalReport:
    task: str
    n_samples: int
    n_train: int
    n_test: int
    class_balance: dict = field(default_factory=dict)
    features: list = field(default_factory=list)
    model: dict = field(default_factory=dict)
    feature_importances: dict = field(default_factory=dict)


def train() -> dict:
    """Fit the classifier on all available windows and persist it. Honest metrics
    come from evaluate() (which fits only on the training split).
    """
    df = _labeled_dataset()
    if len(df) < MIN_ROWS:
        raise ValueError(
            f"need at least {MIN_ROWS} labeled windows (got {len(df)}). "
            "Record more activity and re-run the batch job."
        )
    model = _make_model()
    model.fit(df[FEATURES], df["label"])
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "features": FEATURES}, MODEL_PATH)
    counts = df["label"].value_counts().to_dict()
    return {
        "model_path": str(MODEL_PATH),
        "n_samples": int(len(df)),
        "human": int(counts.get(0, 0)),
        "non_human": int(counts.get(1, 0)),
    }


def evaluate() -> EvalReport:
    """Stratified hold-out evaluation: accuracy, precision, recall, F1, ROC-AUC
    for the non-human class; writes metrics.json.
    """
    df = _labeled_dataset()
    if len(df) < MIN_ROWS:
        raise ValueError(f"need at least {MIN_ROWS} labeled windows (got {len(df)}).")
    X, y = df[FEATURES], df["label"]
    # tune: test_size = hold-out fraction (0.25 = a 75/25 split).
    X_tr, X_te, y_tr, y_te = train_test_split(
        X, y, test_size=0.25, stratify=y, random_state=42
    )
    model = _make_model()
    model.fit(X_tr, y_tr)
    pred = model.predict(X_te)
    proba = model.predict_proba(X_te)[:, 1]

    metrics = {
        "accuracy": float(accuracy_score(y_te, pred)),
        "precision": float(precision_score(y_te, pred, zero_division=0)),
        "recall": float(recall_score(y_te, pred, zero_division=0)),
        "f1": float(f1_score(y_te, pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_te, proba)) if y_te.nunique() > 1 else None,
    }
    counts = y.value_counts().to_dict()
    report = EvalReport(
        task="human vs non-human (input automation) per-window classification",
        n_samples=int(len(df)),
        n_train=int(len(X_tr)),
        n_test=int(len(X_te)),
        class_balance={"human": int(counts.get(0, 0)), "non_human": int(counts.get(1, 0))},
        features=FEATURES,
        model=metrics,
        feature_importances={
            f: float(w) for f, w in zip(FEATURES, model.feature_importances_)
        },
    )
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(json.dumps(asdict(report), indent=2))
    return report


# --------------------------------------------------------------------------
# Scoring / inference (feeds output/liveness.parquet for the dashboard calendar)
# --------------------------------------------------------------------------
def _score_windows() -> pd.DataFrame:
    """One row per (user, window_start) with the model's non-human probability.
    Empty if no model or no events.
    """
    if not MODEL_PATH.exists():
        return pd.DataFrame(columns=["user", "window_start", "nonhuman_proba"])
    bundle = joblib.load(MODEL_PATH)
    feats = _window_features(_load_events())
    if feats.empty:
        return pd.DataFrame(columns=["user", "window_start", "nonhuman_proba"])
    feats["nonhuman_proba"] = bundle["model"].predict_proba(feats[bundle["features"]])[:, 1]
    return feats[["user", "window_start", "nonhuman_proba"]]


def _local_day(window_start: pd.Series) -> pd.Series:
    """Map UTC window-start timestamps to host-local calendar-day keys
    (YYYY-MM-DD), matching the browser's localDayKey and the batch job.
    """
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    return (
        pd.to_datetime(window_start)
        .dt.tz_localize("UTC")
        .dt.tz_convert(local_tz)
        .dt.strftime("%Y-%m-%d")
    )


def score_days() -> pd.DataFrame:
    """Aggregate per-window scores to a per-(user, day) flag: a day is flagged
    when >= MIN_FLAG_WINDOWS of its windows score >= WINDOW_THRESHOLD. Returns
    columns [user, day, nonhuman, score].
    """
    windows = _score_windows()
    if windows.empty:
        return pd.DataFrame(columns=["user", "day", "nonhuman", "score"])
    windows = windows.copy()
    windows["day"] = _local_day(windows["window_start"])
    windows["flagged"] = windows["nonhuman_proba"] >= WINDOW_THRESHOLD
    agg = (
        windows.groupby(["user", "day"])
        .agg(score=("nonhuman_proba", "max"), flagged_windows=("flagged", "sum"))
        .reset_index()
    )
    agg["nonhuman"] = agg["flagged_windows"] >= MIN_FLAG_WINDOWS
    return agg[["user", "day", "nonhuman", "score"]]


def write_liveness() -> int:
    """Score and write output/liveness.parquet. Returns the row count. Called by
    the API's batch scheduler after each compute_all.
    """
    days = score_days()
    days.to_parquet(LIVENESS_PATH, index=False)
    return len(days)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def _format_report(r: EvalReport) -> str:
    lines = [
        f"task:    {r.task}",
        f"data:    {r.n_samples} windows ({r.n_train} train / {r.n_test} test)",
        f"balance: human={r.class_balance.get('human')} "
        f"non_human={r.class_balance.get('non_human')} (class_weight=balanced)",
        "",
        "metrics (held-out, non-human = positive class):",
    ]
    for k in ("accuracy", "precision", "recall", "f1", "roc_auc"):
        v = r.model.get(k)
        lines.append(f"  {k:<10} {v:.3f}" if isinstance(v, float) else f"  {k:<10} n/a")
    lines += ["", "feature importances:"]
    for f, w in sorted(r.feature_importances.items(), key=lambda x: -x[1]):
        lines.append(f"  {f:<16} {w:.3f} {'#' * int(w * 40)}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="KeySpark liveness classifier")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("train", help="fit on all windows and persist the model")
    sub.add_parser("evaluate", help="stratified hold-out metrics -> metrics.json")
    sub.add_parser("predict", help="print per-window non-human probabilities")
    sub.add_parser("score", help="write per-day flags to output/liveness.parquet")
    args = parser.parse_args()

    if args.cmd == "train":
        print(json.dumps(train(), indent=2))
    elif args.cmd == "evaluate":
        print(_format_report(evaluate()))
    elif args.cmd == "predict":
        df = _score_windows()
        if df.empty:
            print("no predictions: train the model first")
        else:
            print(df.sort_values("nonhuman_proba", ascending=False).head(20).to_string(index=False))
            print(f"... {len(df)} windows scored")
    elif args.cmd == "score":
        n = write_liveness()
        print(f"wrote {n} (user, day) rows to {LIVENESS_PATH}")


if __name__ == "__main__":
    main()
