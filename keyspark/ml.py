"""KeySpark human vs non-human (liveness) classifier.

Detects whether a minute of activity was produced by a real person or by
input automation (mouse jiggler, auto-typer, auto-clicker). The signal is
the natural irregularity and variety of human input: humans vary their
timing, movement, and keys; bots are regular and repetitive.

Two classes:
  - human (label 0): the real recorded events in ``output/events``.
  - non-human (label 1): synthetic bot events from ``keyspark.botgen``,
    featurized through the SAME function as real events (no train/serve
    feature skew).

Features are computed per one-minute window and are deliberately
cross-modal (keyboard AND mouse) and shape-based (regularity / diversity
/ input mix), not volume-based:
  - key_diversity : distinct keys / keystrokes (low for an auto-typer)
  - ks_iei_cv     : coeff. of variation of inter-keystroke intervals
  - move_iei_cv   : coeff. of variation of inter-mouse-move intervals
  - iei_cv        : coeff. of variation of all inter-event intervals
  - step_mean     : mean mouse-move step size (px)
  - step_cv       : coeff. of variation of mouse-move step size
  - mouse_fraction: mouse events / all events (jiggler ~1.0, typer ~0.0)

Model: scikit-learn RandomForestClassifier (class_weight balanced).

  uv run python -m keyspark.ml train       # fit on all windows, persist
  uv run python -m keyspark.ml evaluate    # held-out accuracy/.../ROC-AUC
  uv run python -m keyspark.ml predict     # per-window non-human probability
  uv run python -m keyspark.ml score       # write output/liveness.parquet

The model and scoring read only ``output/events`` and the synthetic
generator; they do not depend on the batch job's session summaries.
"""

from __future__ import annotations

import argparse
import json
import logging
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

log = logging.getLogger("keyspark.ml")

EVENTS_PATH = "output/events"
MODEL_DIR = Path("output/models")
MODEL_PATH = MODEL_DIR / "liveness_classifier.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"
LIVENESS_PATH = "output/liveness.parquet"

FEATURES = [
    "key_diversity",
    "ks_iei_cv",
    "move_iei_cv",
    "iei_cv",
    "step_mean",
    "step_cv",
    "mouse_fraction",
]

# A day is flagged non-human only on a SUSTAINED automated burst: at least
# MIN_FLAG_WINDOWS one-minute windows scoring >= WINDOW_THRESHOLD. Requiring
# more than one window stops a single odd human minute from painting a day
# red (human scores are ~all near 0; isolated outliers do happen).
WINDOW_THRESHOLD = 0.8
MIN_FLAG_WINDOWS = 2

# A one-minute window needs at least this many events before its shape features
# are meaningful. With fewer than 3 events every inter-event-interval CV is 0 by
# construction (see _cv) and the mouse-step features need >= 2 moves, so a
# near-empty minute (a single stray keystroke or mouse twitch) collapses to an
# all-zeros feature row that is indistinguishable from a low-variability bot.
# Such windows carry no evidence of automation, so they are dropped from
# training and can never raise a flag - we abstain rather than accuse.
MIN_WINDOW_EVENTS = 5

# Refuse to train/evaluate on too little data to be meaningful.
MIN_ROWS = 30

# Demo/test-injected bot users must NOT count as human ground truth when
# training (they are automation we deliberately fed through Kafka). They
# are still scored/flagged at inference; only the human training class
# excludes them.
HUMAN_EXCLUDE_USERS = frozenset({"bot-test"})


# --------------------------------------------------------------------------
# Feature engineering (shared by training and scoring -> no feature skew)
# --------------------------------------------------------------------------

def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize a raw-event frame (real archive or synthetic) to the
    columns the featurizer needs, deriving ``event_time`` from ``ts`` when
    absent (synthetic events carry only epoch ``ts``).
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
    """Coefficient of variation (std/mean) of the gaps between sorted
    timestamps. 0.0 when there are too few events to form >= 2 gaps.
    Low CV = robotic regular cadence; high CV = human burstiness.
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
    """One row of FEATURES per (user, one-minute window), excluding near-empty
    windows (< MIN_WINDOW_EVENTS events) whose shape features are degenerate
    and carry no evidence either way.
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
    """Read the full raw event archive. pandas/pyarrow reads the whole
    directory directly (ignoring Spark's _spark_metadata); a glob is the
    fallback if a bare-directory read ever errors.
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
    """Human windows (label 0) from the real archive + non-human windows
    (label 1) from synthetic bot events, both via ``_window_features``.

    Demo/test-injected bot users are excluded from the human class so a
    live demo never poisons the ground truth (they are bots, not humans).
    """
    events = _load_events()
    events = events[~events["user"].isin(HUMAN_EXCLUDE_USERS)]
    human = _window_features(events)
    human["label"] = 0
    synth = _window_features(botgen.synthetic_event_frame())
    synth["label"] = 1
    return pd.concat([human, synth], ignore_index=True)


def _make_model() -> RandomForestClassifier:
    return RandomForestClassifier(
        n_estimators=300,
        min_samples_leaf=2,
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
    """Fit the classifier on all available windows and persist it. Honest
    metrics come from ``evaluate`` (which fits only on the training split).
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
    log.info("trained on %d windows; saved model to %s", len(df), MODEL_PATH)
    return {
        "model_path": str(MODEL_PATH),
        "n_samples": int(len(df)),
        "human": int(counts.get(0, 0)),
        "non_human": int(counts.get(1, 0)),
    }


def evaluate() -> EvalReport:
    """Stratified hold-out evaluation. Reports accuracy, precision, recall,
    F1, and ROC-AUC for the non-human class and writes metrics.json.
    """
    df = _labeled_dataset()
    if len(df) < MIN_ROWS:
        raise ValueError(f"need at least {MIN_ROWS} labeled windows (got {len(df)}).")
    X, y = df[FEATURES], df["label"]
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
    log.info("saved metrics to %s", METRICS_PATH)
    return report


# --------------------------------------------------------------------------
# Scoring / inference (real-time layer feeds output/liveness.parquet)
# --------------------------------------------------------------------------

def _score_windows() -> pd.DataFrame:
    """Per-window substrate: one row per (user, window_start) with the
    model's non-human probability. Empty if no model or no events.

    (A future per-session pass would left-join this onto
    output/per_window on (user, window_start) for Spark's session_id.)
    """
    if not MODEL_PATH.exists():
        log.info("no model at %s; run `train` first", MODEL_PATH)
        return pd.DataFrame(columns=["user", "window_start", "nonhuman_proba"])
    bundle = joblib.load(MODEL_PATH)
    feats = _window_features(_load_events())
    if feats.empty:
        return pd.DataFrame(columns=["user", "window_start", "nonhuman_proba"])
    feats["nonhuman_proba"] = bundle["model"].predict_proba(feats[bundle["features"]])[:, 1]
    return feats[["user", "window_start", "nonhuman_proba"]]


def _local_day(window_start: pd.Series) -> pd.Series:
    """Map UTC window-start timestamps to host-local calendar-day keys
    (YYYY-MM-DD), matching the browser's localDayKey and the batch job's
    day_minute_metrics derivation.
    """
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    return (
        pd.to_datetime(window_start)
        .dt.tz_localize("UTC")
        .dt.tz_convert(local_tz)
        .dt.strftime("%Y-%m-%d")
    )


def score_days() -> pd.DataFrame:
    """Aggregate per-window scores to a per-(user, day) non-human flag.
    A day is flagged when at least MIN_FLAG_WINDOWS of its windows score
    >= WINDOW_THRESHOLD - a sustained automated burst, not one odd minute.
    Returns columns [user, day, nonhuman, score].
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
    """Score and write output/liveness.parquet. Returns the row count.
    Called by the API's batch scheduler after each compute_all.
    """
    days = score_days()
    days.to_parquet(LIVENESS_PATH, index=False)
    log.info("wrote %d (user, day) liveness rows to %s", len(days), LIVENESS_PATH)
    return len(days)


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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
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
