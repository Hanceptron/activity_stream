"""StreamGuard fatigue classifier.

Per-window Random Forest that labels each one-minute window as one of
four classes:

  - productive  : typing was improving through the session
  - normal      : neutral within-session trend
  - tired       : mild slowdown / errors creeping in
  - burnt_out   : strong slowdown plus errors

The label for training comes from quartiles of the batch job's
``fatigue_index`` over the user's reliable sessions, so no manual
labeling is needed. The session-level prediction served to the UI is
the mode of its windows' predicted labels.

Training, evaluation, and inference are deliberately decoupled from
the always-on pipeline:

  uv run python -m streamguard.ml train       # offline, manual
  uv run python -m streamguard.ml evaluate    # offline, manual
  uv run python -m streamguard.ml predict     # offline, manual

The 5-minute batch in ``batch_job.py`` loads the persisted model (if
present) and writes ``output/predictions.parquet`` with one
``predicted_label`` per session. A missing model file silently
disables inference.

Why per-window and not per-session: with one user we typically have
~10-50 sessions but hundreds of one-minute windows inside them, so
per-window sampling gives enough rows for credible cross-validation
metrics. To avoid leakage we cross-validate with ``GroupKFold`` on
``session_id`` - windows from the same session always stay together
in train or test.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    f1_score,
)
from sklearn.model_selection import GroupKFold

log = logging.getLogger("streamguard.ml")

SESSIONS_PATH = "output/sessions"
PER_WINDOW_PATH = "output/per_window"
MODEL_DIR = Path("output/models")
MODEL_PATH = MODEL_DIR / "fatigue_classifier.joblib"
METRICS_PATH = MODEL_DIR / "metrics.json"

# Four ordinal classes derived from fatigue_index quartiles. Order
# matters: index 0 is the "best" session and index 3 is the worst,
# matching the natural way a reader interprets the words.
LABELS = ["productive", "normal", "tired", "burnt_out"]

FEATURES = [
    "keystrokes",
    "words",
    "corrections",
    "clicks",
    "correction_ratio",
    "click_ratio",
    "window_idx",
    "session_progress",
]

LABEL_COL = "session_label"


def _quartile_labels(fatigue_index: pd.Series) -> pd.Series:
    """Map a per-session fatigue_index column to the four ordinal
    labels. Uses ``pd.qcut`` so the splits adapt to whatever shape
    the user's data has - 2 weeks of typing vs. 2 months produces
    different absolute fatigue_index distributions, but the relative
    quartile cut is meaningful in both.
    """
    return pd.qcut(
        fatigue_index, q=4, labels=LABELS, duplicates="drop"
    ).astype(str)


def _build_window_features() -> pd.DataFrame:
    """Join per-window aggregates with their parent session label.

    Returns one row per (session_id, window_start) with the eight
    features the model trains on plus the string label. Sessions
    where ``fatigue_reliable`` is False are dropped - an unreliable
    fatigue_index would just inject noise.
    """
    if not Path(SESSIONS_PATH).exists() or not Path(PER_WINDOW_PATH).exists():
        raise FileNotFoundError(
            f"Missing {SESSIONS_PATH} or {PER_WINDOW_PATH}. Run the batch "
            "job at least once (uv run python -m streamguard.batch_job) "
            "to populate them."
        )

    sessions = pd.read_parquet(SESSIONS_PATH)
    sessions = sessions[sessions["fatigue_reliable"]].copy()
    if len(sessions) < 4:
        raise ValueError(
            f"need at least 4 reliable sessions to derive 4-quartile "
            f"labels (got {len(sessions)}). Record more typing data and "
            "re-run the batch job."
        )

    sessions[LABEL_COL] = _quartile_labels(sessions["fatigue_index"])
    label_lookup = sessions.set_index(["session_id", "user"])[
        [LABEL_COL, "window_count"]
    ]

    windows = pd.read_parquet(PER_WINDOW_PATH)
    windows = windows.merge(
        label_lookup, left_on=["session_id", "user"], right_index=True, how="inner"
    )

    windows = windows.sort_values(["session_id", "user", "window_start"]).reset_index(
        drop=True
    )
    windows["window_idx"] = windows.groupby(["session_id", "user"]).cumcount()
    windows["session_progress"] = windows["window_idx"] / windows["window_count"].clip(
        lower=1
    )

    # max(_, 1) avoids divide-by-zero on idle windows where keystrokes=0
    # but the row still got materialized because the user clicked or
    # moved. Same trick as frontend/src/utils.js:correctionRatio.
    windows["correction_ratio"] = windows["corrections"] / windows["keystrokes"].clip(
        lower=1
    )
    denom = (windows["keystrokes"] + windows["clicks"]).clip(lower=1)
    windows["click_ratio"] = windows["clicks"] / denom
    return windows


@dataclass
class EvalReport:
    """Multi-class evaluation summary saved to metrics.json. Not
    rendered in the live dashboard - it is here for the paper.
    """

    n_samples: int
    n_sessions: int
    label_counts: dict = field(default_factory=dict)
    accuracy: float = 0.0
    macro_f1: float = 0.0
    weighted_f1: float = 0.0
    per_class: dict = field(default_factory=dict)
    confusion_matrix: list = field(default_factory=list)
    feature_importances: dict = field(default_factory=dict)
    cv_folds: int = 0
    classes: list = field(default_factory=list)


def _train_model(X: pd.DataFrame, y: pd.Series) -> RandomForestClassifier:
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=None,
        min_samples_leaf=2,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X, y)
    return model


def train() -> dict:
    """Fit the multi-class classifier on every available labeled
    window and persist it. Returns a tiny summary.
    """
    df = _build_window_features()
    X = df[FEATURES]
    y = df[LABEL_COL]

    label_counts = Counter(y)
    log.info(
        "training on %d windows from %d sessions; class counts: %s",
        len(df),
        df["session_id"].nunique(),
        dict(label_counts),
    )

    model = _train_model(X, y)

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(
        {"model": model, "features": FEATURES, "labels": LABELS}, MODEL_PATH
    )
    log.info("saved model to %s", MODEL_PATH)
    return {
        "model_path": str(MODEL_PATH),
        "n_samples": len(df),
        "n_sessions": int(df["session_id"].nunique()),
        "label_counts": dict(label_counts),
    }


def evaluate(cv_folds: int = 5) -> EvalReport:
    """Cross-validated evaluation using GroupKFold on session_id.

    This is the function whose output goes into the paper's results
    section. We never train and test on windows from the same session,
    which would inflate the numbers via leakage.

    ``cv_folds`` is clamped to the smaller of (folds, n_sessions, 2).
    """
    df = _build_window_features()
    X = df[FEATURES].to_numpy()
    y = df[LABEL_COL].to_numpy()
    groups = df["session_id"].to_numpy()

    n_sessions = int(df["session_id"].nunique())
    folds = max(2, min(cv_folds, n_sessions))

    log.info(
        "cross-validating on %d windows across %d sessions, %d folds, GroupKFold",
        len(df),
        n_sessions,
        folds,
    )

    gkf = GroupKFold(n_splits=folds)
    y_true_all: list = []
    y_pred_all: list = []
    importances_acc = np.zeros(len(FEATURES))

    for train_idx, test_idx in gkf.split(X, y, groups):
        X_train_df = pd.DataFrame(X[train_idx], columns=FEATURES)
        X_test_df = pd.DataFrame(X[test_idx], columns=FEATURES)
        model = _train_model(X_train_df, pd.Series(y[train_idx]))
        y_pred = model.predict(X_test_df)
        y_true_all.extend(y[test_idx].tolist())
        y_pred_all.extend(y_pred.tolist())
        importances_acc += model.feature_importances_

    importances = importances_acc / folds
    report = classification_report(
        y_true_all,
        y_pred_all,
        labels=LABELS,
        target_names=LABELS,
        output_dict=True,
        zero_division=0,
    )
    cm = confusion_matrix(y_true_all, y_pred_all, labels=LABELS).tolist()

    out = EvalReport(
        n_samples=len(df),
        n_sessions=n_sessions,
        label_counts=dict(Counter(df[LABEL_COL])),
        accuracy=accuracy_score(y_true_all, y_pred_all),
        macro_f1=f1_score(y_true_all, y_pred_all, average="macro", zero_division=0),
        weighted_f1=f1_score(
            y_true_all, y_pred_all, average="weighted", zero_division=0
        ),
        per_class={k: v for k, v in report.items() if isinstance(v, dict)},
        confusion_matrix=cm,
        feature_importances={f: float(w) for f, w in zip(FEATURES, importances)},
        cv_folds=folds,
        classes=LABELS,
    )

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    METRICS_PATH.write_text(json.dumps(asdict(out), indent=2))
    log.info("saved metrics to %s", METRICS_PATH)
    return out


def predict_sessions() -> pd.DataFrame:
    """Run the persisted model over every window in PER_WINDOW_PATH
    and aggregate to one row per (session_id, user) with the modal
    predicted label across the session's windows.

    Returns an empty DataFrame when no model is on disk yet, so the
    caller can detect "ML disabled" without try/except.
    """
    if not MODEL_PATH.exists():
        log.info("no model at %s; skipping inference", MODEL_PATH)
        return pd.DataFrame()
    if not Path(PER_WINDOW_PATH).exists():
        return pd.DataFrame()

    bundle = joblib.load(MODEL_PATH)
    model = bundle["model"]
    features = bundle["features"]

    windows = pd.read_parquet(PER_WINDOW_PATH)
    if windows.empty:
        return pd.DataFrame()

    sessions = pd.read_parquet(SESSIONS_PATH)[
        ["session_id", "user", "window_count"]
    ]
    windows = windows.merge(sessions, on=["session_id", "user"], how="inner")
    windows = windows.sort_values(["session_id", "user", "window_start"]).reset_index(
        drop=True
    )
    windows["window_idx"] = windows.groupby(["session_id", "user"]).cumcount()
    windows["session_progress"] = windows["window_idx"] / windows["window_count"].clip(
        lower=1
    )
    windows["correction_ratio"] = windows["corrections"] / windows["keystrokes"].clip(
        lower=1
    )
    denom = (windows["keystrokes"] + windows["clicks"]).clip(lower=1)
    windows["click_ratio"] = windows["clicks"] / denom

    windows["predicted_window_label"] = model.predict(windows[features])

    # Modal label across the session's windows. ties broken by the
    # natural ordering of LABELS so a session split 5/5 productive/
    # normal lands on "productive" rather than alphabetical chance.
    def pick_mode(series):
        counts = Counter(series)
        best_count = max(counts.values())
        winners = [c for c in LABELS if counts.get(c, 0) == best_count]
        return winners[0]

    agg = (
        windows.groupby(["session_id", "user"])["predicted_window_label"]
        .apply(pick_mode)
        .reset_index()
        .rename(columns={"predicted_window_label": "predicted_label"})
    )
    return agg


def _format_report(r: EvalReport) -> str:
    lines = [
        f"dataset:  {r.n_samples} windows, {r.n_sessions} sessions, "
        f"{r.cv_folds}-fold GroupKFold",
        f"label counts: {r.label_counts}",
        "",
        f"accuracy:    {r.accuracy:.3f}",
        f"macro f1:    {r.macro_f1:.3f}",
        f"weighted f1: {r.weighted_f1:.3f}",
        "",
        "per class:",
        f"  {'label':<12} {'precision':>10} {'recall':>10} {'f1':>10} {'support':>10}",
    ]
    for cls in r.classes:
        stats = r.per_class.get(cls)
        if not stats:
            continue
        lines.append(
            f"  {cls:<12} {stats['precision']:>10.3f} {stats['recall']:>10.3f} "
            f"{stats['f1-score']:>10.3f} {int(stats['support']):>10}"
        )
    lines += [
        "",
        f"confusion matrix (rows=true, cols=pred), labels order = {r.classes}:",
    ]
    for row in r.confusion_matrix:
        lines.append("  " + " ".join(f"{v:>5}" for v in row))
    lines += ["", "feature importances:"]
    for f, w in sorted(r.feature_importances.items(), key=lambda x: -x[1]):
        bar = "#" * int(w * 40)
        lines.append(f"  {f:<20} {w:.3f} {bar}")
    return "\n".join(lines)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="StreamGuard fatigue classifier")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("train", help="fit on all available windows and save the model")
    p_eval = sub.add_parser(
        "evaluate", help="GroupKFold cross-validate and save metrics.json"
    )
    p_eval.add_argument("--folds", type=int, default=5)
    sub.add_parser(
        "predict",
        help="apply the saved model to current sessions and print one row each",
    )
    args = parser.parse_args()

    if args.cmd == "train":
        summary = train()
        print(json.dumps(summary, indent=2))
    elif args.cmd == "evaluate":
        report = evaluate(cv_folds=args.folds)
        print(_format_report(report))
    elif args.cmd == "predict":
        df = predict_sessions()
        if df.empty:
            print("no predictions: train the model first")
        else:
            print(df.to_string(index=False))


if __name__ == "__main__":
    main()
