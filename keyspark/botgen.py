"""Synthetic input-automation (bot) event generator: the non-human class for the
liveness classifier. Emits raw events in the same JSON shape the capture agent
produces, so keyspark.ml featurizes them exactly like real events (no
train/serve skew).

Four bot kinds, each robotic in one modality:
  - jiggler    : small mouse nudge tracing a FIXED pattern (diagonal / horizontal
                 / octagon) in place, no keys
  - typer      : keystrokes over a small key set, no mouse
  - keep_awake : one function key (F15 / Scroll Lock) on a slow cadence, no mouse
  - clicker    : repeated clicks at one spot

Grounded in how real keep-active tools behave (research/jigglers.md): verified
open-source movers (arkane-systems Mouse Jiggler) trace a fixed relative pattern
and never randomize the path; keyboard keep-awake tools (Caffeine) emit a single
non-character key. The one thing they randomize is timing. So we keep the TIMING
jittered (per-session cadence, jitter, occasional pauses) plus a little
cross-modal contamination, so the task is not a trivial if-statement; the signal
left to learn is the residual GEOMETRY regularity (low step_cv, low key_diversity).
"""

from __future__ import annotations

import random

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
KINDS = ("jiggler", "typer", "keep_awake", "clicker")
DEFAULT_USER = "user-001"

# Per-session base inter-event interval (s) per kind, so different sessions have
# different cadences. The jiggler/keep_awake bands are slow on purpose (real
# movers nudge every few seconds, not sub-second), but fast enough that a
# one-minute window still holds several events.
_BASE_RANGE = {                      # tune: cadence band (s) per bot kind
    "jiggler": (1.0, 8.0),
    "typer": (0.10, 0.40),
    "keep_awake": (6.0, 12.0),
    "clicker": (0.3, 1.5),
}
_JITTER_RANGE = (0.20, 0.50)         # tune: per-session timing jitter fraction
CONTAMINATION = 0.08                 # tune: fraction of off-kind events (blurs mouse_fraction / diversity)
_ANCHOR = (800, 500)
_TYPER_KEYS = ("a", "s", "d", "f", "j", "k", "l", "e")  # small, low-diversity set

# Deterministic relative mouse-move patterns, mirroring arkane-systems Mouse
# Jiggler's JigglePatterns: back-and-forth pairs and a closed octagon, walked in
# order in place, so step size is near-constant (low step_cv). A per-session
# multiplier scales the deltas (the tool's configurable "distance").
_MOVE_PATTERNS = {
    "normal": ((4, 4), (-4, -4)),                     # diagonal nudge
    "linear": ((4, 0), (-4, 0)),                      # horizontal nudge
    "circle": ((3, 2), (2, 3), (-2, 3), (-3, 2),
               (-3, -2), (-2, -3), (2, -3), (3, -2)),  # closed octagon
}
_JIGGLE_PATTERNS = ("normal", "linear", "circle")
# Single non-character keep-awake keys (F15 is the classic Caffeine key). One key
# per session keeps key_diversity low - the keyboard analogue of the rigid mouse pattern.
_KEEPAWAKE_KEYS = ("f15", "f13", "f14", "scroll_lock")


# --------------------------------------------------------------------------
# Event generation
# --------------------------------------------------------------------------
def _next_event(kind: str, state: dict, user: str, ts: float, rng: random.Random) -> dict:
    """One event for bot ``kind`` at time ``ts``. ``state`` carries the mouse
    position plus the per-session jiggle pattern / keep-awake key, so the jiggler
    traces a fixed pattern in place. Occasionally emits an off-kind action.
    """
    kk = kind
    if rng.random() < CONTAMINATION:
        kk = rng.choice([k for k in KINDS if k != kind])
    if kk == "jiggler":
        pattern = _MOVE_PATTERNS[state.setdefault("pattern", "normal")]
        i = state.get("i", 0)
        dx, dy = pattern[i % len(pattern)]
        dist = state.setdefault("dist", 1)
        state["i"] = i + 1
        state["x"] += dx * dist
        state["y"] += dy * dist
        return {"type": "move", "x": state["x"], "y": state["y"],
                "user": user, "ts": round(ts, 3)}
    if kk == "typer":
        return {"type": "key_down", "key": rng.choice(_TYPER_KEYS),
                "user": user, "ts": round(ts, 3)}
    if kk == "keep_awake":
        return {"type": "key_down", "key": state.setdefault("awake_key", "f15"),
                "user": user, "ts": round(ts, 3)}
    return {"type": "click", "x": state["x"], "y": state["y"],
            "button": "Button.left", "pressed": True, "user": user, "ts": round(ts, 3)}


def synthetic_events(kind: str, count: int, user: str = DEFAULT_USER,
                     start_ts: float = 0.0, seed: int = 0,
                     base: float | None = None, jitter: float = 0.35) -> list[dict]:
    """``count`` event dicts for one bot ``kind`` starting at ``start_ts`` (epoch
    seconds), advancing by ``base`` seconds with +/- ``jitter``.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown bot kind {kind!r}; choose from {KINDS}")
    rng = random.Random(seed)
    if base is None:
        base = sum(_BASE_RANGE[kind]) / 2
    ts = float(start_ts)
    # Per-session choices (fixed for the whole session, varied across sessions):
    # the jiggle pattern, its distance multiplier, and the single keep-awake key.
    state = {"x": _ANCHOR[0], "y": _ANCHOR[1], "i": 0,
             "pattern": rng.choice(_JIGGLE_PATTERNS),
             "dist": rng.randint(1, 3),
             "awake_key": rng.choice(_KEEPAWAKE_KEYS)}
    out = []
    for _ in range(count):
        step = base * (1.0 + rng.uniform(-jitter, jitter))
        if rng.random() < 0.10:
            step += base * rng.uniform(3.0, 12.0)  # occasional human-like pause/gap
        ts += step
        out.append(_next_event(kind, state, user, ts, rng))
    return out


def synthetic_event_frame(sessions_per_kind: int = 4, minutes_per_session: int = 30,
                          user: str = DEFAULT_USER, start_ts: float = 1.0e9,
                          seed: int = 0):
    """A DataFrame of synthetic bot events: ``sessions_per_kind`` sessions for each
    kind, each ~``minutes_per_session`` minutes, with per-session randomized
    cadence/jitter/pattern/key so the non-human class spans the feature space.
    """
    import pandas as pd

    rng = random.Random(seed)
    rows: list[dict] = []
    ts = float(start_ts)
    s = 0
    for kind in KINDS:
        for _ in range(sessions_per_kind):
            base = rng.uniform(*_BASE_RANGE[kind])
            jitter = rng.uniform(*_JITTER_RANGE)
            count = int(minutes_per_session * 60 / base)
            evs = synthetic_events(kind, count, user=user, start_ts=ts,
                                   seed=seed + s, base=base, jitter=jitter)
            rows.extend(evs)
            ts = evs[-1]["ts"] + 3600.0  # 1h gap so sessions stay distinct
            s += 1
    return pd.DataFrame(rows)
