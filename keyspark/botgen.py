"""Synthetic input-automation (bot) event generator: the non-human class for the
liveness classifier, plus a live-demo injector. Emits raw events in the same JSON
shape the capture agent produces, so keyspark.ml featurizes them exactly like
real events (no train/serve skew).

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

macOS drops software-injected HID events, so a software bot never reaches the
capture agent - the demo path injects straight into Kafka instead.

  uv run python -m keyspark.botgen demo --kind jiggler --duration 60
"""

from __future__ import annotations

import argparse
import json
import random
import time

# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------
KINDS = ("jiggler", "typer", "keep_awake", "clicker")
DEFAULT_USER = "user-001"
KAFKA_BOOTSTRAP = "localhost:9092"   # tune: Kafka broker for the demo injector
KAFKA_TOPIC = "events.raw"
EVENTS_PATH = "output/events"

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


# --------------------------------------------------------------------------
# Parquet I/O (seed conforming files into output/events/)
# --------------------------------------------------------------------------
def events_arrow_schema():
    """The exact on-disk schema of the streaming event archive (output/events/),
    matching streaming_job.py's parsed projection. x/y/dx/dy are int64 (Spark
    LongType, nullable): keyboard events have null coords, and the batch reader
    requires int64 here - a plain pandas to_parquet would write float64/double and
    make Spark raise PARQUET_COLUMN_DATA_TYPE_MISMATCH.
    """
    import pyarrow as pa

    return pa.schema([
        ("type", pa.string()),
        ("key", pa.string()),
        ("x", pa.int64()),
        ("y", pa.int64()),
        ("button", pa.string()),
        ("pressed", pa.bool_()),
        ("dx", pa.int64()),
        ("dy", pa.int64()),
        ("user", pa.string()),
        ("ts", pa.float64()),
        ("event_time", pa.timestamp("ns")),
    ])


def write_events_parquet(events, path: str) -> int:
    """Write synthetic ``events`` (list of dicts or DataFrame) to ``path`` with the
    exact streaming-sink schema, so the batch read accepts them like real events.
    Coerces every column (crucially x/y/dx/dy to nullable int64) and writes
    event_time as INT96. Returns the row count.
    """
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    df = events if isinstance(events, pd.DataFrame) else pd.DataFrame(events)
    if "event_time" not in df.columns:
        df = df.assign(event_time=pd.to_datetime(df["ts"], unit="s"))

    schema = events_arrow_schema()
    arrays = []
    for field in schema:
        col = df[field.name] if field.name in df.columns else [None] * len(df)
        # from_pandas=True maps NaN/None to typed null, so missing optional columns
        # (key on a mouse event, dx/dy when no scroll) and null coords become typed
        # nulls rather than floats.
        arrays.append(pa.array(col, type=field.type, from_pandas=True))
    # INT96 timestamps: Spark's vectorized reader cannot read INT64-nanosecond
    # timestamps (a plain pyarrow timestamp("ns") write), and the batch read would
    # raise PARQUET_COLUMN_DATA_TYPE_MISMATCH. INT96 is what Spark itself writes,
    # so a seeded file reads back like a captured one.
    pq.write_table(pa.Table.from_arrays(arrays, schema=schema), path,
                   use_deprecated_int96_timestamps=True)
    return len(df)


# --------------------------------------------------------------------------
# Demo (inject to Kafka) + seed (write a backdated day)
# --------------------------------------------------------------------------
def _demo(kind: str, duration: float, rate: float, user: str) -> None:
    """Inject ~``rate`` events/sec of bot ``kind`` for ``duration`` seconds into
    Kafka, timestamped at wall-clock now so the streaming watermark accepts them.
    """
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    rng = random.Random(0)
    interval = 1.0 / rate
    state = {"x": _ANCHOR[0], "y": _ANCHOR[1]}
    end = time.time() + duration
    sent = 0
    while time.time() < end:
        ev = _next_event(kind, state, user, time.time(), rng)
        producer.produce(KAFKA_TOPIC, key=user.encode(), value=json.dumps(ev).encode())
        producer.poll(0)
        sent += 1
        time.sleep(interval * (1.0 + rng.uniform(-0.3, 0.3)))
    producer.flush(5)
    print(f"injected {sent} '{kind}' events as user={user} into {KAFKA_TOPIC}")


def _seed(kind: str, day: str, user: str, minutes: float, seed: int) -> None:
    """Write a conforming synthetic-event parquet file into output/events/ for one
    backdated calendar day, so the batch + liveness demo show a flagged day without
    using the live Kafka path.
    """
    import datetime as dt
    import os

    # Local noon of the target day, so every event lands inside that day.
    start = dt.datetime.strptime(day, "%Y-%m-%d").replace(hour=12)
    base = sum(_BASE_RANGE[kind]) / 2
    count = int(minutes * 60 / base)
    events = synthetic_events(kind, count, user=user, start_ts=start.timestamp(),
                              seed=seed, base=base)
    os.makedirs(EVENTS_PATH, exist_ok=True)
    out = os.path.join(EVENTS_PATH, f"part-{user}-{day.replace('-', '')}.parquet")
    n = write_events_parquet(events, out)
    print(f"wrote {n} '{kind}' events for {user} on {day} -> {out}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="KeySpark synthetic bot event generator")
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo", help="inject synthetic bot events into Kafka events.raw")
    d.add_argument("--kind", choices=KINDS, default="jiggler")
    d.add_argument("--duration", type=float, default=180.0,
                   help="seconds (>= ~120 so it spans the 2 windows a day flag needs)")
    d.add_argument("--rate", type=float, default=20.0, help="events per second")
    d.add_argument("--user", default=DEFAULT_USER)

    s = sub.add_parser("seed",
                       help="write a conforming synthetic bot day into output/events/")
    s.add_argument("--kind", choices=KINDS, default="typer")
    s.add_argument("--day", required=True, help="calendar day YYYY-MM-DD")
    s.add_argument("--user", default="keyspark-bot")
    s.add_argument("--minutes", type=float, default=30.0,
                   help="minutes of synthetic activity (a few or more so the day flags)")
    s.add_argument("--seed", type=int, default=0)

    args = parser.parse_args()
    if args.cmd == "demo":
        _demo(args.kind, args.duration, args.rate, args.user)
    elif args.cmd == "seed":
        _seed(args.kind, args.day, args.user, args.minutes, args.seed)


if __name__ == "__main__":
    main()
