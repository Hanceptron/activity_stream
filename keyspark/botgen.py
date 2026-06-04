"""Synthetic input-automation (bot) event generator for KeySpark.

Emits raw input events in the same JSON shape the capture agent produces
(see ``keyspark/agent.py``), for two uses:

  1. Training the liveness classifier (the non-human class). The same
     events are featurized by ``keyspark.ml`` exactly the way real
     events are, so there is no train/serve feature skew.
  2. A live demo: inject events straight into the Kafka topic
     ``events.raw``. macOS drops software-injected HID events, so a
     software bot never reaches the capture agent - going straight to
     Kafka is the demo path.

Three bot kinds, each robotic in one modality:
  - jiggler : small mouse-move walk at a roughly regular cadence, no keys
  - typer   : keystrokes over a small key set, no mouse
  - clicker : repeated clicks at one spot

To keep the ML task honest (not a trivially separable if-statement), the
bots are deliberately EVASIVE: per-session randomized cadence, sizeable
timing jitter, variable mouse-step magnitude, and a little cross-modal
contamination so the input mix and key variety are not perfect giveaways.
They are still more regular and repetitive than a human - that residual
regularity is what the classifier learns.

  uv run python -m keyspark.botgen demo --kind jiggler --duration 60
"""

from __future__ import annotations

import argparse
import json
import random
import time

KINDS = ("jiggler", "typer", "clicker")
DEFAULT_USER = "user-001"
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "events.raw"

# Per-session base inter-event interval (seconds) is drawn from this range
# so different synthetic sessions have different cadences.
_BASE_RANGE = {"jiggler": (0.3, 2.0), "typer": (0.10, 0.40), "clicker": (0.3, 1.5)}
# Timing jitter fraction drawn per session (events land at base*(1 +/- j)).
_JITTER_RANGE = (0.20, 0.50)
# Fraction of events that are an off-kind action, so mouse_fraction and key
# diversity are blurred rather than perfectly 1.0 / 0.0.
CONTAMINATION = 0.08
_ANCHOR = (800, 500)
_TYPER_KEYS = ("a", "s", "d", "f", "j", "k", "l", "e")  # small, low-diversity set


def _next_event(kind: str, state: dict, user: str, ts: float, rng: random.Random) -> dict:
    """One event for bot ``kind`` at time ``ts``. ``state`` carries the
    mouse position so the jiggler is a random walk of variable step size.
    Occasionally emits an off-kind action (CONTAMINATION).
    """
    kk = kind
    if rng.random() < CONTAMINATION:
        kk = rng.choice([k for k in KINDS if k != kind])
    if kk == "jiggler":
        reach = 60 if rng.random() < 0.10 else 12  # occasional larger, human-like move
        state["x"] += rng.randint(-reach, reach)
        state["y"] += rng.randint(-reach, reach)
        return {"type": "move", "x": state["x"], "y": state["y"],
                "user": user, "ts": round(ts, 3)}
    if kk == "typer":
        return {"type": "key_down", "key": rng.choice(_TYPER_KEYS),
                "user": user, "ts": round(ts, 3)}
    return {"type": "click", "x": state["x"], "y": state["y"],
            "button": "Button.left", "pressed": True, "user": user, "ts": round(ts, 3)}


def synthetic_events(kind: str, count: int, user: str = DEFAULT_USER,
                     start_ts: float = 0.0, seed: int = 0,
                     base: float | None = None, jitter: float = 0.35) -> list[dict]:
    """``count`` event dicts for one bot ``kind`` starting at ``start_ts``
    (epoch seconds), advancing by ``base`` seconds with +/- ``jitter``.
    """
    if kind not in KINDS:
        raise ValueError(f"unknown bot kind {kind!r}; choose from {KINDS}")
    rng = random.Random(seed)
    if base is None:
        base = sum(_BASE_RANGE[kind]) / 2
    ts = float(start_ts)
    state = {"x": _ANCHOR[0], "y": _ANCHOR[1]}
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
    """A pandas DataFrame of synthetic bot events: ``sessions_per_kind``
    sessions for each of the three kinds, each ~``minutes_per_session``
    minutes, with per-session randomized cadence and jitter so the
    non-human class spans a range of the feature space (not one point).
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


def _demo(kind: str, duration: float, rate: float, user: str) -> None:
    """Inject ~``rate`` events/sec of bot ``kind`` for ``duration`` seconds
    into Kafka, timestamped at wall-clock now so the streaming watermark
    accepts them as current.
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


def main() -> None:
    parser = argparse.ArgumentParser(description="KeySpark synthetic bot event generator")
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("demo", help="inject synthetic bot events into Kafka events.raw")
    d.add_argument("--kind", choices=KINDS, default="jiggler")
    d.add_argument("--duration", type=float, default=180.0,
                   help="seconds (>= ~120 so it spans the 2 windows a day flag needs)")
    d.add_argument("--rate", type=float, default=20.0, help="events per second")
    d.add_argument("--user", default=DEFAULT_USER)
    args = parser.parse_args()
    if args.cmd == "demo":
        _demo(args.kind, args.duration, args.rate, args.user)


if __name__ == "__main__":
    main()
