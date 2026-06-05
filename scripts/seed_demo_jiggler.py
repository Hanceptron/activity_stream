"""Seed three synthetic jiggler/keep-awake demo days into output/events.

Backdated days 2026-05-11/13/15, each 09:00-17:00 local, under user-001, mixing
mouse (jiggler moves + clicker clicks) and keyboard (keep_awake key_down) so both
modalities show on the dashboard day view and the day is flagged non-human.

Composes keyspark.botgen (does not modify the generator). These days are kept
OUT of ML training via keyspark.ml.HUMAN_EXCLUDE_DAYS, but are still scored and
flagged at inference. Re-running overwrites the same three files.

  uv run python scripts/seed_demo_jiggler.py
"""

from __future__ import annotations

import datetime as dt
import os
import sys
from pathlib import Path

# Run directly (uv run python scripts/seed_demo_jiggler.py): put the repo root on
# the path so `keyspark` imports without needing `-m` or PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from keyspark.botgen import EVENTS_PATH, synthetic_events, write_events_parquet

USER = "user-001"
DAYS = ("2026-05-11", "2026-05-13", "2026-05-15")
START_HOUR = 9   # 09:00 local
END_HOUR = 17    # 17:00 local (exclusive upper bound for events)

# Non-overlapping segments cycling the kinds so each minute is mostly one kind
# (strong, classifiable windows) while all three event types appear across the
# day. One cycle = 120 min; 4 cycles = 480 min = 8 h (09:00-17:00).
_CYCLE = (("jiggler", 60), ("keep_awake", 30), ("clicker", 30))
_REPEATS = 4
# Per-kind base interval (seconds). Dense enough to clear ml.MIN_WINDOW_EVENTS and
# fill the heatmaps / keystroke timeline; within or at the low end of _BASE_RANGE.
_BASE = {"jiggler": 2.0, "keep_awake": 6.0, "clicker": 0.9}


def _day_events(day: str, day_idx: int) -> list[dict]:
    """All events for one demo day, bounded to [09:00, 17:00) local.

    Each segment is generated with a generous count and then truncated to its
    time bound: synthetic_events averages ~1.75x base per step (jitter + 10%
    long pauses), so truncation - not the count - is what keeps every segment in
    its slot and the whole day inside 9-5 with no overlap.
    """
    start = dt.datetime.strptime(day, "%Y-%m-%d").replace(hour=START_HOUR)
    seg_start = start.timestamp()
    day_end = start.replace(hour=END_HOUR).timestamp()
    events: list[dict] = []
    seg_idx = 0
    for _ in range(_REPEATS):
        for kind, minutes in _CYCLE:
            base = _BASE[kind]
            seg_end = min(seg_start + minutes * 60, day_end)
            count = int((seg_end - seg_start) / base) + 50  # generous; truncated below
            evs = synthetic_events(kind, count, user=USER, start_ts=seg_start,
                                   base=base, seed=1000 * day_idx + seg_idx)
            events.extend(e for e in evs if e["ts"] < seg_end)
            seg_start = seg_end
            seg_idx += 1
    return events


def main() -> None:
    os.makedirs(EVENTS_PATH, exist_ok=True)
    for day_idx, day in enumerate(DAYS):
        events = sorted(_day_events(day, day_idx), key=lambda e: e["ts"])
        out = os.path.join(EVENTS_PATH, f"part-demo-{USER}-{day.replace('-', '')}.parquet")
        n = write_events_parquet(events, out)
        by_type: dict[str, int] = {}
        for e in events:
            by_type[e["type"]] = by_type.get(e["type"], 0) + 1
        print(f"{day}: wrote {n} events -> {out}  {by_type}")


if __name__ == "__main__":
    main()
