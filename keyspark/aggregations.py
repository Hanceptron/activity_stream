"""Shared event-count aggregations.

The per-window keystroke / word / correction / click counts must be
computed identically everywhere they appear - the streaming job's live
metrics, the batch job's session windows, and the batch job's per-day
windows - or the live dashboard, the fatigue features, and the history
drill-down would silently disagree. This module is the single source of
truth for those four counts and the key sets behind them.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

# pynput emits the literal " " for the space character and "Key.<name>"
# reprs for special keys, so a space can arrive in either form. A
# "correction" is a backspace or delete.
WORD_KEYS = (" ", "Key.space")
CORRECTION_KEYS = ("Key.backspace", "Key.delete")


def event_count_exprs() -> list[Column]:
    """The four per-window count aggregations, in a stable order.

    Spread into a groupBy as ``.agg(*event_count_exprs())``. Requires the
    grouped frame to carry the ``type`` and ``key`` columns from the
    parsed event schema.
    """
    is_kd = F.col("type") == "key_down"
    return [
        F.count(F.when(is_kd, 1)).alias("keystrokes"),
        F.count(F.when(is_kd & F.col("key").isin(*WORD_KEYS), 1)).alias("words"),
        F.count(
            F.when(is_kd & F.col("key").isin(*CORRECTION_KEYS), 1)
        ).alias("corrections"),
        F.count(F.when(F.col("type") == "click", 1)).alias("clicks"),
    ]
