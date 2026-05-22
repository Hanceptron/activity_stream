import { ACTIVITY_RANGES, bucketizeWindows } from "../utils";

// Aggregate of active vs idle time over the selected range. A
// bucket counts as active if any keystrokes, words, corrections,
// or clicks were recorded in it — same definition the IdleStrip
// uses, so the two panels stay in sync. The bucket resolution
// comes from ACTIVITY_RANGES, so "% active" means "share of the
// range's buckets that saw activity", at the same granularity the
// strip is drawn.
export function ActivityGauge({ metrics, range = "1h" }) {
  const cfg = ACTIVITY_RANGES[range] ?? ACTIVITY_RANGES["1h"];
  const buckets = bucketizeWindows(metrics, cfg.bucketCount, cfg.bucketSizeMin);
  let activeBuckets = 0;
  for (const b of buckets) {
    const total =
      (b.keystrokes ?? 0) +
      (b.words ?? 0) +
      (b.corrections ?? 0) +
      (b.clicks ?? 0);
    if (total > 0) activeBuckets++;
  }
  const total = buckets.length;
  const inactiveBuckets = total - activeBuckets;
  const activePct = total > 0 ? (activeBuckets / total) * 100 : 0;

  const activeMin = activeBuckets * cfg.bucketSizeMin;
  const idleMin = inactiveBuckets * cfg.bucketSizeMin;

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-sm text-zinc-400">Active time · {cfg.label.toLowerCase()}</div>
        <div className="text-xs text-zinc-500">
          {formatDuration(activeMin)} active · {formatDuration(idleMin)} idle
        </div>
      </div>
      <div className="text-3xl font-semibold text-zinc-100 mt-1">
        {activePct.toFixed(0)}%
      </div>
      <div
        className="mt-3 h-2.5 bg-zinc-700 rounded-full overflow-hidden"
        role="img"
        aria-label={`${activeBuckets} of ${total} buckets active`}
      >
        <div
          className="h-full bg-green-500"
          style={{ width: `${activePct}%` }}
        />
      </div>
    </div>
  );
}

// Compact duration: minutes for short spans, hours (one decimal
// while small, integer once large) for longer ones. Keeps the
// "X active · Y idle" line readable across the full range presets.
function formatDuration(min) {
  if (min < 120) return `${min} min`;
  const hours = min / 60;
  return hours >= 24 ? `${hours.toFixed(0)} h` : `${hours.toFixed(1)} h`;
}
