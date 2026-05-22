import { medianFlightTimeStd, totalLongPauses } from "../utils";
import { RhythmSparkline } from "./RhythmSparkline";

// Hour-aggregate gauges for the typing-rhythm metrics. Both reduce
// every metric row in the polling window down to a single stable
// number rather than showing the noisy latest-minute value:
//
// - Rhythm: median of flight_time_std across minutes where it's
//   defined. Robust to the heavy-tailed outliers that arise when a
//   minute has only a handful of keystrokes with one large gap.
// - Long pauses: sum across the polling window. Reads as "how
//   often did I stop to think this hour?".
//
// Why no BaselineBadge here:
// The badge z-scores the latest-minute value against the per-minute
// baseline mean/std. Once the headline is an hour aggregate the
// scales no longer match — a sum over 60 minutes is not on the
// same axis as a per-minute mean, and a median of 60 minutes has
// a different sampling distribution than a single minute. Mixing
// the two would mislead. The four count cards in MetricCards.jsx
// keep their badges because they remain latest-minute snapshots.
//
// Why the rhythm card has no baseline subtitle:
// The per-user baseline mean of flight_time_std is dominated by
// sparse-typing minutes, so it does not represent "typical rhythm
// when actually typing." Until we filter the baseline to active
// minutes, the number would mislead more than it informs.
//
// Why the long-pause baseline IS shown, scaled:
// The per-minute mean scales cleanly to an hourly figure by
// multiplying by 60. We omit the std because under the naive IID
// scaling (sigma * sqrt(60)) the spread looks tighter than it
// really is, and under-the-hood independence does not hold for
// long pauses across consecutive minutes.
export function RhythmPanel({ metrics, baseline }) {
  const flightVal = medianFlightTimeStd(metrics);
  const pauseVal = totalLongPauses(metrics);

  const pauseMeanHourly =
    baseline?.long_pause_count_mean != null
      ? baseline.long_pause_count_mean * 60
      : null;

  // Baseline mean still passed to the sparkline as its dashed
  // reference line — that comparison is per-minute on both sides
  // (the sparkline plots per-minute flight_time_std), so the units
  // match there.
  const flightMean = baseline?.flight_time_std_mean ?? null;

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
      <div className="relative bg-zinc-800 rounded-lg p-4 border border-zinc-700">
        <div className="text-sm text-zinc-400">Rhythm (flight-time std)</div>
        <div className="text-3xl font-semibold text-zinc-100 mt-1">
          {flightVal != null ? `${flightVal.toFixed(2)}s` : "—"}
        </div>
        <div className="text-xs text-zinc-500 mt-2">
          median over last hour
        </div>
        <div className="mt-3">
          <RhythmSparkline metrics={metrics} baselineMean={flightMean} />
        </div>
      </div>
      <div className="relative bg-zinc-800 rounded-lg p-4 border border-zinc-700">
        <div className="text-sm text-zinc-400">Long pauses (&gt;2s)</div>
        <div className="text-3xl font-semibold text-zinc-100 mt-1">
          {pauseVal != null ? pauseVal : "—"}
        </div>
        <div className="text-xs text-zinc-500 mt-2">
          total over last hour
          {pauseMeanHourly != null && (
            <span> · baseline: ~{pauseMeanHourly.toFixed(0)}/h</span>
          )}
        </div>
      </div>
    </div>
  );
}
