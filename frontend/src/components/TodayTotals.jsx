import { parseUtc } from "../utils";

// Daily summary card aggregating sessions whose start falls within
// today's local-time boundary. Reuses the existing /api/sessions
// poll - no new request.
//
// Honest active time: this sums window_count (one row per minute of
// activity in the batch job) rather than (session_end - session_start),
// because the sessionizer in batch_job.py treats gaps under 5 min as
// the same session and would otherwise inflate the figure.
//
// Multi-user note: this component receives sessions ALREADY filtered
// to the selected user (App.jsx applies filterByUser before passing
// them in), so totals are per-user. To restore cross-user totals,
// pass the unfiltered sessions array.
export function TodayTotals({ sessions }) {
  const midnight = new Date();
  midnight.setHours(0, 0, 0, 0);

  const todays = (sessions || []).filter((s) => {
    const start = parseUtc(s.session_start);
    return start && start >= midnight;
  });

  const totals = todays.reduce(
    (acc, s) => {
      acc.keystrokes += s.keystrokes_total ?? 0;
      acc.words += s.words_total ?? 0;
      acc.corrections += s.corrections_total ?? 0;
      acc.clicks += s.clicks_total ?? 0;
      acc.activeMin += s.window_count ?? 0;
      return acc;
    },
    { keystrokes: 0, words: 0, corrections: 0, clicks: 0, activeMin: 0 }
  );

  // Staleness is measured against the newest session_end across ALL
  // sessions, not just today's, so a user opening the dashboard
  // mid-morning sees "as of 14 hours ago" rather than nothing when
  // the batch job has not yet run today.
  const newestEnd = (sessions || []).reduce((m, s) => {
    const end = parseUtc(s.session_end);
    return end && (!m || end > m) ? end : m;
  }, null);

  const subtitle = formatStaleness(newestEnd);

  const stats = [
    { label: "Keystrokes", value: totals.keystrokes },
    { label: "Words", value: totals.words },
    { label: "Corrections", value: totals.corrections },
    { label: "Clicks", value: totals.clicks },
    { label: "Sessions", value: todays.length },
    { label: "Active", value: `${totals.activeMin} min` },
  ];

  const isEmpty = todays.length === 0;

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <div className="flex items-baseline justify-between mb-3 gap-3">
        <h2 className="text-sm text-zinc-400">
          Today <span className="text-zinc-600">(sessions started since midnight)</span>
        </h2>
        {subtitle && (
          <span className="text-xs text-zinc-500 shrink-0">{subtitle}</span>
        )}
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
        {stats.map((s) => (
          <div key={s.label}>
            <div className="text-xs text-zinc-500">{s.label}</div>
            <div className="text-xl font-semibold text-zinc-100 mt-1">
              {isEmpty ? "—" : s.value}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// Render the newest session_end as a relative duration. Once the
// gap exceeds 30 minutes we append a hint that the batch job is
// likely overdue, because /api/sessions only refreshes on a batch
// run.
function formatStaleness(end) {
  if (!end) return null;
  const ageMs = Date.now() - end.getTime();
  const ageMin = Math.round(ageMs / 60000);

  let text;
  if (ageMin < 1) text = "as of just now";
  else if (ageMin < 60) text = `as of ${ageMin} min ago`;
  else if (ageMin < 60 * 24) text = `as of ${Math.round(ageMin / 60)} h ago`;
  else text = `as of ${Math.round(ageMin / (60 * 24))} d ago`;

  if (ageMin > 30) text += " (batch job pending)";
  return text;
}
