// Four cards showing the most recent window's counts. The optional
// subtitle shows the user's baseline mean from /api/baseline so the
// current value can be eyeballed against the norm.
export function MetricCards({ metrics, baseline }) {
  const latest =
    metrics && metrics.length > 0 ? metrics[metrics.length - 1] : null;
  const userBaseline = baseline && baseline.length > 0 ? baseline[0] : null;

  const cards = [
    { label: "Keystrokes per minute", value: latest?.keystrokes, meanKey: "keystrokes_mean" },
    { label: "Words per minute", value: latest?.words, meanKey: "words_mean" },
    { label: "Corrections", value: latest?.corrections, meanKey: "corrections_mean" },
    { label: "Clicks", value: latest?.clicks, meanKey: "clicks_mean" },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map((c) => {
        const baselineMean = userBaseline ? userBaseline[c.meanKey] : null;
        return (
          <div key={c.label} className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
            <div className="text-sm text-zinc-400">{c.label}</div>
            <div className="text-3xl font-semibold text-zinc-100 mt-1">
              {c.value ?? "—"}
            </div>
            {baselineMean != null && (
              <div className="text-xs text-zinc-500 mt-2">
                baseline avg: {baselineMean.toFixed(1)}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
