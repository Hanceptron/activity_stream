import { correctionRatio, zScore } from "../utils";
import { BaselineBadge } from "./BaselineBadge";

// Five cards showing the most recent window's counts plus a derived
// correction ratio. Each card optionally shows two extra readouts:
// - "baseline avg" subtitle: the user's mean for this metric from
//   /api/baseline, so the current value can be eyeballed.
// - BaselineBadge in the corner: a z-score chip ("+1.6σ ↑") when the
//   current window deviates noticeably from the baseline. Hidden
//   when the deviation is small or when the std is unusable.
//
// `direction` per card controls the badge color: keystrokes/words
// are framed as higher-is-good, corrections as higher-is-bad, clicks
// as neutral (no judgment color, only magnitude). The correction-
// ratio card has no badge because the backend does not expose a
// std for the ratio — inventing one would mislead.
export function MetricCards({ metrics, baseline }) {
  const latest =
    metrics && metrics.length > 0 ? metrics[metrics.length - 1] : null;
  // `baseline` is the single-user baseline object passed down from
  // App (baselineForUser); null until the user is selected and
  // /api/baseline has returned.
  const userBaseline = baseline ?? null;

  const cards = [
    {
      label: "Keystrokes per minute",
      value: latest?.keystrokes,
      meanKey: "keystrokes_mean",
      stdKey: "keystrokes_std",
      direction: "higher_is_good",
    },
    {
      label: "Words per minute",
      value: latest?.words,
      meanKey: "words_mean",
      stdKey: "words_std",
      direction: "higher_is_good",
    },
    {
      label: "Corrections",
      value: latest?.corrections,
      meanKey: "corrections_mean",
      stdKey: "corrections_std",
      direction: "higher_is_bad",
    },
    {
      label: "Clicks",
      value: latest?.clicks,
      meanKey: "clicks_mean",
      stdKey: "clicks_std",
      direction: "neutral",
    },
    {
      label: "Correction ratio",
      // Derived from latest, formatted as a percentage. customValue
      // and customBaseline let this card share the render path
      // without bending the data-keyed descriptors above.
      customValue: (l) =>
        l ? `${(correctionRatio(l) * 100).toFixed(1)}%` : null,
      customBaseline: (b) => {
        if (!b || b.corrections_mean == null || !b.keystrokes_mean) return null;
        const ratio = b.corrections_mean / Math.max(b.keystrokes_mean, 1);
        return `baseline: ${(ratio * 100).toFixed(1)}%`;
      },
      direction: "higher_is_bad",
      // No badge — see header comment.
    },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
      {cards.map((c) => {
        const value = c.customValue ? c.customValue(latest) : c.value;
        const baselineMean = userBaseline ? userBaseline[c.meanKey] : null;
        const baselineStd = userBaseline ? userBaseline[c.stdKey] : null;
        const z = c.customValue
          ? null
          : zScore(c.value, baselineMean, baselineStd);
        const baselineLine = c.customBaseline
          ? c.customBaseline(userBaseline)
          : baselineMean != null
            ? `baseline avg: ${baselineMean.toFixed(1)}`
            : null;

        return (
          <div
            key={c.label}
            className="relative bg-zinc-800 rounded-lg p-4 border border-zinc-700"
          >
            <BaselineBadge z={z} direction={c.direction} label={c.label} />
            <div className="text-sm text-zinc-400">{c.label}</div>
            <div className="text-3xl font-semibold text-zinc-100 mt-1">
              {value ?? "—"}
            </div>
            {baselineLine && (
              <div className="text-xs text-zinc-500 mt-2">{baselineLine}</div>
            )}
          </div>
        );
      })}
    </div>
  );
}
