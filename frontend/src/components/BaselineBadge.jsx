import { formatZ } from "../utils";

// Small chip placed in the corner of a metric card. Encodes the
// current window's z-score against the user's baseline. Hidden when
// the value is within ±0.5σ to reduce visual noise, or when no
// z-score can be computed (thin baseline; see zScore in utils.js).
//
// `direction` controls how the sign maps to color:
// - "higher_is_good" — above-baseline is green, below is red.
// - "higher_is_bad"  — above-baseline is red, below is green.
// - "neutral"        — sign is shown but no judgment color is applied.
//
// Dual encoding for a11y: the arrow glyph and aria-label carry the
// same information as the color.
export function BaselineBadge({ z, direction, label }) {
  if (z == null || Math.abs(z) < 0.5) return null;

  const formatted = formatZ(z);
  if (!formatted) return null;

  let colorClass = "text-zinc-400";
  if (direction === "higher_is_good") {
    colorClass = z > 0 ? "text-green-400" : "text-red-400";
  } else if (direction === "higher_is_bad") {
    colorClass = z > 0 ? "text-red-400" : "text-green-400";
  }

  const directionWord = z > 0 ? "above" : "below";
  const aria = `${label.toLowerCase()} ${Math.abs(z).toFixed(1)} standard deviations ${directionWord} baseline`;

  return (
    <span
      className={`absolute top-3 right-3 text-xs font-medium ${colorClass}`}
      aria-label={aria}
    >
      {formatted.text} {formatted.arrow}
    </span>
  );
}
