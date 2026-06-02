import { formatClock, formatSessionRange, parseUtc } from "../utils";

// Shared session-table primitives: the five-column header and one row,
// reused by the day drill-down (DayDetailPanel). Extracted from the
// former SessionsList so the table outlives the removed "Recent
// sessions" panel.
//
// Fatigue cell: an arrow glyph (↗ degrading / ↘ improving / – when
// unreliable) is rendered alongside the number so the red/green color
// is not the only signal. The aria-label echoes the value and
// direction for screen readers.
//
// Scale column: a 60-px mini SVG bar showing fatigue clamped to
// [-1, +1] with a zero-line marker so the user can rank rows at a
// glance, not just read the number.

// Column headers for the five-column session table. Exported so the
// DayDetailPanel can reuse the exact same layout for its scoped view
// of a single day's sessions.
export function SessionTableHeader() {
  return (
    <div className="grid grid-cols-5 gap-4 text-xs text-zinc-500 pb-2 border-b border-white/10">
      <div>Session</div>
      <div>Duration</div>
      <div>Keystrokes</div>
      <div>Fatigue</div>
      <div>Scale</div>
    </div>
  );
}

// One session row. Exported alongside SessionTableHeader so the
// DayDetailPanel renders rows identical across views. start/end can be
// null if the backend ever emits a malformed timestamp; the guards
// keep the row from rendering "NaN min".
export function SessionRow({ s, live = false, now = 0 }) {
  const start = parseUtc(s.session_start);
  const end = parseUtc(s.session_end);
  // A live session has no real end yet, so its duration counts up from
  // the start to now (floored, to match the header timer's minute);
  // finished sessions use their recorded span.
  const durationMin = live
    ? start
      ? Math.max(1, Math.floor((now - start.getTime()) / 60000))
      : null
    : start && end
      ? Math.max(1, Math.round((end - start) / 60000))
      : null;

  return (
    <div className="grid grid-cols-5 gap-4 text-sm py-2 border-b border-white/5 text-zinc-200 items-center">
      <div className="flex items-center gap-2">
        {live ? (
          <>
            <span>Started {formatClock(start)}</span>
            <LiveBadge />
          </>
        ) : (
          <span>{formatSessionRange(start, end)}</span>
        )}
      </div>
      <div>{durationMin != null ? `${durationMin} min` : "—"}</div>
      <div>{s.keystrokes_total}</div>
      <FatigueCell s={s} />
      <FatigueScale s={s} />
    </div>
  );
}

// Pulsing marker for the session still in progress (its last activity is
// within DayDetailPanel's live window). The aria-label carries the
// meaning so the green dot is not the only signal.
function LiveBadge() {
  return (
    <span
      className="inline-flex items-center gap-1 text-[10px] font-medium uppercase tracking-wide text-green-400"
      aria-label="live, in progress"
    >
      <span
        className="w-1.5 h-1.5 rounded-full bg-green-400 animate-pulse"
        aria-hidden="true"
      />
      live
    </span>
  );
}

function FatigueCell({ s }) {
  // Defensive null check on fatigue_index in addition to the
  // fatigue_reliable flag. The batch job is supposed to set
  // fatigue_reliable=False when any slope (and therefore the index)
  // is null, but treating the index as null-safe here means a
  // future regression in that contract can't crash the table.
  if (!s.fatigue_reliable || s.fatigue_index == null) {
    return (
      <span className="text-zinc-500" aria-label="fatigue: insufficient data">
        – insufficient data
      </span>
    );
  }
  const improving = s.fatigue_index < 0;
  const color = improving ? "text-green-400" : "text-red-400";
  const arrow = improving ? "↘" : "↗";
  const word = improving ? "improving" : "degrading";
  return (
    <span
      className={color}
      aria-label={`fatigue ${s.fatigue_index.toFixed(2)}, ${word}`}
    >
      {arrow} {s.fatigue_index.toFixed(2)}
    </span>
  );
}

function FatigueScale({ s }) {
  if (!s.fatigue_reliable || s.fatigue_index == null) {
    return <div aria-hidden="true" />;
  }

  // Clamp to [-1, +1] and map to a horizontal position. The track
  // is 60 px wide with a 1 px tick at the centered zero line.
  const clamped = Math.max(-1, Math.min(1, s.fatigue_index));
  const markerX = (clamped + 1) * 30; // (clamped + 1) / 2 * 60
  const markerColor = s.fatigue_index < 0 ? "#4ade80" : "#f87171";

  return (
    <svg
      viewBox="0 0 60 8"
      width="60"
      height="8"
      role="img"
      aria-label={`fatigue ${s.fatigue_index.toFixed(2)} on a -1 to +1 scale`}
      className="overflow-visible"
    >
      <rect x={0} y={3} width={60} height={2} fill="#3f3f46" rx={1} />
      <line x1={30} y1={1} x2={30} y2={7} stroke="#52525b" strokeWidth={1} />
      <circle cx={markerX} cy={4} r={3} fill={markerColor} />
    </svg>
  );
}
