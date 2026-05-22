import { parseUtc } from "../utils";
import { SessionFatigueSparkline } from "./SessionFatigueSparkline";
import { StalenessChip } from "./StalenessChip";

// Most-recent-first session table.
//
// Fatigue cell: an arrow glyph (↗ degrading / ↘ improving / – when
// unreliable) is rendered alongside the number so the red/green
// color is not the only signal. The aria-label echoes the value and
// direction for screen readers.
//
// Scale column: a 60-px mini SVG bar showing fatigue clamped to
// [-1, +1] with a zero-line marker so the user can rank rows at a
// glance, not just read the number.
//
// StalenessChip in the header reports when the batch job last
// computed these sessions; the SessionFatigueSparkline on the right
// is a different signal (trend across recent reliable sessions).
export function SessionsList({ sessions, lastRunIso, status }) {
  const rows = (sessions || []).slice(0, 20);

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <div className="flex items-center justify-between mb-3 gap-3">
        <div className="flex items-baseline gap-3">
          <h2 className="text-sm text-zinc-400">Recent sessions</h2>
          <StalenessChip lastRunIso={lastRunIso} status={status} />
        </div>
        <SessionFatigueSparkline sessions={sessions} />
      </div>
      <div className="max-h-96 overflow-y-auto">
        <div className="grid grid-cols-5 gap-4 text-xs text-zinc-500 pb-2 border-b border-zinc-700">
          <div>Started</div>
          <div>Duration</div>
          <div>Keystrokes</div>
          <div>Fatigue</div>
          <div>Scale</div>
        </div>
        {rows.length === 0 && (
          <div className="text-sm text-zinc-500 py-4">
            no sessions yet - run the batch job
          </div>
        )}
        {rows.map((s) => {
          const start = parseUtc(s.session_start);
          const end = parseUtc(s.session_end);
          const durationMin = Math.max(1, Math.round((end - start) / 60000));

          return (
            <div
              key={s.session_id}
              className="grid grid-cols-5 gap-4 text-sm py-2 border-b border-zinc-700/50 text-zinc-200 items-center"
            >
              <div>{start.toLocaleString()}</div>
              <div>{durationMin} min</div>
              <div>{s.keystrokes_total}</div>
              <FatigueCell s={s} />
              <FatigueScale s={s} />
            </div>
          );
        })}
      </div>
    </div>
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
