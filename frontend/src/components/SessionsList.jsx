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
    <div className="glass-panel">
      <div className="flex items-center justify-between mb-3 gap-3">
        <div className="flex items-baseline gap-3">
          <h2 className="text-sm text-zinc-400">Recent sessions</h2>
          <StalenessChip lastRunIso={lastRunIso} status={status} />
        </div>
        <SessionFatigueSparkline sessions={sessions} />
      </div>
      <div className="max-h-96 overflow-y-auto">
        <SessionTableHeader />
        {rows.length === 0 && (
          <div className="text-sm text-zinc-500 py-4">
            no sessions yet - run the batch job
          </div>
        )}
        {rows.map((s) => (
          <SessionRow key={s.session_id} s={s} />
        ))}
      </div>
    </div>
  );
}

// Column headers for the six-column session table. Exported so the
// DayDetailPanel can reuse the exact same layout for its scoped
// view of a single day's sessions.
export function SessionTableHeader() {
  return (
    <div className="grid grid-cols-6 gap-4 text-xs text-zinc-500 pb-2 border-b border-white/10">
      <div>Started</div>
      <div>Duration</div>
      <div>Keystrokes</div>
      <div>Fatigue</div>
      <div>Scale</div>
      <div>Type</div>
    </div>
  );
}

// One session row. Exported alongside SessionTableHeader so the
// DayDetailPanel renders rows identical to the SessionsList. start/
// end can be null if the backend ever emits a malformed timestamp;
// the guards keep the row from rendering "NaN min".
export function SessionRow({ s }) {
  const start = parseUtc(s.session_start);
  const end = parseUtc(s.session_end);
  const durationMin =
    start && end ? Math.max(1, Math.round((end - start) / 60000)) : null;

  return (
    <div className="grid grid-cols-6 gap-4 text-sm py-2 border-b border-white/5 text-zinc-200 items-center">
      <div>{start ? start.toLocaleString() : "—"}</div>
      <div>{durationMin != null ? `${durationMin} min` : "—"}</div>
      <div>{s.keystrokes_total}</div>
      <FatigueCell s={s} />
      <FatigueScale s={s} />
      <SessionTypeCell s={s} />
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

// One of four session-type labels predicted by the offline Random
// Forest (see streamguard/ml.py). The model picks each label by
// taking the modal per-window prediction inside the session, and
// the four classes come from fatigue_index quartiles - so the word
// shown here matches how the model was trained, not a hand-tuned UI
// threshold. Renders "—" if no model has been trained yet.
function SessionTypeCell({ s }) {
  const label = s.predicted_label;
  if (!label) {
    return (
      <span className="text-zinc-500" aria-label="session type unavailable">
        —
      </span>
    );
  }
  const meta = SESSION_TYPE_META[label] ?? {
    text: label,
    color: "text-zinc-300",
  };
  return (
    <span
      className={`${meta.color} font-medium`}
      aria-label={`session type: ${meta.text}`}
    >
      {meta.text}
    </span>
  );
}

// Maps the raw label strings the model emits to the colors and
// human-readable labels rendered in the table. Kept as a flat
// object so adding a new class only takes one row.
const SESSION_TYPE_META = {
  productive: { text: "Productive", color: "text-green-400" },
  normal: { text: "Normal", color: "text-zinc-300" },
  tired: { text: "Tired", color: "text-amber-400" },
  burnt_out: { text: "Burnt out", color: "text-red-400" },
};
