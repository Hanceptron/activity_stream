import { parseUtc } from "../utils";

// Most-recent-first session table. The fatigue value is green when
// negative (flow) and red when positive (degrading). Sessions
// flagged unreliable show "insufficient data" instead so a noisy
// slope from a 2-window session is not reported as fact.
export function SessionsList({ sessions }) {
  const rows = (sessions || []).slice(0, 20);

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <h2 className="text-sm text-zinc-400 mb-3">Recent sessions</h2>
      <div className="max-h-96 overflow-y-auto">
        <div className="grid grid-cols-4 gap-4 text-xs text-zinc-500 pb-2 border-b border-zinc-700">
          <div>Started</div>
          <div>Duration</div>
          <div>Keystrokes</div>
          <div>Fatigue</div>
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
          const fatigueColor =
            s.fatigue_index < 0 ? "text-green-400" : "text-red-400";
          return (
            <div
              key={s.session_id}
              className="grid grid-cols-4 gap-4 text-sm py-2 border-b border-zinc-700/50 text-zinc-200"
            >
              <div>{start.toLocaleString()}</div>
              <div>{durationMin} min</div>
              <div>{s.keystrokes_total}</div>
              <div>
                {s.fatigue_reliable ? (
                  <span className={fatigueColor}>
                    {s.fatigue_index.toFixed(2)}
                  </span>
                ) : (
                  <span className="text-zinc-500">insufficient data</span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
