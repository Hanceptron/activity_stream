import { useMemo } from "react";
import {
  endOfLocalDayMs,
  groupSessionsByDay,
  parseUtc,
  ratingForDay,
} from "../utils";
import { usePolling } from "../usePolling";
import { SessionRow, SessionTableHeader } from "./SessionsList";
import { ActivityGauge } from "./ActivityGauge";
import { ActivityPanel } from "./ActivityPanel";
import { Heatmap } from "./Heatmap";

// Drill-down panel for a single calendar day. Rendered conditionally
// by App.jsx when the user clicks a cell in MonthCalendar. Shows that
// day's keystroke timeline (same style as the top live graph, but
// anchored to the day instead of "now"), the movement + click
// heatmaps, and the day's sessions table.
//
// The graph and heatmaps come from batch outputs sourced from the
// event archive (/api/day_metrics, /api/heatmap_day), so historical
// days work and mouse data is available without touching the live
// streaming path. Both are keyed by dayKey+user, so selecting a
// different day refetches.
const RATING_META = {
  productive: { text: "Productive", color: "text-green-400" },
  normal: { text: "Normal", color: "text-zinc-300" },
  tired: { text: "Tired", color: "text-amber-400" },
  burnt_out: { text: "Burnt out", color: "text-red-400" },
};

export function DayDetailPanel({ sessions, dayKey, user, onClose }) {
  const byDay = useMemo(() => groupSessionsByDay(sessions), [sessions]);
  const daySessions = byDay.get(dayKey) ?? [];
  // Chronological for the detail view (oldest first), the opposite
  // of the main SessionsList which is reverse-chronological. Reading
  // a single day naturally happens forward in time.
  const sorted = [...daySessions].sort((a, b) => {
    const ta = parseUtc(a.session_start)?.getTime() ?? 0;
    const tb = parseUtc(b.session_start)?.getTime() ?? 0;
    return ta - tb;
  });
  const rating = ratingForDay(sorted);
  const totalMin = sorted.reduce((acc, s) => acc + (s.window_count ?? 0), 0);
  const dateLabel = formatDateLabel(dayKey);
  const meta = rating ? RATING_META[rating] : null;

  // Per-day timeline + heatmap from the batch outputs. user is always
  // set when the panel renders (App passes effectiveUser), but guard
  // the URL anyway so a transient null doesn't fetch "/...user=null".
  const dayMetrics = usePolling(
    user ? `/api/day_metrics?day=${dayKey}&user=${user}` : null,
    30_000,
  );
  const dayHeatmap = usePolling(
    user ? `/api/heatmap_day?day=${dayKey}&user=${user}` : null,
    30_000,
  );
  // Primary-screen grid bounds so the heatmaps frame to the real
  // screen and clip any external-monitor tail. Rarely changes, so a
  // slow poll is plenty. Falls back to MacBook 16" defaults until the
  // first response lands.
  const display = usePolling("/api/display", 5 * 60_000);
  const frameW = display?.grid_w ?? 108;
  const frameH = display?.grid_h ?? 70;

  const anchorMs = endOfLocalDayMs(dayKey);

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <div className="flex items-baseline justify-between mb-3 gap-3 flex-wrap">
        <div className="flex items-baseline gap-3 flex-wrap">
          <h2 className="text-sm text-zinc-300">{dateLabel}</h2>
          {meta ? (
            <span className={`text-sm font-medium ${meta.color}`}>
              {meta.text}
            </span>
          ) : (
            <span className="text-xs text-zinc-500">no rating</span>
          )}
          <span className="text-xs text-zinc-500">
            {sorted.length} session{sorted.length === 1 ? "" : "s"} ·{" "}
            {totalMin} active minute{totalMin === 1 ? "" : "s"}
          </span>
        </div>
        <button
          type="button"
          onClick={onClose}
          className="text-zinc-500 hover:text-zinc-200 text-xl leading-none px-2"
          aria-label="Close day detail"
          title="Close"
        >
          ×
        </button>
      </div>

      <div className="space-y-4">
        <ActivityGauge
          metrics={dayMetrics}
          range="1d"
          anchorMs={anchorMs}
          label={dateLabel}
        />
        <ActivityPanel metrics={dayMetrics} range="1d" anchorMs={anchorMs} />
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <Heatmap
            data={dayHeatmap}
            type="move"
            title="Movement"
            frameW={frameW}
            frameH={frameH}
          />
          <Heatmap
            data={dayHeatmap}
            type="click"
            title="Clicks"
            frameW={frameW}
            frameH={frameH}
          />
        </div>

        {sorted.length === 0 ? (
          <div className="text-sm text-zinc-500 py-2">
            no sessions recorded on this date
          </div>
        ) : (
          <div className="max-h-96 overflow-y-auto">
            <SessionTableHeader />
            {sorted.map((s) => (
              <SessionRow key={s.session_id} s={s} />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// "Friday, May 31, 2026" - rebuild the Date from local components so
// the label matches what the user thinks of as that calendar day.
function formatDateLabel(dayKey) {
  const [y, m, d] = dayKey.split("-").map(Number);
  const date = new Date(y, m - 1, d);
  return date.toLocaleDateString(undefined, {
    weekday: "long",
    year: "numeric",
    month: "long",
    day: "numeric",
  });
}
