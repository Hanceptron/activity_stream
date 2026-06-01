import { useEffect, useMemo, useState } from "react";
import { groupSessionsByDay, localDayKey, ratingForDay } from "../utils";

// 8 weeks x 7 days (Mon-Sun) GitHub-contributions style grid. Each
// cell is one day, colored by ratingForDay() and clickable to drill
// down. Reuses the same four labels (productive/normal/tired/
// burnt_out) the model emits, so the color vocabulary is consistent
// across SessionsList, the day drill-down, and this calendar.
//
// Computation is client-side off the sessions prop. No new endpoint.
// A 60-second ticker keeps the "today" pointer up to date so the
// calendar advances even on a long-open browser tab.
const WEEKS = 8;
const DAYS_IN_WEEK = 7;
const DAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const RATING_META = {
  productive: { bg: "bg-green-400", text: "Productive" },
  normal: { bg: "bg-zinc-400", text: "Normal" },
  tired: { bg: "bg-amber-400", text: "Tired" },
  burnt_out: { bg: "bg-red-400", text: "Burnt out" },
};
const RATING_ORDER = ["productive", "normal", "tired", "burnt_out"];

export function MonthCalendar({ sessions, selectedDay, onSelectDay }) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 60_000);
    return () => clearInterval(id);
  }, []);

  const byDay = useMemo(() => groupSessionsByDay(sessions), [sessions]);

  const cells = useMemo(() => {
    const today = new Date(now);
    today.setHours(0, 0, 0, 0);
    // Distance from today to the most recent Monday. JS getDay():
    // Sunday=0 .. Saturday=6. (day+6)%7 maps Mon=0, Sun=6.
    const distToMonday = (today.getDay() + 6) % 7;
    const thisMonday = new Date(today);
    thisMonday.setDate(today.getDate() - distToMonday);

    const out = [];
    // Column-first ordering matches `grid-flow-col`: filling row 0..6
    // before advancing the column. Column 0 is the earliest week,
    // column WEEKS-1 is the current week.
    for (let weekIdx = 0; weekIdx < WEEKS; weekIdx++) {
      const weekStart = new Date(thisMonday);
      weekStart.setDate(thisMonday.getDate() - (WEEKS - 1 - weekIdx) * 7);
      for (let dow = 0; dow < DAYS_IN_WEEK; dow++) {
        const cellDate = new Date(weekStart);
        cellDate.setDate(weekStart.getDate() + dow);
        const key = localDayKey(cellDate);
        const daySessions = byDay.get(key) ?? [];
        const rating = ratingForDay(daySessions);
        const totalMin = daySessions.reduce(
          (acc, s) => acc + (s.window_count ?? 0),
          0,
        );
        out.push({
          key,
          date: cellDate,
          rating,
          totalMin,
          sessionCount: daySessions.length,
          isFuture: cellDate.getTime() > today.getTime(),
        });
      }
    }
    return out;
  }, [byDay, now]);

  return (
    <div className="glass-panel">
      <div className="flex items-baseline justify-between mb-3 gap-3 flex-wrap">
        <h2 className="text-sm text-zinc-400">
          History (last {WEEKS} weeks)
        </h2>
        <Legend />
      </div>
      <div className="flex gap-2">
        <div className="grid grid-rows-7 gap-1 text-[10px] text-zinc-500 pr-1">
          {DAY_LABELS.map((d) => (
            <div key={d} className="h-7 flex items-center">
              {d}
            </div>
          ))}
        </div>
        <div className="grid grid-rows-7 grid-flow-col gap-1 flex-1">
          {cells.map((cell) => (
            <DayCell
              key={cell.key}
              cell={cell}
              isSelected={cell.key === selectedDay}
              onClick={() => onSelectDay(cell.key)}
            />
          ))}
        </div>
      </div>
    </div>
  );
}

// Single calendar cell. Coloring rules:
//  - has rating  -> the rating's solid background, dark text for the date number
//  - no rating   -> bg-zinc-900 (darker than the container), muted zinc-500 date
//  - future day  -> bg-zinc-900 + 30 % opacity, click disabled
// The ring shows selection. aria-label and the native title carry the
// full readable summary so keyboard / screen-reader / hover users
// all get the same information.
function DayCell({ cell, isSelected, onClick }) {
  const meta = cell.rating ? RATING_META[cell.rating] : null;
  const bg = cell.isFuture
    ? "bg-zinc-900 opacity-30"
    : meta
      ? meta.bg
      : "glass-inset";
  const ring = isSelected ? "ring-2 ring-brand-cyan" : "";

  const summary = cell.isFuture
    ? `${cell.date.toDateString()}: not yet`
    : cell.rating
      ? `${cell.date.toDateString()}: ${meta.text}, ${cell.sessionCount} session${cell.sessionCount === 1 ? "" : "s"}, ${cell.totalMin} active minute${cell.totalMin === 1 ? "" : "s"}`
      : `${cell.date.toDateString()}: no sessions recorded`;

  return (
    <button
      type="button"
      onClick={onClick}
      disabled={cell.isFuture}
      className={`h-7 rounded text-[10px] tabular-nums leading-none flex items-center justify-center transition-all ${bg} ${ring} ${
        cell.isFuture
          ? "cursor-not-allowed"
          : "cursor-pointer hover:ring-2 hover:ring-brand-violet/70"
      }`}
      aria-label={summary}
      title={summary}
    >
      <span
        className={
          cell.rating
            ? "text-zinc-900 font-medium"
            : "text-zinc-500"
        }
      >
        {cell.date.getDate()}
      </span>
    </button>
  );
}

function Legend() {
  return (
    <div className="flex items-center gap-3 text-[10px] text-zinc-400 flex-wrap">
      {RATING_ORDER.map((label) => (
        <div key={label} className="flex items-center gap-1">
          <span
            className={`inline-block w-2.5 h-2.5 rounded ${RATING_META[label].bg}`}
            aria-hidden="true"
          />
          {RATING_META[label].text}
        </div>
      ))}
      <div className="flex items-center gap-1">
        <span
          className="inline-block w-2.5 h-2.5 rounded glass-inset"
          aria-hidden="true"
        />
        No data
      </div>
    </div>
  );
}
