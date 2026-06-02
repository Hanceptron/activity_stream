import { useEffect, useState } from "react";
import {
  getTodaysSessions,
  parseUtc,
  peakKsPerMinToday,
  sessionTimer,
} from "../utils";
import { UserSelector } from "./UserSelector";

// Single compact header band. Left group is identity + current
// state, right group is summary chips + live indicator.
//
// The 1-Hz ticker keeps Date.now() out of render (the
// react-hooks/purity rule rejects impure calls during render) and
// drives both the live-status freshness check and the session
// timer. Cleanup on unmount.
//
// Live indicator dual-encodes via shape AND color AND word:
//   live = green dot (round)    + "live"
//   offline = red square        + "offline"
// role="status" makes the span a live region so screen readers
// announce the transition rather than needing the user to revisit.
export function Header({
  metrics,
  sessions,
  users,
  selectedUser,
  onSelectUser,
}) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const latest =
    metrics && metrics.length > 0 ? metrics[metrics.length - 1] : null;
  // window_end (not window_start) so the dot reads "how stale is the
  // freshest measured minute" rather than "how far past the start of
  // the freshest window are we". With Spark's 1-min window + 30 s
  // watermark, window_start age oscillates 90-150 s under continuous
  // activity, which would cross the 2-minute live threshold every
  // minute and flicker the dot. window_end age stays in 30-90 s.
  const latestTime = latest ? parseUtc(latest.window_end)?.getTime() : null;
  const ageMs = latestTime != null ? now - latestTime : Infinity;
  const isLive = ageMs < 2 * 60 * 1000;

  const sessionMs = sessionTimer(metrics, 5 * 60 * 1000, now);
  const todays = getTodaysSessions(sessions, now);
  const todayKeystrokes = todays.reduce(
    (sum, s) => sum + (s.keystrokes_total ?? 0),
    0
  );
  const longestSessionMin =
    todays.length > 0
      ? Math.max(...todays.map((s) => s.window_count ?? 0))
      : 0;
  const peakKpm = peakKsPerMinToday(metrics, now);

  return (
    <header className="flex items-center justify-between gap-4 flex-wrap">
      <div className="flex items-center gap-4 flex-wrap">
        <img
          src="/logo.png"
          alt="Keyspark"
          className="h-14 w-auto object-contain select-none mix-blend-screen drop-shadow-[0_0_18px_rgba(139,92,246,0.5)]"
        />
        <UserSelector
          users={users}
          value={selectedUser}
          onChange={onSelectUser}
        />
        <span className="text-sm text-zinc-400 tabular-nums" aria-live="off">
          {sessionMs != null ? (
            <span className="text-zinc-200">
              {formatSessionTimer(sessionMs)}
            </span>
          ) : (
            <span className="text-zinc-500">idle</span>
          )}
        </span>
        {todayKeystrokes > 0 && (
          <span className="text-sm text-zinc-400">
            <span className="text-zinc-200 font-medium">
              {formatCompact(todayKeystrokes)}
            </span>{" "}
            keys today
          </span>
        )}
      </div>
      <div className="flex items-center gap-3 text-sm flex-wrap">
        {peakKpm != null && peakKpm > 0 && (
          <Chip aria-label={`Peak keystrokes per minute over the last hour: ${peakKpm}`}>
            <span aria-hidden="true">🔥</span> peak {peakKpm} kpm (1h)
          </Chip>
        )}
        {longestSessionMin > 0 && (
          <Chip
            aria-label={`Longest session today: ${longestSessionMin} active minutes`}
          >
            longest {longestSessionMin}m today
          </Chip>
        )}
        <span
          role="status"
          className="flex items-center gap-2 text-zinc-400"
        >
          <span
            className={`inline-block w-2 h-2 transition-all ${
              isLive
                ? "bg-green-500 rounded-full"
                : "bg-red-500 rounded-none"
            }`}
            aria-hidden="true"
          />
          <span>{isLive ? "live" : "offline"}</span>
        </span>
      </div>
    </header>
  );
}

// Small status chip used for personal-best indicators. The visible
// text always carries the same meaning as the aria-label so screen
// reader users hear an equivalent phrasing of what sighted users
// see.
function Chip({ children, ...rest }) {
  return (
    <span
      className="glass-chip text-xs text-zinc-200"
      {...rest}
    >
      {children}
    </span>
  );
}

// h:mm:ss with hours hidden when zero. tabular-nums on the parent
// keeps the timer from jittering when digits change width.
function formatSessionTimer(ms) {
  const totalSec = Math.floor(ms / 1000);
  const hours = Math.floor(totalSec / 3600);
  const min = Math.floor((totalSec % 3600) / 60);
  const sec = totalSec % 60;
  const pad = (n) => String(n).padStart(2, "0");
  if (hours > 0) return `${hours}:${pad(min)}:${pad(sec)}`;
  return `${min}:${pad(sec)}`;
}

// 1234 -> "1.2k", 12345 -> "12.3k", anything under 1000 stays as
// the raw integer. Keeps the header compact.
function formatCompact(n) {
  if (n >= 1000) return `${(n / 1000).toFixed(1)}k`;
  return String(n);
}
