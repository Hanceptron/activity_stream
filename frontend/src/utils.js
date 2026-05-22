// The backend serves timestamps as ISO strings without a timezone
// suffix (e.g. "2026-05-21T13:44:00.000"). Spark stores them in UTC
// and pandas writes them without tz info, but JavaScript's Date
// parser treats a tz-less ISO string as local time. Appending "Z"
// forces UTC interpretation so the displayed times line up with
// real wall-clock times.
export function parseUtc(s) {
  if (!s) return null;
  return new Date(s.endsWith("Z") ? s : s + "Z");
}

// Standard score for a single observation against a per-user
// baseline. Returns null when any input is missing/NaN, or when std
// is too small to be meaningful. Important: value === 0 is a valid
// observation, so this uses explicit null/NaN checks rather than
// truthy tests.
//
// Thin baselines (e.g. a freshly run batch job with only one window
// per user) leave std as NaN; this guard hides downstream badges
// rather than rendering "+Infσ".
export function zScore(value, mean, std) {
  if (value == null || mean == null || std == null) return null;
  if (Number.isNaN(value) || Number.isNaN(mean) || Number.isNaN(std)) return null;
  if (std < 1e-9) return null;
  return (value - mean) / std;
}

// Format a z-score for display: one decimal, clamped to ±3, with a
// leading sign and a σ suffix. Returns the formatted text plus an
// arrow glyph used as the non-color a11y channel. Clamping prevents
// an outlier from blowing out card layouts.
export function formatZ(z) {
  if (z == null) return null;
  const clamped = Math.max(-3, Math.min(3, z));
  const sign = clamped >= 0 ? "+" : "";
  const arrow = z >= 0 ? "↑" : "↓";
  return { text: `${sign}${clamped.toFixed(1)}σ`, arrow };
}

// Filter a list of API rows down to a single user. Preserves the
// null distinction so callers can still tell pre-fetch (null) apart
// from "no rows for this user" ([]). A null user passes the rows
// through unchanged — the brief interval between metrics arriving
// and the auto-select picking a user.
export function filterByUser(rows, user) {
  if (!rows) return null;
  if (!user) return rows;
  return rows.filter((r) => r && r.user === user);
}

// Take the existing /api/metrics rows (which only exist for minutes
// with activity) and produce exactly `minutes` objects, one per
// UTC-minute over the trailing window ending at `now`. Buckets are
// keyed by floor(time / 60_000) so a row falls into whichever
// minute its window_start belongs to. Missing buckets become zero-
// valued synthetic rows, which is what callers need to render an
// idle gap as flat-zero rather than as a connected line.
export function zeroFillWindows(metrics, minutes = 60, now = Date.now()) {
  const byBucket = new Map();
  for (const m of metrics || []) {
    const t = parseUtc(m.window_start);
    if (!t) continue;
    byBucket.set(Math.floor(t.getTime() / 60000), m);
  }

  const nowMinute = Math.floor(now / 60000);
  const result = [];
  for (let i = minutes - 1; i >= 0; i--) {
    const bucket = nowMinute - i;
    const existing = byBucket.get(bucket);
    if (existing) {
      result.push(existing);
    } else {
      result.push({
        window_start: new Date(bucket * 60000).toISOString(),
        keystrokes: 0,
        words: 0,
        corrections: 0,
        clicks: 0,
        synthetic: true,
      });
    }
  }
  return result;
}

// Corrections per keystroke for a single 1-minute window. Returns 0
// (not NaN) when keystrokes is 0; using Math.max(_, 1) keeps the
// expression branchless and safe.
export function correctionRatio(window) {
  if (!window) return 0;
  return (window.corrections ?? 0) / Math.max(window.keystrokes ?? 0, 1);
}

// Share of input that was mouse clicks vs keyboard keys for a
// single window. Returns null when both are zero — the caller
// (InputMixIndicator) treats that as "no input" rather than 0%.
export function keyboardMouseMix(window) {
  if (!window) return null;
  const k = window.keystrokes ?? 0;
  const c = window.clicks ?? 0;
  if (k + c === 0) return null;
  return c / (k + c);
}

// Milliseconds since the start of the current unbroken run of
// metric windows. A run ends when the gap between two consecutive
// window_starts exceeds `gapMs` (default 5 min, matching the batch
// job's session boundary). Returns null when the newest window is
// older than `gapMs` (the user is currently idle, so there is no
// running session).
//
// This is the frontend-side definition of "current session" — the
// batch job's session list lags by however often it runs.
export function sessionTimer(metrics, gapMs = 5 * 60 * 1000, now = Date.now()) {
  if (!metrics || metrics.length === 0) return null;

  const sorted = [...metrics].sort((a, b) => {
    const at = parseUtc(a.window_start);
    const bt = parseUtc(b.window_start);
    return bt.getTime() - at.getTime();
  });

  const newestTime = parseUtc(sorted[0].window_start)?.getTime();
  if (newestTime == null || now - newestTime > gapMs) return null;

  let runStartTime = newestTime;
  for (let i = 0; i < sorted.length - 1; i++) {
    const thisTime = parseUtc(sorted[i].window_start)?.getTime();
    const nextTime = parseUtc(sorted[i + 1].window_start)?.getTime();
    if (thisTime == null || nextTime == null) break;
    if (thisTime - nextTime > gapMs) break;
    runStartTime = nextTime;
  }
  return now - runStartTime;
}

// Top-k cells of a given type from /api/heatmap, sorted by count.
// Used by the HotspotsLeaderboard. Defensive against missing data.
export function pickHotspots(heatmap, type, k = 5) {
  if (!heatmap) return [];
  return heatmap
    .filter((c) => c && c.type === type)
    .sort((a, b) => (b.count ?? 0) - (a.count ?? 0))
    .slice(0, k);
}

// Filter /api/sessions down to sessions started on or after today's
// local midnight. Shared between TodayTotals and the Header's
// personal-best chips so the same "today" boundary is used in both.
export function getTodaysSessions(sessions, now = Date.now()) {
  if (!sessions) return [];
  const midnight = new Date(now);
  midnight.setHours(0, 0, 0, 0);
  return sessions.filter((s) => {
    const start = parseUtc(s.session_start);
    return start && start >= midnight;
  });
}

// Peak keystrokes-per-minute observed today across the supplied
// metrics slice. NOTE: /api/metrics returns only the last 60 min,
// so this is in practice "peak over the last hour" — callers should
// label accordingly. Returns null when nothing has been recorded
// today.
export function peakKsPerMinToday(metrics, now = Date.now()) {
  if (!metrics || metrics.length === 0) return null;
  const midnight = new Date(now);
  midnight.setHours(0, 0, 0, 0);
  const midnightMs = midnight.getTime();

  let peak = 0;
  for (const m of metrics) {
    const t = parseUtc(m.window_start);
    if (!t || t.getTime() < midnightMs) continue;
    if ((m.keystrokes ?? 0) > peak) peak = m.keystrokes;
  }
  return peak > 0 ? peak : null;
}
