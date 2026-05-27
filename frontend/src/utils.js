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

// Range presets shared by the activity gauge, idle strip, and
// metrics chart. Each entry specifies how many buckets to render
// and how wide each bucket is in minutes; total minutes covered =
// bucketCount * bucketSizeMin. Bucket counts are tuned to keep the
// strip and chart readable (60-100 cells) regardless of range.
// pollMs scales with range: short windows poll fast for freshness,
// long windows poll slowly to keep payloads under control.
export const ACTIVITY_RANGES = {
  "1h": { bucketCount: 60, bucketSizeMin: 1,   label: "Last 60 minutes", pollMs: 5_000 },
  "6h": { bucketCount: 72, bucketSizeMin: 5,   label: "Last 6 hours",    pollMs: 15_000 },
  "1d": { bucketCount: 96, bucketSizeMin: 15,  label: "Last 24 hours",   pollMs: 30_000 },
  "3d": { bucketCount: 72, bucketSizeMin: 60,  label: "Last 3 days",     pollMs: 60_000 },
  "1w": { bucketCount: 84, bucketSizeMin: 120, label: "Last 7 days",     pollMs: 60_000 },
};

// A bucket counts as "active" if any keystrokes, words,
// corrections, or clicks were recorded. Shared between
// ActivityGauge and IdleStrip so the two panels stay in sync.
export function isActiveBucket(b) {
  if (!b) return false;
  return (
    (b.keystrokes ?? 0) +
      (b.words ?? 0) +
      (b.corrections ?? 0) +
      (b.clicks ?? 0) >
    0
  );
}

// Aggregate per-minute /api/metrics rows into `bucketCount` fixed-
// width buckets ending at `now`. Each bucket sums the count
// columns across the per-minute rows that fall within it; missing
// buckets become zero-valued synthetic rows so callers can render
// idle stretches as flat zero rather than connecting across gaps.
// Buckets are keyed by floor(time / bucketMs) so a per-minute row
// snaps into whichever bucket its window_start belongs to.
export function bucketizeWindows(metrics, bucketCount, bucketSizeMin, now = Date.now()) {
  const bucketMs = bucketSizeMin * 60_000;
  const nowBucket = Math.floor(now / bucketMs);
  const startBucket = nowBucket - bucketCount + 1;

  const sums = new Map();
  for (const m of metrics || []) {
    const t = parseUtc(m.window_start);
    if (!t) continue;
    const b = Math.floor(t.getTime() / bucketMs);
    if (b < startBucket || b > nowBucket) continue;
    const cur = sums.get(b);
    if (cur) {
      cur.keystrokes += m.keystrokes ?? 0;
      cur.words += m.words ?? 0;
      cur.corrections += m.corrections ?? 0;
      cur.clicks += m.clicks ?? 0;
    } else {
      sums.set(b, {
        keystrokes: m.keystrokes ?? 0,
        words: m.words ?? 0,
        corrections: m.corrections ?? 0,
        clicks: m.clicks ?? 0,
      });
    }
  }

  const result = [];
  for (let b = startBucket; b <= nowBucket; b++) {
    const window_start = new Date(b * bucketMs).toISOString();
    const sum = sums.get(b);
    if (sum) {
      result.push({ window_start, ...sum });
    } else {
      result.push({
        window_start,
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

// Tick / tooltip label for a single bucket. When the total range
// fits in a day the date is implied by "today" and showing only
// HH:MM keeps the axis terse; once the range spans multiple days
// the date is needed to disambiguate the same hour appearing
// repeatedly.
export function formatBucketTime(date, totalMinutes) {
  if (!date) return "";
  if (totalMinutes <= 1440) {
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleString([], {
    month: "numeric",
    day: "numeric",
    hour: "2-digit",
  });
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

// Render a timestamp as a relative-time string like "as of 3 min
// ago", "as of 2 h ago", "as of 4 d ago". Appends "(batch job
// pending)" once the gap exceeds 30 minutes — used both for the
// data-staleness chip in TodayTotals (newest session_end) and the
// compute-staleness chips elsewhere (last batch_status.last_run).
// Returns null when end is missing so the caller can skip rendering.
export function formatStaleness(end, now = Date.now()) {
  if (!end) return null;
  const ageMs = now - end.getTime();
  const ageMin = Math.round(ageMs / 60000);

  let text;
  if (ageMin < 1) text = "as of just now";
  else if (ageMin < 60) text = `as of ${ageMin} min ago`;
  else if (ageMin < 60 * 24) text = `as of ${Math.round(ageMin / 60)} h ago`;
  else text = `as of ${Math.round(ageMin / (60 * 24))} d ago`;

  if (ageMin > 30) text += " (batch job pending)";
  return text;
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
