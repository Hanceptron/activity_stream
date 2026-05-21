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
