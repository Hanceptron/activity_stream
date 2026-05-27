import { useEffect, useState } from "react";

// Fetch `url` once on mount and then every `intervalMs`. Returns the
// most recent parsed JSON response, or null until the first fetch
// resolves. The cleanup function cancels the interval and ignores
// any in-flight response when the component unmounts or the
// dependencies change.
//
// Errors are not surfaced to the UI (the Header's live dot already
// catches the "no fresh data" case) but are logged to the console
// in dev so a backend crash is visible to the developer rather than
// silent.
export function usePolling(url, intervalMs) {
  const [data, setData] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function fetchOnce() {
      try {
        const res = await fetch(url);
        if (!res.ok) {
          throw new Error(`HTTP ${res.status} for ${url}`);
        }
        const json = await res.json();
        if (!cancelled) setData(json);
      } catch (err) {
        if (!cancelled) {
          console.warn(`usePolling: ${err.message ?? err}`);
        }
      }
    }

    fetchOnce();
    const id = setInterval(fetchOnce, intervalMs);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [url, intervalMs]);

  return data;
}
