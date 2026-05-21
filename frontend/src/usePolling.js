import { useEffect, useState } from "react";

// Fetch `url` once on mount and then every `intervalMs`. Returns the
// most recent parsed JSON response, or null until the first fetch
// resolves. The cleanup function cancels the interval and ignores
// any in-flight response when the component unmounts or the
// dependencies change.
export function usePolling(url, intervalMs) {
  const [data, setData] = useState(null);

  useEffect(() => {
    let cancelled = false;

    async function fetchOnce() {
      try {
        const res = await fetch(url);
        const json = await res.json();
        if (!cancelled) setData(json);
      } catch {
        // Backend may be down briefly; keep the previous data.
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
