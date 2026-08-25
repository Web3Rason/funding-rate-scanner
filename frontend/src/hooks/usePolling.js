import { useState, useEffect, useCallback, useRef } from 'react';

export function usePolling(url, interval = 2000) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);
  const intervalRef = useRef(null);

  const fetchData = useCallback(async () => {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [url]);

  useEffect(() => {
    fetchData();
    intervalRef.current = setInterval(fetchData, interval);
    return () => clearInterval(intervalRef.current);
  }, [fetchData, interval]);

  const patchData = useCallback((updater) => {
    setData(prev => typeof updater === 'function' ? updater(prev) : updater);
  }, []);

  return { data, error, loading, refetch: fetchData, patchData };
}
