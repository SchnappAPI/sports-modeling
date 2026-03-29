'use client';

import { useEffect, useRef, useState } from 'react';
import BoxScoreTable from './BoxScoreTable';

const POLL_INTERVAL_MS = 60_000; // 60 seconds

interface Props {
  gameId: string;
  selectedDate: string;
}

export default function LiveBoxScore({ gameId, selectedDate }: Props) {
  const [tick, setTick]         = useState(0);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Start polling when this tab is mounted, stop when unmounted.
  useEffect(() => {
    intervalRef.current = setInterval(() => {
      setTick((t) => t + 1);
      setLastRefresh(new Date());
    }, POLL_INTERVAL_MS);

    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [gameId]);

  // Format last-refresh time as HH:MM:SS
  const refreshStr = lastRefresh.toLocaleTimeString([], {
    hour:   '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });

  return (
    <div>
      {/* Live indicator bar */}
      <div className="flex items-center gap-2 mb-3">
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
        </span>
        <span className="text-xs font-semibold text-red-400 uppercase tracking-wider">Live</span>
        <span className="text-xs text-gray-600 ml-1">Updated {refreshStr} · refreshes every 60s</span>
      </div>

      {/*
        BoxScoreTable is keyed on `tick` so it remounts and re-fetches
        on every poll interval. The key prop change triggers a clean
        unmount + remount, which re-runs the useEffect inside BoxScoreTable
        that calls /api/boxscore. No changes needed to BoxScoreTable itself.
      */}
      <BoxScoreTable
        key={`${gameId}-${tick}`}
        gameId={gameId}
        selectedDate={selectedDate}
      />
    </div>
  );
}
