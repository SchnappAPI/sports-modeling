'use client';

import { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

const POLL_INTERVAL_MS = 30_000; // 30 seconds

interface LivePlayer {
  playerId: number;
  playerName: string;
  teamId: number;
  teamAbbr: string;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  min: number;
  fg3m: number;
  fgm: number;
  fga: number;
  ftm: number;
  fta: number;
}

interface LiveData {
  gameId: string;
  gameStatusText: string;
  players: LivePlayer[];
}

function fmtMin(min: number): string {
  if (min === 0) return '-';
  const m = Math.floor(min);
  const s = Math.round((min - m) * 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

function fmtShoot(made: number, att: number): string {
  return att === 0 ? '-' : `${made}/${att}`;
}

function TeamTable({
  players,
  teamAbbr,
  gameId,
  selectedDate,
}: {
  players: LivePlayer[];
  teamAbbr: string;
  gameId: string;
  selectedDate: string;
}) {
  const searchParams = useSearchParams();
  // Sort by minutes descending (most played first)
  const sorted = [...players].sort((a, b) => b.min - a.min);

  return (
    <div className="overflow-x-auto">
      <div className="text-xs text-gray-600 font-semibold uppercase tracking-wider mb-1">{teamAbbr}</div>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-xs text-gray-500 border-b border-gray-800">
            <th className="text-left py-1 pr-3 font-medium">Player</th>
            <th className="text-right py-1 px-2 font-medium">MIN</th>
            <th className="text-right py-1 px-2 font-medium">PTS</th>
            <th className="text-right py-1 px-2 font-medium">REB</th>
            <th className="text-right py-1 px-2 font-medium">AST</th>
            <th className="text-right py-1 px-2 font-medium">STL</th>
            <th className="text-right py-1 px-2 font-medium">BLK</th>
            <th className="text-right py-1 px-2 font-medium">TOV</th>
            <th className="text-right py-1 px-2 font-medium">FG</th>
            <th className="text-right py-1 px-2 font-medium">3P</th>
            <th className="text-right py-1 pl-2 font-medium">FT</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((p) => {
            const params = new URLSearchParams(searchParams.toString());
            params.set('tab', 'boxscore');
            const href = `/nba/player/${p.playerId}?gameId=${gameId}&tab=boxscore&date=${selectedDate}`;
            return (
              <tr key={p.playerId} className={`border-b border-gray-800 ${p.min === 0 ? 'opacity-40' : ''}`}>
                <td className="py-1 pr-3">
                  <Link href={href} className="text-gray-100 hover:text-blue-400 transition-colors">
                    {p.playerName}
                  </Link>
                </td>
                <td className="py-1 px-2 text-right text-gray-300">{fmtMin(p.min)}</td>
                <td className="py-1 px-2 text-right text-gray-100 font-medium">{p.pts}</td>
                <td className="py-1 px-2 text-right text-gray-300">{p.reb}</td>
                <td className="py-1 px-2 text-right text-gray-300">{p.ast}</td>
                <td className="py-1 px-2 text-right text-gray-300">{p.stl}</td>
                <td className="py-1 px-2 text-right text-gray-300">{p.blk}</td>
                <td className="py-1 px-2 text-right text-gray-300">{p.tov}</td>
                <td className="py-1 px-2 text-right text-gray-300">{fmtShoot(p.fgm, p.fga)}</td>
                <td className="py-1 px-2 text-right text-gray-300">{fmtShoot(p.fg3m, p.fga)}</td>
                <td className="py-1 pl-2 text-right text-gray-300">{fmtShoot(p.ftm, p.fta)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function LiveBoxScore({
  gameId,
  selectedDate,
}: {
  gameId: string;
  selectedDate: string;
}) {
  const [data, setData]           = useState<LiveData | null>(null);
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState<string | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  async function fetchLive() {
    try {
      const r = await fetch(`/api/live-boxscore?gameId=${gameId}`, { cache: 'no-store' });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const json = await r.json();
      setData(json);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
      setLastRefresh(new Date());
    }
  }

  useEffect(() => {
    fetchLive();
    intervalRef.current = setInterval(fetchLive, POLL_INTERVAL_MS);
    return () => { if (intervalRef.current) clearInterval(intervalRef.current); };
  }, [gameId]);

  const refreshStr = lastRefresh.toLocaleTimeString([], {
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });

  // Group players by team
  const teams = data
    ? Array.from(new Map(data.players.map((p) => [p.teamAbbr, p.teamAbbr])).keys()).map((abbr) => ({
        abbr,
        players: data.players.filter((p) => p.teamAbbr === abbr),
      }))
    : [];

  return (
    <div>
      <div className="flex items-center gap-2 mb-3">
        <span className="relative flex h-2 w-2">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
          <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
        </span>
        <span className="text-xs font-semibold text-red-400 uppercase tracking-wider">Live</span>
        {data?.gameStatusText && (
          <span className="text-xs text-gray-400">{data.gameStatusText}</span>
        )}
        <span className="text-xs text-gray-600 ml-1">Updated {refreshStr} · refreshes every 30s</span>
      </div>

      {loading && <div className="text-sm text-gray-500">Loading...</div>}
      {error && <div className="text-sm text-red-400">Error: {error}</div>}

      {!loading && !error && teams.length > 0 && (
        <div className="flex flex-col gap-6">
          {teams.map((t) => (
            <TeamTable
              key={t.abbr}
              players={t.players}
              teamAbbr={t.abbr}
              gameId={gameId}
              selectedDate={selectedDate}
            />
          ))}
        </div>
      )}
    </div>
  );
}
