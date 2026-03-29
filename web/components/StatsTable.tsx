'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

interface PlayerAvg {
  playerId: number;
  playerName: string;
  teamId: number;
  teamAbbr: string;
  games: number;
  avgPts: number | null;
  avgReb: number | null;
  avgAst: number | null;
  avgStl: number | null;
  avgBlk: number | null;
  avgTov: number | null;
  avgMin: number | null;
  avg3pm: number | null;
}

interface Props {
  gameId: string;
  homeTeamId: number;
  awayTeamId: number;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
}

function fmt(val: number | null | undefined, decimals = 1): string {
  if (val == null) return '-';
  return val.toFixed(decimals);
}

const PERIOD_OPTIONS = [
  { label: 'Full', value: 'full' },
  { label: '1Q',   value: '1Q' },
  { label: '2Q',   value: '2Q' },
  { label: '3Q',   value: '3Q' },
  { label: '4Q',   value: '4Q' },
  { label: 'OT',   value: 'OT' },
] as const;

const N_OPTIONS = [
  { label: 'L10',  value: '10' },
  { label: 'L20',  value: '20' },
  { label: 'L40',  value: '40' },
  { label: 'All',  value: 'all' },
];

function TeamStatsTable({
  abbr,
  opponentAbbr,
  players,
  gameId,
}: {
  abbr: string;
  opponentAbbr: string;
  players: PlayerAvg[];
  gameId: string;
}) {
  const searchParams = useSearchParams();
  const tab = searchParams.get('tab') ?? 'stats';

  return (
    <div className="overflow-x-auto">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{abbr}</div>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-xs text-gray-500 border-b border-gray-800">
            <th className="text-left py-1.5 pr-3 font-medium">Player</th>
            <th className="text-right py-1.5 px-2 font-medium">GP</th>
            <th className="text-right py-1.5 px-2 font-medium">MIN</th>
            <th className="text-right py-1.5 px-2 font-medium">PTS</th>
            <th className="text-right py-1.5 px-2 font-medium">REB</th>
            <th className="text-right py-1.5 px-2 font-medium">AST</th>
            <th className="text-right py-1.5 px-2 font-medium">STL</th>
            <th className="text-right py-1.5 px-2 font-medium">BLK</th>
            <th className="text-right py-1.5 px-2 font-medium">TOV</th>
            <th className="text-right py-1.5 pl-2 font-medium">3PM</th>
          </tr>
        </thead>
        <tbody>
          {players.map((p) => (
            <tr key={p.playerId} className="border-b border-gray-800">
              <td className="py-1.5 pr-3">
                <Link
                  href={`/nba/player/${p.playerId}?gameId=${gameId}&tab=${tab}&opp=${opponentAbbr}`}
                  className="text-gray-100 hover:text-blue-400 transition-colors"
                >
                  {p.playerName}
                </Link>
              </td>
              <td className="py-1.5 px-2 text-right text-gray-500 text-xs">{p.games}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgMin)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgPts)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgReb)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgAst)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgStl)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgBlk)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgTov)}</td>
              <td className="py-1.5 pl-2 text-right text-gray-300">{fmt(p.avg3pm)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function StatsTable({ gameId, homeTeamId, awayTeamId, homeTeamAbbr, awayTeamAbbr }: Props) {
  // Period checkboxes. 'full' means no period filter (sum all quarters).
  const [activePeriods, setActivePeriods] = useState<Set<string>>(new Set(['full']));
  const [nGames, setNGames] = useState('20');

  const [players, setPlayers] = useState<PlayerAvg[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  function togglePeriod(value: string) {
    setActivePeriods((prev) => {
      const next = new Set(prev);
      if (value === 'full') {
        // Selecting Full clears all quarter selections.
        return new Set(['full']);
      }
      // Toggling a quarter clears Full.
      next.delete('full');
      if (next.has(value)) {
        next.delete(value);
        // If nothing left, fall back to Full.
        if (next.size === 0) return new Set(['full']);
      } else {
        next.add(value);
      }
      return next;
    });
  }

  const fetchStats = useCallback(() => {
    setLoading(true);
    setError(null);

    const periodsParam = activePeriods.has('full')
      ? ''
      : Array.from(activePeriods).join(',');

    const url = new URL('/api/team-averages', window.location.origin);
    url.searchParams.set('homeTeamId', String(homeTeamId));
    url.searchParams.set('awayTeamId', String(awayTeamId));
    url.searchParams.set('context', nGames);
    if (periodsParam) url.searchParams.set('periods', periodsParam);

    fetch(url.toString())
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => setPlayers(data.players ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [homeTeamId, awayTeamId, nGames, activePeriods]);

  useEffect(() => {
    fetchStats();
  }, [fetchStats]);

  const homePlayers  = players.filter((p) => p.teamAbbr === homeTeamAbbr);
  const awayPlayers  = players.filter((p) => p.teamAbbr === awayTeamAbbr);
  const otherAbbrs   = Array.from(new Set(
    players
      .filter((p) => p.teamAbbr !== homeTeamAbbr && p.teamAbbr !== awayTeamAbbr)
      .map((p) => p.teamAbbr)
  ));

  return (
    <div className="flex flex-col gap-4">
      {/* Controls */}
      <div className="flex flex-wrap items-center gap-4">
        {/* Period toggles */}
        <div className="flex items-center gap-1">
          {PERIOD_OPTIONS.map(({ label, value }) => {
            const active = activePeriods.has(value);
            return (
              <button
                key={value}
                onClick={() => togglePeriod(value)}
                className={[
                  'px-2.5 py-1 text-xs font-medium rounded transition-colors',
                  active
                    ? 'bg-blue-600 text-white'
                    : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
                ].join(' ')}
              >
                {label}
              </button>
            );
          })}
        </div>

        {/* N-game selector */}
        <div className="flex items-center gap-1">
          {N_OPTIONS.map(({ label, value }) => (
            <button
              key={value}
              onClick={() => setNGames(value)}
              className={[
                'px-2.5 py-1 text-xs font-medium rounded transition-colors',
                nGames === value
                  ? 'bg-gray-600 text-white'
                  : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
              ].join(' ')}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      {loading && <div className="text-sm text-gray-500 py-2">Loading stats...</div>}
      {error   && <div className="text-sm text-red-400 py-2">Error: {error}</div>}

      {!loading && !error && (
        <div className="flex flex-col gap-6">
          {awayPlayers.length > 0 && (
            <TeamStatsTable
              abbr={awayTeamAbbr}
              opponentAbbr={homeTeamAbbr}
              players={awayPlayers}
              gameId={gameId}
            />
          )}
          {homePlayers.length > 0 && (
            <TeamStatsTable
              abbr={homeTeamAbbr}
              opponentAbbr={awayTeamAbbr}
              players={homePlayers}
              gameId={gameId}
            />
          )}
          {otherAbbrs.map((abbr) => (
            <TeamStatsTable
              key={abbr}
              abbr={abbr}
              opponentAbbr=''
              players={players.filter((p) => p.teamAbbr === abbr)}
              gameId={gameId}
            />
          ))}
          {players.length === 0 && (
            <div className="text-sm text-gray-500">No stats available.</div>
          )}
        </div>
      )}
    </div>
  );
}
