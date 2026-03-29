'use client';

import { useEffect, useState, useMemo } from 'react';

interface BoxRow {
  playerId: number;
  playerName: string;
  teamId: number;
  period: string;
  pts: number | null;
  reb: number | null;
  ast: number | null;
  stl: number | null;
  blk: number | null;
  tov: number | null;
  min: number | null;
  fg3m: number | null;
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
}

interface PlayerTotals {
  playerId: number;
  playerName: string;
  teamId: number;
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

const PERIODS = ['All', '1Q', '2Q', '3Q', '4Q', 'OT'] as const;
type Period = typeof PERIODS[number];

function sum(rows: BoxRow[], key: keyof BoxRow): number {
  return rows.reduce((acc, r) => acc + ((r[key] as number) ?? 0), 0);
}

function fmtMin(min: number): string {
  if (min === 0) return '0:00';
  const m = Math.floor(min);
  const s = Math.round((min - m) * 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

function fmtShooting(made: number, att: number): string {
  return att === 0 ? '-' : `${made}/${att}`;
}

function TeamBox({ teamId, players }: { teamId: number; players: PlayerTotals[] }) {
  const label = players[0]?.playerName ? `Team ${teamId}` : `Team ${teamId}`;
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-xs text-gray-500 border-b border-gray-800">
            <th className="text-left py-1.5 pr-3 font-medium">Player</th>
            <th className="text-right py-1.5 px-2 font-medium">MIN</th>
            <th className="text-right py-1.5 px-2 font-medium">PTS</th>
            <th className="text-right py-1.5 px-2 font-medium">REB</th>
            <th className="text-right py-1.5 px-2 font-medium">AST</th>
            <th className="text-right py-1.5 px-2 font-medium">STL</th>
            <th className="text-right py-1.5 px-2 font-medium">BLK</th>
            <th className="text-right py-1.5 px-2 font-medium">TOV</th>
            <th className="text-right py-1.5 px-2 font-medium">FG</th>
            <th className="text-right py-1.5 px-2 font-medium">3P</th>
            <th className="text-right py-1.5 pl-2 font-medium">FT</th>
          </tr>
        </thead>
        <tbody>
          {players.map((p) => (
            <tr key={p.playerId} className="border-b border-gray-800">
              <td className="py-1.5 pr-3 text-gray-100">{p.playerName}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmtMin(p.min)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{p.pts}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{p.reb}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{p.ast}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{p.stl}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{p.blk}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{p.tov}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmtShooting(p.fgm, p.fga)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmtShooting(p.fg3m, p.fga)}</td>
              <td className="py-1.5 pl-2 text-right text-gray-300">{fmtShooting(p.ftm, p.fta)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function BoxScoreTable({ gameId }: { gameId: string }) {
  const [rows, setRows] = useState<BoxRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [period, setPeriod] = useState<Period>('All');

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/boxscore?gameId=${gameId}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => setRows(data.rows ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  const totals = useMemo<PlayerTotals[]>(() => {
    const filtered = period === 'All' ? rows : rows.filter((r) => r.period === period);
    const byPlayer = new Map<number, BoxRow[]>();
    filtered.forEach((r) => {
      if (!byPlayer.has(r.playerId)) byPlayer.set(r.playerId, []);
      byPlayer.get(r.playerId)!.push(r);
    });
    return Array.from(byPlayer.values()).map((playerRows) => ({
      playerId: playerRows[0].playerId,
      playerName: playerRows[0].playerName,
      teamId: playerRows[0].teamId,
      pts: sum(playerRows, 'pts'),
      reb: sum(playerRows, 'reb'),
      ast: sum(playerRows, 'ast'),
      stl: sum(playerRows, 'stl'),
      blk: sum(playerRows, 'blk'),
      tov: sum(playerRows, 'tov'),
      min: sum(playerRows, 'min'),
      fg3m: sum(playerRows, 'fg3m'),
      fgm: sum(playerRows, 'fgm'),
      fga: sum(playerRows, 'fga'),
      ftm: sum(playerRows, 'ftm'),
      fta: sum(playerRows, 'fta'),
    }));
  }, [rows, period]);

  const teams = useMemo(() => Array.from(new Set(totals.map((p) => p.teamId))), [totals]);

  if (loading) return <div className="text-sm text-gray-500 py-4">Loading box score...</div>;
  if (error) return <div className="text-sm text-red-400 py-4">Error: {error}</div>;
  if (rows.length === 0) return <div className="text-sm text-gray-500 py-4">No box score data available.</div>;

  const availablePeriods = PERIODS.filter(
    (p) => p === 'All' || rows.some((r) => r.period === p)
  );

  return (
    <div>
      <div className="flex gap-1 mb-4">
        {availablePeriods.map((p) => (
          <button
            key={p}
            onClick={() => setPeriod(p)}
            className={[
              'px-3 py-1 text-xs font-medium rounded transition-colors',
              period === p
                ? 'bg-blue-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
            ].join(' ')}
          >
            {p}
          </button>
        ))}
      </div>
      <div className="flex flex-col gap-6">
        {teams.map((teamId) => (
          <TeamBox
            key={teamId}
            teamId={teamId}
            players={totals.filter((p) => p.teamId === teamId)}
          />
        ))}
      </div>
    </div>
  );
}
