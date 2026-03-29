'use client';

import { useEffect, useState } from 'react';

interface PlayerAvg {
  playerId: number | null;
  playerName: string;
  games: number;
  avgPts: number | null;
  avgReb: number | null;
  avgAst: number | null;
  avgStl: number | null;
  avgBlk: number | null;
  avgTov: number | null;
  avgMin: number | null;
  avg3pm: number | null;
  teamAbbr?: string;
}

interface RosterRow {
  playerName: string;
  teamAbbr: string;
}

interface Props {
  gameId: string;
}

function fmt(val: number | null | undefined, decimals = 1): string {
  if (val == null) return '-';
  return val.toFixed(decimals);
}

function TeamStatsTable({ abbr, players }: { abbr: string; players: PlayerAvg[] }) {
  return (
    <div className="overflow-x-auto">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{abbr}</div>
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
            <th className="text-right py-1.5 px-2 font-medium">3PM</th>
            <th className="text-right py-1.5 pl-2 font-medium">GP</th>
          </tr>
        </thead>
        <tbody>
          {players.map((p) => (
            <tr key={p.playerName} className="border-b border-gray-800">
              <td className="py-1.5 pr-3 text-gray-100">{p.playerName}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgMin)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgPts)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgReb)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgAst)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgStl)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgBlk)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgTov)}</td>
              <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avg3pm)}</td>
              <td className="py-1.5 pl-2 text-right text-gray-500">{p.games}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function StatsTable({ gameId }: Props) {
  const [players, setPlayers] = useState<PlayerAvg[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);

    Promise.all([
      fetch(`/api/player-averages?gameId=${gameId}&context=20`).then((r) => r.json()),
      fetch(`/api/roster?gameId=${gameId}&sport=nba`).then((r) => r.json()),
    ])
      .then(([avgData, rosterData]) => {
        const rosterMap = new Map<string, string>();
        (rosterData.roster ?? []).forEach((r: RosterRow) => {
          rosterMap.set(r.playerName, r.teamAbbr);
        });
        const list: PlayerAvg[] = (avgData.players ?? []).map((p: PlayerAvg) => ({
          ...p,
          teamAbbr: rosterMap.get(p.playerName) ?? 'UNK',
        }));
        setPlayers(list);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  if (loading) return <div className="text-sm text-gray-500 py-4">Loading stats...</div>;
  if (error) return <div className="text-sm text-red-400 py-4">Error: {error}</div>;
  if (players.length === 0) return <div className="text-sm text-gray-500 py-4">No stats available.</div>;

  const teams = Array.from(new Set(players.map((p) => p.teamAbbr ?? 'UNK')));

  return (
    <div className="flex flex-col gap-6">
      {teams.map((abbr) => (
        <TeamStatsTable
          key={abbr}
          abbr={abbr}
          players={players.filter((p) => p.teamAbbr === abbr)}
        />
      ))}
    </div>
  );
}
