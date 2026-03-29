'use client';

import { useEffect, useState } from 'react';
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
  const [players, setPlayers] = useState<PlayerAvg[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/team-averages?homeTeamId=${homeTeamId}&awayTeamId=${awayTeamId}&context=20`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => setPlayers(data.players ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [homeTeamId, awayTeamId]);

  if (loading) return <div className="text-sm text-gray-500 py-4">Loading stats...</div>;
  if (error) return <div className="text-sm text-red-400 py-4">Error: {error}</div>;
  if (players.length === 0) return <div className="text-sm text-gray-500 py-4">No stats available.</div>;

  const homePlayers = players.filter((p) => p.teamAbbr === homeTeamAbbr);
  const awayPlayers = players.filter((p) => p.teamAbbr === awayTeamAbbr);
  // fallback: any players not matching either tricode go into their own group
  const otherAbbrs = Array.from(new Set(
    players
      .filter((p) => p.teamAbbr !== homeTeamAbbr && p.teamAbbr !== awayTeamAbbr)
      .map((p) => p.teamAbbr)
  ));

  return (
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
    </div>
  );
}
