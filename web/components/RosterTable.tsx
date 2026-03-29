'use client';

import { useEffect, useState } from 'react';

interface RosterRow {
  playerId: number | null;
  playerName: string;
  teamAbbr: string;
  position: string | null;
  isStarter: boolean;
  lineupStatus: string | null;  // 'Confirmed' | 'Projected' | null
}

interface Props {
  gameId: string;
}

export default function RosterTable({ gameId }: Props) {
  const [roster, setRoster] = useState<RosterRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/roster?gameId=${gameId}&sport=nba`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => setRoster(data.roster ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  if (loading) return <div className="text-sm text-gray-500 py-4">Loading roster...</div>;
  if (error)   return <div className="text-sm text-red-400 py-4">Error: {error}</div>;
  if (roster.length === 0) return <div className="text-sm text-gray-500 py-4">No lineup data available yet.</div>;

  const teams = Array.from(new Set(roster.map((r) => r.teamAbbr)));

  // Is this team's lineup projected or confirmed?
  // A team is projected if any of its players has lineupStatus = 'Projected'.
  // Once confirmed data arrives the lineup_poll replaces projected rows.
  function teamIsProjected(abbr: string): boolean {
    return roster
      .filter((r) => r.teamAbbr === abbr)
      .some((r) => r.lineupStatus === 'Projected');
  }

  return (
    <div className="grid grid-cols-2 gap-4">
      {teams.map((abbr) => {
        const players   = roster.filter((r) => r.teamAbbr === abbr);
        const starters  = players.filter((p) => p.isStarter);
        const bench     = players.filter((p) => !p.isStarter);
        const projected = teamIsProjected(abbr);

        return (
          <div key={abbr}>
            {/* Team header with confirmation status */}
            <div className="flex items-center gap-2 mb-2">
              <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
                {abbr}
              </span>
              {projected ? (
                <span className="text-xs text-yellow-600 border border-yellow-800 rounded px-1 py-0.5 leading-none">
                  Projected
                </span>
              ) : (
                <span className="text-xs text-green-700 border border-green-900 rounded px-1 py-0.5 leading-none">
                  Confirmed
                </span>
              )}
            </div>

            <table className="w-full text-sm">
              <tbody>
                {starters.map((p) => (
                  <tr key={p.playerName} className="border-b border-gray-800">
                    <td className="py-1.5 pr-2 text-gray-100">{p.playerName}</td>
                    <td className="py-1.5 pr-2 text-gray-500 text-xs">{p.position ?? ''}</td>
                    <td className="py-1.5 text-right">
                      <span className="text-xs bg-blue-900 text-blue-300 px-1 rounded">S</span>
                    </td>
                  </tr>
                ))}
                {bench.map((p) => (
                  <tr key={p.playerName} className="border-b border-gray-800">
                    <td className="py-1.5 pr-2 text-gray-300">{p.playerName}</td>
                    <td className="py-1.5 pr-2 text-gray-500 text-xs">{p.position ?? ''}</td>
                    <td className="py-1.5"></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}
