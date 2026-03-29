'use client';

import { useEffect, useState } from 'react';

interface RosterRow {
  playerId: number | null;
  playerName: string;
  teamAbbr: string;
  position: string | null;
  isStarter: boolean;
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
  if (error) return <div className="text-sm text-red-400 py-4">Error: {error}</div>;
  if (roster.length === 0) return <div className="text-sm text-gray-500 py-4">No lineup data available.</div>;

  const teams = Array.from(new Set(roster.map((r) => r.teamAbbr)));

  return (
    <div className="grid grid-cols-2 gap-4">
      {teams.map((abbr) => {
        const players = roster.filter((r) => r.teamAbbr === abbr);
        const starters = players.filter((p) => p.isStarter);
        const bench = players.filter((p) => !p.isStarter);
        return (
          <div key={abbr}>
            <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{abbr}</div>
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
