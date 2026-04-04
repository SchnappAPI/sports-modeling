'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';

interface RosterRow {
  playerId: number | null;
  playerName: string;
  teamAbbr: string;
  position: string | null;
  // 'Starter' | 'Bench' | 'Inactive'
  starterStatus: string | null;
  lineupStatus: string | null;  // 'Confirmed' | 'Projected' | null
}

interface Props {
  gameId: string;
  selectedDate: string;
}

export default function RosterTable({ gameId, selectedDate }: Props) {
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

  // Badge logic:
  // - Confirmed (green): at least one player has lineupStatus === 'Confirmed'
  // - Projected (yellow): at least one player has lineupStatus === 'Projected' and none are Confirmed
  // - Expected (gray): all lineupStatus values are null — lineup not yet available
  function teamBadge(abbr: string): { label: string; cls: string } {
    const players = roster.filter((r) => r.teamAbbr === abbr);
    const hasConfirmed = players.some((p) => p.lineupStatus === 'Confirmed');
    const hasProjected = players.some((p) => p.lineupStatus === 'Projected');
    if (hasConfirmed) return { label: 'Confirmed', cls: 'text-green-700 border-green-900' };
    if (hasProjected) return { label: 'Projected', cls: 'text-yellow-600 border-yellow-800' };
    return { label: 'Expected', cls: 'text-gray-600 border-gray-700' };
  }

  function playerHref(playerId: number): string {
    const params = new URLSearchParams();
    params.set('gameId', gameId);
    params.set('tab', 'roster');
    if (selectedDate) params.set('date', selectedDate);
    return `/nba/player/${playerId}?${params.toString()}`;
  }

  const renderPlayer = (p: RosterRow, isStarter: boolean, dimmed = false) => (
    <tr key={p.playerName} className={['border-b border-gray-800', dimmed ? 'opacity-40' : ''].join(' ')}>
      <td className="py-1.5 pr-2">
        {p.playerId != null ? (
          <Link
            href={playerHref(p.playerId)}
            className={[
              'transition-colors hover:text-blue-400',
              dimmed ? 'text-gray-500' : isStarter ? 'text-gray-100' : 'text-gray-300',
            ].join(' ')}
          >
            {p.playerName}
          </Link>
        ) : (
          <span className={dimmed ? 'text-gray-500' : isStarter ? 'text-gray-100' : 'text-gray-300'}>
            {p.playerName}
          </span>
        )}
      </td>
      <td className="py-1.5 pr-2 text-gray-500 text-xs">{p.position ?? ''}</td>
      <td className="py-1.5 text-right">
        {isStarter && (
          <span className="text-xs bg-blue-900 text-blue-300 px-1 rounded">S</span>
        )}
      </td>
    </tr>
  );

  const sectionHeader = (label: string) => (
    <tr>
      <td colSpan={3} className="pt-3 pb-0.5 text-xs text-gray-600 font-semibold uppercase tracking-wider">
        {label}
      </td>
    </tr>
  );

  return (
    <div className="grid grid-cols-2 gap-4">
      {teams.map((abbr) => {
        const players  = roster.filter((r) => r.teamAbbr === abbr);
        const starters = players.filter((p) => p.starterStatus === 'Starter');
        const bench    = players.filter((p) => p.starterStatus === 'Bench');
        const inactive = players.filter((p) => p.starterStatus === 'Inactive');
        const badge    = teamBadge(abbr);

        return (
          <div key={abbr}>
            <div className="flex items-center gap-2 mb-2">
              <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
                {abbr}
              </span>
              <span className={`text-xs border rounded px-1 py-0.5 leading-none ${badge.cls}`}>
                {badge.label}
              </span>
            </div>

            <table className="w-full text-sm">
              <tbody>
                {starters.length > 0 && (
                  <>
                    {sectionHeader('Starters')}
                    {starters.map((p) => renderPlayer(p, true))}
                  </>
                )}
                {bench.length > 0 && (
                  <>
                    {sectionHeader('Bench')}
                    {bench.map((p) => renderPlayer(p, false))}
                  </>
                )}
                {inactive.length > 0 && (
                  <>
                    {sectionHeader('Out / Inactive')}
                    {inactive.map((p) => renderPlayer(p, false, true))}
                  </>
                )}
              </tbody>
            </table>
          </div>
        );
      })}
    </div>
  );
}
