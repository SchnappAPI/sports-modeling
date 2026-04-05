'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';

interface StatLine {
  avg: number;
  rank: number;
  gamesDefended: number;
}

interface PosData {
  pts:  StatLine;
  reb:  StatLine;
  ast:  StatLine;
  fg3m: StatLine;
  stl:  StatLine;
  blk:  StatLine;
  tov:  StatLine;
  gamesDefended: number;
}

interface TeamMatchup {
  teamId: number;
  teamAbbr: string;
  positions: Partial<Record<'G' | 'F' | 'C', PosData>>;
}

interface LineupPlayer {
  playerId: number | null;
  playerName: string;
  position: string | null;
  starterStatus: string | null;
  lineupStatus: string | null;
}

interface MatchupGridData {
  home: TeamMatchup;
  away: TeamMatchup;
  lineup: Record<string, Record<string, LineupPlayer[]>>;
  gameId: string;
}

const POS_GROUPS: Array<'G' | 'F' | 'C'> = ['G', 'F', 'C'];

const STATS: Array<{ key: keyof PosData; label: string }> = [
  { key: 'pts',  label: 'PTS' },
  { key: 'reb',  label: 'REB' },
  { key: 'ast',  label: 'AST' },
  { key: 'fg3m', label: '3PM' },
  { key: 'stl',  label: 'STL' },
  { key: 'blk',  label: 'BLK' },
];

function rankBg(rank: number): string {
  if (rank <= 5)  return 'bg-green-900/50 text-green-300';
  if (rank <= 10) return 'bg-green-900/30 text-green-400';
  if (rank <= 20) return 'text-gray-400';
  if (rank <= 25) return 'bg-red-900/20 text-red-400';
  return 'bg-red-900/40 text-red-300';
}

function rankLabel(rank: number): string {
  if (rank === 1) return '1st';
  if (rank === 2) return '2nd';
  if (rank === 3) return '3rd';
  return `${rank}th`;
}

function TeamDefensePanel({
  team,
  lineup,
  gameId,
  selectedDate,
}: {
  team: TeamMatchup;
  lineup: Record<string, LineupPlayer[]>;
  gameId: string;
  selectedDate: string;
}) {
  const [expandedPos, setExpandedPos] = useState<string | null>(null);

  function togglePos(pos: string) {
    setExpandedPos((prev) => (prev === pos ? null : pos));
  }

  return (
    <div className="flex-1 min-w-0">
      <div className="text-sm font-semibold text-gray-300 mb-3 text-center">
        vs {team.teamAbbr} Defense
      </div>

      <div
        className="grid text-xs text-gray-500 font-medium mb-1 px-1"
        style={{ gridTemplateColumns: '60px repeat(6, 1fr)' }}
      >
        <div />
        {STATS.map((s) => (
          <div key={s.key} className="text-center">{s.label}</div>
        ))}
      </div>

      {POS_GROUPS.map((pos) => {
        const data      = team.positions[pos];
        const players   = lineup[pos] ?? [];
        const isExpanded = expandedPos === pos;

        return (
          <div key={pos} className="mb-1">
            <button
              onClick={() => togglePos(pos)}
              className={[
                'w-full grid items-center text-xs rounded py-1.5 px-1 transition-colors hover:bg-gray-800/60',
                isExpanded ? 'bg-gray-800/60' : '',
              ].join(' ')}
              style={{ gridTemplateColumns: '60px repeat(6, 1fr)' }}
            >
              <div className="text-left">
                <span className="text-gray-400 font-medium">{pos}</span>
                {players.length > 0 && (
                  <span className="text-gray-600 text-xs ml-1">
                    {isExpanded ? '\u25b2' : '\u25bc'}
                  </span>
                )}
              </div>

              {STATS.map((s) => {
                if (!data) {
                  return <div key={s.key} className="text-center text-gray-700">&mdash;</div>;
                }
                const sl = data[s.key] as StatLine;
                return (
                  <div key={s.key} className={`text-center rounded py-0.5 ${rankBg(sl.rank)}`}>
                    <div className="font-semibold tabular-nums leading-none">{sl.avg.toFixed(1)}</div>
                    <div className="text-gray-500 leading-none text-[10px]">{rankLabel(sl.rank)}</div>
                  </div>
                );
              })}
            </button>

            {isExpanded && players.length > 0 && (
              <div className="ml-1 mt-1 mb-2 space-y-0.5">
                {players.map((p) => {
                  const href = p.playerId
                    ? `/nba/player/${p.playerId}?gameId=${gameId}&tab=matchups&date=${selectedDate}`
                    : null;
                  const isStarter = p.starterStatus === 'Starter';

                  return (
                    <div
                      key={p.playerName}
                      className="flex items-center gap-2 px-2 py-1 rounded text-xs hover:bg-gray-800/40"
                    >
                      {href ? (
                        <Link
                          href={href}
                          className={`flex-1 transition-colors hover:text-blue-400 ${
                            isStarter ? 'text-gray-200 font-medium' : 'text-gray-400'
                          }`}
                        >
                          {p.playerName}
                        </Link>
                      ) : (
                        <span className="flex-1 text-gray-500">{p.playerName}</span>
                      )}
                      {p.position && (
                        <span className="text-gray-600 text-[10px]">{p.position}</span>
                      )}
                      {isStarter && (
                        <span className="text-blue-700 text-[10px] font-semibold">S</span>
                      )}
                    </div>
                  );
                })}
              </div>
            )}

            {isExpanded && players.length === 0 && (
              <div className="ml-4 mt-1 mb-2 text-xs text-gray-600">No lineup data available.</div>
            )}
          </div>
        );
      })}

      {Object.keys(team.positions).length > 0 && (
        <div className="mt-3 px-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-gray-600">
          <span className="text-green-500">Green = soft (exploitable)</span>
          <span className="text-red-500">Red = tough</span>
          <span>Rank out of 30</span>
        </div>
      )}
    </div>
  );
}

export default function MatchupGrid({
  gameId,
  homeTeamAbbr,
  awayTeamAbbr,
  selectedDate,
}: {
  gameId: string;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
  selectedDate: string;
}) {
  const [data, setData]       = useState<MatchupGridData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/matchup-grid?gameId=${gameId}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: MatchupGridData) => setData(d))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  if (loading) return <div className="py-6 text-sm text-gray-500">Loading matchups...</div>;
  if (error)   return <div className="py-6 text-sm text-red-400">Error: {error}</div>;
  if (!data)   return null;

  const playersVsHomeDefense = data.lineup[awayTeamAbbr] ?? {};
  const playersVsAwayDefense = data.lineup[homeTeamAbbr] ?? {};

  return (
    <div className="space-y-6">
      <div className="text-xs text-gray-600 px-1">
        Each row is a position group. Green = soft defense (exploitable), red = tough. Tap a row to see today&apos;s players.
      </div>

      <div className="flex flex-col gap-8 md:flex-row md:gap-6">
        <TeamDefensePanel
          team={data.away}
          lineup={playersVsAwayDefense}
          gameId={gameId}
          selectedDate={selectedDate}
        />
        <div className="hidden md:block w-px bg-gray-800 flex-none" />
        <TeamDefensePanel
          team={data.home}
          lineup={playersVsHomeDefense}
          gameId={gameId}
          selectedDate={selectedDate}
        />
      </div>
    </div>
  );
}
