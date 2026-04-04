'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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
const POS_LABELS: Record<string, string> = { G: 'Guards', F: 'Forwards', C: 'Centers' };
const STATS: Array<{ key: keyof PosData; label: string; lowerIsBetter?: boolean }> = [
  { key: 'pts',  label: 'PTS' },
  { key: 'reb',  label: 'REB' },
  { key: 'ast',  label: 'AST' },
  { key: 'fg3m', label: '3PM' },
  { key: 'stl',  label: 'STL' },
  { key: 'blk',  label: 'BLK' },
  { key: 'tov',  label: 'TOV' },
];

// ---------------------------------------------------------------------------
// Rank coloring: high rank (bad defense = exploitable) = green
// Rank 1 = allows most = easiest to exploit
// ---------------------------------------------------------------------------

function rankBg(rank: number): string {
  // 1-10: green (soft defense), 11-20: neutral, 21-30: red (tough defense)
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

// ---------------------------------------------------------------------------
// Single team defense panel
// ---------------------------------------------------------------------------

function TeamDefensePanel({
  team,
  lineup,
  gameId,
  selectedDate,
  tab,
}: {
  team: TeamMatchup;
  lineup: Record<string, LineupPlayer[]>; // keyed by pos group
  gameId: string;
  selectedDate: string;
  tab: string;
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

      {/* Stat header row */}
      <div className="grid text-xs text-gray-500 font-medium mb-1 px-1"
           style={{ gridTemplateColumns: '60px repeat(7, 1fr)' }}>
        <div />
        {STATS.map((s) => (
          <div key={s.key} className="text-center">{s.label}</div>
        ))}
      </div>

      {POS_GROUPS.map((pos) => {
        const data     = team.positions[pos];
        const players  = lineup[pos] ?? [];
        const isExpanded = expandedPos === pos;

        return (
          <div key={pos} className="mb-1">
            {/* Position row */}
            <button
              onClick={() => togglePos(pos)}
              className={[
                'w-full grid items-center text-xs rounded py-1.5 px-1 transition-colors',
                'hover:bg-gray-800/60',
                isExpanded ? 'bg-gray-800/60' : '',
              ].join(' ')}
              style={{ gridTemplateColumns: '60px repeat(7, 1fr)' }}
            >
              {/* Position label */}
              <div className="text-left">
                <span className="text-gray-400 font-medium">{pos}</span>
                {players.length > 0 && (
                  <span className="text-gray-600 text-xs ml-1">
                    {isExpanded ? '▲' : '▼'}
                  </span>
                )}
              </div>

              {/* Stat cells */}
              {STATS.map((s) => {
                if (!data) {
                  return <div key={s.key} className="text-center text-gray-700">—</div>;
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

            {/* Expanded player list */}
            {isExpanded && players.length > 0 && (
              <div className="ml-1 mt-1 mb-2 space-y-0.5">
                {players.map((p) => {
                  const href = p.playerId
                    ? `/nba/player/${p.playerId}?gameId=${gameId}&tab=${tab}&date=${selectedDate}`
                    : null;
                  const isStarter  = p.starterStatus === 'Starter';
                  const isInactive = p.starterStatus === 'Inactive';

                  return (
                    <div
                      key={p.playerName}
                      className={`flex items-center gap-2 px-2 py-1 rounded text-xs ${
                        isInactive ? 'opacity-40' : 'hover:bg-gray-800/40'
                      }`}
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

      {/* Legend */}
      {Object.keys(team.positions).length > 0 && (
        <div className="mt-3 px-1 flex flex-wrap gap-x-3 gap-y-1 text-[10px] text-gray-600">
          <span className="text-green-500">Green = soft (exploitable)</span>
          <span className="text-red-500">Red = tough</span>
          <span>Rank = vs all 30 teams</span>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

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
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  if (loading) return <div className="py-6 text-sm text-gray-500">Loading matchups...</div>;
  if (error)   return <div className="py-6 text-sm text-red-400">Error: {error}</div>;
  if (!data)   return null;

  // For each team panel:
  // - home team defense faces the AWAY team's players (lineup keyed by awayTeamAbbr)
  // - away team defense faces the HOME team's players (lineup keyed by homeTeamAbbr)
  const homeLineupByPos = data.lineup[awayTeamAbbr] ?? {};
  const awayLineupByPos = data.lineup[homeTeamAbbr] ?? {};

  return (
    <div className="space-y-6">
      <div className="text-xs text-gray-600 px-1">
        Rows are position groups. Each cell shows the season average allowed and rank (1 = most allowed). Tap a row to see today's players.
      </div>

      {/* Two panels side by side on wide screens, stacked on narrow */}
      <div className="flex flex-col gap-8 md:flex-row md:gap-6">
        <TeamDefensePanel
          team={data.away}
          lineup={homeLineupByPos}
          gameId={gameId}
          selectedDate={selectedDate}
          tab="matchups"
        />
        <div className="hidden md:block w-px bg-gray-800 flex-none" />
        <TeamDefensePanel
          team={data.home}
          lineup={awayLineupByPos}
          gameId={gameId}
          selectedDate={selectedDate}
          tab="matchups"
        />
      </div>
    </div>
  );
}
