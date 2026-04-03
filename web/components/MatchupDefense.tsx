'use client';

import { useEffect, useState } from 'react';

interface StatLine {
  avg: number;
  rank: number;
  gamesDefended: number;
}

interface MatchupData {
  oppTeamId: number;
  oppTeamAbbr: string;
  position: string;
  gamesDefended: number;
  pts: StatLine;
  reb: StatLine;
  ast: StatLine;
  stl: StatLine;
  blk: StatLine;
  fg3m: StatLine;
  tov: StatLine;
}

// Maps the grading market key to the stat field in MatchupData.
// Used to highlight the most relevant stat for the player's active prop.
export const MARKET_TO_STAT: Record<string, keyof MatchupData> = {
  player_points:             'pts',
  player_points_alternate:   'pts',
  player_rebounds:           'reb',
  player_rebounds_alternate: 'reb',
  player_assists:            'ast',
  player_assists_alternate:  'ast',
  player_steals:             'stl',
  player_steals_alternate:   'stl',
  player_blocks:             'blk',
  player_blocks_alternate:   'blk',
  player_threes:             'fg3m',
  player_threes_alternate:   'fg3m',
  player_turnovers:          'tov',
};

// Order matches game log column order: PTS, 3PT, REB, AST, STL, BLK, TOV
const STAT_LABELS: { key: keyof MatchupData; label: string }[] = [
  { key: 'pts',  label: 'PTS' },
  { key: 'fg3m', label: '3PM' },
  { key: 'reb',  label: 'REB' },
  { key: 'ast',  label: 'AST' },
  { key: 'stl',  label: 'STL' },
  { key: 'blk',  label: 'BLK' },
  { key: 'tov',  label: 'TOV' },
];

function ordinal(n: number): string {
  if (n === 11 || n === 12 || n === 13) return `${n}th`;
  const s = ['th', 'st', 'nd', 'rd'];
  const v = n % 10;
  return `${n}${s[v] || 'th'}`;
}

// Rank 1 = most allowed = favorable for overs.
// Green: ranks 1-10, Yellow: 11-20, Red: 21-30.
function rankColor(rank: number): string {
  if (rank <= 10) return 'text-green-400';
  if (rank <= 20) return 'text-yellow-400';
  return 'text-red-400';
}

function matchupLabel(rank: number): { label: string; cls: string } {
  if (rank <= 10) return { label: 'Favorable', cls: 'text-green-400' };
  if (rank <= 20) return { label: 'Neutral',   cls: 'text-yellow-400' };
  return            { label: 'Tough',     cls: 'text-red-400' };
}

interface Props {
  oppTeamId: number;
  position: string;
  highlightMarket?: string;  // active prop market key — highlights the relevant stat
}

export default function MatchupDefense({ oppTeamId, position, highlightMarket }: Props) {
  const [data, setData]       = useState<MatchupData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setData(null);
    fetch(`/api/contextual?oppTeamId=${oppTeamId}&position=${encodeURIComponent(position)}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(setData)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [oppTeamId, position]);

  const highlightStat = highlightMarket ? MARKET_TO_STAT[highlightMarket] : undefined;

  if (loading) return <div className="px-4 py-3 text-xs text-gray-600">Loading matchup...</div>;
  if (error)   return <div className="px-4 py-3 text-xs text-red-500">Matchup unavailable</div>;
  if (!data)   return null;

  // Headline line for the highlighted stat
  const headlineStat = highlightStat && typeof data[highlightStat] === 'object'
    ? data[highlightStat] as StatLine
    : null;
  const headlineMeta = headlineStat ? matchupLabel(headlineStat.rank) : null;

  return (
    <div className="border-t border-gray-800 px-4 py-3">
      <div className="flex items-baseline gap-2 mb-2">
        <span className="text-xs font-semibold text-gray-400 uppercase tracking-wider">
          vs {data.oppTeamAbbr} Defense
        </span>
        <span className="text-xs text-gray-600">
          {data.position} position &middot; {data.gamesDefended} games this season
        </span>
        {headlineMeta && headlineStat && (
          <span className={`text-xs font-semibold ml-auto ${headlineMeta.cls}`}>
            {headlineMeta.label} &mdash; {ordinal(headlineStat.rank)} most allowed
          </span>
        )}
      </div>

      <div className="overflow-x-auto">
        <table className="text-xs w-full">
          <thead>
            <tr className="text-gray-600">
              {STAT_LABELS.map(({ key, label }) => (
                <th
                  key={key}
                  className={`text-right py-1 px-2 font-medium ${
                    key === highlightStat ? 'text-gray-300' : ''
                  }`}
                >
                  {label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {/* Averages row */}
            <tr>
              {STAT_LABELS.map(({ key }) => {
                const s = data[key] as StatLine;
                return (
                  <td
                    key={key}
                    className={`text-right py-0.5 px-2 tabular-nums ${
                      key === highlightStat ? 'text-gray-200 font-semibold' : 'text-gray-400'
                    }`}
                  >
                    {s.avg.toFixed(1)}
                  </td>
                );
              })}
            </tr>
            {/* Rank row */}
            <tr>
              {STAT_LABELS.map(({ key }) => {
                const s = data[key] as StatLine;
                return (
                  <td
                    key={key}
                    className={`text-right py-0.5 px-2 tabular-nums text-xs ${
                      rankColor(s.rank)
                    } ${
                      key === highlightStat ? 'font-semibold' : 'opacity-70'
                    }`}
                  >
                    {ordinal(s.rank)}
                  </td>
                );
              })}
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}
