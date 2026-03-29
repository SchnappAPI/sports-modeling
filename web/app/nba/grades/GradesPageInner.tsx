'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

interface GradeRow {
  gradeId: number;
  gradeDate: string;
  playerId: number;
  playerName: string;
  marketKey: string;
  lineValue: number;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  weightedHitRate: number | null;
  grade: number | null;
  oppTeamId: number | null;
  position: string | null;
}

interface DefenseCache {
  [key: string]: Record<string, number> | 'loading' | 'error';
}

// ---------------------------------------------------------------------------
// Market helpers
// ---------------------------------------------------------------------------

// Maps any market key to a short abbreviation for display in the table.
const MARKET_ABBR: Record<string, string> = {
  player_points:                       'PTS',
  player_points_alternate:             'PTS',
  player_rebounds:                     'REB',
  player_rebounds_alternate:           'REB',
  player_assists:                      'AST',
  player_assists_alternate:            'AST',
  player_steals:                       'STL',
  player_steals_alternate:             'STL',
  player_blocks:                       'BLK',
  player_blocks_alternate:             'BLK',
  player_threes:                       '3PM',
  player_threes_alternate:             '3PM',
  player_turnovers:                    'TOV',
  player_turnovers_alternate:          'TOV',
  player_points_rebounds_assists:      'PRA',
  player_points_rebounds_assists_alternate: 'PRA',
  player_points_rebounds:              'PR',
  player_points_rebounds_alternate:    'PR',
  player_points_assists:               'PA',
  player_points_assists_alternate:     'PA',
  player_rebounds_assists:             'RA',
  player_rebounds_assists_alternate:   'RA',
};

// Maps a market key to the canonical (non-alternate) base key, used for
// collapsing the market dropdown so "Points" and "Points (Alt)" become
// a single "PTS" option.
function baseMarket(key: string): string {
  return key.replace(/_alternate$/, '');
}

function isAlternate(key: string): boolean {
  return key.endsWith('_alternate');
}

function marketAbbr(key: string): string {
  return MARKET_ABBR[key] ?? key.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}

// Human-readable label for the dropdown (uses abbreviation only).
function marketDropdownLabel(baseKey: string): string {
  return MARKET_ABBR[baseKey] ?? baseKey.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}

function gradeColor(grade: number | null): string {
  if (grade == null) return 'text-gray-500';
  if (grade >= 70) return 'text-green-400';
  if (grade >= 55) return 'text-yellow-400';
  return 'text-gray-400';
}

function fmt(val: number | null, decimals = 1): string {
  if (val == null) return '-';
  return val.toFixed(decimals);
}

function fmtPct(val: number | null): string {
  if (val == null) return '-';
  return `${(val * 100).toFixed(0)}%`;
}

function fmtOdds(price: number | null): string {
  if (price == null) return '-';
  return price >= 0 ? `+${price}` : `${price}`;
}

function oddsColor(price: number | null): string {
  if (price == null) return 'text-gray-600';
  if (price > 0) return 'text-gray-400';
  if (price >= -115) return 'text-gray-500';
  return 'text-gray-400';
}

function todayLocal(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function ordinal(n: number): string {
  if (n === 11 || n === 12 || n === 13) return `${n}th`;
  const s = ['th', 'st', 'nd', 'rd'];
  const v = n % 10;
  return `${n}${s[v] || 'th'}`;
}

function rankColor(rank: number): string {
  if (rank <= 10) return 'text-green-400';
  if (rank <= 20) return 'text-yellow-400';
  return 'text-red-400';
}

const MARKET_TO_STAT_KEY: Record<string, string> = {
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

function posGroup(position: string | null): string | null {
  if (!position) return null;
  if (position.startsWith('G')) return 'G';
  if (position.startsWith('F')) return 'F';
  if (position.startsWith('C')) return 'C';
  return null;
}

const ODDS_MIN = -300;
const ODDS_MAX = 300;

export default function GradesPageInner() {
  const searchParams = useSearchParams();
  const [grades, setGrades]           = useState<GradeRow[]>([]);
  const [loading, setLoading]         = useState(true);
  const [error, setError]             = useState<string | null>(null);
  const [selectedMarket, setSelectedMarket] = useState<string>('');
  const [playerFilter, setPlayerFilter]     = useState<string>('');
  const [oddsRange, setOddsRange]           = useState<[number, number]>([ODDS_MIN, ODDS_MAX]);
  const [defenseCache, setDefenseCache]     = useState<DefenseCache>({});

  const backGameId = searchParams.get('gameId');
  const gradeDate  = searchParams.get('date') ?? todayLocal();
  const backHref   = backGameId ? `/nba?gameId=${backGameId}` : '/nba';

  useEffect(() => {
    setLoading(true);
    setError(null);
    setSelectedMarket('');
    setPlayerFilter('');
    setOddsRange([ODDS_MIN, ODDS_MAX]);
    setDefenseCache({});

    const url = backGameId
      ? `/api/grades?date=${gradeDate}&gameId=${backGameId}`
      : `/api/grades?date=${gradeDate}`;

    fetch(url)
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((data) => setGrades(data.grades ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gradeDate, backGameId]);

  // After grades load, fetch defense data for each unique (oppTeamId, posGroup) pair.
  useEffect(() => {
    if (grades.length === 0) return;

    const pairs = new Set<string>();
    for (const g of grades) {
      const pg = posGroup(g.position);
      if (g.oppTeamId && pg) pairs.add(`${g.oppTeamId}:${pg}`);
    }

    for (const key of pairs) {
      setDefenseCache((prev) => {
        if (prev[key]) return prev;
        return { ...prev, [key]: 'loading' };
      });

      const [tid, pg] = key.split(':');
      fetch(`/api/contextual?oppTeamId=${tid}&position=${pg}`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
        .then((data) => {
          const ranks: Record<string, number> = {};
          for (const stat of ['pts', 'reb', 'ast', 'stl', 'blk', 'fg3m', 'tov']) {
            if (data[stat]?.rank != null) ranks[stat] = data[stat].rank;
          }
          setDefenseCache((prev) => ({ ...prev, [key]: ranks }));
        })
        .catch(() => {
          setDefenseCache((prev) => ({ ...prev, [key]: 'error' }));
        });
    }
  }, [grades]);

  // Dropdown options: unique base market keys (collapse regular + alternate into one).
  const marketOptions = useMemo(() => {
    const seen = new Set<string>();
    const opts: string[] = [];
    for (const row of grades) {
      const base = baseMarket(row.marketKey);
      if (!seen.has(base)) {
        seen.add(base);
        opts.push(base);
      }
    }
    return opts.sort();
  }, [grades]);

  const filtered = useMemo(() => {
    let rows = grades;
    // When a base market is selected, include both the standard and alternate keys.
    if (selectedMarket) {
      rows = rows.filter((r) =>
        r.marketKey === selectedMarket || r.marketKey === `${selectedMarket}_alternate`
      );
    }
    if (playerFilter.trim()) {
      const q = playerFilter.trim().toLowerCase();
      rows = rows.filter((r) => r.playerName.toLowerCase().includes(q));
    }
    const [lo, hi] = oddsRange;
    const sliderActive = lo > ODDS_MIN || hi < ODDS_MAX;
    if (sliderActive) {
      rows = rows.filter((r) => r.overPrice == null || (r.overPrice >= lo && r.overPrice <= hi));
    }
    return rows;
  }, [grades, selectedMarket, playerFilter, oddsRange]);

  const sliderActive = oddsRange[0] > ODDS_MIN || oddsRange[1] < ODDS_MAX;

  function defRankCell(row: GradeRow): { rank: number | null; label: string } {
    const pg = posGroup(row.position);
    if (!row.oppTeamId || !pg) return { rank: null, label: '-' };
    const key = `${row.oppTeamId}:${pg}`;
    const entry = defenseCache[key];
    if (!entry || entry === 'loading') return { rank: null, label: '...' };
    if (entry === 'error') return { rank: null, label: '-' };
    const statKey = MARKET_TO_STAT_KEY[row.marketKey];
    if (!statKey) return { rank: null, label: '-' };
    const rank = (entry as Record<string, number>)[statKey];
    if (rank == null) return { rank: null, label: '-' };
    return { rank, label: ordinal(rank) };
  }

  // Build the player link: go to the player page, pass gameId (if available) for
  // matchup context. No date needed since all rows on this page share gradeDate.
  function playerHref(row: GradeRow): string {
    const params = new URLSearchParams();
    if (backGameId) params.set('gameId', backGameId);
    const qs = params.toString();
    return `/nba/player/${row.playerId}${qs ? `?${qs}` : ''}`;
  }

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header row */}
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3 flex-wrap">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">
          &#8592; Games
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          At a Glance
        </span>
        <span className="text-xs text-gray-600">{gradeDate}</span>

        {!loading && !error && grades.length > 0 && (
          <>
            <select
              value={selectedMarket}
              onChange={(e) => setSelectedMarket(e.target.value)}
              className="bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500"
            >
              <option value="">All markets</option>
              {marketOptions.map((key) => (
                <option key={key} value={key}>{marketDropdownLabel(key)}</option>
              ))}
            </select>

            <input
              type="text"
              placeholder="Player..."
              value={playerFilter}
              onChange={(e) => setPlayerFilter(e.target.value)}
              className="bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 w-28 focus:outline-none focus:border-gray-500 placeholder-gray-600"
            />
          </>
        )}

        {!loading && !error && (
          <span className="text-xs text-gray-600 ml-auto">
            {filtered.length}{filtered.length !== grades.length ? ` / ${grades.length}` : ''} props
          </span>
        )}
      </div>

      {!loading && !error && grades.length > 0 && (
        <div className="px-4 py-2 border-b border-gray-800 flex items-center gap-3">
          <span className="text-xs text-gray-600 whitespace-nowrap">Odds</span>
          <div className="flex items-center gap-1 flex-1">
            <span className={`text-xs tabular-nums w-10 text-right ${
              sliderActive ? 'text-gray-300' : 'text-gray-600'
            }`}>
              {oddsRange[0] >= 0 ? `+${oddsRange[0]}` : `${oddsRange[0]}`}
            </span>
            <input
              type="range" min={ODDS_MIN} max={ODDS_MAX} step={5}
              value={oddsRange[0]}
              onChange={(e) => {
                const v = parseInt(e.target.value);
                setOddsRange([Math.min(v, oddsRange[1] - 5), oddsRange[1]]);
              }}
              className="flex-1 accent-blue-500 h-1"
            />
            <span className="text-xs text-gray-600">to</span>
            <input
              type="range" min={ODDS_MIN} max={ODDS_MAX} step={5}
              value={oddsRange[1]}
              onChange={(e) => {
                const v = parseInt(e.target.value);
                setOddsRange([oddsRange[0], Math.max(v, oddsRange[0] + 5)]);
              }}
              className="flex-1 accent-blue-500 h-1"
            />
            <span className={`text-xs tabular-nums w-10 ${
              sliderActive ? 'text-gray-300' : 'text-gray-600'
            }`}>
              {oddsRange[1] >= 0 ? `+${oddsRange[1]}` : `${oddsRange[1]}`}
            </span>
          </div>
          {sliderActive && (
            <button
              onClick={() => setOddsRange([ODDS_MIN, ODDS_MAX])}
              className="text-xs text-gray-600 hover:text-gray-400"
            >
              Reset
            </button>
          )}
        </div>
      )}

      <div className="flex-1 px-4 py-4">
        {loading && <div className="text-sm text-gray-500">Loading grades...</div>}
        {error   && <div className="text-sm text-red-400">Error: {error}</div>}
        {!loading && !error && grades.length === 0 && (
          <div className="text-sm text-gray-500">
            No grades available for {gradeDate}. Grades populate nightly after the ETL runs.
          </div>
        )}
        {!loading && !error && grades.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-gray-500 border-b border-gray-800">
                  <th className="text-left py-1.5 pr-3 font-medium">Player</th>
                  <th className="text-left py-1.5 pr-1 font-medium">Mkt</th>
                  <th className="text-center py-1.5 px-1 font-medium" title="Alternate line">Alt</th>
                  <th className="text-right py-1.5 px-2 font-medium">Line</th>
                  <th className="text-right py-1.5 px-2 font-medium">Odds</th>
                  <th className="text-right py-1.5 px-2 font-medium">Grade</th>
                  <th className="text-right py-1.5 px-2 font-medium">L20%</th>
                  <th className="text-right py-1.5 px-2 font-medium">L60%</th>
                  <th className="text-right py-1.5 px-2 font-medium">N20</th>
                  <th className="text-right py-1.5 px-2 font-medium">N60</th>
                  <th className="text-right py-1.5 pl-2 font-medium" title="Opponent defense rank for this stat at this position. 1st = most allowed.">Def</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((row) => {
                  const def = defRankCell(row);
                  const alt = isAlternate(row.marketKey);
                  return (
                    <tr key={row.gradeId} className="border-b border-gray-800">
                      <td className="py-1.5 pr-3">
                        <Link
                          href={playerHref(row)}
                          className="text-gray-100 hover:text-blue-400 transition-colors"
                        >
                          {row.playerName}
                        </Link>
                      </td>
                      <td className="py-1.5 pr-1 text-gray-400 text-xs font-mono">
                        {marketAbbr(row.marketKey)}
                      </td>
                      <td className="py-1.5 px-1 text-center text-gray-500 text-xs">
                        {alt ? <span className="text-yellow-600">*</span> : ''}
                      </td>
                      <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.lineValue)}</td>
                      <td className={`py-1.5 px-2 text-right tabular-nums ${oddsColor(row.overPrice)}`}>
                        {fmtOdds(row.overPrice)}
                      </td>
                      <td className={`py-1.5 px-2 text-right font-semibold ${gradeColor(row.grade)}`}>
                        {fmt(row.grade)}
                      </td>
                      <td className="py-1.5 px-2 text-right text-gray-300">{fmtPct(row.hitRate20)}</td>
                      <td className="py-1.5 px-2 text-right text-gray-300">{fmtPct(row.hitRate60)}</td>
                      <td className="py-1.5 px-2 text-right text-gray-500">{row.sampleSize20 ?? '-'}</td>
                      <td className="py-1.5 px-2 text-right text-gray-500">{row.sampleSize60 ?? '-'}</td>
                      <td className={`py-1.5 pl-2 text-right tabular-nums text-xs ${
                        def.rank != null ? rankColor(def.rank) : 'text-gray-600'
                      }`}>
                        {def.label}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
