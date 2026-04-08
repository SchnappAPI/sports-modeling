'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import RefreshDataButton from '@/components/RefreshDataButton';

interface GradeRow {
  gradeId: number;
  gradeDate: string;
  playerId: number;
  playerName: string;
  marketKey: string;
  outcomeName: string;
  lineValue: number;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  weightedHitRate: number | null;
  grade: number | null;
  compositeGrade: number | null;
  trendGrade: number | null;
  momentumGrade: number | null;
  matchupGrade: number | null;
  regressionGrade: number | null;
  hitRateOpp: number | null;
  sampleSizeOpp: number | null;
  oppTeamAbbr: string | null;
  oppTeamId: number | null;
  position: string | null;
  gameId: string | null;
  homeTeamAbbr: string | null;
  awayTeamAbbr: string | null;
  outcome: string | null;
  eventId: string | null;
}

interface DefenseCache {
  [key: string]: Record<string, number> | 'loading' | 'error';
}

// Live prices fetched from /api/live-props
// key: "eventId|playerName|marketKey|lineValue|outcomeName" -> american price
type LivePrices = Record<string, number>;

const MARKET_ABBR: Record<string, string> = {
  player_points:                           'PTS',
  player_points_alternate:                 'PTS',
  player_rebounds:                         'REB',
  player_rebounds_alternate:               'REB',
  player_assists:                          'AST',
  player_assists_alternate:                'AST',
  player_steals:                           'STL',
  player_steals_alternate:                 'STL',
  player_blocks:                           'BLK',
  player_blocks_alternate:                 'BLK',
  player_threes:                           '3PM',
  player_threes_alternate:                 '3PM',
  player_turnovers:                        'TOV',
  player_turnovers_alternate:              'TOV',
  player_points_rebounds_assists:          'PRA',
  player_points_rebounds_assists_alternate:'PRA',
  player_points_rebounds:                  'PR',
  player_points_rebounds_alternate:        'PR',
  player_points_assists:                   'PA',
  player_points_assists_alternate:         'PA',
  player_rebounds_assists:                 'RA',
  player_rebounds_assists_alternate:       'RA',
};

function baseMarket(key: string): string { return key.replace(/_alternate$/, ''); }
function isAlternate(key: string): boolean { return key.endsWith('_alternate'); }
function marketAbbr(key: string): string {
  return MARKET_ABBR[key] ?? key.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}
function marketDropdownLabel(baseKey: string): string {
  return MARKET_ABBR[baseKey] ?? baseKey.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}
function impliedProb(price: number | null): string {
  if (price == null) return '-';
  const prob = price < 0 ? Math.abs(price) / (Math.abs(price) + 100) : 100 / (price + 100);
  return `${(prob * 100).toFixed(0)}%`;
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
  return `${n}${s[n % 10] || 'th'}`;
}
function rankColor(rank: number): string {
  if (rank <= 10) return 'text-green-400';
  if (rank <= 20) return 'text-yellow-400';
  return 'text-red-400';
}

const MARKET_TO_STAT_KEY: Record<string, string> = {
  player_points: 'pts', player_points_alternate: 'pts',
  player_rebounds: 'reb', player_rebounds_alternate: 'reb',
  player_assists: 'ast', player_assists_alternate: 'ast',
  player_steals: 'stl', player_steals_alternate: 'stl',
  player_blocks: 'blk', player_blocks_alternate: 'blk',
  player_threes: 'fg3m', player_threes_alternate: 'fg3m',
  player_turnovers: 'tov',
};

function posGroup(position: string | null): string | null {
  if (!position) return null;
  if (position.startsWith('G')) return 'G';
  if (position.startsWith('F')) return 'F';
  if (position.startsWith('C')) return 'C';
  return null;
}

const ODDS_MIN     = -1000;
const ODDS_MAX     = 200;
const ODDS_DEFAULT = -600;

type OutcomeFilter = 'Over' | 'Under';
type ResultFilter  = 'all' | 'open' | 'Won' | 'Lost';

type SortKey =
  | 'playerName' | 'marketKey' | 'lineValue' | 'overPrice'
  | 'grade' | 'compositeGrade' | 'hitRate20' | 'hitRate60'
  | 'hitRateOpp' | 'sampleSize20' | 'sampleSize60' | 'def';
type SortDir = 'asc' | 'desc';

const SORT_NULLS_LAST_DESC: SortKey[] = [
  'grade', 'compositeGrade', 'hitRate20', 'hitRate60',
  'hitRateOpp', 'sampleSize20', 'sampleSize60', 'overPrice', 'def',
];

export default function GradesPageInner() {
  const searchParams = useSearchParams();
  const [grades, setGrades]         = useState<GradeRow[]>([]);
  const [loading, setLoading]       = useState(true);
  const [error, setError]           = useState<string | null>(null);
  const [selectedMarket, setSelectedMarket] = useState<string>('');
  const [playerFilter, setPlayerFilter]     = useState<string>('');
  const [minOdds, setMinOdds]               = useState<number>(ODDS_DEFAULT);
  const [selectedGameId, setSelectedGameId] = useState<string>('');
  const [defenseCache, setDefenseCache]     = useState<DefenseCache>({});
  const [sortKey, setSortKey]               = useState<SortKey>('compositeGrade');
  const [sortDir, setSortDir]               = useState<SortDir>('desc');
  const [outcomeFilter, setOutcomeFilter]   = useState<OutcomeFilter>('Over');
  const [resultFilter, setResultFilter]     = useState<ResultFilter>('all');

  // Live odds state
  const [livePrices, setLivePrices]     = useState<LivePrices>({});
  const [liveEventIds, setLiveEventIds] = useState<Set<string>>(new Set());
  const liveIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const gradeDate = searchParams.get('date') ?? todayLocal();
  const isToday   = gradeDate === todayLocal();
  const backHref  = '/nba';

  // Fetch live props when viewing today and there are live event IDs
  const fetchLiveProps = useCallback(async () => {
    try {
      const r = await fetch('/api/live-props');
      if (!r.ok) return;
      const data = await r.json();
      setLivePrices(data.prices ?? {});
      setLiveEventIds(new Set(data.liveEventIds ?? []));
    } catch {
      // silently ignore
    }
  }, []);

  const loadGrades = useCallback(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/grades?date=${gradeDate}`)
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((data) => setGrades(data.grades ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gradeDate]);

  useEffect(() => {
    setSelectedMarket('');
    setPlayerFilter('');
    setMinOdds(ODDS_DEFAULT);
    setSelectedGameId('');
    setDefenseCache({});
    setResultFilter('all');
    loadGrades();
  }, [loadGrades]);

  // Start / stop live odds polling based on whether we're on today
  useEffect(() => {
    if (liveIntervalRef.current) {
      clearInterval(liveIntervalRef.current);
      liveIntervalRef.current = null;
    }
    if (!isToday) {
      setLivePrices({});
      setLiveEventIds(new Set());
      return;
    }
    // Initial fetch, then refresh every 60s
    fetchLiveProps();
    liveIntervalRef.current = setInterval(fetchLiveProps, 60_000);
    return () => {
      if (liveIntervalRef.current) clearInterval(liveIntervalRef.current);
    };
  }, [isToday, fetchLiveProps]);

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
        .catch(() => setDefenseCache((prev) => ({ ...prev, [key]: 'error' })));
    }
  }, [grades]);

  function handleRefreshComplete() {
    loadGrades();
    setDefenseCache({});
    if (isToday) fetchLiveProps();
  }

  // Resolve the displayed price for a row:
  // - If the game is live, show the current live price (or '-' if pulled)
  // - Otherwise show the stored pre-game price
  function getLivePrice(row: GradeRow): number | null | 'live-unavailable' {
    if (!row.eventId || !liveEventIds.has(row.eventId)) return null;
    const key = `${row.eventId}|${row.playerName}|${row.marketKey}|${row.lineValue}|${row.outcomeName ?? 'Over'}`;
    if (key in livePrices) return livePrices[key];
    return 'live-unavailable';
  }

  const gameOptions = useMemo(() => {
    const seen = new Map<string, string>();
    for (const row of grades) {
      if (row.gameId && !seen.has(row.gameId)) {
        const label = (row.awayTeamAbbr && row.homeTeamAbbr)
          ? `${row.awayTeamAbbr} @ ${row.homeTeamAbbr}`
          : row.gameId;
        seen.set(row.gameId, label);
      }
    }
    return Array.from(seen.entries());
  }, [grades]);

  const marketOptions = useMemo(() => {
    const seen = new Set<string>();
    const opts: string[] = [];
    for (const row of grades) {
      const base = baseMarket(row.marketKey);
      if (!seen.has(base)) { seen.add(base); opts.push(base); }
    }
    return opts.sort();
  }, [grades]);

  const overCount  = useMemo(() => grades.filter((r) => (r.outcomeName ?? 'Over') === 'Over' && r.overPrice != null).length, [grades]);
  const underCount = useMemo(() => grades.filter((r) => r.outcomeName === 'Under' && r.overPrice != null).length, [grades]);

  // Counts for result filter tabs (scoped to current outcomeFilter)
  const resultCounts = useMemo(() => {
    const base = grades.filter((r) => r.overPrice != null && (r.outcomeName ?? 'Over') === outcomeFilter);
    return {
      won:  base.filter((r) => r.outcome === 'Won').length,
      lost: base.filter((r) => r.outcome === 'Lost').length,
      open: base.filter((r) => r.outcome == null).length,
    };
  }, [grades, outcomeFilter]);

  const hasLiveGames = useMemo(() => {
    return grades.some((r) => r.eventId && liveEventIds.has(r.eventId));
  }, [grades, liveEventIds]);

  const filtered = useMemo(() => {
    let rows = grades.filter((r) => r.overPrice != null);
    rows = rows.filter((r) => (r.outcomeName ?? 'Over') === outcomeFilter);

    // Result filter
    if (resultFilter === 'Won')  rows = rows.filter((r) => r.outcome === 'Won');
    else if (resultFilter === 'Lost') rows = rows.filter((r) => r.outcome === 'Lost');
    else if (resultFilter === 'open') rows = rows.filter((r) => r.outcome == null);

    if (selectedGameId) rows = rows.filter((r) => r.gameId === selectedGameId);
    if (selectedMarket) {
      rows = rows.filter((r) =>
        r.marketKey === selectedMarket || r.marketKey === `${selectedMarket}_alternate`
      );
    }
    if (playerFilter.trim()) {
      const q = playerFilter.trim().toLowerCase();
      rows = rows.filter((r) => r.playerName.toLowerCase().includes(q));
    }
    if (minOdds > ODDS_MIN) {
      rows = rows.filter((r) => r.overPrice != null && r.overPrice >= minOdds);
    }
    if (outcomeFilter === 'Over') {
      const standardKeys = new Set<string>();
      for (const r of rows) {
        if (!isAlternate(r.marketKey)) standardKeys.add(`${r.playerId}:${r.marketKey}:${r.lineValue}`);
      }
      rows = rows.filter((r) => {
        if (!isAlternate(r.marketKey)) return true;
        return !standardKeys.has(`${r.playerId}:${baseMarket(r.marketKey)}:${r.lineValue}`);
      });
    }
    return rows;
  }, [grades, selectedGameId, selectedMarket, playerFilter, minOdds, outcomeFilter, resultFilter]);

  function getDefRank(row: GradeRow): number | null {
    const pg = posGroup(row.position);
    if (!row.oppTeamId || !pg) return null;
    const entry = defenseCache[`${row.oppTeamId}:${pg}`];
    if (!entry || entry === 'loading' || entry === 'error') return null;
    const statKey = MARKET_TO_STAT_KEY[row.marketKey];
    if (!statKey) return null;
    return (entry as Record<string, number>)[statKey] ?? null;
  }

  const sorted = useMemo(() => {
    const rows = [...filtered];
    const dir = sortDir === 'desc' ? -1 : 1;
    const nullsLast = SORT_NULLS_LAST_DESC.includes(sortKey);
    rows.sort((a, b) => {
      let va: number | string | null = null;
      let vb: number | string | null = null;
      if (sortKey === 'playerName')          { va = a.playerName; vb = b.playerName; }
      else if (sortKey === 'marketKey')      { va = marketAbbr(a.marketKey); vb = marketAbbr(b.marketKey); }
      else if (sortKey === 'lineValue')      { va = a.lineValue; vb = b.lineValue; }
      else if (sortKey === 'overPrice')      { va = a.overPrice; vb = b.overPrice; }
      else if (sortKey === 'grade')          { va = a.grade; vb = b.grade; }
      else if (sortKey === 'compositeGrade') { va = a.compositeGrade; vb = b.compositeGrade; }
      else if (sortKey === 'hitRate20')      { va = a.hitRate20; vb = b.hitRate20; }
      else if (sortKey === 'hitRate60')      { va = a.hitRate60; vb = b.hitRate60; }
      else if (sortKey === 'hitRateOpp')     { va = a.hitRateOpp; vb = b.hitRateOpp; }
      else if (sortKey === 'sampleSize20')   { va = a.sampleSize20; vb = b.sampleSize20; }
      else if (sortKey === 'sampleSize60')   { va = a.sampleSize60; vb = b.sampleSize60; }
      else if (sortKey === 'def')            { va = getDefRank(a); vb = getDefRank(b); }
      const aN = va == null, bN = vb == null;
      if (aN && bN) return 0;
      if (aN) return nullsLast ? 1 : -1 * dir;
      if (bN) return nullsLast ? -1 : 1 * dir;
      if (typeof va === 'string' && typeof vb === 'string') return dir * va.localeCompare(vb);
      return dir * ((va as number) - (vb as number));
    });
    return rows;
  }, [filtered, sortKey, sortDir, defenseCache]);

  function handleSort(key: SortKey) {
    if (sortKey === key) setSortDir((d) => d === 'desc' ? 'asc' : 'desc');
    else { setSortKey(key); setSortDir(SORT_NULLS_LAST_DESC.includes(key) ? 'desc' : 'asc'); }
  }

  function sortIndicator(key: SortKey) {
    if (sortKey !== key) return <span className="text-gray-700 ml-0.5">&#8597;</span>;
    return sortDir === 'desc'
      ? <span className="text-blue-400 ml-0.5">&#8595;</span>
      : <span className="text-blue-400 ml-0.5">&#8593;</span>;
  }

  const oddsFilterActive = minOdds > ODDS_MIN;

  function defRankCell(row: GradeRow): { rank: number | null; label: string } {
    const pg = posGroup(row.position);
    if (!row.oppTeamId || !pg) return { rank: null, label: '-' };
    const key   = `${row.oppTeamId}:${pg}`;
    const entry = defenseCache[key];
    if (!entry || entry === 'loading') return { rank: null, label: '...' };
    if (entry === 'error') return { rank: null, label: '-' };
    const statKey = MARKET_TO_STAT_KEY[row.marketKey];
    if (!statKey) return { rank: null, label: '-' };
    const rank = (entry as Record<string, number>)[statKey];
    if (rank == null) return { rank: null, label: '-' };
    return { rank, label: ordinal(rank) };
  }

  function playerHref(row: GradeRow): string {
    const params = new URLSearchParams();
    if (row.gameId) params.set('gameId', row.gameId);
    params.set('date', gradeDate);
    const qs = params.toString();
    return `/nba/player/${row.playerId}${qs ? `?${qs}` : ''}`;
  }

  const oppLabel = useMemo(() => {
    const abbrs = new Set(sorted.map((r) => r.oppTeamAbbr).filter(Boolean));
    if (abbrs.size === 1) return `vs ${Array.from(abbrs)[0]}`;
    return 'vs Opp';
  }, [sorted]);

  const priceColLabel = outcomeFilter === 'Under' ? 'U Odds' : 'Odds';

  function SortTh({ col, label, title, right }: { col: SortKey; label: string; title?: string; right?: boolean }) {
    return (
      <th
        className={`py-1.5 ${right ? 'px-2 text-right' : 'pr-3 text-left'} font-medium cursor-pointer select-none whitespace-nowrap hover:text-gray-300 transition-colors`}
        title={title}
        onClick={() => handleSort(col)}
      >
        {label}{sortIndicator(col)}
      </th>
    );
  }

  const totalForFilter = grades.filter(r => r.overPrice != null && (r.outcomeName ?? 'Over') === outcomeFilter).length;

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3 flex-wrap">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">
          &#8592; Games
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          At a Glance
        </span>
        <span className="text-xs text-gray-600">{gradeDate}</span>

        {/* Live indicator */}
        {hasLiveGames && (
          <span className="text-xs text-green-500 font-medium">&#9679; Live</span>
        )}

        {/* Over / Under toggle */}
        {!loading && !error && grades.length > 0 && (
          <div className="flex rounded border border-gray-700 overflow-hidden text-xs font-medium">
            <button
              onClick={() => setOutcomeFilter('Over')}
              className={`px-3 py-1 transition-colors ${
                outcomeFilter === 'Over' ? 'bg-gray-700 text-gray-100' : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Over{overCount > 0 ? ` (${overCount})` : ''}
            </button>
            <button
              onClick={() => setOutcomeFilter('Under')}
              className={`px-3 py-1 transition-colors border-l border-gray-700 ${
                outcomeFilter === 'Under' ? 'bg-gray-700 text-gray-100' : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Under{underCount > 0 ? ` (${underCount})` : ''}
            </button>
          </div>
        )}

        {/* Won / Lost / Open filter */}
        {!loading && !error && grades.length > 0 && (
          <div className="flex rounded border border-gray-700 overflow-hidden text-xs font-medium">
            {(['all', 'open', 'Won', 'Lost'] as ResultFilter[]).map((f, i) => {
              const label = f === 'all' ? 'All'
                : f === 'open' ? `Open${resultCounts.open > 0 ? ` (${resultCounts.open})` : ''}`
                : f === 'Won'  ? `Won${resultCounts.won  > 0 ? ` (${resultCounts.won})`  : ''}`
                :                `Lost${resultCounts.lost > 0 ? ` (${resultCounts.lost})` : ''}`;
              const active = resultFilter === f;
              const borderLeft = i > 0 ? 'border-l border-gray-700' : '';
              const activeColor = f === 'Won' ? 'bg-green-900 text-green-300'
                : f === 'Lost' ? 'bg-red-900 text-red-300'
                : 'bg-gray-700 text-gray-100';
              return (
                <button
                  key={f}
                  onClick={() => setResultFilter(f)}
                  className={`px-3 py-1 transition-colors ${borderLeft} ${
                    active ? activeColor : 'text-gray-500 hover:text-gray-300'
                  }`}
                >
                  {label}
                </button>
              );
            })}
          </div>
        )}

        {!loading && !error && grades.length > 0 && (
          <>
            {gameOptions.length > 1 && (
              <select
                value={selectedGameId}
                onChange={(e) => setSelectedGameId(e.target.value)}
                className="bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500"
              >
                <option value="">All games</option>
                {gameOptions.map(([gid, label]) => (
                  <option key={gid} value={gid}>{label}</option>
                ))}
              </select>
            )}
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
          <RefreshDataButton onComplete={handleRefreshComplete} />
        )}

        {!loading && !error && (
          <span className="text-xs text-gray-600 ml-auto">
            {sorted.length}{sorted.length !== totalForFilter
              ? ` / ${totalForFilter}` : ''} props
          </span>
        )}
      </div>

      {/* Odds floor slider */}
      {!loading && !error && grades.length > 0 && (
        <div className="px-4 py-2 border-b border-gray-800 flex items-center gap-3">
          <span className="text-xs text-gray-600 whitespace-nowrap">Min odds</span>
          <input
            type="range"
            min={ODDS_MIN}
            max={ODDS_MAX}
            step={5}
            value={minOdds}
            onChange={(e) => setMinOdds(parseInt(e.target.value))}
            className="flex-1 accent-blue-500 h-1"
          />
          <span className={`text-xs tabular-nums w-14 text-right ${oddsFilterActive ? 'text-gray-300' : 'text-gray-600'}`}>
            {minOdds >= 0 ? `+${minOdds}` : `${minOdds}`}
          </span>
          {oddsFilterActive && (
            <button onClick={() => setMinOdds(ODDS_MIN)} className="text-xs text-gray-600 hover:text-gray-400">
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
                  <SortTh col="playerName" label="Player" />
                  <SortTh col="marketKey" label="Mkt" />
                  <th className="text-center py-1.5 px-1 font-medium text-xs text-gray-500" title="Alternate line">Alt</th>
                  <SortTh col="lineValue" label="Line" right />
                  <SortTh col="overPrice" label={priceColLabel} right />
                  <th className="text-right py-1.5 px-2 font-medium text-gray-500 text-xs" title="Implied probability from odds">Imp%</th>
                  <SortTh col="compositeGrade" label="Comp" title="Composite grade" right />
                  <SortTh col="grade" label="HR%" title="Hit rate grade (weighted 20/60 day)" right />
                  <SortTh col="hitRate20" label="L20%" right />
                  <SortTh col="hitRate60" label="L60%" right />
                  <SortTh col="hitRateOpp" label={oppLabel} title="Hit rate vs today's opponent (full season)" right />
                  <SortTh col="sampleSize20" label="N20" right />
                  <SortTh col="sampleSize60" label="N60" right />
                  <SortTh col="def" label="Def" title="Opponent defense rank. 1st = most allowed." right />
                </tr>
              </thead>
              <tbody>
                {sorted.map((row) => {
                  const def = defRankCell(row);
                  const alt = isAlternate(row.marketKey);
                  const oppPct   = fmtPct(row.hitRateOpp);
                  const oppTitle = row.sampleSizeOpp
                    ? `${row.sampleSizeOpp} game${row.sampleSizeOpp === 1 ? '' : 's'} vs ${row.oppTeamAbbr ?? 'opp'} (full season)`
                    : undefined;

                  // Determine displayed price:
                  // - live game: use live price (null if pulled)
                  // - pre-game or final: use stored pre-game price
                  const livePrice = getLivePrice(row);
                  const isLive    = livePrice !== null;
                  const displayPrice: number | null = isLive && livePrice !== 'live-unavailable'
                    ? livePrice
                    : isLive && livePrice === 'live-unavailable'
                    ? null
                    : row.overPrice;

                  return (
                    <tr key={row.gradeId} className="border-b border-gray-800">
                      <td className="py-1.5 pr-3">
                        <div className="flex items-center gap-1.5">
                          <Link href={playerHref(row)} className="text-gray-100 hover:text-blue-400 transition-colors">
                            {row.playerName}
                          </Link>
                          {/* Outcome badge */}
                          {row.outcome === 'Won' && (
                            <span className="text-xs font-medium text-green-400 bg-green-900/40 px-1 rounded">W</span>
                          )}
                          {row.outcome === 'Lost' && (
                            <span className="text-xs font-medium text-red-400 bg-red-900/40 px-1 rounded">L</span>
                          )}
                          {/* Live indicator per row */}
                          {isLive && (
                            <span className="text-xs text-green-500">&#9679;</span>
                          )}
                        </div>
                      </td>
                      <td className="py-1.5 pr-1 text-gray-400 text-xs font-mono">{marketAbbr(row.marketKey)}</td>
                      <td className="py-1.5 px-1 text-center text-xs">
                        {alt ? <span className="text-yellow-600">*</span> : ''}
                      </td>
                      <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.lineValue)}</td>
                      <td className={`py-1.5 px-2 text-right tabular-nums ${isLive ? 'text-green-400' : oddsColor(displayPrice)}`}>
                        {fmtOdds(displayPrice)}
                        {isLive && displayPrice != null && (
                          <span className="text-gray-600 text-xs ml-0.5">L</span>
                        )}
                      </td>
                      <td className="py-1.5 px-2 text-right tabular-nums text-gray-500 text-xs">{impliedProb(displayPrice)}</td>
                      <td className={`py-1.5 px-2 text-right font-semibold ${gradeColor(row.compositeGrade)}`}>{fmt(row.compositeGrade)}</td>
                      <td className={`py-1.5 px-2 text-right ${gradeColor(row.grade)}`}>{fmt(row.grade)}</td>
                      <td className="py-1.5 px-2 text-right text-gray-300">{fmtPct(row.hitRate20)}</td>
                      <td className="py-1.5 px-2 text-right text-gray-300">{fmtPct(row.hitRate60)}</td>
                      <td
                        className={`py-1.5 px-2 text-right tabular-nums ${
                          row.hitRateOpp != null ? gradeColor(row.hitRateOpp * 100) : 'text-gray-600'
                        }`}
                        title={oppTitle}
                      >
                        {oppPct}
                        {row.sampleSizeOpp != null && (
                          <span className="text-gray-600 text-xs ml-0.5">({row.sampleSizeOpp})</span>
                        )}
                      </td>
                      <td className="py-1.5 px-2 text-right text-gray-500">{row.sampleSize20 ?? '-'}</td>
                      <td className="py-1.5 px-2 text-right text-gray-500">{row.sampleSize60 ?? '-'}</td>
                      <td className={`py-1.5 pl-2 text-right tabular-nums text-xs ${
                        def.rank != null ? rankColor(def.rank) : 'text-gray-600'
                      }`}>{def.label}</td>
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
