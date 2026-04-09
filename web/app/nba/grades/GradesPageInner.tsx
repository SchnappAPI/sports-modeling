'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';
import RefreshDataButton from '@/components/RefreshDataButton';
import PropMatrix from '@/components/PropMatrix';
import {
  getAllSignals,
  getPlayerSignals,
  getCellValueSignals,
  SIGNAL_DEFS,
  type Signal,
  type SignalType,
} from '@/lib/signals';

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
  link: string | null;
}

interface DefenseCache {
  [key: string]: Record<string, number> | 'loading' | 'error';
}

type LivePrices = Record<string, number>;

interface LiveGame {
  gameId: string;
  awayTeamAbbr: string;
  homeTeamAbbr: string;
  awayScore: number;
  homeScore: number;
  gameStatus: number;
  gameStatusText: string;
}

interface LivePlayer {
  playerId: number;
  pts: number;
  reb: number;
  ast: number;
  fg3m: number;
  stl: number;
  blk: number;
  tov: number;
  min: number;
  oncourt: boolean;
}

type LiveBoxScores = Record<number, LivePlayer>;

// ---------------------------------------------------------------------------
// Signal helpers (using shared @/lib/signals)
// ---------------------------------------------------------------------------

// All signals for a row — used for filter matching and signal counts
function getSignals(row: GradeRow): Signal[] {
  return getAllSignals({
    trendGrade:      row.trendGrade,
    regressionGrade: row.regressionGrade,
    momentumGrade:   row.momentumGrade,
    overPrice:       row.overPrice,
    hitRate20:       row.hitRate20,
    hitRate60:       row.hitRate60,
  }).all;
}

// Player-level signals only (HOT/COLD/DUE/FADE) for row-level chips.
// STREAK/SLUMP/LONGSHOT are line-specific and shown only in expanded panels.
function getRowSignals(row: GradeRow): Signal[] {
  return getPlayerSignals({ trendGrade: row.trendGrade, regressionGrade: row.regressionGrade });
}

// Cell-level value signals (LONGSHOT) for expanded panel.
function getRowValueSignals(row: GradeRow): Signal[] {
  return getCellValueSignals({ overPrice: row.overPrice, hitRate20: row.hitRate20, hitRate60: row.hitRate60 });
}

function SignalChip({ signal }: { signal: Signal }) {
  return (
    <span
      className={`inline-block text-[9px] font-semibold px-1 py-0.5 rounded border leading-none tracking-wide ${signal.chipClass}`}
      title={signal.title}
    >
      {signal.label}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Market / odds helpers
// ---------------------------------------------------------------------------

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

const MARKET_STAT: Record<string, string> = {
  player_points: 'pts', player_points_alternate: 'pts',
  player_rebounds: 'reb', player_rebounds_alternate: 'reb',
  player_assists: 'ast', player_assists_alternate: 'ast',
  player_steals: 'stl', player_steals_alternate: 'stl',
  player_blocks: 'blk', player_blocks_alternate: 'blk',
  player_threes: 'fg3m', player_threes_alternate: 'fg3m',
  player_turnovers: 'tov', player_turnovers_alternate: 'tov',
  player_points_rebounds_assists: 'pra', player_points_rebounds_assists_alternate: 'pra',
  player_points_rebounds: 'pr', player_points_rebounds_alternate: 'pr',
  player_points_assists: 'pa', player_points_assists_alternate: 'pa',
  player_rebounds_assists: 'ra', player_rebounds_assists_alternate: 'ra',
};

function liveStatForMarket(p: LivePlayer, marketKey: string): number | null {
  const k = MARKET_STAT[marketKey];
  if (!k) return null;
  if (k === 'pra') return p.pts + p.reb + p.ast;
  if (k === 'pr')  return p.pts + p.reb;
  if (k === 'pa')  return p.pts + p.ast;
  if (k === 'ra')  return p.reb + p.ast;
  return (p as unknown as Record<string, number>)[k] ?? null;
}

function baseMarket(key: string): string { return key.replace(/_alternate$/, ''); }
function isAlternate(key: string): boolean { return key.endsWith('_alternate'); }
function marketAbbr(key: string): string {
  return MARKET_ABBR[key] ?? key.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}
function marketDropdownLabel(baseKey: string): string {
  return MARKET_ABBR[baseKey] ?? baseKey.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}

function fmtLineLabel(marketKey: string, outcomeName: string, lineValue: number): string {
  if (isAlternate(marketKey) && outcomeName !== 'Under') {
    return `${Math.floor(lineValue + 0.5)}+`;
  }
  const prefix = outcomeName === 'Under' ? 'U' : 'O';
  return `${prefix} ${lineValue % 1 === 0 ? lineValue.toFixed(1) : lineValue}`;
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
const ODDS_DEFAULT = -600;

type OutcomeFilter = 'Over' | 'Under';
type ResultFilter  = 'all' | 'open' | 'Won' | 'Lost';
type ViewMode      = 'list' | 'matrix';
type SignalFilter  = '' | SignalType;

type SortKey =
  | 'playerName' | 'marketKey' | 'lineValue' | 'overPrice'
  | 'grade' | 'compositeGrade' | 'hitRate20' | 'hitRate60'
  | 'hitRateOpp' | 'sampleSize20' | 'sampleSize60' | 'def';
type SortDir = 'asc' | 'desc';

const SORT_NULLS_LAST_DESC: SortKey[] = [
  'grade', 'compositeGrade', 'hitRate20', 'hitRate60',
  'hitRateOpp', 'sampleSize20', 'sampleSize60', 'overPrice', 'def',
];

// ---------------------------------------------------------------------------
// Mini dot plot for expanded row
// ---------------------------------------------------------------------------

function MiniDotPlot({ values, lineValue }: { values: number[]; lineValue: number }) {
  if (values.length === 0) return null;
  const minVal = Math.min(...values, lineValue);
  const maxVal = Math.max(...values, lineValue);
  const range  = maxVal - minVal || 1;
  const VW = 600; const VH = 56;
  const PX = 8; const PY = 10;
  const plotW = VW - PX * 2; const plotH = VH - PY * 2;
  const xPos = (i: number) => PX + (values.length <= 1 ? plotW / 2 : (i / (values.length - 1)) * plotW);
  const yPos = (v: number) => PY + plotH - ((v - minVal) / range) * plotH;
  const lineY = yPos(lineValue);
  return (
    <svg viewBox={`0 0 ${VW} ${VH}`} preserveAspectRatio="none" className="w-full" style={{ height: VH }}>
      <line x1={PX} y1={lineY} x2={VW - PX} y2={lineY} stroke="#4b5563" strokeWidth="1.5" strokeDasharray="4 4" />
      <text x={VW - PX - 4} y={lineY - 4} fill="#6b7280" fontSize="9" textAnchor="end"
        style={{ fontVariantNumeric: 'tabular-nums' }}>{lineValue.toFixed(1)}</text>
      {values.map((v, i) => (
        <circle key={i} cx={xPos(i)} cy={yPos(v)} r={4}
          fill={v > lineValue ? '#4ade80' : '#f87171'} opacity={0.9} />
      ))}
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Signal legend tooltip
// ---------------------------------------------------------------------------

function SignalLegend() {
  const items: { type: SignalType; desc: string }[] = [
    { type: 'HOT',      desc: 'L10 stat avg above L30 avg' },
    { type: 'COLD',     desc: 'L10 stat avg below L30 avg' },
    { type: 'DUE',      desc: 'L10 below season avg (bounce-back candidate)' },
    { type: 'FADE',     desc: 'L10 above season avg (regression risk)' },
    { type: 'STREAK',   desc: 'Active hit streak for this line' },
    { type: 'SLUMP',    desc: 'Active miss streak for this line' },
    { type: 'LONGSHOT', desc: 'Long odds (+250) but has hit recently and in 12%+ of L60' },
  ];
  return (
    <div className="flex flex-wrap gap-2 px-4 py-2 border-b border-gray-800">
      {items.map(({ type, desc }) => (
        <span key={type} className="flex items-center gap-1 text-xs text-gray-500" title={desc}>
          <SignalChip signal={{ type, ...SIGNAL_DEFS[type] }} />
          <span className="hidden sm:inline">{desc}</span>
        </span>
      ))}
    </div>
  );
}

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
  const [expandedRowKey, setExpandedRowKey] = useState<string | null>(null);
  const [viewMode, setViewMode]             = useState<ViewMode>('list');
  const [signalFilter, setSignalFilter]     = useState<SignalFilter>('');
  const [showLegend, setShowLegend]         = useState(false);

  const [livePrices, setLivePrices]         = useState<LivePrices>({});
  const [liveEventIds, setLiveEventIds]     = useState<Set<string>>(new Set());
  const [liveGames, setLiveGames]           = useState<LiveGame[]>([]);
  const [liveBoxScores, setLiveBoxScores]   = useState<LiveBoxScores>({});
  const liveIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const gradeDate = searchParams.get('date') ?? todayLocal();
  const isToday   = gradeDate === todayLocal();
  const backHref  = '/nba';

  const oddsMax = useMemo(() => {
    const prices = grades.map((r) => r.overPrice).filter((p): p is number => p != null);
    if (prices.length === 0) return ODDS_DEFAULT;
    return Math.max(...prices);
  }, [grades]);

  const fetchLiveData = useCallback(async () => {
    try {
      const propsRes = await fetch('/api/live-props');
      if (propsRes.ok) {
        const data = await propsRes.json();
        setLivePrices(data.prices ?? {});
        setLiveEventIds(new Set(data.liveEventIds ?? []));
      }
      const sbRes = await fetch('/api/scoreboard');
      if (sbRes.ok) {
        const sbData = await sbRes.json();
        const games: LiveGame[] = sbData.games ?? [];
        setLiveGames(games);
        const liveGameIds = games.filter((g) => g.gameStatus === 2).map((g) => g.gameId);
        if (liveGameIds.length > 0) {
          const results = await Promise.allSettled(
            liveGameIds.map((gid) => fetch(`/api/live-boxscore?gameId=${gid}`).then((r) => r.json()))
          );
          const merged: LiveBoxScores = {};
          for (const r of results) {
            if (r.status === 'fulfilled' && r.value?.players) {
              for (const p of r.value.players) merged[p.playerId] = p;
            }
          }
          setLiveBoxScores(merged);
        } else {
          setLiveBoxScores({});
        }
      }
    } catch { /* silently ignore */ }
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
    setExpandedRowKey(null);
    setSignalFilter('');
    loadGrades();
  }, [loadGrades]);

  useEffect(() => {
    if (minOdds > oddsMax) setMinOdds(oddsMax);
  }, [oddsMax]);

  useEffect(() => {
    if (liveIntervalRef.current) {
      clearInterval(liveIntervalRef.current);
      liveIntervalRef.current = null;
    }
    if (!isToday) {
      setLivePrices({});
      setLiveEventIds(new Set());
      setLiveGames([]);
      setLiveBoxScores({});
      return;
    }
    fetchLiveData();
    liveIntervalRef.current = setInterval(fetchLiveData, 30_000);
    return () => { if (liveIntervalRef.current) clearInterval(liveIntervalRef.current); };
  }, [isToday, fetchLiveData]);

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
    if (isToday) fetchLiveData();
  }

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

    if (resultFilter === 'Won')       rows = rows.filter((r) => r.outcome === 'Won');
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
    if (signalFilter) {
      rows = rows.filter((r) => getSignals(r).some((s) => s.type === signalFilter));
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
  }, [grades, selectedGameId, selectedMarket, playerFilter, minOdds, outcomeFilter, resultFilter, signalFilter]);

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

  const scoreboardGames = useMemo(() => {
    if (liveGames.length > 0) return liveGames;
    return gameOptions.map(([gid, label]) => {
      const parts = label.split(' @ ');
      return {
        gameId: gid,
        awayTeamAbbr: parts[0] ?? '',
        homeTeamAbbr: parts[1] ?? '',
        awayScore: 0,
        homeScore: 0,
        gameStatus: 1,
        gameStatusText: '',
      } as LiveGame;
    });
  }, [liveGames, gameOptions]);

  // Signal counts for filter dropdown labels
  const signalCounts = useMemo(() => {
    const base = grades.filter((r) => r.overPrice != null && (r.outcomeName ?? 'Over') === outcomeFilter);
    const counts: Partial<Record<SignalType, number>> = {};
    for (const row of base) {
      for (const s of getSignals(row)) {
        counts[s.type] = (counts[s.type] ?? 0) + 1;
      }
    }
    return counts;
  }, [grades, outcomeFilter]);

  return (
    <div className="flex flex-col min-h-screen">
      {/* Top bar */}
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3 flex-wrap">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">
          &#8592; Games
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          At a Glance
        </span>
        <span className="text-xs text-gray-600">{gradeDate}</span>

        {hasLiveGames && (
          <span className="text-xs text-green-500 font-medium">&#9679; Live</span>
        )}

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

        {/* Signal filter */}
        {!loading && !error && grades.length > 0 && (
          <div className="flex items-center gap-1.5">
            <select
              value={signalFilter}
              onChange={(e) => setSignalFilter(e.target.value as SignalFilter)}
              className={`bg-gray-900 border text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500 ${
                signalFilter ? 'border-blue-500 text-blue-300' : 'border-gray-700 text-gray-300'
              }`}
            >
              <option value="">All signals</option>
              {(Object.keys(SIGNAL_DEFS) as SignalType[]).map((t) => (
                <option key={t} value={t}>
                  {SIGNAL_DEFS[t].label}{signalCounts[t] ? ` (${signalCounts[t]})` : ''}
                </option>
              ))}
            </select>
            <button
              onClick={() => setShowLegend((v) => !v)}
              className="text-gray-600 hover:text-gray-400 text-xs"
              title="Signal legend"
            >
              ?
            </button>
          </div>
        )}

        {!loading && !error && (
          <RefreshDataButton onComplete={handleRefreshComplete} />
        )}

        {/* View toggle */}
        {!loading && !error && grades.length > 0 && (
          <div className="flex rounded border border-gray-700 overflow-hidden text-xs font-medium ml-auto">
            <button
              onClick={() => setViewMode('list')}
              className={`px-3 py-1 transition-colors ${
                viewMode === 'list' ? 'bg-gray-700 text-gray-100' : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              List
            </button>
            <button
              onClick={() => setViewMode('matrix')}
              className={`px-3 py-1 transition-colors border-l border-gray-700 ${
                viewMode === 'matrix' ? 'bg-gray-700 text-gray-100' : 'text-gray-500 hover:text-gray-300'
              }`}
            >
              Matrix
            </button>
          </div>
        )}

        {!loading && !error && viewMode === 'list' && (
          <span className="text-xs text-gray-600">
            {sorted.length}{sorted.length !== totalForFilter ? ` / ${totalForFilter}` : ''} props
          </span>
        )}
      </div>

      {/* Signal legend (collapsible) */}
      {showLegend && <SignalLegend />}

      {/* Condensed scoreboard strip */}
      {isToday && scoreboardGames.length > 0 && (
        <div className="border-b border-gray-800 px-4 py-1.5">
          <div className="flex gap-1.5 overflow-x-auto flex-wrap">
            {scoreboardGames.map((g) => {
              const isActive   = selectedGameId === g.gameId;
              const isLive     = g.gameStatus === 2;
              const isFinal    = g.gameStatus === 3;
              const isUpcoming = g.gameStatus === 1;
              const awayWin    = isFinal && g.awayScore > g.homeScore;
              const homeWin    = isFinal && g.homeScore > g.awayScore;
              const statusText = isUpcoming ? (g.gameStatusText || '') : g.gameStatusText;
              return (
                <button
                  key={g.gameId}
                  onClick={() => setSelectedGameId((prev) => prev === g.gameId ? '' : g.gameId)}
                  className={`flex items-center gap-1.5 rounded px-2.5 py-1 text-xs transition-colors border whitespace-nowrap ${
                    isActive
                      ? 'border-blue-500 bg-blue-950/40 text-gray-100'
                      : 'border-gray-800 hover:border-gray-600 text-gray-400 hover:text-gray-200'
                  }`}
                >
                  <span className={awayWin ? 'font-semibold text-gray-100' : isFinal ? 'text-gray-600' : ''}>
                    {g.awayTeamAbbr}
                  </span>
                  {!isUpcoming && (
                    <span className={`tabular-nums font-semibold ${awayWin ? 'text-gray-100' : isFinal ? 'text-gray-600' : 'text-gray-200'}`}>
                      {g.awayScore}
                    </span>
                  )}
                  <span className="text-gray-700">&#8211;</span>
                  {!isUpcoming && (
                    <span className={`tabular-nums font-semibold ${homeWin ? 'text-gray-100' : isFinal ? 'text-gray-600' : 'text-gray-200'}`}>
                      {g.homeScore}
                    </span>
                  )}
                  <span className={homeWin ? 'font-semibold text-gray-100' : isFinal ? 'text-gray-600' : ''}>
                    {g.homeTeamAbbr}
                  </span>
                  {statusText && (
                    <span className={`ml-0.5 ${isLive ? 'text-green-400' : isFinal ? 'text-gray-600' : 'text-gray-600'}`}>
                      {isLive && <span className="mr-0.5">&#9679;</span>}
                      {statusText}
                    </span>
                  )}
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Odds range slider */}
      {!loading && !error && grades.length > 0 && (
        <div className="px-4 py-2 border-b border-gray-800 flex items-center gap-3">
          <span className="text-xs text-gray-600 whitespace-nowrap">Min odds</span>
          <input
            type="range"
            min={ODDS_MIN}
            max={oddsMax}
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

        {/* Matrix view */}
        {!loading && !error && grades.length > 0 && viewMode === 'matrix' && (
          <PropMatrix
            rows={filtered}
            gradeDate={gradeDate}
            outcomeFilter={outcomeFilter}
          />
        )}

        {/* List view */}
        {!loading && !error && grades.length > 0 && viewMode === 'list' && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-gray-500 border-b border-gray-800">
                  <SortTh col="playerName" label="Player" />
                  <SortTh col="marketKey" label="Mkt" />
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
                  const rowSignals = getRowSignals(row);
                  const allSignals = getSignals(row);
                  const valueSignals = getRowValueSignals(row);
                  const oppPct   = fmtPct(row.hitRateOpp);
                  const oppTitle = row.sampleSizeOpp
                    ? `${row.sampleSizeOpp} game${row.sampleSizeOpp === 1 ? '' : 's'} vs ${row.oppTeamAbbr ?? 'opp'} (full season)`
                    : undefined;

                  const livePrice = getLivePrice(row);
                  const isLive    = livePrice !== null;
                  const displayPrice: number | null = isLive && livePrice !== 'live-unavailable'
                    ? livePrice
                    : isLive && livePrice === 'live-unavailable'
                    ? null
                    : row.overPrice;

                  const lineLabel = fmtLineLabel(row.marketKey, row.outcomeName ?? 'Over', row.lineValue);
                  const showLink  = row.link != null && row.outcome == null;

                  const livePlayer = liveBoxScores[row.playerId] ?? null;
                  const liveStat   = livePlayer ? liveStatForMarket(livePlayer, row.marketKey) : null;

                  const rowKey = `${row.gradeId}`;
                  const isExpanded = expandedRowKey === rowKey;

                  const oddsContent = (
                    <span className={isLive ? 'text-green-400' : oddsColor(displayPrice)}>
                      {fmtOdds(displayPrice)}
                      {isLive && displayPrice != null && (
                        <span className="text-gray-600 text-xs ml-0.5">L</span>
                      )}
                    </span>
                  );

                  return (
                    <>
                      <tr
                        key={rowKey}
                        className="border-b border-gray-800 cursor-pointer hover:bg-gray-900/40 transition-colors"
                        onClick={() => setExpandedRowKey((prev) => prev === rowKey ? null : rowKey)}
                      >
                        <td className="py-1.5 pr-3">
                          <div className="flex items-center gap-1.5 flex-wrap">
                            <Link
                              href={playerHref(row)}
                              className="text-gray-100 hover:text-blue-400 transition-colors"
                              onClick={(e) => e.stopPropagation()}
                            >
                              {row.playerName}
                            </Link>
                            {row.outcome === 'Won' && (
                              <span className="text-xs font-medium text-green-400 bg-green-900/40 px-1 rounded">W</span>
                            )}
                            {row.outcome === 'Lost' && (
                              <span className="text-xs font-medium text-red-400 bg-red-900/40 px-1 rounded">L</span>
                            )}
                            {isLive && (
                              <span className="text-xs text-green-500">&#9679;</span>
                            )}
                            {rowSignals.map((s) => (
                              <SignalChip key={s.type} signal={s} />
                            ))}
                          </div>
                        </td>
                        <td className="py-1.5 pr-1 text-gray-400 text-xs font-mono">{marketAbbr(row.marketKey)}</td>

                        <td className="py-1.5 px-2 text-right text-gray-300 tabular-nums">
                          <div className="flex items-center justify-end gap-1">
                            {showLink ? (
                              <a
                                href={row.link!}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="hover:text-blue-400 transition-colors"
                                onClick={(e) => e.stopPropagation()}
                              >
                                {lineLabel}
                              </a>
                            ) : (
                              lineLabel
                            )}
                            {liveStat !== null && (
                              <span className={`text-xs font-semibold tabular-nums ${
                                liveStat > row.lineValue ? 'text-green-400' : 'text-red-400'
                              }`}>
                                ({liveStat})
                              </span>
                            )}
                          </div>
                        </td>

                        <td className="py-1.5 px-2 text-right tabular-nums">
                          {showLink ? (
                            <a
                              href={row.link!}
                              target="_blank"
                              rel="noopener noreferrer"
                              className={`hover:text-blue-400 transition-colors ${isLive ? 'text-green-400' : oddsColor(displayPrice)}`}
                              onClick={(e) => e.stopPropagation()}
                            >
                              {fmtOdds(displayPrice)}
                              {isLive && displayPrice != null && (
                                <span className="text-gray-600 text-xs ml-0.5">L</span>
                              )}
                            </a>
                          ) : oddsContent}
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

                      {isExpanded && (
                        <tr key={`${rowKey}-exp`} className="border-b border-gray-800 bg-gray-900/30">
                          <td colSpan={13} className="px-4 py-3">
                            {/* Grade component breakdown */}
                            <div className="flex flex-wrap gap-4 text-xs mb-3">
                              {[
                                { label: 'Trend',      val: row.trendGrade,      title: 'L10 vs L30 stat mean. >50 = trending up.' },
                                { label: 'Regression', val: row.regressionGrade, title: 'L10 vs season avg. >50 = below avg (due up).' },
                                { label: 'Momentum',   val: row.momentumGrade,   title: 'Hit/miss streak for this line. >50 = hit streak.' },
                                { label: 'Matchup',    val: row.matchupGrade,    title: 'Opponent defense rank for this position+stat.' },
                              ].map(({ label, val, title }) => (
                                <div key={label} className="text-center" title={title}>
                                  <div className="text-gray-600 text-xs">{label}</div>
                                  <div className={`text-sm font-semibold tabular-nums ${gradeColor(val)}`}>
                                    {val != null ? val.toFixed(0) : '-'}
                                  </div>
                                </div>
                              ))}
                              {allSignals.filter(s => s.type === 'STREAK' || s.type === 'SLUMP').length > 0 && (
                                <div className="flex items-center gap-1 ml-2 flex-wrap" title="Line-specific signals for this exact prop line">
                                  {allSignals.filter(s => s.type === 'STREAK' || s.type === 'SLUMP').map((s) => <SignalChip key={s.type} signal={s} />)}
                                </div>
                              )}
                              {valueSignals.length > 0 && (
                                <div className="flex items-center gap-1 ml-2 flex-wrap">
                                  {valueSignals.map((s) => <SignalChip key={s.type} signal={s} />)}
                                </div>
                              )}
                            </div>
                            {/* Live stat line */}
                            {livePlayer ? (
                              <div className="flex flex-wrap gap-4 text-xs">
                                {[
                                  { label: 'PTS', val: livePlayer.pts, stat: 'pts' },
                                  { label: 'REB', val: livePlayer.reb, stat: 'reb' },
                                  { label: 'AST', val: livePlayer.ast, stat: 'ast' },
                                  { label: '3PM', val: livePlayer.fg3m, stat: 'fg3m' },
                                  { label: 'STL', val: livePlayer.stl, stat: 'stl' },
                                  { label: 'BLK', val: livePlayer.blk, stat: 'blk' },
                                  { label: 'TOV', val: livePlayer.tov, stat: 'tov' },
                                  { label: 'MIN', val: Math.round(livePlayer.min), stat: '' },
                                ].map(({ label, val, stat }) => (
                                  <div key={label} className="text-gray-400">
                                    <span className="text-gray-600 mr-1">{label}</span>
                                    <span className={`font-semibold tabular-nums ${
                                      stat && MARKET_STAT[row.marketKey] === stat && val > row.lineValue
                                        ? 'text-green-400'
                                        : 'text-gray-200'
                                    }`}>{val}</span>
                                  </div>
                                ))}
                                {livePlayer.oncourt && (
                                  <span className="text-green-500 text-xs">&#9679; on court</span>
                                )}
                              </div>
                            ) : (
                              <div className="text-xs text-gray-600">
                                {row.outcome != null
                                  ? `Game finished. ${row.playerName} ${row.outcome === 'Won' ? 'hit' : 'missed'} ${fmtLineLabel(row.marketKey, row.outcomeName ?? 'Over', row.lineValue)}.`
                                  : 'Live stats not yet available for this game.'}
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </>
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
