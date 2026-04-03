'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import Link from 'next/link';
import MatchupDefense from '@/components/MatchupDefense';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GameLogRow {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
  dnp: boolean;
  started: boolean | null;
  period: string;
  pts: number | null;
  reb: number | null;
  ast: number | null;
  stl: number | null;
  blk: number | null;
  tov: number | null;
  min: number | null;
  fg3m: number | null;
  fg3a: number | null;
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
  potentialAst: number | null;
  rebChances: number | null;
}

interface GradeLine {
  gameId: string;
  marketKey: string;
  lineValue: number;
}

interface TodayGradeRow {
  gradeId: number;
  gradeDate: string;
  playerId: number;
  playerName: string;
  marketKey: string;
  lineValue: number;
  outcomeName: string;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  weightedHitRate: number | null;
  grade: number | null;
  compositeGrade: number | null;
  oppTeamId: number | null;
  position: string | null;
  gameId: string | null;
  homeTeamAbbr: string | null;
  awayTeamAbbr: string | null;
}

interface GameSummary {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
  dnp: boolean;
  started: boolean | null;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  min: number;
  fg3m: number;
  fg3a: number;
  fgm: number;
  fga: number;
  ftm: number;
  fta: number;
  potentialAst: number | null;
  rebChances: number | null;
}

interface PlayerInfo {
  oppTeamId: number | null;
  position: string | null;
  playerName: string | null;
  teamId: number | null;
}

// ---------------------------------------------------------------------------
// Market helpers
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

function marketLabel(key: string): string {
  return MARKET_ABBR[key] ?? key.replace('player_', '').replace(/_/g, ' ').toUpperCase();
}

function baseMarket(key: string): string {
  return key.replace(/_alternate$/, '');
}

function isAlternate(key: string): boolean {
  return key.endsWith('_alternate');
}

function fmtOdds(price: number | null): string {
  if (price == null) return '-';
  return price >= 0 ? `+${price}` : `${price}`;
}

function fmtPct(val: number | null): string {
  if (val == null) return '-';
  return `${(val * 100).toFixed(0)}%`;
}

function gradeColor(grade: number | null): string {
  if (grade == null) return 'text-gray-500';
  if (grade >= 70) return 'text-green-400';
  if (grade >= 55) return 'text-yellow-400';
  return 'text-gray-400';
}

function todayLocal(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ALL_PERIODS = ['1Q', '2Q', '3Q', '4Q', 'OT'] as const;
type QuarterKey = typeof ALL_PERIODS[number];

function buildGameSummaries(
  rows: GameLogRow[],
  selectedPeriods: Set<QuarterKey>,
): GameSummary[] {
  const gameOrder: string[] = [];
  const gameMeta = new Map<string, Pick<GameSummary,
    'gameDate' | 'opponentAbbr' | 'isHome' | 'dnp' | 'started' | 'potentialAst' | 'rebChances'
  >>();
  for (const r of rows) {
    if (!gameMeta.has(r.gameId)) {
      gameOrder.push(r.gameId);
      gameMeta.set(r.gameId, {
        gameDate:     r.gameDate,
        opponentAbbr: r.opponentAbbr,
        isHome:       r.isHome,
        dnp:          r.dnp,
        started:      r.started,
        potentialAst: r.potentialAst ?? null,
        rebChances:   r.rebChances   ?? null,
      });
    }
  }

  const filtered = selectedPeriods.size === 0
    ? rows
    : rows.filter((r) => selectedPeriods.has(r.period as QuarterKey));

  const totals = new Map<string, Omit<GameSummary,
    'gameId' | 'gameDate' | 'opponentAbbr' | 'isHome' | 'dnp' | 'started' | 'potentialAst' | 'rebChances'
  >>();
  for (const r of filtered) {
    if (r.dnp) continue;
    const t = totals.get(r.gameId) ?? { pts:0, reb:0, ast:0, stl:0, blk:0, tov:0, min:0, fg3m:0, fg3a:0, fgm:0, fga:0, ftm:0, fta:0 };
    t.pts  += r.pts  ?? 0;
    t.reb  += r.reb  ?? 0;
    t.ast  += r.ast  ?? 0;
    t.stl  += r.stl  ?? 0;
    t.blk  += r.blk  ?? 0;
    t.tov  += r.tov  ?? 0;
    t.min  += r.min  ?? 0;
    t.fg3m += r.fg3m ?? 0;
    t.fg3a += r.fg3a ?? 0;
    t.fgm  += r.fgm  ?? 0;
    t.fga  += r.fga  ?? 0;
    t.ftm  += r.ftm  ?? 0;
    t.fta  += r.fta  ?? 0;
    totals.set(r.gameId, t);
  }

  const ZERO = { pts:0, reb:0, ast:0, stl:0, blk:0, tov:0, min:0, fg3m:0, fg3a:0, fgm:0, fga:0, ftm:0, fta:0 };
  return gameOrder.map((gid) => ({
    gameId: gid,
    ...gameMeta.get(gid)!,
    ...(totals.get(gid) ?? ZERO),
  }));
}

type SplitKey = 'season' | 'l10' | 'opp';

interface SplitStats {
  gp: number;
  pts: number; reb: number; ast: number; stl: number; blk: number; tov: number;
  min: number; fg3m: number; fg3a: number; fgm: number; fga: number; ftm: number; fta: number;
}

function computeSplit(summaries: GameSummary[], opp: string | null): Record<SplitKey, SplitStats> {
  const zero = (): SplitStats => ({ gp:0, pts:0, reb:0, ast:0, stl:0, blk:0, tov:0, min:0, fg3m:0, fg3a:0, fgm:0, fga:0, ftm:0, fta:0 });
  const acc  = { season: zero(), l10: zero(), opp: zero() };

  const played = summaries.filter((g) => !g.dnp);
  const l10    = played.slice(0, 10);
  const vs     = opp ? played.filter((g) => g.opponentAbbr === opp) : [];

  function add(target: SplitStats, g: GameSummary) {
    target.gp++;  target.pts += g.pts; target.reb += g.reb; target.ast += g.ast;
    target.stl += g.stl; target.blk += g.blk; target.tov += g.tov; target.min += g.min;
    target.fg3m += g.fg3m; target.fg3a += g.fg3a; target.fgm += g.fgm; target.fga += g.fga;
    target.ftm += g.ftm;  target.fta += g.fta;
  }
  played.forEach((g) => add(acc.season, g));
  l10.forEach((g)    => add(acc.l10, g));
  vs.forEach((g)     => add(acc.opp, g));
  return acc;
}

function avg(total: number, gp: number, decimals = 1): string {
  if (gp === 0) return '-';
  return (total / gp).toFixed(decimals);
}

function fmtMin(min: number, gp: number): string {
  if (gp === 0) return '-';
  const m = Math.floor(min / gp);
  const s = Math.round(((min / gp) - m) * 60);
  return `${m}:${s.toString().padStart(2, '00')}`;
}

function fmtShoot(made: number, att: number, gp: number): string {
  if (gp === 0 || att === 0) return '-';
  const pct = ((made / att) * 100).toFixed(0);
  return `${pct}%`;
}

const MARKET_STAT: Record<string, keyof GameSummary> = {
  player_points:            'pts',
  player_points_alternate:  'pts',
  player_rebounds:          'reb',
  player_rebounds_alternate:'reb',
  player_assists:           'ast',
  player_assists_alternate: 'ast',
  player_steals:            'stl',
  player_steals_alternate:  'stl',
  player_blocks:            'blk',
  player_blocks_alternate:  'blk',
  player_threes:            'fg3m',
  player_threes_alternate:  'fg3m',
  player_turnovers:         'tov',
};

// ---------------------------------------------------------------------------
// Props section — new design
// ---------------------------------------------------------------------------

interface LinePair {
  lineValue: number;
  over: TodayGradeRow | null;
  under: TodayGradeRow | null;
}

interface MarketGroup {
  baseKey: string;
  label: string;
  standardLines: LinePair[];
  altLines: LinePair[];
}

function buildMarketGroups(grades: TodayGradeRow[]): MarketGroup[] {
  // Separate standard and alt rows
  const stdRows  = grades.filter((g) => !isAlternate(g.marketKey));
  const altRows  = grades.filter((g) =>  isAlternate(g.marketKey));

  // Build pairs keyed by baseMarket + lineValue
  function pairRows(rows: TodayGradeRow[]): Map<string, Map<number, LinePair>> {
    const grouped = new Map<string, Map<number, LinePair>>();
    for (const r of rows) {
      const base = baseMarket(r.marketKey);
      if (!grouped.has(base)) grouped.set(base, new Map());
      const byLine = grouped.get(base)!;
      const existing = byLine.get(r.lineValue) ?? { lineValue: r.lineValue, over: null, under: null };
      if (r.outcomeName === 'Over') existing.over = r;
      else existing.under = r;
      byLine.set(r.lineValue, existing);
    }
    return grouped;
  }

  const stdPaired = pairRows(stdRows);
  const altPaired = pairRows(altRows);

  // Collect all base market keys in order they first appear
  const order: string[] = [];
  const seen = new Set<string>();
  for (const r of grades) {
    const base = baseMarket(r.marketKey);
    if (!seen.has(base)) { order.push(base); seen.add(base); }
  }

  return order.map((base) => {
    const stdMap = stdPaired.get(base);
    const altMap = altPaired.get(base);
    const sortPairs = (m: Map<number, LinePair> | undefined): LinePair[] =>
      m ? Array.from(m.values()).sort((a, b) => a.lineValue - b.lineValue) : [];
    return {
      baseKey: base,
      label: marketLabel(base),
      standardLines: sortPairs(stdMap),
      altLines:      sortPairs(altMap),
    };
  }).filter((g) => g.standardLines.length > 0 || g.altLines.length > 0);
}

function LinePairRow({ pair }: { pair: LinePair }) {
  const over  = pair.over;
  const under = pair.under;
  // Use Over row for grades; Under row only for price
  const grade  = over?.compositeGrade ?? null;
  const hrOver = over?.grade ?? null;
  const hr20   = over?.hitRate20 ?? null;
  const hr60   = over?.hitRate60 ?? null;

  return (
    <div className="flex items-center gap-3 py-1 text-xs">
      {/* Line value */}
      <span className="tabular-nums font-semibold text-gray-100 w-10 shrink-0">
        {pair.lineValue.toFixed(1)}
      </span>
      {/* Over price */}
      <span className="tabular-nums text-gray-400 w-14 shrink-0">
        O {fmtOdds(over?.overPrice ?? null)}
      </span>
      {/* Under price */}
      <span className="tabular-nums text-gray-400 w-14 shrink-0">
        U {fmtOdds(under?.overPrice ?? null)}
      </span>
      {/* Grades */}
      <span className="flex gap-2 ml-auto">
        {grade != null && (
          <span className={`font-medium ${gradeColor(grade)}`}>C:{grade.toFixed(0)}</span>
        )}
        {hrOver != null && (
          <span className={gradeColor(hrOver)}>HR:{hrOver.toFixed(0)}</span>
        )}
      </span>
      {/* Hit rates */}
      <span className="flex gap-1.5 text-gray-600 w-16 justify-end shrink-0">
        <span>{fmtPct(hr20)}</span>
        <span>{fmtPct(hr60)}</span>
      </span>
    </div>
  );
}

function MarketSection({ group }: { group: MarketGroup }) {
  const [open, setOpen]       = useState(true);
  const [altsOpen, setAltsOpen] = useState(false);

  // Summary line for header: posted line value + over price from first standard line
  const posted = group.standardLines[0];

  return (
    <div className="border-b border-gray-800 last:border-b-0">
      {/* Market header row — tappable to collapse */}
      <button
        className="w-full flex items-center gap-2 px-4 py-2 text-left"
        onClick={() => setOpen((o) => !o)}
      >
        <span className="text-xs font-semibold text-gray-300 w-8 shrink-0">{group.label}</span>
        {posted && (
          <span className="text-xs text-gray-500 tabular-nums">
            {posted.lineValue.toFixed(1)}
            {posted.over && (
              <span className="ml-1.5">{fmtOdds(posted.over.overPrice)}</span>
            )}
            {posted.under && (
              <span className="ml-1 text-gray-600">/ {fmtOdds(posted.under.overPrice)}</span>
            )}
          </span>
        )}
        {posted?.over?.compositeGrade != null && (
          <span className={`text-xs font-medium ml-auto ${gradeColor(posted.over.compositeGrade)}`}>
            {posted.over.compositeGrade.toFixed(0)}
          </span>
        )}
        <span className="text-gray-600 text-xs ml-1">{open ? '▾' : '▸'}</span>
      </button>

      {open && (
        <div className="px-4 pb-2">
          {/* Standard lines */}
          {group.standardLines.map((pair) => (
            <LinePairRow key={pair.lineValue} pair={pair} />
          ))}

          {/* Alt lines sub-section */}
          {group.altLines.length > 0 && (
            <div className="mt-1">
              <button
                className="flex items-center gap-1 text-xs text-yellow-700 hover:text-yellow-500 py-0.5"
                onClick={() => setAltsOpen((o) => !o)}
              >
                <span>{altsOpen ? '▾' : '▸'}</span>
                <span>Alt lines ({group.altLines.length})</span>
              </button>
              {altsOpen && (
                <div className="mt-1 pl-2 border-l border-yellow-900/40">
                  {group.altLines.map((pair) => (
                    <LinePairRow key={pair.lineValue} pair={pair} />
                  ))}
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TodayPropsSection({ playerId, gradeDate }: { playerId: string; gradeDate: string }) {
  const [grades, setGrades] = useState<TodayGradeRow[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetch(`/api/grades?date=${gradeDate}`)
      .then((r) => r.ok ? r.json() : { grades: [] })
      .then((data) => {
        const rows: TodayGradeRow[] = (data.grades ?? []).filter(
          (g: TodayGradeRow) => String(g.playerId) === String(playerId)
        );
        setGrades(rows);
      })
      .catch(() => setGrades([]))
      .finally(() => setLoading(false));
  }, [playerId, gradeDate]);

  const groups = useMemo(() => buildMarketGroups(grades), [grades]);

  if (loading) return (
    <div className="px-4 py-3 border-b border-gray-800 text-xs text-gray-600">Loading props...</div>
  );
  if (groups.length === 0) return null;

  return (
    <div className="border-b border-gray-800">
      <div className="px-4 pt-2 pb-1 flex items-center">
        <span className="text-xs font-semibold text-gray-500 uppercase tracking-wider">Today's Props</span>
      </div>
      <div>
        {groups.map((group) => (
          <MarketSection key={group.baseKey} group={group} />
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Stats toggle button
// ---------------------------------------------------------------------------

function StatsToggle({ showAll, onToggle }: { showAll: boolean; onToggle: () => void }) {
  return (
    <button
      onClick={onToggle}
      className={[
        'px-2.5 py-1 text-xs font-medium rounded transition-colors whitespace-nowrap',
        showAll ? 'bg-gray-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
      ].join(' ')}
    >
      {showAll ? 'Compact' : 'All Stats'}
    </button>
  );
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function PlayerPageInner({ playerId }: { playerId: string }) {
  const router       = useRouter();
  const searchParams = useSearchParams();

  const backGameId   = searchParams.get('gameId');
  const backTab      = searchParams.get('tab') ?? 'boxscore';
  const backDate     = searchParams.get('date');
  const oppParam     = searchParams.get('opp');

  const gradeDate = backDate ?? todayLocal();

  const backHref = (() => {
    const p = new URLSearchParams();
    if (backGameId) p.set('gameId', backGameId);
    if (backTab)    p.set('tab', backTab);
    if (backDate)   p.set('date', backDate);
    const qs = p.toString();
    return qs ? `/nba?${qs}` : '/nba';
  })();

  const [log, setLog]               = useState<GameLogRow[]>([]);
  const [grades, setGrades]         = useState<GradeLine[]>([]);
  const [playerInfo, setPlayerInfo] = useState<PlayerInfo>({ oppTeamId: null, position: null, playerName: null, teamId: null });
  const [loading, setLoading]       = useState(true);
  const [error, setError]           = useState<string | null>(null);
  const [selectedPeriods, setSelectedPeriods] = useState<Set<QuarterKey>>(new Set());
  const [teamPlayers, setTeamPlayers] = useState<{playerId: number; playerName: string}[]>([]);
  const [showAllStats, setShowAllStats] = useState(false);

  // Full game = no period filter active
  const isFullGame = selectedPeriods.size === 0;

  useEffect(() => {
    setLoading(true);
    setError(null);
    setLog([]);
    setGrades([]);
    setPlayerInfo({ oppTeamId: null, position: null, playerName: null, teamId: null });
    setSelectedPeriods(new Set());
    setTeamPlayers([]);

    Promise.all([
      fetch(`/api/player?playerId=${playerId}&lastN=9999&sport=nba`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
      fetch(`/api/player-grades?playerId=${playerId}`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
    ])
      .then(([playerData, gradeData]) => {
        setLog(playerData.log ?? []);
        setGrades(gradeData.grades ?? []);

        const info: PlayerInfo = {
          playerName: playerData.playerName ?? null,
          position:   playerData.position   ?? null,
          oppTeamId:  playerData.lastOppTeamId ?? null,
          teamId:     playerData.teamId ?? null,
        };
        setPlayerInfo(info);

        if (playerData.teamId) {
          fetch(`/api/team-players?teamId=${playerData.teamId}`)
            .then((r) => r.json())
            .then((d) => setTeamPlayers(d.players ?? []))
            .catch(() => {});
        }

        if (backGameId) {
          fetch(`/api/game-grades?gameId=${backGameId}`)
            .then((r) => r.json())
            .then((d) => {
              const myGrade = (d.grades ?? []).find(
                (g: any) => String(g.playerId) === String(playerId)
              );
              if (myGrade?.oppTeamId) {
                setPlayerInfo((prev) => ({
                  ...prev,
                  oppTeamId: myGrade.oppTeamId,
                  position:  myGrade.position ?? prev.position,
                }));
              }
            })
            .catch(() => {});
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [playerId]);

  const gradeMap = useMemo(() => {
    const m = new Map<string, Map<string, number>>();
    for (const g of grades) {
      if (!m.has(g.gameId)) m.set(g.gameId, new Map());
      if (!m.get(g.gameId)!.has(g.marketKey)) {
        m.get(g.gameId)!.set(g.marketKey, g.lineValue);
      }
    }
    return m;
  }, [grades]);

  const summaries = useMemo(
    () => buildGameSummaries(log, selectedPeriods),
    [log, selectedPeriods],
  );

  const splits = useMemo(
    () => computeSplit(summaries, oppParam),
    [summaries, oppParam],
  );

  const availablePeriods = useMemo(
    () => ALL_PERIODS.filter((p) => log.some((r) => r.period === p)),
    [log],
  );

  const showPropColors = isFullGame;

  const teamGameCount = useMemo(() => new Set(summaries.map((s) => s.gameId)).size, [summaries]);
  const playedCount   = useMemo(() => summaries.filter((s) => !s.dnp).length, [summaries]);

  function togglePeriod(p: QuarterKey) {
    setSelectedPeriods((prev) => {
      const next = new Set(prev);
      if (next.has(p)) next.delete(p); else next.add(p);
      return next;
    });
  }

  function getLineCls(gameId: string, market: keyof typeof MARKET_STAT, value: number): string {
    if (!showPropColors) return 'text-gray-300';
    const statKey = MARKET_STAT[market];
    const gameMap = gradeMap.get(gameId);
    if (!gameMap || !statKey) return 'text-gray-300';
    const line = gameMap.get(market);
    if (line == null) return 'text-gray-300';
    return value > line ? 'text-green-400' : 'text-red-400';
  }

  function getComboLineCls(gameId: string, value: number, markets: string[]): string {
    if (!showPropColors) return 'text-gray-300';
    const gameMap = gradeMap.get(gameId);
    if (!gameMap) return 'text-gray-300';
    for (const market of markets) {
      const line = gameMap.get(market);
      if (line != null) return value > line ? 'text-green-400' : 'text-red-400';
    }
    return 'text-gray-300';
  }

  const displayName = playerInfo.playerName ?? `Player ${playerId}`;

  const todayMarket = useMemo(() => {
    if (!backGameId) return undefined;
    const gm = gradeMap.get(backGameId);
    return gm ? Array.from(gm.keys())[0] : undefined;
  }, [gradeMap, backGameId]);

  const showMatchup = playerInfo.oppTeamId != null && playerInfo.position != null;

  if (loading) return <div className="px-4 py-6 text-sm text-gray-500">Loading...</div>;
  if (error)   return <div className="px-4 py-6 text-sm text-red-400">Error: {error}</div>;

  const splitLabels: { key: SplitKey; label: string }[] = [
    { key: 'season', label: 'Season' },
    { key: 'l10',    label: 'Last 10' },
    ...(oppParam ? [{ key: 'opp' as SplitKey, label: `vs ${oppParam}` }] : []),
  ];

  const compactSplitHeaders = ['MIN', 'PTS', '3PT', 'REB', 'AST', 'PRA', 'PR', 'PA', 'RA'];
  const allStatsSplitHeaders = ['MIN', 'PTS', 'FG', '3PT', 'FT', 'REB', 'AST', 'PRA', 'PR', 'PA', 'RA', 'STL', 'BLK', 'TOV'];
  const splitHeaders = showAllStats ? allStatsSplitHeaders : compactSplitHeaders;

  function renderSplitCells(s: SplitStats) {
    const pra = s.gp === 0 ? '-' : ((s.pts + s.reb + s.ast) / s.gp).toFixed(1);
    const pr  = s.gp === 0 ? '-' : ((s.pts + s.reb) / s.gp).toFixed(1);
    const pa  = s.gp === 0 ? '-' : ((s.pts + s.ast) / s.gp).toFixed(1);
    const ra  = s.gp === 0 ? '-' : ((s.reb + s.ast) / s.gp).toFixed(1);
    if (showAllStats) {
      return (
        <>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{fmtMin(s.min, s.gp)}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.pts, s.gp)}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap tabular-nums">
            {s.gp === 0 ? '-' : `${(s.fgm/s.gp).toFixed(1)}-${(s.fga/s.gp).toFixed(1)}`}
          </td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap tabular-nums">
            {s.gp === 0 ? '-' : `${(s.fg3m/s.gp).toFixed(1)}-${(s.fg3a/s.gp).toFixed(1)}`}
          </td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap tabular-nums">
            {s.gp === 0 ? '-' : `${(s.ftm/s.gp).toFixed(1)}-${(s.fta/s.gp).toFixed(1)}`}
          </td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.reb, s.gp)}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.ast, s.gp)}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{pra}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{pr}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{pa}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{ra}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.stl, s.gp)}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.blk, s.gp)}</td>
          <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.tov, s.gp)}</td>
        </>
      );
    }
    return (
      <>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{fmtMin(s.min, s.gp)}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.pts, s.gp)}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap tabular-nums">
          {s.gp === 0 ? '-' : `${(s.fg3m/s.gp).toFixed(1)}-${(s.fg3a/s.gp).toFixed(1)}`}
        </td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.reb, s.gp)}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{avg(s.ast, s.gp)}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{pra}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{pr}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{pa}</td>
        <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{ra}</td>
      </>
    );
  }

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">&#8592;</Link>

        {teamPlayers.length > 0 ? (
          <select
            value={playerId}
            onChange={(e) => {
              const params = new URLSearchParams(searchParams.toString());
              router.push(`/nba/player/${e.target.value}?${params.toString()}`);
            }}
            className="bg-transparent text-sm font-semibold text-gray-200 border-none outline-none cursor-pointer"
          >
            {teamPlayers.map((p) => (
              <option key={p.playerId} value={String(p.playerId)}
                      className="bg-gray-900 text-gray-200">
                {p.playerName}
              </option>
            ))}
          </select>
        ) : (
          <span className="text-sm font-semibold text-gray-200">{displayName}</span>
        )}

        <span className="text-xs text-gray-600 ml-auto">
          {playedCount} GP / {teamGameCount} team games
        </span>
      </div>

      {/* Splits strip */}
      <div className="overflow-x-auto border-b border-gray-800">
        <table className="text-xs w-full">
          <thead>
            <tr className="text-gray-500">
              <th className="text-left px-4 py-2 font-medium sticky left-0 bg-gray-950 z-10">Split</th>
              <th className="text-right px-2 py-2 font-medium whitespace-nowrap">GP</th>
              {splitHeaders.map((h) => (
                <th key={h} className="text-right px-2 py-2 font-medium whitespace-nowrap">{h}</th>
              ))}
              <th className="text-right py-2 pl-2 pr-4">
                <StatsToggle showAll={showAllStats} onToggle={() => setShowAllStats((v) => !v)} />
              </th>
            </tr>
          </thead>
          <tbody>
            {splitLabels.map(({ key, label }) => {
              const s = splits[key];
              return (
                <tr key={key} className="border-t border-gray-800">
                  <td className="px-4 py-2 text-gray-400 font-medium sticky left-0 bg-gray-950 z-10 whitespace-nowrap">{label}</td>
                  <td className="px-2 py-2 text-right text-gray-300 whitespace-nowrap">{s.gp}</td>
                  {renderSplitCells(s)}
                  <td />
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Matchup defense */}
      {showMatchup && (
        <MatchupDefense
          oppTeamId={playerInfo.oppTeamId!}
          position={playerInfo.position!}
          highlightMarket={todayMarket}
        />
      )}

      {/* Today's props */}
      <TodayPropsSection playerId={playerId} gradeDate={gradeDate} />

      {/* Period filter */}
      <div className="flex items-center gap-2 px-4 py-3 border-b border-gray-800">
        <span className="text-xs text-gray-600">All</span>
        {availablePeriods.map((p) => (
          <button
            key={p}
            onClick={() => togglePeriod(p)}
            className={[
              'px-3 py-1 text-xs font-medium rounded transition-colors',
              selectedPeriods.has(p)
                ? 'bg-blue-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
            ].join(' ')}
          >
            {p}
          </button>
        ))}
        {selectedPeriods.size > 0 && (
          <button
            onClick={() => setSelectedPeriods(new Set())}
            className="text-xs text-gray-600 hover:text-gray-400 ml-1"
          >
            Clear
          </button>
        )}
        {!showPropColors && grades.length > 0 && (
          <span className="text-xs text-gray-600 ml-2">Prop coloring off (full game only)</span>
        )}
      </div>

      {/* Game log */}
      <div className="flex-1 overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-20 bg-gray-950">
            <tr className="text-xs text-gray-500 border-b border-gray-800">
              <th className="text-left px-4 py-1.5 font-medium sticky left-0 bg-gray-950 z-30 whitespace-nowrap">Date</th>
              <th className="text-left px-2 py-1.5 font-medium whitespace-nowrap">Opp</th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap" title="* = Starter">MIN</th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">PTS</th>
              {showAllStats ? (
                <>
                  <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">FG</th>
                  <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">3PT</th>
                  <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">FT</th>
                </>
              ) : (
                <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">3PT</th>
              )}
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap" title="REB / REB Chances">
                REB
              </th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap" title="AST / Potential AST">
                AST
              </th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">PRA</th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">PR</th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">PA</th>
              <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">RA</th>
              {showAllStats && (
                <>
                  <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">STL</th>
                  <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">BLK</th>
                  <th className="text-right px-2 py-1.5 font-medium whitespace-nowrap">TOV</th>
                </>
              )}
              <th className="text-right py-1.5 pl-2 pr-4" />
            </tr>
          </thead>
          <tbody>
            {summaries.map((g) => {
              const fmtM = (min: number, started: boolean | null): string => {
                const m = Math.floor(min);
                const s = Math.round((min - m) * 60);
                const t = `${m}:${s.toString().padStart(2, '0')}`;
                return started === true ? `*${t}` : t;
              };
              const fmtS = (made: number, att: number) =>
                att === 0 ? '-' : `${made}-${att}`;
              const fmtPT = (actual: number, potential: number | null): string => {
                if (potential == null) return String(actual);
                return `${actual}-${Math.round(potential)}`;
              };

              if (g.dnp) {
                return (
                  <tr key={g.gameId} className="border-b border-gray-800 opacity-40">
                    <td className="px-4 py-1.5 text-gray-400 sticky left-0 bg-gray-950 z-10 whitespace-nowrap">{g.gameDate.slice(5)}</td>
                    <td className="px-2 py-1.5 text-gray-400 whitespace-nowrap">
                      {g.isHome ? '' : '@'}{g.opponentAbbr}
                    </td>
                    <td className="px-2 py-1.5 text-right text-gray-600 text-xs whitespace-nowrap">DNP</td>
                    <td colSpan={showAllStats ? 12 : 9} />
                  </tr>
                );
              }

              const ptsLine  = getLineCls(g.gameId, 'player_points', g.pts);
              const rebLine  = getLineCls(g.gameId, 'player_rebounds', g.reb);
              const astLine  = getLineCls(g.gameId, 'player_assists', g.ast);
              const stlLine  = getLineCls(g.gameId, 'player_steals', g.stl);
              const blkLine  = getLineCls(g.gameId, 'player_blocks', g.blk);
              const fg3Line  = getLineCls(g.gameId, 'player_threes', g.fg3m);
              const praLine  = getComboLineCls(g.gameId, g.pts + g.reb + g.ast, ['player_points_rebounds_assists', 'player_points_rebounds_assists_alternate']);
              const prLine   = getComboLineCls(g.gameId, g.pts + g.reb, ['player_points_rebounds', 'player_points_rebounds_alternate']);
              const paLine   = getComboLineCls(g.gameId, g.pts + g.ast, ['player_points_assists', 'player_points_assists_alternate']);
              const raLine   = getComboLineCls(g.gameId, g.reb + g.ast, ['player_rebounds_assists', 'player_rebounds_assists_alternate']);

              return (
                <tr key={g.gameId} className="border-b border-gray-800">
                  <td className="px-4 py-1.5 text-gray-400 sticky left-0 bg-gray-950 z-10 whitespace-nowrap">{g.gameDate.slice(5)}</td>
                  <td className="px-2 py-1.5 text-gray-400 whitespace-nowrap">
                    {g.isHome ? '' : '@'}{g.opponentAbbr}
                  </td>
                  <td className="px-2 py-1.5 text-right text-gray-300 whitespace-nowrap tabular-nums">
                    {fmtM(g.min, g.started)}
                  </td>
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${ptsLine}`}>{g.pts}</td>
                  {showAllStats ? (
                    <>
                      <td className="px-2 py-1.5 text-right text-gray-300 whitespace-nowrap tabular-nums">{fmtS(g.fgm, g.fga)}</td>
                      <td className={`px-2 py-1.5 text-right whitespace-nowrap ${fg3Line} tabular-nums`}>{fmtS(g.fg3m, g.fg3a)}</td>
                      <td className="px-2 py-1.5 text-right text-gray-300 whitespace-nowrap tabular-nums">{fmtS(g.ftm, g.fta)}</td>
                    </>
                  ) : (
                    <td className={`px-2 py-1.5 text-right whitespace-nowrap ${fg3Line} tabular-nums`}>{fmtS(g.fg3m, g.fg3a)}</td>
                  )}
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${rebLine} tabular-nums`}>
                    {isFullGame ? fmtPT(g.reb, g.rebChances) : g.reb}
                  </td>
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${astLine} tabular-nums`}>
                    {isFullGame ? fmtPT(g.ast, g.potentialAst) : g.ast}
                  </td>
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${praLine} tabular-nums`}>
                    {g.pts + g.reb + g.ast}
                  </td>
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${prLine} tabular-nums`}>
                    {g.pts + g.reb}
                  </td>
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${paLine} tabular-nums`}>
                    {g.pts + g.ast}
                  </td>
                  <td className={`px-2 py-1.5 text-right whitespace-nowrap ${raLine} tabular-nums`}>
                    {g.reb + g.ast}
                  </td>
                  {showAllStats && (
                    <>
                      <td className={`px-2 py-1.5 text-right whitespace-nowrap ${stlLine}`}>{g.stl}</td>
                      <td className={`px-2 py-1.5 text-right whitespace-nowrap ${blkLine}`}>{g.blk}</td>
                      <td className="px-2 py-1.5 text-right text-gray-300 whitespace-nowrap">{g.tov}</td>
                    </>
                  )}
                  <td />
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
