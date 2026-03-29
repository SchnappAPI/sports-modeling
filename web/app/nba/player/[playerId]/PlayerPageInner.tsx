'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

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
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
}

interface PropLine {
  gameId: string;
  marketKey: string;
  lineValue: number;
}

interface GameSummary {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
  dnp: boolean;
  started: boolean | null;
  pts: number | null;
  reb: number | null;
  ast: number | null;
  stl: number | null;
  blk: number | null;
  tov: number | null;
  min: number | null;
  fg3m: number | null;
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
}

interface SplitStats {
  games: number;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  min: number;
  fg3m: number;
}

// Map Odds API market keys to the stat column they correspond to.
const MARKET_TO_STAT: Record<string, keyof GameSummary> = {
  player_points:            'pts',
  player_rebounds:          'reb',
  player_assists:           'ast',
  player_steals:            'stl',
  player_blocks:            'blk',
  player_turnovers:         'tov',
  player_threes:            'fg3m',
  player_points_rebounds_assists: 'pts', // composite — skip coloring for now
};

const PERIOD_OPTIONS = [
  { label: 'Full', value: 'full' },
  { label: '1Q',   value: '1Q' },
  { label: '2Q',   value: '2Q' },
  { label: '3Q',   value: '3Q' },
  { label: '4Q',   value: '4Q' },
  { label: 'OT',   value: 'OT' },
] as const;

const QUARTER_KEYS = ['1Q', '2Q', '3Q', '4Q', 'OT'];

function sumQuarters(rows: GameLogRow[]): Omit<GameSummary, 'gameId' | 'gameDate' | 'opponentAbbr' | 'isHome' | 'dnp' | 'started'> {
  const n = (key: keyof GameLogRow) =>
    rows.reduce((s, r) => s + ((r[key] as number) ?? 0), 0);
  return {
    pts: n('pts'), reb: n('reb'), ast: n('ast'), stl: n('stl'),
    blk: n('blk'), tov: n('tov'), min: n('min'), fg3m: n('fg3m'),
    fgm: n('fgm'), fga: n('fga'), ftm: n('ftm'), fta: n('fta'),
  };
}

// Collapse per-quarter rows into per-game summaries, applying the active period filter.
function buildGameSummaries(rows: GameLogRow[], activePeriods: Set<string>): GameSummary[] {
  const byGame = new Map<string, GameLogRow[]>();
  for (const r of rows) {
    if (!byGame.has(r.gameId)) byGame.set(r.gameId, []);
    byGame.get(r.gameId)!.push(r);
  }

  const results: GameSummary[] = [];
  for (const [gameId, gameRows] of byGame) {
    const meta = gameRows[0];
    if (meta.dnp) {
      results.push({
        gameId, gameDate: meta.gameDate, opponentAbbr: meta.opponentAbbr,
        isHome: meta.isHome, dnp: true, started: meta.started,
        pts: null, reb: null, ast: null, stl: null, blk: null, tov: null,
        min: null, fg3m: null, fgm: null, fga: null, ftm: null, fta: null,
      });
      continue;
    }

    const isFull = activePeriods.has('full') || activePeriods.size === 0;
    const filtered = isFull
      ? gameRows.filter((r) => QUARTER_KEYS.includes(r.period))
      : gameRows.filter((r) => activePeriods.has(r.period));

    const totals = sumQuarters(filtered);
    results.push({
      gameId, gameDate: meta.gameDate, opponentAbbr: meta.opponentAbbr,
      isHome: meta.isHome, dnp: false, started: meta.started,
      ...totals,
    });
  }

  // Sort most recent first.
  results.sort((a, b) => b.gameDate.localeCompare(a.gameDate));
  return results;
}

function computeSplit(games: GameSummary[]): SplitStats | null {
  const played = games.filter((g) => !g.dnp);
  if (played.length === 0) return null;
  const n = played.length;
  return {
    games: n,
    pts:  played.reduce((s, g) => s + (g.pts  ?? 0), 0) / n,
    reb:  played.reduce((s, g) => s + (g.reb  ?? 0), 0) / n,
    ast:  played.reduce((s, g) => s + (g.ast  ?? 0), 0) / n,
    stl:  played.reduce((s, g) => s + (g.stl  ?? 0), 0) / n,
    blk:  played.reduce((s, g) => s + (g.blk  ?? 0), 0) / n,
    tov:  played.reduce((s, g) => s + (g.tov  ?? 0), 0) / n,
    min:  played.reduce((s, g) => s + (g.min  ?? 0), 0) / n,
    fg3m: played.reduce((s, g) => s + (g.fg3m ?? 0), 0) / n,
  };
}

function fmt1(val: number): string { return val.toFixed(1); }
function fmt(val: number | null | undefined, decimals = 0): string {
  if (val == null) return '-';
  return val.toFixed(decimals);
}
function fmtMin(val: number | null): string {
  if (val == null) return '-';
  const m = Math.floor(val);
  const s = Math.round((val - m) * 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}
function fmtShooting(made: number | null, att: number | null): string {
  if (made == null || att == null || att === 0) return '-';
  return `${made}/${att}`;
}

function SplitRow({ label, split }: { label: string; split: SplitStats | null }) {
  if (!split) {
    return (
      <tr className="border-b border-gray-800">
        <td className="py-1.5 pr-3 text-xs text-gray-400 font-medium">{label}</td>
        <td colSpan={8} className="py-1.5 text-xs text-gray-600">No games</td>
      </tr>
    );
  }
  return (
    <tr className="border-b border-gray-800">
      <td className="py-1.5 pr-3 text-xs text-gray-400 font-medium whitespace-nowrap">
        {label}
        <span className="ml-1 text-gray-600">({split.games}G)</span>
      </td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-300">{fmt1(split.min)}</td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-100 font-medium">{fmt1(split.pts)}</td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-300">{fmt1(split.reb)}</td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-300">{fmt1(split.ast)}</td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-300">{fmt1(split.stl)}</td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-300">{fmt1(split.blk)}</td>
      <td className="py-1.5 px-2 text-right text-xs text-gray-300">{fmt1(split.tov)}</td>
      <td className="py-1.5 pl-2 text-right text-xs text-gray-300">{fmt1(split.fg3m)}</td>
    </tr>
  );
}

// Colour a stat cell based on whether it beat or missed a prop line.
function statColor(value: number | null, line: number | null | undefined): string {
  if (value == null || line == null) return 'text-gray-300';
  return value > line ? 'text-green-400 font-medium' : 'text-red-400';
}

export default function PlayerPageInner({ playerId }: { playerId: string }) {
  const searchParams = useSearchParams();
  const [rawLog, setRawLog] = useState<GameLogRow[]>([]);
  const [propLines, setPropLines] = useState<PropLine[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Period filter. 'full' = sum all quarters.
  const [activePeriods, setActivePeriods] = useState<Set<string>>(new Set(['full']));

  const backGameId = searchParams.get('gameId');
  const backTab    = searchParams.get('tab') ?? 'stats';
  const opp        = searchParams.get('opp') ?? '';
  const backHref   = backGameId ? `/nba?gameId=${backGameId}&tab=${backTab}` : '/nba';

  useEffect(() => {
    Promise.all([
      fetch(`/api/player?playerId=${playerId}&games=100&sport=nba`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
      fetch(`/api/player-grades?playerId=${playerId}`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
    ])
      .then(([logData, gradesData]) => {
        setRawLog(logData.log ?? []);
        setPropLines(gradesData.grades ?? []);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [playerId]);

  function togglePeriod(value: string) {
    setActivePeriods((prev) => {
      const next = new Set(prev);
      if (value === 'full') return new Set(['full']);
      next.delete('full');
      if (next.has(value)) {
        next.delete(value);
        if (next.size === 0) return new Set(['full']);
      } else {
        next.add(value);
      }
      return next;
    });
  }

  // Build a lookup: gameId -> marketKey -> lineValue
  const propMap = useMemo(() => {
    const map = new Map<string, Map<string, number>>();
    for (const p of propLines) {
      if (!map.has(p.gameId)) map.set(p.gameId, new Map());
      map.get(p.gameId)!.set(p.marketKey, p.lineValue);
    }
    return map;
  }, [propLines]);

  const games = useMemo(
    () => buildGameSummaries(rawLog, activePeriods),
    [rawLog, activePeriods]
  );

  const played = useMemo(() => games.filter((g) => !g.dnp), [games]);

  const seasonSplit = useMemo(() => computeSplit(played), [played]);
  const last10Split = useMemo(() => computeSplit(played.slice(0, 10)), [played]);
  const vsOppSplit  = useMemo(
    () => opp ? computeSplit(played.filter((g) => g.opponentAbbr === opp)) : null,
    [played, opp]
  );

  // Prop coloring is only meaningful for full-game stats since lines are for full games.
  const showPropColors = activePeriods.has('full') || activePeriods.size === 0;

  function propLine(gameId: string, market: keyof typeof MARKET_TO_STAT): number | null {
    if (!showPropColors) return null;
    return propMap.get(gameId)?.get(market) ?? null;
  }

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">
          &#8592; Back
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          Player Game Log
        </span>
        {!loading && !error && (
          <span className="text-xs text-gray-600 ml-auto">
            {played.length} GP / {games.length} team games
          </span>
        )}
      </div>

      <div className="flex-1 px-4 py-4">
        {loading && <div className="text-sm text-gray-500">Loading game log...</div>}
        {error   && <div className="text-sm text-red-400">Error: {error}</div>}

        {!loading && !error && games.length > 0 && (
          <>
            {/* Period filter */}
            <div className="flex items-center gap-1 mb-5">
              {PERIOD_OPTIONS.map(({ label, value }) => {
                const active = activePeriods.has(value);
                return (
                  <button
                    key={value}
                    onClick={() => togglePeriod(value)}
                    className={[
                      'px-2.5 py-1 text-xs font-medium rounded transition-colors',
                      active
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
                    ].join(' ')}
                  >
                    {label}
                  </button>
                );
              })}
            </div>

            {/* Splits strip */}
            <div className="overflow-x-auto mb-6">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-xs text-gray-500 border-b border-gray-800">
                    <th className="text-left py-1.5 pr-3 font-medium">Split</th>
                    <th className="text-right py-1.5 px-2 font-medium">MIN</th>
                    <th className="text-right py-1.5 px-2 font-medium">PTS</th>
                    <th className="text-right py-1.5 px-2 font-medium">REB</th>
                    <th className="text-right py-1.5 px-2 font-medium">AST</th>
                    <th className="text-right py-1.5 px-2 font-medium">STL</th>
                    <th className="text-right py-1.5 px-2 font-medium">BLK</th>
                    <th className="text-right py-1.5 px-2 font-medium">TOV</th>
                    <th className="text-right py-1.5 pl-2 font-medium">3PM</th>
                  </tr>
                </thead>
                <tbody>
                  <SplitRow label="Season" split={seasonSplit} />
                  <SplitRow label="Last 10" split={last10Split} />
                  {opp && <SplitRow label={`vs ${opp}`} split={vsOppSplit} />}
                </tbody>
              </table>
            </div>

            {/* Game log */}
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-xs text-gray-500 border-b border-gray-800">
                    <th className="text-left py-1.5 pr-1 font-medium">Date</th>
                    <th className="text-left py-1.5 pr-3 font-medium">Opp</th>
                    <th className="text-left py-1.5 pr-3 font-medium">Str</th>
                    <th className="text-right py-1.5 px-2 font-medium">MIN</th>
                    <th className="text-right py-1.5 px-2 font-medium">PTS</th>
                    <th className="text-right py-1.5 px-2 font-medium">REB</th>
                    <th className="text-right py-1.5 px-2 font-medium">AST</th>
                    <th className="text-right py-1.5 px-2 font-medium">STL</th>
                    <th className="text-right py-1.5 px-2 font-medium">BLK</th>
                    <th className="text-right py-1.5 px-2 font-medium">TOV</th>
                    <th className="text-right py-1.5 px-2 font-medium">FG</th>
                    <th className="text-right py-1.5 px-2 font-medium">3P</th>
                    <th className="text-right py-1.5 pl-2 font-medium">FT</th>
                  </tr>
                </thead>
                <tbody>
                  {games.map((g) => {
                    const ptsLine  = propLine(g.gameId, 'player_points');
                    const rebLine  = propLine(g.gameId, 'player_rebounds');
                    const astLine  = propLine(g.gameId, 'player_assists');
                    const stlLine  = propLine(g.gameId, 'player_steals');
                    const blkLine  = propLine(g.gameId, 'player_blocks');
                    const tovLine  = propLine(g.gameId, 'player_turnovers');
                    const fg3mLine = propLine(g.gameId, 'player_threes');

                    const starterLabel = g.started === true ? 'S' : g.started === false ? 'B' : '';

                    return (
                      <tr
                        key={g.gameId}
                        className={['border-b border-gray-800', g.dnp ? 'opacity-40' : ''].join(' ')}
                      >
                        <td className="py-1.5 pr-1 text-gray-300">{g.gameDate}</td>
                        <td className="py-1.5 pr-3 text-gray-400">
                          {g.isHome ? '' : '@'}{g.opponentAbbr}
                        </td>
                        <td className="py-1.5 pr-3">
                          {starterLabel && (
                            <span className={[
                              'text-xs px-1 rounded',
                              g.started ? 'bg-blue-900 text-blue-300' : 'text-gray-600'
                            ].join(' ')}>
                              {starterLabel}
                            </span>
                          )}
                        </td>
                        {g.dnp ? (
                          <td colSpan={10} className="py-1.5 px-2 text-gray-500 text-xs">DNP</td>
                        ) : (
                          <>
                            <td className="py-1.5 px-2 text-right text-gray-300">{fmtMin(g.min)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.pts, ptsLine)}`}>{fmt(g.pts)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.reb, rebLine)}`}>{fmt(g.reb)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.ast, astLine)}`}>{fmt(g.ast)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.stl, stlLine)}`}>{fmt(g.stl)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.blk, blkLine)}`}>{fmt(g.blk)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.tov, tovLine)}`}>{fmt(g.tov)}</td>
                            <td className="py-1.5 px-2 text-right text-gray-300">{fmtShooting(g.fgm, g.fga)}</td>
                            <td className={`py-1.5 px-2 text-right ${statColor(g.fg3m, fg3mLine)}`}>{fmtShooting(g.fg3m, g.fga)}</td>
                            <td className="py-1.5 pl-2 text-right text-gray-300">{fmtShooting(g.ftm, g.fta)}</td>
                          </>
                        )}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </>
        )}

        {!loading && !error && games.length === 0 && (
          <div className="text-sm text-gray-500">No game log available.</div>
        )}
      </div>
    </div>
  );
}
