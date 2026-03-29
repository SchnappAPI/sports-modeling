'use client';

import { useEffect, useState, useMemo } from 'react';

interface BoxRow {
  playerId: number;
  playerName: string;
  teamId: number;
  period: string;
  starterStatus: string | null;
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

interface PlayerTotals {
  playerId: number;
  playerName: string;
  teamId: number;
  starterStatus: string | null;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  min: number;
  fg3m: number;
  fgm: number;
  fga: number;
  ftm: number;
  fta: number;
}

interface GradeEntry {
  playerId: number;
  marketKey: string;
  lineValue: number;
}

// playerId -> marketKey -> lineValue
type PropMap = Map<number, Map<string, number>>;

const ALL_PERIODS = ['1Q', '2Q', '3Q', '4Q', 'OT'] as const;
type QuarterKey = typeof ALL_PERIODS[number];

const MARKET_TO_STAT: Record<string, keyof PlayerTotals> = {
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
  player_turnovers:         'tov',
  player_threes:            'fg3m',
  player_threes_alternate:  'fg3m',
};

function sum(rows: BoxRow[], key: keyof BoxRow): number {
  return rows.reduce((acc, r) => acc + ((r[key] as number) ?? 0), 0);
}

function fmtMin(min: number): string {
  if (min === 0) return '0:00';
  const m = Math.floor(min);
  const s = Math.round((min - m) * 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

function fmtShoot(made: number, att: number): string {
  return att === 0 ? '-' : `${made}/${att}`;
}

// First matching market line for a player among the stat markets.
function getLine(propMap: PropMap, playerId: number, statKey: keyof PlayerTotals): number | null {
  const playerMap = propMap.get(playerId);
  if (!playerMap) return null;
  for (const [market, statCol] of Object.entries(MARKET_TO_STAT)) {
    if (statCol === statKey) {
      const line = playerMap.get(market);
      if (line != null) return line;
    }
  }
  return null;
}

function statCls(value: number, line: number | null): string {
  if (line == null) return 'text-gray-300';
  return value > line ? 'text-green-400 font-medium' : 'text-red-400';
}

function TeamBox({
  starters,
  bench,
  dnp,
  hasLineup,
  propMap,
  showColors,
}: {
  starters: PlayerTotals[];
  bench: PlayerTotals[];
  dnp: PlayerTotals[];
  hasLineup: boolean;
  propMap: PropMap;
  showColors: boolean;
}) {
  const renderRow = (p: PlayerTotals) => {
    const line = (sk: keyof PlayerTotals) =>
      showColors ? getLine(propMap, p.playerId, sk) : null;
    return (
      <tr key={p.playerId} className="border-b border-gray-800">
        <td className="py-1.5 pr-3 text-gray-100">{p.playerName}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmtMin(p.min)}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.pts,  line('pts'))}`}>{p.pts}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.reb,  line('reb'))}`}>{p.reb}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.ast,  line('ast'))}`}>{p.ast}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.stl,  line('stl'))}`}>{p.stl}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.blk,  line('blk'))}`}>{p.blk}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.tov,  line('tov'))}`}>{p.tov}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmtShoot(p.fgm, p.fga)}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(p.fg3m, line('fg3m'))}`}>{fmtShoot(p.fg3m, p.fga)}</td>
        <td className="py-1.5 pl-2 text-right text-gray-300">{fmtShoot(p.ftm, p.fta)}</td>
      </tr>
    );
  };

  const sectionHeader = (label: string) => (
    <tr>
      <td colSpan={11} className="pt-3 pb-1 text-xs text-gray-600 font-semibold uppercase tracking-wider">
        {label}
      </td>
    </tr>
  );

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-xs text-gray-500 border-b border-gray-800">
            <th className="text-left py-1.5 pr-3 font-medium">Player</th>
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
          {hasLineup ? (
            <>
              {starters.length > 0 && sectionHeader('Starters')}
              {starters.map(renderRow)}
              {bench.length > 0 && sectionHeader('Bench')}
              {bench.map(renderRow)}
              {dnp.length > 0 && sectionHeader('Did Not Play')}
              {dnp.map((p) => (
                <tr key={p.playerId} className="border-b border-gray-800 opacity-40">
                  <td className="py-1.5 pr-3 text-gray-300">{p.playerName}</td>
                  <td colSpan={10} className="py-1.5 px-2 text-xs text-gray-500">DNP</td>
                </tr>
              ))}
            </>
          ) : (
            [...starters, ...bench].sort((a, b) => b.min - a.min).map(renderRow)
          )}
        </tbody>
      </table>
    </div>
  );
}

export default function BoxScoreTable({ gameId }: { gameId: string }) {
  const [rows, setRows]           = useState<BoxRow[]>([]);
  const [grades, setGrades]       = useState<GradeEntry[]>([]);
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState<string | null>(null);
  const [selectedPeriods, setSelectedPeriods] = useState<Set<QuarterKey>>(new Set());

  useEffect(() => {
    setLoading(true);
    setError(null);
    setSelectedPeriods(new Set());
    setGrades([]);

    Promise.all([
      fetch(`/api/boxscore?gameId=${gameId}`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
      fetch(`/api/game-grades?gameId=${gameId}`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
    ])
      .then(([boxData, gradeData]) => {
        setRows(boxData.rows ?? []);
        setGrades(gradeData.grades ?? []);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId]);

  function togglePeriod(p: QuarterKey) {
    setSelectedPeriods((prev) => {
      const next = new Set(prev);
      if (next.has(p)) next.delete(p); else next.add(p);
      return next;
    });
  }

  const propMap = useMemo<PropMap>(() => {
    const map: PropMap = new Map();
    for (const g of grades) {
      if (!map.has(g.playerId)) map.set(g.playerId, new Map());
      // Keep the first (lowest alternate / standard) line per market.
      // Grade rows are ordered by player_id, market_key so we get the
      // standard line before any alternate-line duplicate.
      if (!map.get(g.playerId)!.has(g.marketKey)) {
        map.get(g.playerId)!.set(g.marketKey, g.lineValue);
      }
    }
    return map;
  }, [grades]);

  // Prop coloring only makes sense for full-game totals.
  const showColors = selectedPeriods.size === 0;

  const availablePeriods = useMemo(
    () => ALL_PERIODS.filter((p) => rows.some((r) => r.period === p)),
    [rows]
  );

  const totals = useMemo<PlayerTotals[]>(() => {
    const filtered = selectedPeriods.size === 0
      ? rows
      : rows.filter((r) => selectedPeriods.has(r.period as QuarterKey));
    const byPlayer = new Map<number, BoxRow[]>();
    filtered.forEach((r) => {
      if (!byPlayer.has(r.playerId)) byPlayer.set(r.playerId, []);
      byPlayer.get(r.playerId)!.push(r);
    });
    return Array.from(byPlayer.values()).map((pr) => ({
      playerId:      pr[0].playerId,
      playerName:    pr[0].playerName,
      teamId:        pr[0].teamId,
      starterStatus: pr[0].starterStatus,
      pts:  sum(pr, 'pts'),
      reb:  sum(pr, 'reb'),
      ast:  sum(pr, 'ast'),
      stl:  sum(pr, 'stl'),
      blk:  sum(pr, 'blk'),
      tov:  sum(pr, 'tov'),
      min:  sum(pr, 'min'),
      fg3m: sum(pr, 'fg3m'),
      fgm:  sum(pr, 'fgm'),
      fga:  sum(pr, 'fga'),
      ftm:  sum(pr, 'ftm'),
      fta:  sum(pr, 'fta'),
    }));
  }, [rows, selectedPeriods]);

  const teamIds = useMemo(() => Array.from(new Set(totals.map((p) => p.teamId))), [totals]);

  if (loading) return <div className="text-sm text-gray-500 py-4">Loading box score...</div>;
  if (error)   return <div className="text-sm text-red-400 py-4">Error: {error}</div>;
  if (rows.length === 0) return <div className="text-sm text-gray-500 py-4">No box score data available.</div>;

  return (
    <div>
      <div className="flex items-center gap-2 mb-4">
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
        {!showColors && grades.length > 0 && (
          <span className="text-xs text-gray-600 ml-2">Prop coloring off (full game only)</span>
        )}
      </div>

      <div className="flex flex-col gap-6">
        {teamIds.map((teamId) => {
          const tp       = totals.filter((p) => p.teamId === teamId);
          const hasLineup = tp.some((p) => p.starterStatus != null);
          const starters  = tp.filter((p) => p.starterStatus === 'Starter');
          const bench     = tp.filter((p) =>
            p.starterStatus === 'Bench' || (hasLineup && p.starterStatus == null && p.min > 0)
          );
          const dnp       = hasLineup ? tp.filter((p) => p.min === 0) : [];
          return (
            <TeamBox
              key={teamId}
              starters={starters}
              bench={bench}
              dnp={dnp}
              hasLineup={hasLineup}
              propMap={propMap}
              showColors={showColors}
            />
          );
        })}
      </div>
    </div>
  );
}
