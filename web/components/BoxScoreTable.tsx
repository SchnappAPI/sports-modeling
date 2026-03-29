'use client';

import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

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

// Represents a player's position in the game roster (always present once they
// appeared in the game), separate from their filtered-period stat totals.
interface PlayerSlot {
  playerId: number;
  playerName: string;
  teamId: number;
  starterStatus: string | null;
  // Whether the player had any box score rows at all (appeared in the game).
  appearedInGame: boolean;
}

interface PlayerTotals {
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

type PropMap = Map<number, Map<string, number>>;

const ALL_PERIODS = ['1Q', '2Q', '3Q', '4Q', 'OT'] as const;
type QuarterKey = typeof ALL_PERIODS[number];

const MARKET_TO_STAT: Record<string, keyof PlayerTotals> = {
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
  player_turnovers:          'tov',
  player_threes:             'fg3m',
  player_threes_alternate:   'fg3m',
};

function sumRows(rows: BoxRow[], key: keyof BoxRow): number {
  return rows.reduce((acc, r) => acc + ((r[key] as number) ?? 0), 0);
}

const ZERO_TOTALS: PlayerTotals = {
  pts: 0, reb: 0, ast: 0, stl: 0, blk: 0, tov: 0,
  min: 0, fg3m: 0, fgm: 0, fga: 0, ftm: 0, fta: 0,
};

function buildTotals(rows: BoxRow[]): PlayerTotals {
  if (rows.length === 0) return { ...ZERO_TOTALS };
  return {
    pts:  sumRows(rows, 'pts'),
    reb:  sumRows(rows, 'reb'),
    ast:  sumRows(rows, 'ast'),
    stl:  sumRows(rows, 'stl'),
    blk:  sumRows(rows, 'blk'),
    tov:  sumRows(rows, 'tov'),
    min:  sumRows(rows, 'min'),
    fg3m: sumRows(rows, 'fg3m'),
    fgm:  sumRows(rows, 'fgm'),
    fga:  sumRows(rows, 'fga'),
    ftm:  sumRows(rows, 'ftm'),
    fta:  sumRows(rows, 'fta'),
  };
}

function fmtMin(min: number): string {
  if (min === 0) return '-';
  const m = Math.floor(min);
  const s = Math.round((min - m) * 60);
  return `${m}:${s.toString().padStart(2, '0')}`;
}

function fmtShoot(made: number, att: number): string {
  return att === 0 ? '-' : `${made}/${att}`;
}

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
  slots,
  filteredTotals,
  hasLineup,
  propMap,
  showColors,
  gameId,
  selectedDate,
}: {
  slots: PlayerSlot[];
  // playerId -> totals for the currently selected period(s)
  filteredTotals: Map<number, PlayerTotals>;
  hasLineup: boolean;
  propMap: PropMap;
  showColors: boolean;
  gameId: string;
  selectedDate: string;
}) {
  const searchParams = useSearchParams();

  const renderRow = (slot: PlayerSlot) => {
    const t = filteredTotals.get(slot.playerId) ?? { ...ZERO_TOTALS };
    // A player who appeared in the game but had zero activity in the
    // selected period is shown dimmed with zeroes/dashes rather than hidden.
    const inactive = slot.appearedInGame && t.min === 0;
    const line = (sk: keyof PlayerTotals) =>
      showColors && !inactive ? getLine(propMap, slot.playerId, sk) : null;

    const playerHref =
      `/nba/player/${slot.playerId}?gameId=${gameId}&tab=boxscore&date=${selectedDate}`;

    return (
      <tr
        key={slot.playerId}
        className={['border-b border-gray-800', inactive ? 'opacity-40' : ''].join(' ')}
      >
        <td className="py-1.5 pr-3">
          <Link
            href={playerHref}
            className="text-gray-100 hover:text-blue-400 transition-colors"
          >
            {slot.playerName}
          </Link>
        </td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmtMin(t.min)}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.pts,  line('pts'))}`}>{t.pts}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.reb,  line('reb'))}`}>{t.reb}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.ast,  line('ast'))}`}>{t.ast}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.stl,  line('stl'))}`}>{t.stl}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.blk,  line('blk'))}`}>{t.blk}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.tov,  line('tov'))}`}>{t.tov}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmtShoot(t.fgm, t.fga)}</td>
        <td className={`py-1.5 px-2 text-right ${statCls(t.fg3m, line('fg3m'))}`}>{fmtShoot(t.fg3m, t.fga)}</td>
        <td className="py-1.5 pl-2 text-right text-gray-300">{fmtShoot(t.ftm, t.fta)}</td>
      </tr>
    );
  };

  const sectionHeader = (label: string) => (
    <tr>
      <td
        colSpan={11}
        className="pt-3 pb-1 text-xs text-gray-600 font-semibold uppercase tracking-wider"
      >
        {label}
      </td>
    </tr>
  );

  const starters = slots.filter((s) => s.starterStatus === 'Starter');
  const bench    = slots.filter(
    (s) => s.starterStatus === 'Bench' || (hasLineup && s.starterStatus == null && s.appearedInGame)
  );
  const dnp      = hasLineup ? slots.filter((s) => !s.appearedInGame) : [];

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
              {dnp.map((s) => (
                <tr key={s.playerId} className="border-b border-gray-800 opacity-40">
                  <td className="py-1.5 pr-3">
                    <Link
                      href={`/nba/player/${s.playerId}?gameId=${gameId}&tab=boxscore&date=${selectedDate}`}
                      className="text-gray-300 hover:text-blue-400 transition-colors"
                    >
                      {s.playerName}
                    </Link>
                  </td>
                  <td colSpan={10} className="py-1.5 px-2 text-xs text-gray-500">DNP</td>
                </tr>
              ))}
            </>
          ) : (
            // No lineup data — all slots appeared in the game; sort by full-game minutes desc.
            [...slots]
              .sort((a, b) => {
                const ma = filteredTotals.get(a.playerId)?.min ?? 0;
                const mb = filteredTotals.get(b.playerId)?.min ?? 0;
                return mb - ma;
              })
              .map(renderRow)
          )}
        </tbody>
      </table>
    </div>

  );
}

export default function BoxScoreTable({
  gameId,
  selectedDate,
}: {
  gameId: string;
  selectedDate: string;
}) {
  const [rows, setRows]     = useState<BoxRow[]>([]);
  const [grades, setGrades] = useState<GradeEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);
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
      if (!map.get(g.playerId)!.has(g.marketKey)) {
        map.get(g.playerId)!.set(g.marketKey, g.lineValue);
      }
    }
    return map;
  }, [grades]);

  const showColors = selectedPeriods.size === 0;

  const availablePeriods = useMemo(
    () => ALL_PERIODS.filter((p) => rows.some((r) => r.period === p)),
    [rows]
  );

  // Build the canonical player slot list from ALL rows (full game).
  // This is the roster of everyone who appeared, with their starter status.
  // This never changes when the period filter changes.
  const allSlots = useMemo<PlayerSlot[]>(() => {
    const byPlayer = new Map<number, BoxRow>();
    for (const r of rows) {
      if (!byPlayer.has(r.playerId)) byPlayer.set(r.playerId, r);
    }
    return Array.from(byPlayer.values()).map((r) => ({
      playerId:       r.playerId,
      playerName:     r.playerName,
      teamId:         r.teamId,
      starterStatus:  r.starterStatus,
      appearedInGame: true,   // every slot here had at least one box score row
    }));
  }, [rows]);

  // Build per-player totals from only the selected periods.
  const filteredTotalsMap = useMemo<Map<number, PlayerTotals>>(() => {
    const filtered = selectedPeriods.size === 0
      ? rows
      : rows.filter((r) => selectedPeriods.has(r.period as QuarterKey));
    const byPlayer = new Map<number, BoxRow[]>();
    for (const r of filtered) {
      if (!byPlayer.has(r.playerId)) byPlayer.set(r.playerId, []);
      byPlayer.get(r.playerId)!.push(r);
    }
    const result = new Map<number, PlayerTotals>();
    for (const [pid, playerRows] of byPlayer) {
      result.set(pid, buildTotals(playerRows));
    }
    return result;
  }, [rows, selectedPeriods]);

  // Group slots by team.
  const teamIds = useMemo(
    () => Array.from(new Set(allSlots.map((s) => s.teamId))),
    [allSlots]
  );

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
          const teamSlots = allSlots.filter((s) => s.teamId === teamId);
          const hasLineup  = teamSlots.some((s) => s.starterStatus != null);
          return (
            <TeamBox
              key={teamId}
              slots={teamSlots}
              filteredTotals={filteredTotalsMap}
              hasLineup={hasLineup}
              propMap={propMap}
              showColors={showColors}
              gameId={gameId}
              selectedDate={selectedDate}
            />
          );
        })}
      </div>
    </div>
  );
}
