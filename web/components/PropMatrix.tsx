'use client';

import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface MatrixRow {
  gradeId: number;
  playerId: number;
  playerName: string;
  marketKey: string;
  lineValue: number;
  overPrice: number | null;
  compositeGrade: number | null;
  hitRate20: number | null;
  gameId: string | null;
  homeTeamAbbr: string | null;
  awayTeamAbbr: string | null;
  oppTeamAbbr: string | null;
  position: string | null;
  outcome: string | null;
  outcomeName: string | null;
  link: string | null;
  eventId: string | null;
}

interface PlayerStats {
  log: Array<{
    gameDate: string;
    oppAbbr: string;
    home: boolean;
    pts: number;
    reb: number;
    ast: number;
    fg3m: number;
    stl: number;
    blk: number;
    tov: number;
    min: number;
    dnp: boolean;
  }>;
  playerName: string | null;
}

interface PlayerPanelProps {
  playerId: number;
  playerName: string;
  focusStat: StatKey;
  gradeDate: string;
  gameId: string | null;
  onClose: () => void;
}

// ---------------------------------------------------------------------------
// Constants — canonical matrix columns per market group
// Column values are integers representing the "N+" display label.
// Database line_values are stored as N-0.5 (e.g. 4.5 = 5+), so we round
// row.lineValue to the nearest integer before matching.
// ---------------------------------------------------------------------------

type StatKey = 'pts' | 'reb' | 'ast' | 'fg3m' | 'pra' | 'pr' | 'pa' | 'ra' | 'stl' | 'blk';

const MATRIX_COLS: Record<StatKey, number[]> = {
  pts:  [5,  10, 15, 20, 25, 30, 35, 40],
  reb:  [4,  6,  8,  10, 12, 14, 16],
  ast:  [2,  4,  6,  8,  10, 12, 14],
  fg3m: [1,  2,  3,  4,  5,  6,  7],
  pra:  [10, 15, 20, 25, 30, 35, 40, 45, 50],
  pr:   [10, 15, 20, 25, 30, 35, 40, 45],
  pa:   [10, 15, 20, 25, 30, 35, 40, 45],
  ra:   [10, 15, 20, 25],
  stl:  [1,  2,  3,  4],
  blk:  [1,  2,  3,  4],
};

const GROUP_ORDER: StatKey[] = ['pts', 'reb', 'ast', 'fg3m', 'pra', 'pr', 'pa', 'ra', 'stl', 'blk'];

const GROUP_LABELS: Record<StatKey, string> = {
  pts: 'PTS', reb: 'REB', ast: 'AST', fg3m: '3PM',
  pra: 'PRA', pr: 'PR',  pa: 'PA',   ra: 'RA',
  stl: 'STL', blk: 'BLK',
};

// Map market_key -> stat group key
function marketToStat(marketKey: string): StatKey | null {
  if (marketKey.startsWith('player_points_rebounds_assists')) return 'pra';
  if (marketKey.startsWith('player_points_rebounds'))         return 'pr';
  if (marketKey.startsWith('player_points_assists'))          return 'pa';
  if (marketKey.startsWith('player_rebounds_assists'))        return 'ra';
  if (marketKey.startsWith('player_points'))                  return 'pts';
  if (marketKey.startsWith('player_rebounds'))                return 'reb';
  if (marketKey.startsWith('player_assists'))                 return 'ast';
  if (marketKey.startsWith('player_threes'))                  return 'fg3m';
  if (marketKey.startsWith('player_steals'))                  return 'stl';
  if (marketKey.startsWith('player_blocks'))                  return 'blk';
  return null;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fmtOdds(price: number | null): string {
  if (price == null) return '-';
  return price >= 0 ? `+${price}` : `${price}`;
}

function gradeColor(grade: number | null): string {
  if (grade == null) return 'text-gray-600';
  if (grade >= 70) return 'text-green-400';
  if (grade >= 55) return 'text-yellow-400';
  return 'text-gray-500';
}

function statForKey(game: PlayerStats['log'][0], key: StatKey): number {
  switch (key) {
    case 'pts':  return game.pts;
    case 'reb':  return game.reb;
    case 'ast':  return game.ast;
    case 'fg3m': return game.fg3m;
    case 'stl':  return game.stl;
    case 'blk':  return game.blk;
    case 'pra':  return game.pts + game.reb + game.ast;
    case 'pr':   return game.pts + game.reb;
    case 'pa':   return game.pts + game.ast;
    case 'ra':   return game.reb + game.ast;
    default:     return 0;
  }
}

// ---------------------------------------------------------------------------
// Player stats slide-in panel
// ---------------------------------------------------------------------------

function PlayerPanel({ playerId, playerName, focusStat, gradeDate, gameId, onClose }: PlayerPanelProps) {
  const [data, setData]       = useState<PlayerStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  const cols = MATRIX_COLS[focusStat] ?? [];

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    const params = new URLSearchParams({ playerId: String(playerId), games: '20' });
    if (gameId) params.set('gameId', gameId);
    fetch(`/api/player?${params}`)
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((d) => setData(d))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [playerId, gameId]);

  useEffect(() => { load(); }, [load]);

  const playerHref = `/nba/player/${playerId}?${new URLSearchParams({
    date: gradeDate,
    ...(gameId ? { gameId } : {}),
  })}`;

  const games = data?.log.filter((g) => !g.dnp).slice(0, 20) ?? [];

  return (
    <div className="fixed inset-y-0 right-0 z-50 w-full max-w-md bg-gray-950 border-l border-gray-700 shadow-2xl flex flex-col">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 border-b border-gray-800">
        <div className="flex items-center gap-2">
          <Link
            href={playerHref}
            className="text-gray-100 font-semibold hover:text-blue-400 transition-colors text-sm"
          >
            {playerName}
          </Link>
          <span className="text-xs text-gray-500 uppercase tracking-wider">
            {GROUP_LABELS[focusStat]}
          </span>
        </div>
        <button onClick={onClose} className="text-gray-500 hover:text-gray-200 text-lg leading-none px-1">
          &times;
        </button>
      </div>

      {/* Body */}
      <div className="flex-1 overflow-y-auto px-4 py-3">
        {loading && <div className="text-sm text-gray-500">Loading...</div>}
        {error   && <div className="text-sm text-red-400">Error: {error}</div>}
        {!loading && !error && games.length === 0 && (
          <div className="text-sm text-gray-500">No recent games found.</div>
        )}
        {!loading && !error && games.length > 0 && (
          <>
            {/* Hit rate summary */}
            <div className="flex gap-3 mb-4 flex-wrap">
              {cols.map((line) => {
                // Stats are integers; "5+" means val >= 5
                const hits  = games.filter((g) => statForKey(g, focusStat) >= line).length;
                const pct   = games.length > 0 ? hits / games.length : null;
                const color = pct == null ? 'text-gray-600'
                  : pct >= 0.65 ? 'text-green-400'
                  : pct >= 0.50 ? 'text-yellow-400'
                  : 'text-gray-500';
                return (
                  <div key={line} className="text-center">
                    <div className="text-gray-600 text-xs">{line}+</div>
                    <div className={`text-sm font-semibold tabular-nums ${color}`}>
                      {pct != null ? `${Math.round(pct * 100)}%` : '-'}
                    </div>
                    <div className="text-gray-700 text-xs">{hits}/{games.length}</div>
                  </div>
                );
              })}
            </div>

            {/* Game log */}
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-600 border-b border-gray-800">
                  <th className="text-left py-1 pr-2">Date</th>
                  <th className="text-left py-1 pr-2">Opp</th>
                  {cols.map((line) => (
                    <th key={line} className="text-right py-1 px-1 tabular-nums">{line}+</th>
                  ))}
                  <th className="text-right py-1 pl-2 font-medium text-gray-500">Val</th>
                </tr>
              </thead>
              <tbody>
                {games.map((g, i) => {
                  const val = statForKey(g, focusStat);
                  return (
                    <tr key={i} className="border-b border-gray-900">
                      <td className="py-1 pr-2 text-gray-500">{g.gameDate.slice(5)}</td>
                      <td className="py-1 pr-2 text-gray-400">{g.home ? '' : '@'}{g.oppAbbr}</td>
                      {cols.map((line) => {
                        const hit = val >= line;
                        return (
                          <td key={line} className={`py-1 px-1 text-right tabular-nums ${hit ? 'text-green-500' : 'text-gray-700'}`}>
                            {hit ? '\u2714' : '\u2013'}
                          </td>
                        );
                      })}
                      <td className="py-1 pl-2 text-right font-semibold tabular-nums text-gray-200">{val}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// PropMatrix props
// ---------------------------------------------------------------------------

interface PropMatrixProps {
  rows: MatrixRow[];
  gradeDate: string;
  outcomeFilter: 'Over' | 'Under';
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function PropMatrix({ rows, gradeDate, outcomeFilter }: PropMatrixProps) {
  const [panelPlayer, setPanelPlayer] = useState<{
    playerId: number;
    playerName: string;
    focusStat: StatKey;
    gameId: string | null;
  } | null>(null);

  // Reserved for future under matrix support
  void outcomeFilter;

  type CellData = {
    price: number | null;
    compositeGrade: number | null;
    outcome: string | null;
    link: string | null;
  };
  type PlayerEntry = {
    playerId: number;
    playerName: string;
    gameId: string | null;
    cells: Record<number, CellData>;
  };
  type GameGroup = { label: string; players: PlayerEntry[] };
  type GroupData = { stat: StatKey; games: GameGroup[] };

  // Build game label map: gameId -> "AWAY @ HOME"
  const gameLabelMap = new Map<string, string>();
  for (const row of rows) {
    if (row.gameId && !gameLabelMap.has(row.gameId)) {
      gameLabelMap.set(
        row.gameId,
        row.awayTeamAbbr && row.homeTeamAbbr
          ? `${row.awayTeamAbbr} @ ${row.homeTeamAbbr}`
          : row.gameId
      );
    }
  }

  const groups: GroupData[] = [];

  for (const stat of GROUP_ORDER) {
    const statRows = rows.filter((r) => marketToStat(r.marketKey) === stat);
    if (statRows.length === 0) continue;

    const cols = MATRIX_COLS[stat];
    const colSet = new Set(cols);

    const gameMap = new Map<string, { playerMap: Map<number, PlayerEntry> }>();

    for (const row of statRows) {
      // DB stores N-0.5 thresholds (e.g. 4.5); round to integer "N+" display key
      const colKey = Math.round(row.lineValue);
      if (!colSet.has(colKey)) continue;

      const gameKey = row.gameId ?? 'unknown';
      if (!gameMap.has(gameKey)) gameMap.set(gameKey, { playerMap: new Map() });
      const gEntry = gameMap.get(gameKey)!;

      if (!gEntry.playerMap.has(row.playerId)) {
        gEntry.playerMap.set(row.playerId, {
          playerId: row.playerId,
          playerName: row.playerName,
          gameId: row.gameId,
          cells: {},
        });
      }
      const pEntry = gEntry.playerMap.get(row.playerId)!;

      // If standard and alternate both map to the same rounded key, keep the better grade
      const existing = pEntry.cells[colKey];
      if (!existing || (row.compositeGrade ?? -Infinity) > (existing.compositeGrade ?? -Infinity)) {
        pEntry.cells[colKey] = {
          price: row.overPrice,
          compositeGrade: row.compositeGrade,
          outcome: row.outcome,
          link: row.link,
        };
      }
    }

    const games: GameGroup[] = [];
    for (const [gameKey, gEntry] of gameMap) {
      const players = Array.from(gEntry.playerMap.values())
        .filter((p) => Object.keys(p.cells).length > 0)
        .sort((a, b) => a.playerName.localeCompare(b.playerName));
      if (players.length > 0) {
        games.push({ label: gameLabelMap.get(gameKey) ?? gameKey, players });
      }
    }

    if (games.length > 0) groups.push({ stat, games });
  }

  return (
    <>
      {/* Backdrop */}
      {panelPlayer && (
        <div className="fixed inset-0 z-40 bg-black/30" onClick={() => setPanelPlayer(null)} />
      )}

      {/* Slide-in panel */}
      {panelPlayer && (
        <PlayerPanel
          playerId={panelPlayer.playerId}
          playerName={panelPlayer.playerName}
          focusStat={panelPlayer.focusStat}
          gradeDate={gradeDate}
          gameId={panelPlayer.gameId}
          onClose={() => setPanelPlayer(null)}
        />
      )}

      <div className="space-y-8 pb-8">
        {groups.map(({ stat, games }) => {
          const cols = MATRIX_COLS[stat];
          return (
            <div key={stat}>
              {/* Group header */}
              <div className="flex items-center gap-3 mb-2">
                <span className="text-xs font-semibold text-gray-400 uppercase tracking-widest">
                  {GROUP_LABELS[stat]}
                </span>
                <div className="flex-1 h-px bg-gray-800" />
              </div>

              {games.map(({ label, players }) => (
                <div key={label} className="mb-4">
                  {games.length > 1 && (
                    <div className="text-xs text-gray-600 mb-1 ml-1">{label}</div>
                  )}

                  <div className="overflow-x-auto">
                    <table className="text-xs border-collapse">
                      <thead>
                        <tr className="text-gray-600">
                          <th className="text-left py-1 pr-4 font-normal min-w-[130px]">Player</th>
                          {cols.map((line) => (
                            <th key={line} className="text-right py-1 px-2 font-normal tabular-nums whitespace-nowrap min-w-[44px]">
                              {line}+
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {players.map((player) => (
                          <tr key={player.playerId} className="border-t border-gray-900 hover:bg-gray-900/30 transition-colors">
                            <td className="py-1.5 pr-4">
                              <button
                                className="text-gray-100 hover:text-blue-400 transition-colors text-left"
                                onClick={() => setPanelPlayer({
                                  playerId: player.playerId,
                                  playerName: player.playerName,
                                  focusStat: stat,
                                  gameId: player.gameId,
                                })}
                              >
                                {player.playerName}
                              </button>
                            </td>
                            {cols.map((line) => {
                              const cell = player.cells[line];
                              if (!cell) {
                                return (
                                  <td key={line} className="py-1.5 px-2 text-right text-gray-800 tabular-nums">
                                    &ndash;
                                  </td>
                                );
                              }
                              const won  = cell.outcome === 'Won';
                              const lost = cell.outcome === 'Lost';
                              const bg   = won ? 'bg-green-900/20' : lost ? 'bg-red-900/20' : '';
                              const text = (
                                <span className={`tabular-nums ${gradeColor(cell.compositeGrade)}`}>
                                  {fmtOdds(cell.price)}
                                </span>
                              );
                              return (
                                <td key={line} className={`py-1.5 px-2 text-right ${bg}`}>
                                  {cell.link && cell.outcome == null ? (
                                    <a href={cell.link} target="_blank" rel="noopener noreferrer" className="hover:text-blue-400 transition-colors">
                                      {text}
                                    </a>
                                  ) : text}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ))}
            </div>
          );
        })}

        {groups.length === 0 && (
          <div className="text-sm text-gray-500">No props match the current filters.</div>
        )}
      </div>
    </>
  );
}
