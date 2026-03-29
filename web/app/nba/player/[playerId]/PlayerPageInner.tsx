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
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
}

interface GradeLine {
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

interface PlayerInfo {
  oppTeamId: number | null;
  position: string | null;
  playerName: string | null;
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
  const gameMeta = new Map<string, Pick<GameSummary, 'gameDate' | 'opponentAbbr' | 'isHome' | 'dnp' | 'started'>>();
  for (const r of rows) {
    if (!gameMeta.has(r.gameId)) {
      gameOrder.push(r.gameId);
      gameMeta.set(r.gameId, {
        gameDate:     r.gameDate,
        opponentAbbr: r.opponentAbbr,
        isHome:       r.isHome,
        dnp:          r.dnp,
        started:      r.started,
      });
    }
  }

  const filtered = selectedPeriods.size === 0
    ? rows
    : rows.filter((r) => selectedPeriods.has(r.period as QuarterKey));

  const totals = new Map<string, Omit<GameSummary, 'gameId' | 'gameDate' | 'opponentAbbr' | 'isHome' | 'dnp' | 'started'>>();
  for (const r of filtered) {
    if (r.dnp) continue;
    const t = totals.get(r.gameId) ?? { pts:0, reb:0, ast:0, stl:0, blk:0, tov:0, min:0, fg3m:0, fgm:0, fga:0, ftm:0, fta:0 };
    t.pts  += r.pts  ?? 0;
    t.reb  += r.reb  ?? 0;
    t.ast  += r.ast  ?? 0;
    t.stl  += r.stl  ?? 0;
    t.blk  += r.blk  ?? 0;
    t.tov  += r.tov  ?? 0;
    t.min  += r.min  ?? 0;
    t.fg3m += r.fg3m ?? 0;
    t.fgm  += r.fgm  ?? 0;
    t.fga  += r.fga  ?? 0;
    t.ftm  += r.ftm  ?? 0;
    t.fta  += r.fta  ?? 0;
    totals.set(r.gameId, t);
  }

  const ZERO = { pts:0, reb:0, ast:0, stl:0, blk:0, tov:0, min:0, fg3m:0, fgm:0, fga:0, ftm:0, fta:0 };
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
  min: number; fg3m: number; fgm: number; fga: number; ftm: number; fta: number;
}

function computeSplit(summaries: GameSummary[], opp: string | null): Record<SplitKey, SplitStats> {
  const zero = (): SplitStats => ({ gp:0, pts:0, reb:0, ast:0, stl:0, blk:0, tov:0, min:0, fg3m:0, fgm:0, fga:0, ftm:0, fta:0 });
  const acc  = { season: zero(), l10: zero(), opp: zero() };

  const played = summaries.filter((g) => !g.dnp);
  const l10    = played.slice(0, 10);
  const vs     = opp ? played.filter((g) => g.opponentAbbr === opp) : [];

  function add(target: SplitStats, g: GameSummary) {
    target.gp++;  target.pts += g.pts; target.reb += g.reb; target.ast += g.ast;
    target.stl += g.stl; target.blk += g.blk; target.tov += g.tov; target.min += g.min;
    target.fg3m += g.fg3m; target.fgm += g.fgm; target.fga += g.fga;
    target.ftm += g.ftm;  target.fta += g.fta;
  }
  played.forEach((g) => add(acc.season, g));
  l10.forEach((g)    => add(acc.l10, g));
  vs.forEach((g)     => add(acc.opp, g));
  return acc;
}

function avg(total: number, gp: number): string {
  if (gp === 0) return '-';
  return (total / gp).toFixed(1);
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
// Component
// ---------------------------------------------------------------------------

export default function PlayerPageInner({ playerId }: { playerId: string }) {
  const router       = useRouter();
  const searchParams = useSearchParams();

  const backGameId   = searchParams.get('gameId');
  const backTab      = searchParams.get('tab') ?? 'boxscore';
  const backDate     = searchParams.get('date');
  const oppParam     = searchParams.get('opp');

  const backHref = (() => {
    const p = new URLSearchParams();
    if (backGameId) p.set('gameId', backGameId);
    if (backTab)    p.set('tab', backTab);
    if (backDate)   p.set('date', backDate);
    const qs = p.toString();
    return qs ? `/nba?${qs}` : '/nba';
  })();

  const [log, setLog]             = useState<GameLogRow[]>([]);
  const [grades, setGrades]       = useState<GradeLine[]>([]);
  const [playerInfo, setPlayerInfo] = useState<PlayerInfo>({ oppTeamId: null, position: null, playerName: null });
  const [loading, setLoading]     = useState(true);
  const [error, setError]         = useState<string | null>(null);
  const [selectedPeriods, setSelectedPeriods] = useState<Set<QuarterKey>>(new Set());
  const [teamPlayers, setTeamPlayers] = useState<{playerId: number; playerName: string}[]>([]);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setLog([]);
    setGrades([]);
    setPlayerInfo({ oppTeamId: null, position: null, playerName: null });
    setSelectedPeriods(new Set());

    Promise.all([
      fetch(`/api/player?playerId=${playerId}&lastN=9999&sport=nba`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
      fetch(`/api/player-grades?playerId=${playerId}`)
        .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); }),
    ])
      .then(([playerData, gradeData]) => {
        setLog(playerData.log ?? []);
        setGrades(gradeData.grades ?? []);

        // /api/player now returns playerName, position, and lastOppTeamId directly.
        // Use these as the baseline for the matchup section — always available.
        setPlayerInfo({
          playerName: playerData.playerName ?? null,
          position:   playerData.position   ?? null,
          oppTeamId:  playerData.lastOppTeamId ?? null,
        });

        // If we have a gameId (arrived from a game context), override with the
        // game-specific opponent so the matchup reflects today's actual matchup
        // rather than the most recently played opponent.
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

          fetch(`/api/team-players?gameId=${backGameId}`)
            .then((r) => r.json())
            .then((d) => setTeamPlayers(d.players ?? []))
            .catch(() => {});
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [playerId]);

  // Grade lookup: gameId -> marketKey -> lineValue
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

  const showPropColors = selectedPeriods.size === 0;

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

  // Display name: from the API response (always available), not from log rows.
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
              <th className="text-left px-4 py-2 font-medium">Split</th>
              <th className="text-right px-2 py-2 font-medium">GP</th>
              <th className="text-right px-2 py-2 font-medium">MIN</th>
              <th className="text-right px-2 py-2 font-medium">PTS</th>
              <th className="text-right px-2 py-2 font-medium">REB</th>
              <th className="text-right px-2 py-2 font-medium">AST</th>
              <th className="text-right px-2 py-2 font-medium">STL</th>
              <th className="text-right px-2 py-2 font-medium">BLK</th>
              <th className="text-right px-2 py-2 font-medium">TOV</th>
              <th className="text-right px-2 py-2 font-medium">FG%</th>
              <th className="text-right px-2 py-2 font-medium">3P%</th>
              <th className="text-right px-2 py-2 font-medium">FT%</th>
            </tr>
          </thead>
          <tbody>
            {splitLabels.map(({ key, label }) => {
              const s = splits[key];
              return (
                <tr key={key} className="border-t border-gray-800">
                  <td className="px-4 py-2 text-gray-400 font-medium">{label}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{s.gp}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{fmtMin(s.min, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{avg(s.pts, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{avg(s.reb, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{avg(s.ast, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{avg(s.stl, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{avg(s.blk, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{avg(s.tov, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{fmtShoot(s.fgm, s.fga, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{fmtShoot(s.fg3m, s.fg3m > 0 ? s.fg3m / s.gp * s.gp : s.fga, s.gp)}</td>
                  <td className="px-2 py-2 text-right text-gray-300">{fmtShoot(s.ftm, s.fta, s.gp)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Matchup defense — always shown when position and opponent are known */}
      {showMatchup && (
        <MatchupDefense
          oppTeamId={playerInfo.oppTeamId!}
          position={playerInfo.position!}
          highlightMarket={todayMarket}
        />
      )}

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
          <thead>
            <tr className="text-xs text-gray-500 border-b border-gray-800">
              <th className="text-left px-4 py-1.5 font-medium">Date</th>
              <th className="text-left px-2 py-1.5 font-medium">Opp</th>
              <th className="text-right px-2 py-1.5 font-medium">Str</th>
              <th className="text-right px-2 py-1.5 font-medium">MIN</th>
              <th className="text-right px-2 py-1.5 font-medium">PTS</th>
              <th className="text-right px-2 py-1.5 font-medium">REB</th>
              <th className="text-right px-2 py-1.5 font-medium">AST</th>
              <th className="text-right px-2 py-1.5 font-medium">STL</th>
              <th className="text-right px-2 py-1.5 font-medium">BLK</th>
              <th className="text-right px-2 py-1.5 font-medium">TOV</th>
              <th className="text-right px-2 py-1.5 font-medium">3PM</th>
              <th className="text-right px-2 py-1.5 font-medium">FG</th>
              <th className="text-right px-4 py-1.5 font-medium">FT</th>
            </tr>
          </thead>
          <tbody>
            {summaries.map((g) => {
              if (g.dnp) {
                return (
                  <tr key={g.gameId} className="border-b border-gray-800 opacity-40">
                    <td className="px-4 py-1.5 text-gray-400">{g.gameDate.slice(5)}</td>
                    <td className="px-2 py-1.5 text-gray-400">
                      {g.isHome ? '' : '@'}{g.opponentAbbr}
                    </td>
                    <td colSpan={11} className="px-2 py-1.5 text-xs text-gray-600">DNP</td>
                  </tr>
                );
              }
              const ptsLine = getLineCls(g.gameId, 'player_points', g.pts);
              const rebLine = getLineCls(g.gameId, 'player_rebounds', g.reb);
              const astLine = getLineCls(g.gameId, 'player_assists', g.ast);
              const stlLine = getLineCls(g.gameId, 'player_steals', g.stl);
              const blkLine = getLineCls(g.gameId, 'player_blocks', g.blk);
              const fg3Line = getLineCls(g.gameId, 'player_threes', g.fg3m);
              const fmtM = (min: number) => {
                const m = Math.floor(min);
                const s = Math.round((min - m) * 60);
                return `${m}:${s.toString().padStart(2, '0')}`;
              };
              const fmtS = (made: number, att: number) =>
                att === 0 ? '-' : `${made}/${att}`;
              const starterBadge = g.started === true
                ? <span className="text-blue-500 font-medium">S</span>
                : g.started === false
                ? <span className="text-gray-600">B</span>
                : null;
              return (
                <tr key={g.gameId} className="border-b border-gray-800">
                  <td className="px-4 py-1.5 text-gray-400">{g.gameDate.slice(5)}</td>
                  <td className="px-2 py-1.5 text-gray-400">
                    {g.isHome ? '' : '@'}{g.opponentAbbr}
                  </td>
                  <td className="px-2 py-1.5 text-right text-xs">{starterBadge}</td>
                  <td className="px-2 py-1.5 text-right text-gray-300">{fmtM(g.min)}</td>
                  <td className={`px-2 py-1.5 text-right ${ptsLine}`}>{g.pts}</td>
                  <td className={`px-2 py-1.5 text-right ${rebLine}`}>{g.reb}</td>
                  <td className={`px-2 py-1.5 text-right ${astLine}`}>{g.ast}</td>
                  <td className={`px-2 py-1.5 text-right ${stlLine}`}>{g.stl}</td>
                  <td className={`px-2 py-1.5 text-right ${blkLine}`}>{g.blk}</td>
                  <td className="px-2 py-1.5 text-right text-gray-300">{g.tov}</td>
                  <td className={`px-2 py-1.5 text-right ${fg3Line}`}>{g.fg3m}</td>
                  <td className="px-2 py-1.5 text-right text-gray-300">{fmtS(g.fgm, g.fga)}</td>
                  <td className="px-4 py-1.5 text-right text-gray-300">{fmtS(g.ftm, g.fta)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
