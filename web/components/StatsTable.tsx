'use client';

import { useEffect, useState, useCallback } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

interface PlayerAvg {
  playerId: number;
  playerName: string;
  teamId: number;
  teamAbbr: string;
  starterStatus: string | null;
  games: number;
  avgPts: number | null;
  avgReb: number | null;
  avgAst: number | null;
  avgStl: number | null;
  avgBlk: number | null;
  avgTov: number | null;
  avgMin: number | null;
  avg3pm: number | null;
  avg3pa: number | null;
  avgFgm: number | null;
  avgFga: number | null;
  avgFtm: number | null;
  avgFta: number | null;
}

interface Props {
  gameId: string;
  homeTeamId: number;
  awayTeamId: number;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
  selectedDate: string;
}

function fmt(val: number | null | undefined, decimals = 1): string {
  if (val == null) return '-';
  return val.toFixed(decimals);
}

const PERIOD_OPTIONS = [
  { label: 'Full', value: 'full' },
  { label: '1Q',   value: '1Q' },
  { label: '2Q',   value: '2Q' },
  { label: '3Q',   value: '3Q' },
  { label: '4Q',   value: '4Q' },
  { label: 'OT',   value: 'OT' },
] as const;

const N_OPTIONS = [
  { label: 'L10',    value: '10' },
  { label: 'L20',    value: '20' },
  { label: 'L40',    value: '40' },
  { label: 'All',    value: 'all' },
  { label: 'vs Opp', value: 'opp' },
];

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

function TeamStatsTable({
  abbr,
  opponentAbbr,
  players,
  gameId,
  selectedDate,
  showAllStats,
}: {
  abbr: string;
  opponentAbbr: string;
  players: PlayerAvg[];
  gameId: string;
  selectedDate: string;
  showAllStats: boolean;
}) {
  const searchParams = useSearchParams();
  const tab = searchParams.get('tab') ?? 'stats';
  const [benchOpen, setBenchOpen] = useState(true);
  const [inactiveOpen, setInactiveOpen] = useState(false);

  const starters  = players.filter((p) => p.starterStatus === 'Starter');
  const bench     = players.filter((p) => p.starterStatus === 'Bench');
  const inactive  = players.filter((p) => p.starterStatus === 'Inactive');
  const noLineup  = players.filter(
    (p) => p.starterStatus == null || (p.starterStatus !== 'Starter' && p.starterStatus !== 'Bench' && p.starterStatus !== 'Inactive')
  );
  const hasLineup = players.some((p) => p.starterStatus != null);

  // compact:   Player GP MIN PTS 3PM REB AST PRA PR PA RA = 11 cols
  // all-stats: Player GP MIN PTS FGM FGA 3PM 3PA FTM FTA REB AST PRA PR PA RA STL BLK TOV = 21 cols
  const colSpanTotal = showAllStats ? 21 : 11;

  const renderRow = (p: PlayerAvg, dimmed = false) => {
    const pra = (p.avgPts ?? 0) + (p.avgReb ?? 0) + (p.avgAst ?? 0);
    const pr  = (p.avgPts ?? 0) + (p.avgReb ?? 0);
    const pa  = (p.avgPts ?? 0) + (p.avgAst ?? 0);
    const ra  = (p.avgReb ?? 0) + (p.avgAst ?? 0);
    return (
      <tr key={p.playerId} className={['border-b border-gray-800', dimmed ? 'opacity-40' : ''].join(' ')}>
        <td className="py-1.5 pr-3">
          <Link
            href={`/nba/player/${p.playerId}?gameId=${gameId}&tab=${tab}&opp=${opponentAbbr}&date=${selectedDate}`}
            className="text-gray-100 hover:text-blue-400 transition-colors"
          >
            {p.playerName}
          </Link>
        </td>
        <td className="py-1.5 px-2 text-right text-gray-500 text-xs">{p.games}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgMin)}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgPts)}</td>
        {showAllStats ? (
          <>
            {/* All Stats: FGM FGA 3PM 3PA FTM FTA — all separate columns */}
            <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avgFgm)}</td>
            <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avgFga)}</td>
            <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avg3pm)}</td>
            <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avg3pa)}</td>
            <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avgFtm)}</td>
            <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avgFta)}</td>
          </>
        ) : (
          <td className="py-1.5 px-2 text-right text-gray-400 tabular-nums">{fmt(p.avg3pm)}</td>
        )}
        <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgReb)}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgAst)}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{pra > 0 ? pra.toFixed(1) : '-'}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{pr  > 0 ? pr.toFixed(1)  : '-'}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{pa  > 0 ? pa.toFixed(1)  : '-'}</td>
        <td className="py-1.5 px-2 text-right text-gray-300">{ra  > 0 ? ra.toFixed(1)  : '-'}</td>
        {showAllStats && (
          <>
            <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgStl)}</td>
            <td className="py-1.5 px-2 text-right text-gray-300">{fmt(p.avgBlk)}</td>
            <td className="py-1.5 pl-2 text-right text-gray-300">{fmt(p.avgTov)}</td>
          </>
        )}
      </tr>
    );
  };

  const collapsibleSection = (
    label: string,
    count: number,
    open: boolean,
    toggle: () => void,
    rows: PlayerAvg[],
    dimmed = false,
  ) => (
    <>
      <tr
        className="border-b border-gray-800 cursor-pointer select-none"
        onClick={toggle}
      >
        <td
          colSpan={colSpanTotal}
          className="py-1.5 text-xs text-gray-500 font-semibold uppercase tracking-wider"
        >
          <span className="mr-1.5 text-gray-600">{open ? '\u25be' : '\u25b8'}</span>
          {label} ({count})
        </td>
      </tr>
      {open && rows.map((p) => renderRow(p, dimmed))}
    </>
  );

  return (
    <div className="overflow-x-auto">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{abbr}</div>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-xs text-gray-500 border-b border-gray-800">
            <th className="text-left py-1.5 pr-3 font-medium">Player</th>
            <th className="text-right py-1.5 px-2 font-medium">GP</th>
            <th className="text-right py-1.5 px-2 font-medium">MIN</th>
            <th className="text-right py-1.5 px-2 font-medium">PTS</th>
            {showAllStats ? (
              <>
                <th className="text-right py-1.5 px-2 font-medium">FGM</th>
                <th className="text-right py-1.5 px-2 font-medium">FGA</th>
                <th className="text-right py-1.5 px-2 font-medium">3PM</th>
                <th className="text-right py-1.5 px-2 font-medium">3PA</th>
                <th className="text-right py-1.5 px-2 font-medium">FTM</th>
                <th className="text-right py-1.5 px-2 font-medium">FTA</th>
              </>
            ) : (
              <th className="text-right py-1.5 px-2 font-medium">3PM</th>
            )}
            <th className="text-right py-1.5 px-2 font-medium">REB</th>
            <th className="text-right py-1.5 px-2 font-medium">AST</th>
            <th className="text-right py-1.5 px-2 font-medium">PRA</th>
            <th className="text-right py-1.5 px-2 font-medium">PR</th>
            <th className="text-right py-1.5 px-2 font-medium">PA</th>
            <th className="text-right py-1.5 px-2 font-medium">RA</th>
            {showAllStats && (
              <>
                <th className="text-right py-1.5 px-2 font-medium">STL</th>
                <th className="text-right py-1.5 px-2 font-medium">BLK</th>
                <th className="text-right py-1.5 pl-2 font-medium">TOV</th>
              </>
            )}
          </tr>
        </thead>
        <tbody>
          {hasLineup ? (
            <>
              {starters.map((p) => renderRow(p))}
              {bench.length > 0 && collapsibleSection('Bench', bench.length, benchOpen, () => setBenchOpen((o) => !o), bench)}
              {noLineup.length > 0 && collapsibleSection('Bench', noLineup.length, benchOpen, () => setBenchOpen((o) => !o), noLineup)}
              {inactive.length > 0 && collapsibleSection('Inactive', inactive.length, inactiveOpen, () => setInactiveOpen((o) => !o), inactive, true)}
            </>
          ) : (
            players.map((p) => renderRow(p))
          )}
        </tbody>
      </table>
    </div>
  );
}

export default function StatsTable({ gameId, homeTeamId, awayTeamId, homeTeamAbbr, awayTeamAbbr, selectedDate }: Props) {
  const [activePeriods, setActivePeriods] = useState<Set<string>>(new Set(['full']));
  const [nGames, setNGames] = useState('20');
  const [players, setPlayers] = useState<PlayerAvg[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showAllStats, setShowAllStats] = useState(false);

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

  const fetchStats = useCallback(() => {
    setLoading(true);
    setError(null);
    const periodsParam = activePeriods.has('full') ? '' : Array.from(activePeriods).join(',');
    const url = new URL('/api/team-averages', window.location.origin);
    url.searchParams.set('homeTeamId', String(homeTeamId));
    url.searchParams.set('awayTeamId', String(awayTeamId));
    url.searchParams.set('context', nGames);
    url.searchParams.set('gameId', gameId);
    if (periodsParam) url.searchParams.set('periods', periodsParam);
    if (nGames === 'opp') {
      url.searchParams.set('opp', `${awayTeamAbbr},${homeTeamAbbr}`);
    }
    fetch(url.toString())
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((data) => setPlayers(data.players ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [homeTeamId, awayTeamId, nGames, activePeriods, gameId, homeTeamAbbr, awayTeamAbbr]);

  useEffect(() => { fetchStats(); }, [fetchStats]);

  const homePlayers = players.filter((p) => p.teamAbbr === homeTeamAbbr);
  const awayPlayers = players.filter((p) => p.teamAbbr === awayTeamAbbr);
  const otherAbbrs  = Array.from(new Set(
    players.filter((p) => p.teamAbbr !== homeTeamAbbr && p.teamAbbr !== awayTeamAbbr).map((p) => p.teamAbbr)
  ));

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-4">
        <div className="flex items-center gap-1">
          {PERIOD_OPTIONS.map(({ label, value }) => (
            <button
              key={value}
              onClick={() => togglePeriod(value)}
              className={[
                'px-2.5 py-1 text-xs font-medium rounded transition-colors',
                activePeriods.has(value) ? 'bg-blue-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
              ].join(' ')}
            >
              {label}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          {N_OPTIONS.map(({ label, value }) => (
            <button
              key={value}
              onClick={() => setNGames(value)}
              className={[
                'px-2.5 py-1 text-xs font-medium rounded transition-colors',
                nGames === value ? 'bg-gray-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700',
              ].join(' ')}
            >
              {label}
            </button>
          ))}
        </div>
        <StatsToggle showAll={showAllStats} onToggle={() => setShowAllStats((v) => !v)} />
      </div>

      {loading && <div className="text-sm text-gray-500 py-2">Loading stats...</div>}
      {error   && <div className="text-sm text-red-400 py-2">Error: {error}</div>}

      {!loading && !error && (
        <div className="flex flex-col gap-6">
          {awayPlayers.length > 0 && (
            <TeamStatsTable abbr={awayTeamAbbr} opponentAbbr={homeTeamAbbr} players={awayPlayers} gameId={gameId} selectedDate={selectedDate} showAllStats={showAllStats} />
          )}
          {homePlayers.length > 0 && (
            <TeamStatsTable abbr={homeTeamAbbr} opponentAbbr={awayTeamAbbr} players={homePlayers} gameId={gameId} selectedDate={selectedDate} showAllStats={showAllStats} />
          )}
          {otherAbbrs.map((abbr) => (
            <TeamStatsTable
              key={abbr} abbr={abbr} opponentAbbr=''
              players={players.filter((p) => p.teamAbbr === abbr)}
              gameId={gameId} selectedDate={selectedDate} showAllStats={showAllStats}
            />
          ))}
          {players.length === 0 && <div className="text-sm text-gray-500">No stats available.</div>}
        </div>
      )}
    </div>
  );
}
