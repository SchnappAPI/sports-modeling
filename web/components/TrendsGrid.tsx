'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';

// Renders the Trends Grid: per-player tier lines + standard FanDuel line
// + per-game stat history for the selected market. Fed by /api/tier-grid.
//
// See /docs/ROADMAP.md :: NBA Trends Grid and ADR-20260423-1 for the
// tier system this UI surfaces.

interface GameLogEntry {
  gameId: string;
  gameDate: string;
  oppTricode: string;
  minutes: number | null;
  stat: number;
  hit: boolean | null;
}

interface TrendPlayer {
  playerId: number;
  playerName: string;
  teamTricode: string | null;
  position: string | null;
  lineupStatus: string | null;
  starterStatus: string | null;
  compositeGrade: number | null;
  kdeWindow: number | null;
  blowoutDampened: boolean;
  safeLine: number | null;
  safeProb: number | null;
  valueLine: number | null;
  valueProb: number | null;
  highriskLine: number | null;
  highriskProb: number | null;
  highriskPrice: number | null;
  lottoLine: number | null;
  lottoProb: number | null;
  lottoPrice: number | null;
  standardLine: number | null;
  standardPrice: number | null;
  gameLog: GameLogEntry[];
}

interface TrendsData {
  gameId: string;
  marketKey: string;
  gameDate: string;
  home: { teamId: number; teamAbbr: string };
  away: { teamId: number; teamAbbr: string };
  players: TrendPlayer[];
}

interface Props {
  gameId: string;
  selectedDate: string;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
}

// Markets the Trends Grid supports, in the order they appear in the toggle.
const STAT_OPTIONS = [
  { key: 'player_points',                  label: 'PTS' },
  { key: 'player_rebounds',                label: 'REB' },
  { key: 'player_assists',                 label: 'AST' },
  { key: 'player_threes',                  label: '3PM' },
  { key: 'player_points_rebounds_assists', label: 'PRA' },
  { key: 'player_points_rebounds',         label: 'PR'  },
  { key: 'player_points_assists',          label: 'PA'  },
  { key: 'player_rebounds_assists',        label: 'RA'  },
] as const;
type MarketKey = (typeof STAT_OPTIONS)[number]['key'];

const WINDOW_OPTIONS = [
  { key: '10',  label: '10G' },
  { key: '30',  label: '30G' },
  { key: 'all', label: 'All' },
] as const;
type WindowKey = (typeof WINDOW_OPTIONS)[number]['key'];

function isMarketKey(s: string | null): s is MarketKey {
  return s !== null && STAT_OPTIONS.some(o => o.key === s);
}
function isWindowKey(s: string | null): s is WindowKey {
  return s !== null && WINDOW_OPTIONS.some(o => o.key === s);
}

// Format a number cell; null -> em dash, probability as percentage, etc.
function fmtLine(n: number | null): string {
  if (n === null || n === undefined) return '\u2014';
  return n.toFixed(1);
}
function fmtProb(n: number | null): string {
  if (n === null || n === undefined) return '';
  return `${Math.round(n * 100)}%`;
}
function fmtPrice(n: number | null): string {
  if (n === null || n === undefined) return '';
  return n > 0 ? `+${n}` : `${n}`;
}

// Color the player's composite grade cell using calibrated thresholds
// for the new 40/40/20 formula (ADR-20260423-1). Empirical hit rates:
//   80+ -> 74.8-82.4%  (green)
//   60-80 -> 60-74%    (yellow)
//   <60 -> below 60%   (gray / red at extremes)
function gradeColor(grade: number | null): string {
  if (grade === null) return 'text-gray-500';
  if (grade >= 80) return 'text-green-400';
  if (grade >= 60) return 'text-yellow-400';
  if (grade >= 40) return 'text-gray-400';
  return 'text-red-400';
}

// Stat color vs the posted standard line.
function statCls(stat: number, standardLine: number | null): string {
  if (standardLine === null) return 'text-gray-500';
  if (stat > standardLine)  return 'text-green-400';
  if (stat < standardLine)  return 'text-red-400';
  return 'text-yellow-400';
}

function TeamGroup({
  teamAbbr,
  players,
  visibleGameCount,
  onPlayerClick,
}: {
  teamAbbr: string;
  players: TrendPlayer[];
  visibleGameCount: number;
  onPlayerClick: (playerId: number) => string;
}) {
  if (players.length === 0) return null;

  const starters = players.filter(p => p.starterStatus === 'Starter');
  const bench    = players.filter(p => p.starterStatus === 'Bench');
  const inactive = players.filter(p => p.starterStatus === 'Inactive');
  const other    = players.filter(p => !p.starterStatus);

  const sectionHeader = (label: string, count: number) => (
    <tr key={`hdr-${teamAbbr}-${label}`}>
      <td
        colSpan={6 + visibleGameCount}
        className="pt-3 pb-0.5 text-xs text-gray-600 font-semibold uppercase tracking-wider sticky left-0 bg-gray-950"
      >
        {label} {count > 0 && <span className="text-gray-700">({count})</span>}
      </td>
    </tr>
  );

  const row = (p: TrendPlayer, dimmed = false) => {
    const rowCls = [
      'border-b border-gray-900',
      dimmed ? 'opacity-40' : '',
    ].join(' ');
    return (
      <tr key={p.playerId} className={rowCls}>
        <td className="py-1.5 pr-3 sticky left-0 bg-gray-950 z-10">
          <Link href={onPlayerClick(p.playerId)} className="text-gray-200 hover:text-blue-400 whitespace-nowrap">
            {p.playerName}
          </Link>
          {p.blowoutDampened && (
            <span className="ml-1 text-xs text-orange-500" title="Blowout risk dampening applied">&#9888;</span>
          )}
        </td>
        <td className={`py-1.5 pr-3 text-right tabular-nums font-semibold ${gradeColor(p.compositeGrade)}`}>
          {p.compositeGrade !== null ? p.compositeGrade.toFixed(0) : '\u2014'}
        </td>
        <td className="py-1.5 pr-3 text-right tabular-nums text-gray-300">
          {fmtLine(p.standardLine)}
        </td>
        <td className="py-1.5 pr-3 text-right tabular-nums text-green-400" title={p.safeProb !== null ? `${fmtProb(p.safeProb)} probability` : ''}>
          {fmtLine(p.safeLine)}
        </td>
        <td className="py-1.5 pr-3 text-right tabular-nums text-yellow-300" title={p.valueProb !== null ? `${fmtProb(p.valueProb)} probability` : ''}>
          {fmtLine(p.valueLine)}
        </td>
        <td className="py-1.5 pr-3 text-right tabular-nums text-orange-400" title={p.highriskPrice !== null ? `${fmtProb(p.highriskProb)} @ ${fmtPrice(p.highriskPrice)}` : ''}>
          {p.highriskLine !== null ? fmtLine(p.highriskLine) : '\u2014'}
        </td>
        <td className="py-1.5 pr-3 text-right tabular-nums text-purple-400" title={p.lottoPrice !== null ? `${fmtProb(p.lottoProb)} @ ${fmtPrice(p.lottoPrice)}` : ''}>
          {p.lottoLine !== null ? fmtLine(p.lottoLine) : '\u2014'}
        </td>
        {p.gameLog.slice(0, visibleGameCount).map((g) => (
          <td
            key={g.gameId}
            className={`py-1.5 px-2 text-right tabular-nums ${statCls(g.stat, p.standardLine)}`}
            title={`${g.gameDate} vs ${g.oppTricode} (${g.minutes !== null ? g.minutes.toFixed(0) : '-'} min)`}
          >
            {g.stat.toFixed(0)}
          </td>
        ))}
        {/* Pad short game logs so the row keeps width */}
        {Array.from({ length: Math.max(0, visibleGameCount - p.gameLog.length) }).map((_, i) => (
          <td key={`pad-${p.playerId}-${i}`} className="py-1.5 px-2 text-right text-gray-700">&ndash;</td>
        ))}
      </tr>
    );
  };

  return (
    <>
      <tr>
        <td
          colSpan={6 + visibleGameCount}
          className="pt-4 pb-1 text-sm font-bold text-blue-300 uppercase tracking-wide sticky left-0 bg-gray-950"
        >
          {teamAbbr}
        </td>
      </tr>
      {starters.length > 0 && sectionHeader('Starters', starters.length)}
      {starters.map((p) => row(p))}
      {bench.length > 0 && sectionHeader('Bench', bench.length)}
      {bench.map((p) => row(p))}
      {other.length > 0 && sectionHeader('Other', other.length)}
      {other.map((p) => row(p))}
      {inactive.length > 0 && sectionHeader('Out / Inactive', inactive.length)}
      {inactive.map((p) => row(p, true))}
    </>
  );
}

export default function TrendsGrid({
  gameId,
  selectedDate,
  homeTeamAbbr,
  awayTeamAbbr,
}: Props) {
  const router      = useRouter();
  const searchParams = useSearchParams();

  const rawMarket  = searchParams.get('stat');
  const rawWindow  = searchParams.get('window');
  const market: MarketKey  = isMarketKey(rawMarket)   ? rawMarket  : 'player_points';
  const win:    WindowKey  = isWindowKey(rawWindow)   ? rawWindow  : '30';

  const [data, setData]       = useState<TrendsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/tier-grid?gameId=${gameId}&market=${market}&window=${win}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: TrendsData) => setData(d))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId, market, win]);

  function setParam(key: string, value: string) {
    const params = new URLSearchParams(searchParams.toString());
    params.set(key, value);
    router.replace(`/nba?${params.toString()}`);
  }

  function playerHref(playerId: number): string {
    const params = new URLSearchParams();
    params.set('gameId', gameId);
    params.set('tab', 'trends');
    params.set('stat', market);
    if (selectedDate) params.set('date', selectedDate);
    return `/nba/player/${playerId}?${params.toString()}`;
  }

  if (loading) return <div className="text-sm text-gray-500 py-6">Loading tier lines...</div>;
  if (error)   return <div className="text-sm text-red-400 py-6">Error: {error}</div>;
  if (!data || data.players.length === 0) {
    return <div className="text-sm text-gray-500 py-6">No tier data available for this game yet.</div>;
  }

  const visibleGameCount = win === 'all' ? 82 : parseInt(win, 10);
  const awayPlayers = data.players.filter((p) => p.teamTricode === awayTeamAbbr);
  const homePlayers = data.players.filter((p) => p.teamTricode === homeTeamAbbr);
  const unknownTeamPlayers = data.players.filter(
    (p) => p.teamTricode !== awayTeamAbbr && p.teamTricode !== homeTeamAbbr
  );

  return (
    <div>
      {/* Stat + Window toggles */}
      <div className="flex items-center flex-wrap gap-2 mb-3">
        <div className="flex items-center gap-1 mr-4">
          <span className="text-xs text-gray-500 uppercase tracking-wider mr-1">Stat</span>
          {STAT_OPTIONS.map((opt) => (
            <button
              key={opt.key}
              onClick={() => setParam('stat', opt.key)}
              className={[
                'px-2 py-1 text-xs rounded border transition-colors',
                market === opt.key
                  ? 'bg-blue-900 border-blue-700 text-blue-200'
                  : 'border-gray-800 text-gray-400 hover:text-gray-200 hover:border-gray-600',
              ].join(' ')}
            >
              {opt.label}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-xs text-gray-500 uppercase tracking-wider mr-1">Window</span>
          {WINDOW_OPTIONS.map((opt) => (
            <button
              key={opt.key}
              onClick={() => setParam('window', opt.key)}
              className={[
                'px-2 py-1 text-xs rounded border transition-colors',
                win === opt.key
                  ? 'bg-blue-900 border-blue-700 text-blue-200'
                  : 'border-gray-800 text-gray-400 hover:text-gray-200 hover:border-gray-600',
              ].join(' ')}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* Legend */}
      <div className="text-xs text-gray-500 mb-2">
        <span className="text-gray-400">Tier lines:</span>{' '}
        <span className="text-green-400">Safe</span> &ge;80% /{' '}
        <span className="text-yellow-300">Value</span> &ge;58% /{' '}
        <span className="text-orange-400">HR</span> &ge;28% @ +150 /{' '}
        <span className="text-purple-400">Lotto</span> &ge;7% @ +400
      </div>

      {/* Scrollable grid */}
      <div className="overflow-x-auto">
        <table className="text-sm border-collapse">
          <thead>
            <tr className="border-b border-gray-800 text-xs text-gray-500 uppercase tracking-wider">
              <th className="py-2 pr-3 text-left sticky left-0 bg-gray-950 z-20">Player</th>
              <th className="py-2 pr-3 text-right">Grade</th>
              <th className="py-2 pr-3 text-right">Line</th>
              <th className="py-2 pr-3 text-right text-green-500">Safe</th>
              <th className="py-2 pr-3 text-right text-yellow-500">Value</th>
              <th className="py-2 pr-3 text-right text-orange-500">HR</th>
              <th className="py-2 pr-3 text-right text-purple-500">Lotto</th>
              {/* Game column headers */}
              {(() => {
                const allGames = data.players.flatMap((p) => p.gameLog.slice(0, visibleGameCount));
                const uniqueGames = new Map<string, GameLogEntry>();
                for (const g of allGames) {
                  if (!uniqueGames.has(g.gameId)) uniqueGames.set(g.gameId, g);
                }
                const sorted = [...uniqueGames.values()]
                  .sort((a, b) => (a.gameDate > b.gameDate ? -1 : 1))
                  .slice(0, visibleGameCount);
                // Note: per-player game columns may not align 1:1 across players since
                // each player has their own game log. This header shows representative
                // dates based on the most games-played slice.
                return sorted.map((g) => (
                  <th
                    key={g.gameId}
                    className="py-2 px-2 text-right tabular-nums whitespace-nowrap"
                    title={g.gameDate}
                  >
                    {g.gameDate.slice(5).replace('-', '/')}
                  </th>
                ));
              })()}
            </tr>
          </thead>
          <tbody>
            <TeamGroup
              teamAbbr={awayTeamAbbr}
              players={awayPlayers}
              visibleGameCount={visibleGameCount}
              onPlayerClick={playerHref}
            />
            <TeamGroup
              teamAbbr={homeTeamAbbr}
              players={homePlayers}
              visibleGameCount={visibleGameCount}
              onPlayerClick={playerHref}
            />
            {unknownTeamPlayers.length > 0 && (
              <TeamGroup
                teamAbbr="Other"
                players={unknownTeamPlayers}
                visibleGameCount={visibleGameCount}
                onPlayerClick={playerHref}
              />
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
