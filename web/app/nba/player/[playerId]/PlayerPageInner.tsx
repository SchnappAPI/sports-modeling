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

function computeSplit(rows: GameLogRow[]): SplitStats | null {
  const played = rows.filter((r) => !r.dnp);
  if (played.length === 0) return null;
  const n = played.length;
  return {
    games: n,
    pts:  played.reduce((s, r) => s + (r.pts  ?? 0), 0) / n,
    reb:  played.reduce((s, r) => s + (r.reb  ?? 0), 0) / n,
    ast:  played.reduce((s, r) => s + (r.ast  ?? 0), 0) / n,
    stl:  played.reduce((s, r) => s + (r.stl  ?? 0), 0) / n,
    blk:  played.reduce((s, r) => s + (r.blk  ?? 0), 0) / n,
    tov:  played.reduce((s, r) => s + (r.tov  ?? 0), 0) / n,
    min:  played.reduce((s, r) => s + (r.min  ?? 0), 0) / n,
    fg3m: played.reduce((s, r) => s + (r.fg3m ?? 0), 0) / n,
  };
}

function fmt1(val: number): string {
  return val.toFixed(1);
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

export default function PlayerPageInner({ playerId }: { playerId: string }) {
  const searchParams = useSearchParams();
  const [log, setLog] = useState<GameLogRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const backGameId = searchParams.get('gameId');
  const backTab = searchParams.get('tab') ?? 'stats';
  const opp = searchParams.get('opp') ?? '';
  const backHref = backGameId ? `/nba?gameId=${backGameId}&tab=${backTab}` : '/nba';

  useEffect(() => {
    fetch(`/api/player?playerId=${playerId}&games=100&sport=nba`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => setLog(data.log ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [playerId]);

  const played = useMemo(() => log.filter((r) => !r.dnp), [log]);

  const seasonSplit = useMemo(() => computeSplit(played), [played]);
  const last10Split = useMemo(() => computeSplit(played.slice(0, 10)), [played]);
  const vsOppSplit  = useMemo(
    () => opp ? computeSplit(played.filter((r) => r.opponentAbbr === opp)) : null,
    [played, opp]
  );

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
            {played.length} GP / {log.length} team games
          </span>
        )}
      </div>

      <div className="flex-1 px-4 py-4">
        {loading && <div className="text-sm text-gray-500">Loading game log...</div>}
        {error && <div className="text-sm text-red-400">Error: {error}</div>}

        {!loading && !error && log.length > 0 && (
          <>
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
                    <th className="text-left py-1.5 pr-3 font-medium">Date</th>
                    <th className="text-left py-1.5 pr-3 font-medium">Opp</th>
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
                  {log.map((row) => (
                    <tr
                      key={row.gameId}
                      className={['border-b border-gray-800', row.dnp ? 'opacity-40' : ''].join(' ')}
                    >
                      <td className="py-1.5 pr-3 text-gray-300">{row.gameDate}</td>
                      <td className="py-1.5 pr-3 text-gray-400">
                        {row.isHome ? '' : '@'}{row.opponentAbbr}
                      </td>
                      {row.dnp ? (
                        <td colSpan={10} className="py-1.5 px-2 text-gray-500 text-xs">DNP</td>
                      ) : (
                        <>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmtMin(row.min)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-100 font-medium">{fmt(row.pts)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.reb)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.ast)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.stl)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.blk)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.tov)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmtShooting(row.fgm, row.fga)}</td>
                          <td className="py-1.5 px-2 text-right text-gray-300">{fmtShooting(row.fg3m, row.fga)}</td>
                          <td className="py-1.5 pl-2 text-right text-gray-300">{fmtShooting(row.ftm, row.fta)}</td>
                        </>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        )}

        {!loading && !error && log.length === 0 && (
          <div className="text-sm text-gray-500">No game log available.</div>
        )}
      </div>
    </div>
  );
}
