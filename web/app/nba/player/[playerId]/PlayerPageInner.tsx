'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

interface GameLogRow {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
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
  const [playerName, setPlayerName] = useState<string>('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const backGameId = searchParams.get('gameId');
  const backTab = searchParams.get('tab') ?? 'stats';
  const backHref = backGameId
    ? `/nba?gameId=${backGameId}&tab=${backTab}`
    : '/nba';

  useEffect(() => {
    fetch(`/api/player?playerId=${playerId}&games=20&sport=nba`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => {
        const rows: GameLogRow[] = data.log ?? [];
        setLog(rows);
        if (rows.length > 0) {
          // Player name isn't in the log rows directly, use the API name field if present
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [playerId]);

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3">
        <Link
          href={backHref}
          className="text-gray-400 hover:text-gray-200 text-sm"
        >
          ← Back
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          Player Game Log
        </span>
      </div>

      <div className="flex-1 px-4 py-4">
        {loading && <div className="text-sm text-gray-500">Loading game log...</div>}
        {error && <div className="text-sm text-red-400">Error: {error}</div>}
        {!loading && !error && log.length === 0 && (
          <div className="text-sm text-gray-500">No game log available.</div>
        )}
        {!loading && !error && log.length > 0 && (
          <div className="overflow-x-auto">
            <div className="text-xs text-gray-500 mb-3">Last {log.length} games</div>
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
                  <tr key={row.gameId} className="border-b border-gray-800">
                    <td className="py-1.5 pr-3 text-gray-300">{row.gameDate}</td>
                    <td className="py-1.5 pr-3 text-gray-400">
                      {row.isHome ? '' : '@'}{row.opponentAbbr}
                    </td>
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
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
