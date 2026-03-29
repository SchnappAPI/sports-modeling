'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

interface PropRow {
  gradeId: number;
  gradeDate: string;
  marketKey: string;
  lineValue: number;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  grade: number | null;
}

function formatMarket(key: string): string {
  return key
    .replace('player_', '')
    .replace(/_over_under$/, '')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function gradeColor(grade: number | null): string {
  if (grade == null) return 'text-gray-500';
  if (grade >= 70) return 'text-green-400';
  if (grade >= 55) return 'text-yellow-400';
  return 'text-gray-400';
}

function fmt(val: number | null, decimals = 1): string {
  if (val == null) return '-';
  return val.toFixed(decimals);
}

function fmtPct(val: number | null): string {
  if (val == null) return '-';
  return `${(val * 100).toFixed(0)}%`;
}

function fmtOdds(price: number | null): string {
  if (price == null) return '-';
  return price >= 0 ? `+${price}` : `${price}`;
}

function oddsColor(price: number | null): string {
  if (price == null) return 'text-gray-600';
  if (price > 0) return 'text-gray-400';
  if (price >= -115) return 'text-gray-500';
  return 'text-gray-400';
}

export default function PlayerPropsInner({ playerId }: { playerId: string }) {
  const searchParams  = useSearchParams();
  const backGameId    = searchParams.get('gameId');
  const backDate      = searchParams.get('date');
  const [props, setProps]   = useState<PropRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState<string | null>(null);
  const [selectedMarket, setSelectedMarket] = useState<string>('');

  // Derive player name from first row
  const [playerName, setPlayerName] = useState<string>('');

  const backHref = (() => {
    const p = new URLSearchParams();
    if (backGameId) p.set('gameId', backGameId);
    if (backDate)   p.set('date', backDate);
    const qs = p.toString();
    return qs ? `/nba?${qs}` : '/nba';
  })();

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/player-props?playerId=${playerId}`)
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((data) => {
        setProps(data.props ?? []);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));

    // Fetch player name from the player API
    fetch(`/api/player?playerId=${playerId}&lastN=1&sport=nba`)
      .then((r) => r.json())
      .then((data) => {
        if (data.playerName) setPlayerName(data.playerName);
      })
      .catch(() => {});
  }, [playerId]);

  const marketOptions = useMemo(
    () => Array.from(new Set(props.map((r) => r.marketKey))).sort(),
    [props]
  );

  // Group by market key for summary stats (most recent grade per market)
  const marketSummary = useMemo(() => {
    const byMarket = new Map<string, PropRow[]>();
    for (const r of props) {
      const arr = byMarket.get(r.marketKey) ?? [];
      arr.push(r);
      byMarket.set(r.marketKey, arr);
    }
    // For each market, most recent row is first (query orders by date desc)
    return byMarket;
  }, [props]);

  const filtered = useMemo(
    () => (selectedMarket ? props.filter((r) => r.marketKey === selectedMarket) : props),
    [props, selectedMarket]
  );

  if (loading) return <div className="px-4 py-6 text-sm text-gray-500">Loading props...</div>;
  if (error)   return <div className="px-4 py-6 text-sm text-red-400">Error: {error}</div>;

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3">
        <Link
          href={`/nba/player/${playerId}${backGameId ? `?gameId=${backGameId}` : ''}`}
          className="text-gray-400 hover:text-gray-200 text-sm"
        >
          &#8592;
        </Link>
        <span className="text-sm font-semibold text-gray-200">
          {playerName || `Player ${playerId}`}
        </span>
        <span className="text-xs text-gray-600 uppercase tracking-wider">Props</span>

        {props.length > 0 && (
          <select
            value={selectedMarket}
            onChange={(e) => setSelectedMarket(e.target.value)}
            className="ml-auto bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500"
          >
            <option value="">All markets</option>
            {marketOptions.map((key) => (
              <option key={key} value={key}>{formatMarket(key)}</option>
            ))}
          </select>
        )}
      </div>

      {/* Market summary cards — most recent grade per market */}
      {!selectedMarket && props.length > 0 && (
        <div className="px-4 py-3 border-b border-gray-800">
          <div className="flex gap-2 overflow-x-auto pb-1">
            {Array.from(marketSummary.entries()).map(([market, rows]) => {
              const latest = rows[0]; // most recent date first
              return (
                <button
                  key={market}
                  onClick={() => setSelectedMarket(market)}
                  className="flex-shrink-0 rounded-lg border border-gray-700 bg-gray-900 hover:border-gray-500 px-3 py-2 text-left transition-colors"
                >
                  <div className="text-xs text-gray-500 mb-1">{formatMarket(market)}</div>
                  <div className={`text-base font-bold ${gradeColor(latest.grade)}`}>
                    {fmt(latest.grade)}
                  </div>
                  <div className="text-xs text-gray-500 mt-0.5">
                    {fmt(latest.lineValue)} &middot; {fmtOdds(latest.overPrice)}
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Props table */}
      <div className="flex-1 px-4 py-4">
        {props.length === 0 ? (
          <div className="text-sm text-gray-500">No prop grades found for this player.</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-gray-500 border-b border-gray-800">
                  <th className="text-left py-1.5 pr-3 font-medium">Date</th>
                  <th className="text-left py-1.5 pr-3 font-medium">Market</th>
                  <th className="text-right py-1.5 px-2 font-medium">Line</th>
                  <th className="text-right py-1.5 px-2 font-medium">Odds</th>
                  <th className="text-right py-1.5 px-2 font-medium">Grade</th>
                  <th className="text-right py-1.5 px-2 font-medium">L20%</th>
                  <th className="text-right py-1.5 px-2 font-medium">L60%</th>
                  <th className="text-right py-1.5 px-2 font-medium">N20</th>
                  <th className="text-right py-1.5 pl-2 font-medium">N60</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((row) => (
                  <tr key={row.gradeId} className="border-b border-gray-800">
                    <td className="py-1.5 pr-3 text-gray-500 tabular-nums">{row.gradeDate.slice(5)}</td>
                    <td className="py-1.5 pr-3 text-gray-400">{formatMarket(row.marketKey)}</td>
                    <td className="py-1.5 px-2 text-right text-gray-300">{fmt(row.lineValue)}</td>
                    <td className={`py-1.5 px-2 text-right tabular-nums ${oddsColor(row.overPrice)}`}>
                      {fmtOdds(row.overPrice)}
                    </td>
                    <td className={`py-1.5 px-2 text-right font-semibold ${gradeColor(row.grade)}`}>
                      {fmt(row.grade)}
                    </td>
                    <td className="py-1.5 px-2 text-right text-gray-300">{fmtPct(row.hitRate20)}</td>
                    <td className="py-1.5 px-2 text-right text-gray-300">{fmtPct(row.hitRate60)}</td>
                    <td className="py-1.5 px-2 text-right text-gray-500">{row.sampleSize20 ?? '-'}</td>
                    <td className="py-1.5 pl-2 text-right text-gray-500">{row.sampleSize60 ?? '-'}</td>
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
