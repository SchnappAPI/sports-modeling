'use client';

import { useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useSearchParams } from 'next/navigation';

interface GradeRow {
  gradeId: number;
  gradeDate: string;
  playerId: number;
  playerName: string;
  marketKey: string;
  lineValue: number;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  weightedHitRate: number | null;
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

function todayLocal(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

export default function GradesPageInner() {
  const searchParams = useSearchParams();
  const [grades, setGrades] = useState<GradeRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedMarket, setSelectedMarket] = useState<string>('');

  const backGameId = searchParams.get('gameId');
  const gradeDate = searchParams.get('date') ?? todayLocal();
  const backHref = backGameId ? `/nba?gameId=${backGameId}` : '/nba';

  useEffect(() => {
    setLoading(true);
    setError(null);
    setSelectedMarket('');

    const url = backGameId
      ? `/api/grades?date=${gradeDate}&gameId=${backGameId}`
      : `/api/grades?date=${gradeDate}`;

    fetch(url)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => setGrades(data.grades ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gradeDate, backGameId]);

  // Sorted unique market keys present in the loaded data.
  const marketOptions = useMemo(() => {
    const keys = Array.from(new Set(grades.map((r) => r.marketKey))).sort();
    return keys;
  }, [grades]);

  const filtered = useMemo(
    () => (selectedMarket ? grades.filter((r) => r.marketKey === selectedMarket) : grades),
    [grades, selectedMarket]
  );

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">
          &#8592; Games
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          At a Glance
        </span>
        <span className="text-xs text-gray-600">{gradeDate}</span>
        {!loading && !error && grades.length > 0 && (
          <select
            value={selectedMarket}
            onChange={(e) => setSelectedMarket(e.target.value)}
            className="ml-3 bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500"
          >
            <option value="">All markets</option>
            {marketOptions.map((key) => (
              <option key={key} value={key}>
                {formatMarket(key)}
              </option>
            ))}
          </select>
        )}
        {!loading && !error && (
          <span className="text-xs text-gray-600 ml-auto">
            {filtered.length}{selectedMarket ? ` / ${grades.length}` : ''} props
          </span>
        )}
      </div>

      <div className="flex-1 px-4 py-4">
        {loading && <div className="text-sm text-gray-500">Loading grades...</div>}
        {error && <div className="text-sm text-red-400">Error: {error}</div>}
        {!loading && !error && grades.length === 0 && (
          <div className="text-sm text-gray-500">
            No grades available for {gradeDate}. Grades populate nightly after the ETL runs.
          </div>
        )}
        {!loading && !error && grades.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-xs text-gray-500 border-b border-gray-800">
                  <th className="text-left py-1.5 pr-3 font-medium">Player</th>
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
                    <td className="py-1.5 pr-3">
                      <Link
                        href={`/nba/player/${row.playerId}${backGameId ? `?gameId=${backGameId}` : ''}`}
                        className="text-gray-100 hover:text-blue-400 transition-colors"
                      >
                        {row.playerName}
                      </Link>
                    </td>
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
