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

// Odds slider bounds (American odds). -200 to +300 covers the vast majority
// of prop markets. Users drag to narrow this window.
const ODDS_MIN = -300;
const ODDS_MAX = 300;

export default function GradesPageInner() {
  const searchParams = useSearchParams();
  const [grades, setGrades]           = useState<GradeRow[]>([]);
  const [loading, setLoading]         = useState(true);
  const [error, setError]             = useState<string | null>(null);
  const [selectedMarket, setSelectedMarket] = useState<string>('');
  const [playerFilter, setPlayerFilter]     = useState<string>('');
  const [oddsRange, setOddsRange]           = useState<[number, number]>([ODDS_MIN, ODDS_MAX]);

  const backGameId = searchParams.get('gameId');
  const gradeDate  = searchParams.get('date') ?? todayLocal();
  const backHref   = backGameId ? `/nba?gameId=${backGameId}` : '/nba';

  useEffect(() => {
    setLoading(true);
    setError(null);
    setSelectedMarket('');
    setPlayerFilter('');
    setOddsRange([ODDS_MIN, ODDS_MAX]);

    const url = backGameId
      ? `/api/grades?date=${gradeDate}&gameId=${backGameId}`
      : `/api/grades?date=${gradeDate}`;

    fetch(url)
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((data) => setGrades(data.grades ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gradeDate, backGameId]);

  const marketOptions = useMemo(
    () => Array.from(new Set(grades.map((r) => r.marketKey))).sort(),
    [grades]
  );

  // Derive the actual odds range present in the data so the slider bounds
  // are meaningful. Rows with null overPrice are included regardless of slider.
  const dataOddsMin = useMemo(() => {
    const prices = grades.map((r) => r.overPrice).filter((p): p is number => p != null);
    return prices.length ? Math.min(...prices) : ODDS_MIN;
  }, [grades]);
  const dataOddsMax = useMemo(() => {
    const prices = grades.map((r) => r.overPrice).filter((p): p is number => p != null);
    return prices.length ? Math.max(...prices) : ODDS_MAX;
  }, [grades]);

  const filtered = useMemo(() => {
    let rows = grades;
    if (selectedMarket) rows = rows.filter((r) => r.marketKey === selectedMarket);
    if (playerFilter.trim()) {
      const q = playerFilter.trim().toLowerCase();
      rows = rows.filter((r) => r.playerName.toLowerCase().includes(q));
    }
    // Odds filter: include rows that have a price within range, OR have no price.
    const [lo, hi] = oddsRange;
    const sliderActive = lo > ODDS_MIN || hi < ODDS_MAX;
    if (sliderActive) {
      rows = rows.filter((r) => r.overPrice == null || (r.overPrice >= lo && r.overPrice <= hi));
    }
    return rows;
  }, [grades, selectedMarket, playerFilter, oddsRange]);

  const sliderActive = oddsRange[0] > ODDS_MIN || oddsRange[1] < ODDS_MAX;

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header row */}
      <div className="px-4 py-3 border-b border-gray-800 flex items-center gap-3 flex-wrap">
        <Link href={backHref} className="text-gray-400 hover:text-gray-200 text-sm">
          &#8592; Games
        </Link>
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">
          At a Glance
        </span>
        <span className="text-xs text-gray-600">{gradeDate}</span>

        {!loading && !error && grades.length > 0 && (
          <>
            {/* Market dropdown */}
            <select
              value={selectedMarket}
              onChange={(e) => setSelectedMarket(e.target.value)}
              className="bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 focus:outline-none focus:border-gray-500"
            >
              <option value="">All markets</option>
              {marketOptions.map((key) => (
                <option key={key} value={key}>{formatMarket(key)}</option>
              ))}
            </select>

            {/* Player search */}
            <input
              type="text"
              placeholder="Player..."
              value={playerFilter}
              onChange={(e) => setPlayerFilter(e.target.value)}
              className="bg-gray-900 border border-gray-700 text-gray-300 text-xs rounded px-2 py-1 w-28 focus:outline-none focus:border-gray-500 placeholder-gray-600"
            />
          </>
        )}

        {!loading && !error && (
          <span className="text-xs text-gray-600 ml-auto">
            {filtered.length}{filtered.length !== grades.length ? ` / ${grades.length}` : ''} props
          </span>
        )}
      </div>

      {/* Odds slider row — only shown once data is loaded */}
      {!loading && !error && grades.length > 0 && (
        <div className="px-4 py-2 border-b border-gray-800 flex items-center gap-3">
          <span className="text-xs text-gray-600 whitespace-nowrap">Odds</span>

          {/* Min handle */}
          <div className="flex items-center gap-1 flex-1">
            <span className={`text-xs tabular-nums w-10 text-right ${
              sliderActive ? 'text-gray-300' : 'text-gray-600'
            }`}>
              {oddsRange[0] >= 0 ? `+${oddsRange[0]}` : `${oddsRange[0]}`}
            </span>
            <input
              type="range"
              min={ODDS_MIN}
              max={ODDS_MAX}
              step={5}
              value={oddsRange[0]}
              onChange={(e) => {
                const v = parseInt(e.target.value);
                setOddsRange([Math.min(v, oddsRange[1] - 5), oddsRange[1]]);
              }}
              className="flex-1 accent-blue-500 h-1"
            />
            <span className="text-xs text-gray-600">to</span>
            <input
              type="range"
              min={ODDS_MIN}
              max={ODDS_MAX}
              step={5}
              value={oddsRange[1]}
              onChange={(e) => {
                const v = parseInt(e.target.value);
                setOddsRange([oddsRange[0], Math.max(v, oddsRange[0] + 5)]);
              }}
              className="flex-1 accent-blue-500 h-1"
            />
            <span className={`text-xs tabular-nums w-10 ${
              sliderActive ? 'text-gray-300' : 'text-gray-600'
            }`}>
              {oddsRange[1] >= 0 ? `+${oddsRange[1]}` : `${oddsRange[1]}`}
            </span>
          </div>

          {sliderActive && (
            <button
              onClick={() => setOddsRange([ODDS_MIN, ODDS_MAX])}
              className="text-xs text-gray-600 hover:text-gray-400"
            >
              Reset
            </button>
          )}
        </div>
      )}

      {/* Table */}
      <div className="flex-1 px-4 py-4">
        {loading && <div className="text-sm text-gray-500">Loading grades...</div>}
        {error   && <div className="text-sm text-red-400">Error: {error}</div>}
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
                        href={`/nba/player/${row.playerId}/props`}
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
