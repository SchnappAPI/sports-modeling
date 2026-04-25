'use client';

import { useEffect, useMemo, useState } from 'react';

// /transparency client component.
// Three sections:
//   1. Daily accuracy chart: per-tier actual hit rate per day (one line per tier).
//   2. Daily breakdown table: one row per date, columns per tier, showing actual hit
//      rate and n alongside a gap-colored cell. Most recent first.
//   3. Calibration buckets: per-bucket empirical vs isotonic hit rates and the
//      output cap (max_well_sampled_rate). Read from /api/calibration-buckets.

type Bucket = {
  bucket_min: number;
  bucket_max: number;
  sample_size: number;
  empirical_hit_rate: number;
  isotonic_hit_rate: number;
  max_well_sampled_rate: number | null;
};

type DailyPoint = {
  grade_date: string;
  tier: 'safe' | 'value' | 'highrisk' | 'lotto';
  n: number;
  predicted_prob: number;
  actual_hit_rate: number;
};

type Tier = 'safe' | 'value' | 'highrisk' | 'lotto';
const TIERS: Tier[] = ['safe', 'value', 'highrisk', 'lotto'];

const TIER_COLORS: Record<Tier, string> = {
  safe: '#22c55e',
  value: '#3b82f6',
  highrisk: '#f59e0b',
  lotto: '#ef4444',
};

const TIER_LABELS: Record<Tier, string> = {
  safe: 'Safe',
  value: 'Value',
  highrisk: 'High Risk',
  lotto: 'Lotto',
};

const TIER_LABELS_SHORT: Record<Tier, string> = {
  safe: 'Safe',
  value: 'Val',
  highrisk: 'HR',
  lotto: 'Lot',
};

function fmtPct(p: number | null | undefined): string {
  if (p == null || !Number.isFinite(p)) return '-';
  return `${(p * 100).toFixed(1)}%`;
}

function fmtGap(gap: number | null | undefined): string {
  if (gap == null || !Number.isFinite(gap)) return '-';
  const sign = gap >= 0 ? '+' : '';
  return `${sign}${(gap * 100).toFixed(1)}pts`;
}

function gapColor(gap: number): string {
  const a = Math.abs(gap);
  if (a < 0.02) return 'text-gray-300';
  if (gap >= 0) return a >= 0.05 ? 'text-green-400' : 'text-green-500';
  return a >= 0.05 ? 'text-red-400' : 'text-red-500';
}

export default function TransparencyInner() {
  const [buckets, setBuckets] = useState<Bucket[] | null>(null);
  const [bucketsErr, setBucketsErr] = useState<string | null>(null);
  const [daily, setDaily] = useState<DailyPoint[] | null>(null);
  const [dailyErr, setDailyErr] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [cap, setCap] = useState<number | null>(null);

  useEffect(() => {
    fetch('/api/calibration-buckets')
      .then(r => r.json())
      .then(d => {
        if (d.error) { setBucketsErr(d.error); return; }
        setBuckets(d.buckets);
        setLastUpdated(d.last_updated);
        setCap(d.max_well_sampled_rate);
      })
      .catch(e => setBucketsErr(String(e)));
  }, []);

  useEffect(() => {
    fetch('/api/tier-accuracy-daily')
      .then(r => r.json())
      .then(d => {
        if (d.error) { setDailyErr(d.error); return; }
        setDaily(d.points);
      })
      .catch(e => setDailyErr(String(e)));
  }, []);

  // Pivot points into a date -> tier -> stats map.
  const byDate = useMemo(() => {
    const m: Record<string, Partial<Record<Tier, DailyPoint>>> = {};
    if (daily) {
      for (const p of daily) {
        if (!m[p.grade_date]) m[p.grade_date] = {};
        m[p.grade_date][p.tier] = p;
      }
    }
    return m;
  }, [daily]);

  // Dates sorted most recent first.
  const dates = useMemo(() => {
    return Object.keys(byDate).sort((a, b) => b.localeCompare(a));
  }, [byDate]);

  return (
    <main className="min-h-screen px-4 py-6 max-w-4xl mx-auto">
      <header className="mb-6">
        <h1 className="text-xl font-medium text-gray-200">Model Transparency</h1>
        <p className="text-xs text-gray-500 mt-1 leading-relaxed">
          Live accuracy data for the prop grading model. Below is the daily evolution of per-tier hit rates and the calibration buckets used to map raw model probabilities to published tier probabilities. Calibration recomputes weekly from resolved outcomes; the goal is for the gap between predicted and actual to shrink as more season data accumulates.
        </p>
        {lastUpdated && (
          <p className="text-[10px] text-gray-600 mt-2">
            Calibration last updated: {new Date(lastUpdated).toLocaleString()}
          </p>
        )}
      </header>

      <section className="mb-8">
        <h2 className="text-sm font-medium text-gray-300 mb-3">Daily accuracy</h2>
        <p className="text-[11px] text-gray-500 mb-3 leading-relaxed">
          Actual hit rate by day for each tier. Watch for the lines to converge toward their target rates as the model adapts.
        </p>
        {dailyErr && <div className="text-xs text-red-400">{dailyErr}</div>}
        {!daily && !dailyErr && <div className="text-xs text-gray-500">Loading...</div>}
        {daily && daily.length > 0 && <DailyTrendChart points={daily} />}
        {daily && daily.length === 0 && <div className="text-xs text-gray-500">Not enough resolved data yet.</div>}
      </section>

      <section className="mb-8">
        <h2 className="text-sm font-medium text-gray-300 mb-3">Daily breakdown</h2>
        <p className="text-[11px] text-gray-500 mb-3 leading-relaxed">
          One row per date. Each cell shows the actual hit rate for that tier with sample size (n) and is color-coded by the gap to predicted.
        </p>
        {dailyErr && <div className="text-xs text-red-400">{dailyErr}</div>}
        {daily && dates.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-500 border-b border-gray-800 sticky top-0 bg-black">
                  <th className="text-left py-2 pr-3 font-normal">Date</th>
                  {TIERS.map(t => (
                    <th key={t} className="text-right py-2 px-2 font-normal">
                      <span className="inline-block w-2 h-2 rounded-full mr-1 align-middle"
                            style={{ backgroundColor: TIER_COLORS[t] }} />
                      {TIER_LABELS_SHORT[t]}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {dates.map(d => (
                  <tr key={d} className="border-b border-gray-900">
                    <td className="py-2 pr-3 text-gray-400 font-mono whitespace-nowrap">{d}</td>
                    {TIERS.map(t => {
                      const p = byDate[d][t];
                      if (!p || p.n === 0) {
                        return <td key={t} className="text-right py-2 px-2 text-gray-700">-</td>;
                      }
                      const gap = p.actual_hit_rate - p.predicted_prob;
                      return (
                        <td key={t} className={`text-right py-2 px-2 ${gapColor(gap)}`}
                            title={`predicted ${fmtPct(p.predicted_prob)} / actual ${fmtPct(p.actual_hit_rate)} / gap ${fmtGap(gap)} / n=${p.n}`}>
                          <div>{fmtPct(p.actual_hit_rate)}</div>
                          <div className="text-[9px] text-gray-600 font-mono">n={p.n}</div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {daily && dates.length === 0 && <div className="text-xs text-gray-500">No resolved data yet.</div>}
      </section>

      <section>
        <h2 className="text-sm font-medium text-gray-300 mb-3">Calibration buckets</h2>
        <p className="text-[11px] text-gray-500 mb-3 leading-relaxed">
          Raw model probability is binned into 5-point buckets. The empirical hit rate is what actually happened in those buckets; the isotonic rate is the smoothed monotonic version published as the calibrated probability. The output cap below is the highest empirical rate observed in any well-sampled bucket. The model never claims a probability higher than this value.
        </p>
        {cap != null && (
          <div className="mb-3 px-3 py-2 bg-gray-900 rounded text-[11px] text-gray-400">
            Output cap: <span className="text-gray-200 font-medium">{fmtPct(cap)}</span>
          </div>
        )}
        {bucketsErr && <div className="text-xs text-red-400">{bucketsErr}</div>}
        {!buckets && !bucketsErr && <div className="text-xs text-gray-500">Loading...</div>}
        {buckets && buckets.length === 0 && <div className="text-xs text-gray-500">No calibration data yet.</div>}
        {buckets && buckets.length > 0 && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-500 border-b border-gray-800">
                <th className="text-left py-2 pr-3 font-normal">Bucket</th>
                <th className="text-right py-2 px-3 font-normal">n</th>
                <th className="text-right py-2 px-3 font-normal">Empirical</th>
                <th className="text-right py-2 px-3 font-normal">Isotonic</th>
                <th className="text-right py-2 pl-3 font-normal">Diff</th>
              </tr>
            </thead>
            <tbody>
              {buckets.map(b => {
                const diff = b.isotonic_hit_rate - b.empirical_hit_rate;
                return (
                  <tr key={b.bucket_min} className="border-b border-gray-900">
                    <td className="py-2 pr-3 text-gray-300 font-mono">
                      {b.bucket_min.toFixed(2)} - {b.bucket_max.toFixed(2)}
                    </td>
                    <td className="text-right py-2 px-3 text-gray-400">{b.sample_size.toLocaleString()}</td>
                    <td className="text-right py-2 px-3 text-gray-300">{fmtPct(b.empirical_hit_rate)}</td>
                    <td className="text-right py-2 px-3 text-gray-300">{fmtPct(b.isotonic_hit_rate)}</td>
                    <td className={`text-right py-2 pl-3 ${gapColor(diff)}`}>{fmtGap(diff)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </section>
    </main>
  );
}

function DailyTrendChart({ points }: { points: DailyPoint[] }) {
  const byTier = useMemo(() => {
    const m: Record<string, DailyPoint[]> = {};
    for (const p of points) {
      if (!m[p.tier]) m[p.tier] = [];
      m[p.tier].push(p);
    }
    for (const t of Object.keys(m)) {
      m[t].sort((a, b) => a.grade_date.localeCompare(b.grade_date));
    }
    return m;
  }, [points]);

  const dates = useMemo(() => {
    const s = new Set<string>();
    for (const p of points) s.add(p.grade_date);
    return Array.from(s).sort();
  }, [points]);

  const W = 600;
  const H = 220;
  const PAD_L = 40;
  const PAD_R = 12;
  const PAD_T = 12;
  const PAD_B = 30;
  const innerW = W - PAD_L - PAD_R;
  const innerH = H - PAD_T - PAD_B;

  if (dates.length < 2) {
    return <div className="text-xs text-gray-500">Need at least two days of data.</div>;
  }

  function xFor(date: string): number {
    const idx = dates.indexOf(date);
    return PAD_L + (idx / (dates.length - 1)) * innerW;
  }
  function yFor(rate: number): number {
    return PAD_T + (1 - rate) * innerH;
  }

  const yTicks = [0, 0.25, 0.5, 0.75, 1.0];
  // X tick labels: roughly 6 labels evenly spaced.
  const labelCount = 6;
  const xTickIdxs: number[] = [];
  if (dates.length <= labelCount) {
    for (let i = 0; i < dates.length; i++) xTickIdxs.push(i);
  } else {
    const step = (dates.length - 1) / (labelCount - 1);
    for (let i = 0; i < labelCount; i++) xTickIdxs.push(Math.round(i * step));
  }

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto" style={{ maxWidth: W }}>
        {yTicks.map(t => (
          <g key={t}>
            <line x1={PAD_L} y1={yFor(t)} x2={W - PAD_R} y2={yFor(t)}
                  stroke="#1f2937" strokeWidth={1} />
            <text x={PAD_L - 6} y={yFor(t) + 3} fontSize="9" fill="#6b7280" textAnchor="end">
              {(t * 100).toFixed(0)}%
            </text>
          </g>
        ))}
        {xTickIdxs.map(i => (
          <text key={i}
                x={xFor(dates[i])}
                y={H - PAD_B + 14}
                fontSize="9"
                fill="#6b7280"
                textAnchor="middle">
            {dates[i].slice(5)}
          </text>
        ))}
        {TIERS.map(tier => {
          const pts = byTier[tier] || [];
          if (pts.length < 2) return null;
          const pathPts = pts.map(p => `${xFor(p.grade_date)},${yFor(p.actual_hit_rate)}`).join(' ');
          return (
            <g key={tier}>
              <polyline
                points={pathPts}
                fill="none"
                stroke={TIER_COLORS[tier]}
                strokeWidth={1.25}
                opacity={0.85}
              />
              {pts.map((p, i) => (
                <circle
                  key={i}
                  cx={xFor(p.grade_date)}
                  cy={yFor(p.actual_hit_rate)}
                  r={1.5}
                  fill={TIER_COLORS[tier]}
                />
              ))}
            </g>
          );
        })}
      </svg>
      <div className="flex gap-4 mt-2 text-[10px] text-gray-500">
        {TIERS.map(tier => (
          <span key={tier} className="flex items-center gap-1">
            <span className="inline-block w-2 h-2 rounded-full"
                  style={{ backgroundColor: TIER_COLORS[tier] }} />
            {TIER_LABELS[tier]}
          </span>
        ))}
      </div>
    </div>
  );
}
