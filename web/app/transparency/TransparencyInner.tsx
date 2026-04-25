'use client';

import { useEffect, useMemo, useState } from 'react';

// /transparency client component.
// Two views:
//   1. Calibration buckets: per-bucket empirical vs isotonic hit rates and the
//      output cap (max_well_sampled_rate). Read from /api/calibration-buckets.
//   2. Tier accuracy: per-tier predicted vs actual hit rates with a window
//      toggle (30/90/all). Read from /api/tier-accuracy.
// Plus a trend chart of weekly tier hit rates from /api/tier-accuracy-trend.

type Bucket = {
  bucket_min: number;
  bucket_max: number;
  sample_size: number;
  empirical_hit_rate: number;
  isotonic_hit_rate: number;
  max_well_sampled_rate: number | null;
};

type TierStats = {
  tier: 'safe' | 'value' | 'highrisk' | 'lotto';
  n: number;
  predicted_prob: number;
  actual_hit_rate: number;
  gap: number;
};

type TrendPoint = {
  week_start: string;
  tier: 'safe' | 'value' | 'highrisk' | 'lotto';
  n: number;
  predicted_prob: number;
  actual_hit_rate: number;
};

type WindowChoice = '30' | '90' | 'all';

const TIER_COLORS = {
  safe: '#22c55e',
  value: '#3b82f6',
  highrisk: '#f59e0b',
  lotto: '#ef4444',
} as const;

const TIER_LABELS = {
  safe: 'Safe',
  value: 'Value',
  highrisk: 'High Risk',
  lotto: 'Lotto',
} as const;

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
  const [tierWindow, setTierWindow] = useState<WindowChoice>('30');
  const [tiers, setTiers] = useState<TierStats[] | null>(null);
  const [tiersErr, setTiersErr] = useState<string | null>(null);
  const [trend, setTrend] = useState<TrendPoint[] | null>(null);
  const [trendErr, setTrendErr] = useState<string | null>(null);
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
    setTiers(null);
    fetch(`/api/tier-accuracy?window=${tierWindow}`)
      .then(r => r.json())
      .then(d => {
        if (d.error) { setTiersErr(d.error); return; }
        setTiers(d.tiers);
      })
      .catch(e => setTiersErr(String(e)));
  }, [tierWindow]);

  useEffect(() => {
    fetch('/api/tier-accuracy-trend')
      .then(r => r.json())
      .then(d => {
        if (d.error) { setTrendErr(d.error); return; }
        setTrend(d.points);
      })
      .catch(e => setTrendErr(String(e)));
  }, []);

  return (
    <main className="min-h-screen px-4 py-6 max-w-4xl mx-auto">
      <header className="mb-6">
        <h1 className="text-xl font-medium text-gray-200">Model Transparency</h1>
        <p className="text-xs text-gray-500 mt-1 leading-relaxed">
          Live accuracy data for the prop grading model. Below are the calibration buckets used to map raw model probabilities to the published tier probabilities, and the per-tier hit rates over the chosen time window. Calibration recomputes weekly from resolved outcomes; the goal is for the gap between predicted and actual to shrink as more season data accumulates.
        </p>
        {lastUpdated && (
          <p className="text-[10px] text-gray-600 mt-2">
            Calibration last updated: {new Date(lastUpdated).toLocaleString()}
          </p>
        )}
      </header>

      <section className="mb-8">
        <h2 className="text-sm font-medium text-gray-300 mb-3">Tier accuracy</h2>
        <div className="flex gap-2 mb-3">
          {(['30', '90', 'all'] as WindowChoice[]).map(w => (
            <button
              key={w}
              onClick={() => setTierWindow(w)}
              className={`px-3 py-1 text-xs rounded ${
                tierWindow === w
                  ? 'bg-blue-600 text-white'
                  : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
              }`}
            >
              {w === 'all' ? 'All-time' : `Last ${w} days`}
            </button>
          ))}
        </div>
        {tiersErr && <div className="text-xs text-red-400">{tiersErr}</div>}
        {!tiers && !tiersErr && <div className="text-xs text-gray-500">Loading...</div>}
        {tiers && (
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-500 border-b border-gray-800">
                <th className="text-left py-2 pr-3 font-normal">Tier</th>
                <th className="text-right py-2 px-3 font-normal">n</th>
                <th className="text-right py-2 px-3 font-normal">Predicted</th>
                <th className="text-right py-2 px-3 font-normal">Actual</th>
                <th className="text-right py-2 pl-3 font-normal">Gap</th>
              </tr>
            </thead>
            <tbody>
              {tiers.map(t => (
                <tr key={t.tier} className="border-b border-gray-900">
                  <td className="py-2 pr-3">
                    <span className="inline-block w-2 h-2 rounded-full mr-2 align-middle"
                          style={{ backgroundColor: TIER_COLORS[t.tier] }} />
                    <span className="text-gray-300">{TIER_LABELS[t.tier]}</span>
                  </td>
                  <td className="text-right py-2 px-3 text-gray-400">{t.n.toLocaleString()}</td>
                  <td className="text-right py-2 px-3 text-gray-300">{fmtPct(t.predicted_prob)}</td>
                  <td className="text-right py-2 px-3 text-gray-300">{fmtPct(t.actual_hit_rate)}</td>
                  <td className={`text-right py-2 pl-3 ${gapColor(t.gap)}`}>{fmtGap(t.gap)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </section>

      <section className="mb-8">
        <h2 className="text-sm font-medium text-gray-300 mb-3">Weekly trend</h2>
        <p className="text-[11px] text-gray-500 mb-3 leading-relaxed">
          Actual hit rate by week for each tier. Watch for the lines to converge toward their target rates as the model adapts.
        </p>
        {trendErr && <div className="text-xs text-red-400">{trendErr}</div>}
        {!trend && !trendErr && <div className="text-xs text-gray-500">Loading...</div>}
        {trend && trend.length > 0 && <TrendChart points={trend} />}
        {trend && trend.length === 0 && <div className="text-xs text-gray-500">Not enough resolved data yet.</div>}
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

function TrendChart({ points }: { points: TrendPoint[] }) {
  // Group points by tier and produce one polyline each.
  // Uses inline SVG, no external lib.
  const byTier = useMemo(() => {
    const m: Record<string, TrendPoint[]> = {};
    for (const p of points) {
      if (!m[p.tier]) m[p.tier] = [];
      m[p.tier].push(p);
    }
    for (const t of Object.keys(m)) {
      m[t].sort((a, b) => a.week_start.localeCompare(b.week_start));
    }
    return m;
  }, [points]);

  // X axis: union of weeks, sorted.
  const weeks = useMemo(() => {
    const s = new Set<string>();
    for (const p of points) s.add(p.week_start);
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

  if (weeks.length < 2) {
    return <div className="text-xs text-gray-500">Need at least two weeks of data.</div>;
  }

  function xFor(week: string): number {
    const idx = weeks.indexOf(week);
    return PAD_L + (idx / (weeks.length - 1)) * innerW;
  }
  function yFor(rate: number): number {
    return PAD_T + (1 - rate) * innerH;
  }

  // Y gridlines at 0, 25, 50, 75, 100
  const yTicks = [0, 0.25, 0.5, 0.75, 1.0];
  // X tick labels: every 4 weeks for readability
  const xTickIdxs = weeks.map((_, i) => i).filter(i =>
    i === 0 || i === weeks.length - 1 || i % 4 === 0
  );

  return (
    <div className="overflow-x-auto">
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full h-auto" style={{ maxWidth: W }}>
        {/* Y gridlines */}
        {yTicks.map(t => (
          <g key={t}>
            <line x1={PAD_L} y1={yFor(t)} x2={W - PAD_R} y2={yFor(t)}
                  stroke="#1f2937" strokeWidth={1} />
            <text x={PAD_L - 6} y={yFor(t) + 3} fontSize="9" fill="#6b7280" textAnchor="end">
              {(t * 100).toFixed(0)}%
            </text>
          </g>
        ))}
        {/* X tick labels */}
        {xTickIdxs.map(i => (
          <text key={i}
                x={xFor(weeks[i])}
                y={H - PAD_B + 14}
                fontSize="9"
                fill="#6b7280"
                textAnchor="middle">
            {weeks[i].slice(5)}
          </text>
        ))}
        {/* Polylines per tier */}
        {(['safe', 'value', 'highrisk', 'lotto'] as const).map(tier => {
          const pts = byTier[tier] || [];
          if (pts.length < 2) return null;
          const pathPts = pts.map(p => `${xFor(p.week_start)},${yFor(p.actual_hit_rate)}`).join(' ');
          return (
            <g key={tier}>
              <polyline
                points={pathPts}
                fill="none"
                stroke={TIER_COLORS[tier]}
                strokeWidth={1.5}
              />
              {pts.map((p, i) => (
                <circle
                  key={i}
                  cx={xFor(p.week_start)}
                  cy={yFor(p.actual_hit_rate)}
                  r={2}
                  fill={TIER_COLORS[tier]}
                />
              ))}
            </g>
          );
        })}
      </svg>
      {/* Legend */}
      <div className="flex gap-4 mt-2 text-[10px] text-gray-500">
        {(['safe', 'value', 'highrisk', 'lotto'] as const).map(tier => (
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
