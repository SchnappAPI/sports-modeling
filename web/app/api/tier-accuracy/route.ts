import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

// Computes per-tier accuracy on demand from player_tier_lines + daily_grades.
// Window options:
//   ?window=30  -> last 30 days
//   ?window=90  -> last 90 days
//   ?window=all -> all-time (default)
//
// For each tier, returns: n, predicted_prob (mean), actual_hit_rate, gap.

export const dynamic = 'force-dynamic';

type TierStats = {
  tier: 'safe' | 'value' | 'highrisk' | 'lotto';
  n: number;
  predicted_prob: number;
  actual_hit_rate: number;
  gap: number;
};

export async function GET(req: NextRequest) {
  const window = req.nextUrl.searchParams.get('window') ?? 'all';
  let windowClause = '';
  if (window === '30') {
    windowClause = `AND tp.grade_date >= DATEADD(day, -30, CAST(GETUTCDATE() AS DATE))`;
  } else if (window === '90') {
    windowClause = `AND tp.grade_date >= DATEADD(day, -90, CAST(GETUTCDATE() AS DATE))`;
  }

  try {
    const pool = await getPool();

    // Single UNION query splits into per-tier blocks. Each block reads its
    // own tier columns and joins to daily_grades for the resolved outcome.
    const sql = `
WITH tier_picks AS (
    SELECT 'safe' AS tier, t.grade_date, t.game_id, t.player_id, t.market_key,
           t.safe_line AS line, t.safe_prob AS prob
      FROM common.player_tier_lines t
     WHERE t.safe_line IS NOT NULL AND t.safe_prob IS NOT NULL
    UNION ALL
    SELECT 'value', t.grade_date, t.game_id, t.player_id, t.market_key,
           t.value_line, t.value_prob
      FROM common.player_tier_lines t
     WHERE t.value_line IS NOT NULL AND t.value_prob IS NOT NULL
    UNION ALL
    SELECT 'highrisk', t.grade_date, t.game_id, t.player_id, t.market_key,
           t.highrisk_line, t.highrisk_prob
      FROM common.player_tier_lines t
     WHERE t.highrisk_line IS NOT NULL AND t.highrisk_prob IS NOT NULL
    UNION ALL
    SELECT 'lotto', t.grade_date, t.game_id, t.player_id, t.market_key,
           t.lotto_line, t.lotto_prob
      FROM common.player_tier_lines t
     WHERE t.lotto_line IS NOT NULL AND t.lotto_prob IS NOT NULL
)
SELECT tp.tier,
       COUNT(*) AS n,
       AVG(tp.prob) AS predicted_prob,
       AVG(CASE WHEN dg.outcome = 'Won' THEN 1.0 ELSE 0.0 END) AS actual_hit_rate
  FROM tier_picks tp
 INNER JOIN common.daily_grades dg
        ON dg.grade_date = tp.grade_date
       AND dg.game_id = tp.game_id
       AND dg.player_id = tp.player_id
       AND dg.market_key = tp.market_key
       AND dg.line_value = tp.line
       AND dg.outcome_name = 'Over'
 WHERE dg.outcome IN ('Won','Lost')
   ${windowClause}
 GROUP BY tp.tier
`;

    const result = await pool.request().query(sql);
    const tiers: TierStats[] = result.recordset.map((r: Record<string, unknown>) => {
      const predicted = Number(r.predicted_prob);
      const actual = Number(r.actual_hit_rate);
      return {
        tier: r.tier as TierStats['tier'],
        n: Number(r.n),
        predicted_prob: predicted,
        actual_hit_rate: actual,
        gap: actual - predicted,
      };
    });
    // Stable ordering: Safe, Value, HighRisk, Lotto
    const order = { safe: 0, value: 1, highrisk: 2, lotto: 3 };
    tiers.sort((a, b) => order[a.tier] - order[b.tier]);
    return NextResponse.json({ window, tiers });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
