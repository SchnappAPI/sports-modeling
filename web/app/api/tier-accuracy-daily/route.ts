import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

// Per-day per-tier accuracy: one row per (grade_date, tier) with n,
// predicted_prob (mean), actual_hit_rate. Replaces the period-toggle and
// weekly-bucket views per Austin's request for daily granularity.
//
// Optional ?days=N filter to restrict to the past N days. Default: all.

export const dynamic = 'force-dynamic';

type DailyPoint = {
  grade_date: string; // YYYY-MM-DD
  tier: 'safe' | 'value' | 'highrisk' | 'lotto';
  n: number;
  predicted_prob: number;
  actual_hit_rate: number;
};

export async function GET(req: NextRequest) {
  const daysParam = req.nextUrl.searchParams.get('days');
  const days = daysParam ? parseInt(daysParam, 10) : null;

  let dayFilter = '';
  if (days && Number.isFinite(days) && days > 0) {
    dayFilter = `AND tp.grade_date >= DATEADD(day, -${days}, CAST(GETUTCDATE() AS DATE))`;
  }

  try {
    const pool = await getPool();
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
       tp.grade_date,
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
   ${dayFilter}
 GROUP BY tp.tier, tp.grade_date
 ORDER BY tp.grade_date DESC, tp.tier ASC
`;

    const result = await pool.request().query(sql);
    const points: DailyPoint[] = result.recordset.map((r: Record<string, unknown>) => ({
      tier: r.tier as DailyPoint['tier'],
      grade_date: (r.grade_date as Date).toISOString().slice(0, 10),
      n: Number(r.n),
      predicted_prob: Number(r.predicted_prob),
      actual_hit_rate: Number(r.actual_hit_rate),
    }));
    return NextResponse.json({ points });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
