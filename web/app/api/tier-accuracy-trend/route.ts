import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

// Returns weekly tier accuracy for trend chart. Groups resolved tier-line
// rows by ISO week. One point per (week, tier).
// Optional ?weeks=N to limit how far back; default all-time.

export const dynamic = 'force-dynamic';

type Point = {
  week_start: string; // YYYY-MM-DD (Monday)
  tier: 'safe' | 'value' | 'highrisk' | 'lotto';
  n: number;
  predicted_prob: number;
  actual_hit_rate: number;
};

export async function GET(req: NextRequest) {
  const weeksParam = req.nextUrl.searchParams.get('weeks');
  const weeks = weeksParam ? parseInt(weeksParam, 10) : null;

  let weekFilter = '';
  if (weeks && Number.isFinite(weeks) && weeks > 0) {
    weekFilter = `AND tp.grade_date >= DATEADD(week, -${weeks}, CAST(GETUTCDATE() AS DATE))`;
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
),
resolved AS (
    SELECT tp.tier, tp.prob,
           DATEADD(day, 1 - DATEPART(weekday, tp.grade_date) + 1, CAST(tp.grade_date AS DATE)) AS week_start_raw,
           tp.grade_date,
           CASE WHEN dg.outcome = 'Won' THEN 1.0 ELSE 0.0 END AS hit
      FROM tier_picks tp
     INNER JOIN common.daily_grades dg
            ON dg.grade_date = tp.grade_date AND dg.game_id = tp.game_id
           AND dg.player_id = tp.player_id  AND dg.market_key = tp.market_key
           AND dg.line_value = tp.line AND dg.outcome_name = 'Over'
     WHERE dg.outcome IN ('Won','Lost')
       ${weekFilter}
)
SELECT tier,
       DATEADD(day, -((DATEPART(weekday, grade_date) + @@DATEFIRST - 2) % 7), CAST(grade_date AS DATE)) AS week_start,
       COUNT(*) AS n,
       AVG(prob) AS predicted_prob,
       AVG(hit) AS actual_hit_rate
  FROM resolved
 GROUP BY tier,
          DATEADD(day, -((DATEPART(weekday, grade_date) + @@DATEFIRST - 2) % 7), CAST(grade_date AS DATE))
 ORDER BY week_start ASC, tier ASC
`;

    const result = await pool.request().query(sql);
    const points: Point[] = result.recordset.map((r: Record<string, unknown>) => ({
      tier: r.tier as Point['tier'],
      week_start: (r.week_start as Date).toISOString().slice(0, 10),
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
