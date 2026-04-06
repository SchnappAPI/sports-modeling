import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const gamePk = req.nextUrl.searchParams.get('gamePk');
  if (!gamePk) return NextResponse.json({ error: 'gamePk required' }, { status: 400 });

  const pool = await getPool();

  // Check if play-by-play data exists for this game
  const checkResult = await pool
    .request()
    .input('gamePk', mssql.Int, parseInt(gamePk))
    .query<{ cnt: number }>(
      `SELECT COUNT(DISTINCT play_event_id) AS cnt FROM mlb.play_by_play WHERE game_pk = @gamePk`
    );

  const hasPbp = (checkResult.recordset[0]?.cnt ?? 0) > 0;

  if (!hasPbp) {
    return NextResponse.json({ gamePk: parseInt(gamePk), innings: [], hasPbp: false });
  }

  // Derive runs per half-inning from play-by-play scoring plays
  const result = await pool
    .request()
    .input('gamePk', mssql.Int, parseInt(gamePk))
    .query<{ inning: number; isTop: boolean; runs: number; hits: number }>(
      `SELECT
         inning,
         is_top_inning        AS isTop,
         SUM(COALESCE(result_rbi, 0)) AS runs
       FROM mlb.play_by_play
       WHERE game_pk = @gamePk
         AND is_last_pitch = 1
         AND inning IS NOT NULL
       GROUP BY inning, is_top_inning
       ORDER BY inning, is_top_inning DESC`
    );

  // Also get total H and E from batting_stats for the R/H/E summary row
  const summaryResult = await pool
    .request()
    .input('gamePk', mssql.Int, parseInt(gamePk))
    .query<{ side: string; runs: number; hits: number }>(
      `SELECT
         side,
         SUM(COALESCE(runs, 0))       AS runs,
         SUM(COALESCE(hits, 0))       AS hits
       FROM mlb.batting_stats
       WHERE game_pk = @gamePk
       GROUP BY side`
    );

  const summary: Record<string, { runs: number; hits: number }> = {};
  for (const row of summaryResult.recordset) {
    summary[row.side] = { runs: row.runs, hits: row.hits };
  }

  return NextResponse.json({
    gamePk: parseInt(gamePk),
    hasPbp: true,
    innings: result.recordset,
    summary,
  });
}
