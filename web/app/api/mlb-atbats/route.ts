import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const gamePk = req.nextUrl.searchParams.get('gamePk');
  if (!gamePk) return NextResponse.json({ error: 'gamePk required' }, { status: 400 });

  const pool = await getPool();

  // One row per completed at-bat with the last pitch event's hit data
  const result = await pool
    .request()
    .input('gamePk', mssql.Int, parseInt(gamePk))
    .query(
      `SELECT
         p.at_bat_number       AS atBatNumber,
         p.inning              AS inning,
         p.is_top_inning       AS isTop,
         p.batter_id           AS batterId,
         pb.player_name        AS batterName,
         p.pitcher_id          AS pitcherId,
         pp.player_name        AS pitcherName,
         p.result_event_type   AS resultType,
         p.result_description  AS resultDesc,
         p.result_rbi          AS rbi,
         p.hit_launch_speed    AS exitVelo,
         p.hit_launch_angle    AS launchAngle,
         p.hit_total_distance  AS distance,
         p.hit_trajectory      AS trajectory,
         p.hit_hardness        AS hardness,
         p.hit_probability     AS hitProb,
         p.hit_bat_speed       AS batSpeed,
         p.home_run_ballparks  AS hrBallparks,
         p.away_team_id        AS awayTeamId,
         p.home_team_id        AS homeTeamId
       FROM mlb.play_by_play p
       LEFT JOIN mlb.players pb ON pb.player_id = p.batter_id
       LEFT JOIN mlb.players pp ON pp.player_id = p.pitcher_id
       WHERE p.game_pk = @gamePk
         AND p.is_last_pitch = 1
         AND p.result_event_type IS NOT NULL
       ORDER BY p.at_bat_number`
    );

  return NextResponse.json({
    gamePk: parseInt(gamePk),
    atBats: result.recordset,
  });
}
