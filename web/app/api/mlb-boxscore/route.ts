import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const gamePk = req.nextUrl.searchParams.get('gamePk');
  if (!gamePk) return NextResponse.json({ error: 'gamePk required' }, { status: 400 });

  const pool = await getPool();

  const [battersResult, pitchersResult] = await Promise.all([
    pool
      .request()
      .input('gamePk', mssql.Int, parseInt(gamePk))
      .query(
        `SELECT
           b.player_id      AS playerId,
           p.player_name    AS playerName,
           b.team_id        AS teamId,
           b.side           AS side,
           b.position       AS position,
           b.batting_order  AS battingOrder,
           b.at_bats        AS ab,
           b.runs           AS r,
           b.hits           AS h,
           b.doubles        AS doubles,
           b.triples        AS triples,
           b.home_runs      AS hr,
           b.rbi            AS rbi,
           b.walks          AS bb,
           b.strikeouts     AS k,
           b.stolen_bases   AS sb,
           b.total_bases    AS tb,
           b.batting_avg    AS avg,
           b.obp            AS obp,
           b.slg            AS slg,
           b.ops            AS ops
         FROM mlb.batting_stats b
         LEFT JOIN mlb.players p ON p.player_id = b.player_id
         WHERE b.game_pk = @gamePk
         ORDER BY b.side, b.batting_order, b.player_id`
      ),
    pool
      .request()
      .input('gamePk', mssql.Int, parseInt(gamePk))
      .query(
        `SELECT
           ps.player_id       AS playerId,
           p.player_name      AS playerName,
           ps.team_id         AS teamId,
           ps.side            AS side,
           ps.note            AS note,
           ps.innings_pitched AS ip,
           ps.hits_allowed    AS h,
           ps.runs_allowed    AS r,
           ps.earned_runs     AS er,
           ps.walks           AS bb,
           ps.strikeouts      AS k,
           ps.hr_allowed      AS hr,
           ps.era             AS era,
           ps.pitches         AS pitches,
           ps.strikes         AS strikes
         FROM mlb.pitching_stats ps
         LEFT JOIN mlb.players p ON p.player_id = ps.player_id
         WHERE ps.game_pk = @gamePk
         ORDER BY ps.side, CASE WHEN ps.note = 'SP' THEN 0 ELSE 1 END, ps.player_id`
      ),
  ]);

  return NextResponse.json({
    gamePk: parseInt(gamePk),
    batters: battersResult.recordset,
    pitchers: pitchersResult.recordset,
  });
}
