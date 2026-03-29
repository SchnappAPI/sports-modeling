import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const homeTeamId = req.nextUrl.searchParams.get('homeTeamId');
  const awayTeamId = req.nextUrl.searchParams.get('awayTeamId');
  const context    = req.nextUrl.searchParams.get('context') ?? '20';

  if (!homeTeamId || !awayTeamId) {
    return NextResponse.json({ error: 'homeTeamId and awayTeamId required' }, { status: 400 });
  }

  const lastN = Math.max(1, parseInt(context, 10) || 20);

  try {
    const pool = await getPool();
    const result = await pool
      .request()
      .input('homeTeamId', mssql.BigInt, parseInt(homeTeamId))
      .input('awayTeamId', mssql.BigInt, parseInt(awayTeamId))
      .input('lastN', mssql.Int, lastN)
      .query(
        `WITH team_players AS (
           SELECT player_id, player_name, team_id, team_tricode
           FROM nba.players
           WHERE team_id IN (@homeTeamId, @awayTeamId)
             AND roster_status = 1
         ),
         game_totals AS (
           SELECT
             pbs.player_id,
             pbs.game_id,
             pbs.game_date,
             SUM(pbs.pts)     AS pts,
             SUM(pbs.reb)     AS reb,
             SUM(pbs.ast)     AS ast,
             SUM(pbs.stl)     AS stl,
             SUM(pbs.blk)     AS blk,
             SUM(pbs.tov)     AS tov,
             SUM(pbs.minutes) AS minutes,
             SUM(pbs.fg3m)    AS fg3m,
             SUM(pbs.fgm)     AS fgm,
             SUM(pbs.fga)     AS fga,
             SUM(pbs.ftm)     AS ftm,
             SUM(pbs.fta)     AS fta
           FROM nba.player_box_score_stats pbs
           JOIN team_players tp ON tp.player_id = pbs.player_id
           GROUP BY pbs.player_id, pbs.game_id, pbs.game_date
         ),
         ranked AS (
           SELECT *,
             ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
           FROM game_totals
         ),
         recent AS (
           SELECT * FROM ranked WHERE rn <= @lastN
         )
         SELECT
           tp.player_id                               AS playerId,
           tp.player_name                             AS playerName,
           tp.team_id                                 AS teamId,
           tp.team_tricode                            AS teamAbbr,
           COUNT(r.game_id)                           AS games,
           AVG(CAST(r.pts     AS FLOAT))              AS avgPts,
           AVG(CAST(r.reb     AS FLOAT))              AS avgReb,
           AVG(CAST(r.ast     AS FLOAT))              AS avgAst,
           AVG(CAST(r.stl     AS FLOAT))              AS avgStl,
           AVG(CAST(r.blk     AS FLOAT))              AS avgBlk,
           AVG(CAST(r.tov     AS FLOAT))              AS avgTov,
           AVG(CAST(r.minutes AS FLOAT))              AS avgMin,
           AVG(CAST(r.fg3m    AS FLOAT))              AS avg3pm
         FROM team_players tp
         LEFT JOIN recent r ON r.player_id = tp.player_id
         GROUP BY tp.player_id, tp.player_name, tp.team_id, tp.team_tricode
         ORDER BY tp.team_id, avgPts DESC`
      );
    return NextResponse.json({ players: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
