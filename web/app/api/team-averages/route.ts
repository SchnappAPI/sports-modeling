import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const homeTeamId = req.nextUrl.searchParams.get('homeTeamId');
  const awayTeamId = req.nextUrl.searchParams.get('awayTeamId');
  const context    = req.nextUrl.searchParams.get('context') ?? '20';
  const periodsRaw = req.nextUrl.searchParams.get('periods') ?? '';
  const oppRaw     = req.nextUrl.searchParams.get('opp') ?? '';  // comma-sep "AWAY_OPP,HOME_OPP"
  const gameId     = req.nextUrl.searchParams.get('gameId') ?? '';  // for starter status

  if (!homeTeamId || !awayTeamId) {
    return NextResponse.json({ error: 'homeTeamId and awayTeamId required' }, { status: 400 });
  }

  // context='opp' means filter to games vs today's opponent.
  // In that case the caller passes opp=AWAY_OPP,HOME_OPP and we treat lastN=9999
  // but add a WHERE on matchup.
  const isOppMode = context === 'opp';
  const lastN = isOppMode ? 9999 : context === 'all' ? 9999 : Math.max(1, parseInt(context, 10) || 20);

  const VALID_PERIODS = ['1Q', '2Q', '3Q', '4Q', 'OT'];
  const periods = periodsRaw
    ? periodsRaw.split(',').map((p) => p.trim()).filter((p) => VALID_PERIODS.includes(p))
    : [];

  const periodClause = periods.length > 0
    ? `AND pbs.period IN (${periods.map((_, i) => `@period${i}`).join(', ')})`
    : '';

  // opp mode: opp param is "AWAY_OPP,HOME_OPP"
  // away players' games vs home opponent, home players' games vs away opponent.
  // We pass both and filter per-player in SQL using team_id.
  // Simpler: pass two matchup patterns and join via CASE on team_id.
  const [awayOpp, homeOpp] = oppRaw ? oppRaw.split(',') : ['', ''];
  const oppClause = isOppMode && awayOpp && homeOpp
    ? `AND (
         (tp.team_id = @awayTeamId AND pbs.matchup LIKE @awayOppPattern)
         OR (tp.team_id = @homeTeamId AND pbs.matchup LIKE @homeOppPattern)
       )`
    : '';

  try {
    const pool = await getPool();
    const req2 = pool
      .request()
      .input('homeTeamId', mssql.BigInt, parseInt(homeTeamId))
      .input('awayTeamId', mssql.BigInt, parseInt(awayTeamId))
      .input('lastN', mssql.Int, lastN);
    periods.forEach((p, i) => req2.input(`period${i}`, mssql.VarChar, p));
    if (isOppMode && awayOpp && homeOpp) {
      req2.input('awayOppPattern', mssql.VarChar, `%${homeOpp}%`);
      req2.input('homeOppPattern', mssql.VarChar, `%${awayOpp}%`);
    }
    if (gameId) req2.input('gameId', mssql.VarChar, gameId);

    const result = await req2.query(
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
         WHERE 1=1 ${periodClause} ${oppClause}
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
         ${gameId ? `dl.starter_status              AS starterStatus,` : `NULL                           AS starterStatus,`}
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
       ${gameId ? `LEFT JOIN nba.daily_lineups dl ON dl.game_id = @gameId AND dl.player_name = tp.player_name` : ''}
       GROUP BY tp.player_id, tp.player_name, tp.team_id, tp.team_tricode${gameId ? `, dl.starter_status` : ''}
       ORDER BY tp.team_id,
                CASE WHEN ${gameId ? `dl.starter_status` : `NULL`} = 'Starter' THEN 0 ELSE 1 END,
                avgPts DESC`
    );
    return NextResponse.json({ players: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
