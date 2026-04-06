import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

function todayCT(): string {
  const now = new Date();
  const ct = new Date(now.toLocaleString('en-US', { timeZone: 'America/Chicago' }));
  return `${ct.getFullYear()}-${String(ct.getMonth() + 1).padStart(2, '0')}-${String(ct.getDate()).padStart(2, '0')}`;
}

export async function GET(req: NextRequest) {
  const date = req.nextUrl.searchParams.get('date') ?? todayCT();

  const pool = await getPool();
  const result = await pool
    .request()
    .input('date', mssql.VarChar, date)
    .query(
      `SELECT
         g.game_pk           AS gameId,
         CONVERT(VARCHAR(10), g.game_date, 120) AS gameDate,
         g.game_status       AS gameStatus,
         g.game_display      AS gameDisplay,
         g.away_team_id      AS awayTeamId,
         g.home_team_id      AS homeTeamId,
         at.team_abbreviation AS awayTeamAbbr,
         ht.team_abbreviation AS homeTeamAbbr,
         at.full_name        AS awayTeamName,
         ht.full_name        AS homeTeamName,
         g.away_team_score   AS awayScore,
         g.home_team_score   AS homeScore,
         g.game_datetime     AS gameDateTime,
         g.away_pitcher_name AS awayPitcher,
         g.home_pitcher_name AS homePitcher
       FROM mlb.games g
       JOIN mlb.teams at ON at.team_id = g.away_team_id
       JOIN mlb.teams ht ON ht.team_id = g.home_team_id
       WHERE CONVERT(VARCHAR(10), g.game_date, 120) = @date
       ORDER BY g.game_datetime, g.game_pk`
    );

  return NextResponse.json({ date, games: result.recordset });
}
