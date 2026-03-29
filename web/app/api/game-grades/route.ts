import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns FanDuel grade rows for every player in a game.
// Keyed by player_id + market_key so BoxScoreTable can colour stat cells.
// Also returns oppTeamId and position so the player page can render
// the matchup defense section without an extra fetch.
export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }
  try {
    const pool = await getPool();
    const result = await pool
      .request()
      .input('gameId', mssql.VarChar, gameId)
      .query(
        `SELECT
           dg.player_id   AS playerId,
           dg.market_key  AS marketKey,
           dg.line_value  AS lineValue,
           CASE
             WHEN p.team_id = s.home_team_id THEN s.away_team_id
             ELSE s.home_team_id
           END            AS oppTeamId,
           p.position     AS position
         FROM common.daily_grades dg
         JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
         LEFT JOIN nba.schedule s ON s.game_id = egm.game_id
         LEFT JOIN nba.players p  ON p.player_id = dg.player_id
         WHERE egm.game_id        = @gameId
           AND dg.bookmaker_key   = 'fanduel'
         ORDER BY dg.player_id, dg.market_key`
      );
    return NextResponse.json({ gameId, grades: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
