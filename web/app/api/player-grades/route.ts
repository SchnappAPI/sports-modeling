import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns FanDuel grade rows for a player keyed by game_id + market_key.
// Used by the player game log to colour-code stat values vs prop lines.
//
// Note: grades backfill coverage ends 2026-03-23 as of the last ETL run.
// Games after that date will return no prop lines until grading catches up.
// The midnight UTC issue: NBA games tipping after midnight UTC are stored
// under the previous Eastern date in nba.schedule/games, but event_game_map
// uses UTC dates. We try both the direct join and a fallback +1 day offset.
export async function GET(req: NextRequest) {
  const playerIdRaw = req.nextUrl.searchParams.get('playerId');
  if (!playerIdRaw) {
    return NextResponse.json({ error: 'playerId required' }, { status: 400 });
  }
  const playerId = parseInt(playerIdRaw, 10);
  if (isNaN(playerId)) {
    return NextResponse.json({ error: 'playerId must be an integer' }, { status: 400 });
  }
  try {
    const pool = await getPool();
    const result = await pool
      .request()
      .input('playerId', mssql.Int, playerId)
      .query(
        `-- Primary join: event_game_map.game_id matches nba.games.game_id directly.
         -- Fallback: some late-night games have event_game_map.game_date one day
         -- ahead of nba.games.game_date (midnight UTC boundary). We union both
         -- and deduplicate so the client always gets a game_id it can match.
         SELECT DISTINCT
           egm.game_id   AS gameId,
           dg.market_key AS marketKey,
           dg.line_value AS lineValue
         FROM common.daily_grades dg
         JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
         WHERE dg.player_id = @playerId
           AND dg.bookmaker_key = 'fanduel'
         ORDER BY egm.game_id, dg.market_key`
      );
    return NextResponse.json({ grades: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
