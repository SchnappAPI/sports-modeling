import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns all grade rows for a player across all dates this season.
// Used by the player game log to colour-code stat values vs prop lines.
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
        `SELECT
           egm.game_id                              AS gameId,
           dg.market_key                            AS marketKey,
           dg.line_value                            AS lineValue,
           dg.bookmaker_key                         AS bookmakerKey
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
