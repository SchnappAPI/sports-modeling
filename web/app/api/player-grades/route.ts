import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns the canonical FanDuel posted line per game per market for a player.
// Used by the player game log to colour-code stat values vs prop lines.
//
// For standard (non-alternate) markets we take the row with the lowest line
// value. FanDuel's posted line is always the lowest standard line — bracket
// lines written around it have higher values and must not shadow it.
//
// For alternate markets we fall back to grade_id ASC (earliest written) as a
// stable tiebreaker, since alternate lines have no canonical "posted" concept.
//
// Only fanduel rows are returned. Rows without a game mapping are excluded.
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
        `WITH ranked AS (
           SELECT
             egm.game_id   AS gameId,
             dg.market_key AS marketKey,
             dg.line_value AS lineValue,
             ROW_NUMBER() OVER (
               PARTITION BY egm.game_id, dg.market_key
               ORDER BY
                 -- Standard markets: lowest line_value = the actual posted line.
                 -- Bracket lines always have higher values and must be skipped.
                 -- Alternate markets: grade_id ASC as a stable fallback.
                 CASE WHEN dg.market_key NOT LIKE '%_alternate' THEN dg.line_value ELSE 9999 END ASC,
                 dg.grade_id ASC
             ) AS rn
           FROM common.daily_grades dg
           JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
           WHERE dg.player_id     = @playerId
             AND dg.bookmaker_key = 'fanduel'
         )
         SELECT gameId, marketKey, lineValue
         FROM ranked
         WHERE rn = 1
         ORDER BY gameId, marketKey`
      );
    return NextResponse.json({ grades: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
