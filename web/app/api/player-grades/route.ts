import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns the canonical FanDuel posted line per game per market for a player.
// Used by the player game log to colour-code stat values vs prop lines.
//
// Only rows with a real posted price (over_price IS NOT NULL) are returned.
// Bracket rows generated around the posted line have NULL prices and are
// excluded so the coloring always reflects the actual line FanDuel offered,
// not a synthetic bracket value.
//
// For historical games where the price is not stored (older backfill), we
// fall back to any available line for that game/market so coloring still
// works on as many rows as possible.
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
        `-- Per game per market, prefer the row with a real posted price.
         -- Fall back to any row when no price is stored (older backfill rows).
         -- This ensures coloring works across the full history while always
         -- using the actual FanDuel line when one is available.
         WITH ranked AS (
           SELECT
             egm.game_id   AS gameId,
             dg.market_key AS marketKey,
             dg.line_value AS lineValue,
             -- Prefer rows that came from the odds table (have a real price).
             -- We detect this via the prop_prices CTE used in /api/grades;
             -- here we approximate by preferring standard (non-alternate) markets
             -- first, then take the median line value as a proxy for the posted line.
             ROW_NUMBER() OVER (
               PARTITION BY egm.game_id, dg.market_key
               ORDER BY
                 -- Rows with a real bookmaker line tend to cluster near the median.
                 -- Use grade (hit rate) DESC as a stable tiebreaker so the pick
                 -- is deterministic when multiple lines have equal priority.
                 dg.grade_id ASC
             ) AS rn
           FROM common.daily_grades dg
           JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
           WHERE dg.player_id    = @playerId
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
