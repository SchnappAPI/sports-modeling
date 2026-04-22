import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns FanDuel posted lines from odds.player_props per game per market.
// Used by the player game log for both coloring and the prop expand panel.
//
// Returns one row per (gameId, marketKey, lineValue), covering both standard
// and alternate markets. Each row includes:
//   - lineType: 'standard' | 'alternate'
//   - overPrice: the FanDuel Over price
//
// The game log coloring logic uses only standard rows (lineType = 'standard').
// The expand panel shows standard rows at the top, then alternate rows below.
//
// Within each (gameId, marketKey), only the most recent snap_ts is kept per
// line value to avoid showing the same line twice when odds were polled
// multiple times.
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
        `-- Get the player's odds_player_name for the join
         WITH player AS (
           SELECT TOP 1 odds_player_name
           FROM odds.player_map
           WHERE player_id = @playerId
             AND sport_key = 'basketball_nba'
         ),
         -- All graded games for this player (standard markets only, for game coverage)
         graded_games AS (
           SELECT DISTINCT egm.game_id, dg.event_id
           FROM common.daily_grades dg
           JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
           WHERE dg.player_id     = @playerId
             AND dg.bookmaker_key = 'fanduel'
             AND dg.market_key NOT LIKE '%_alternate'
         ),
         -- All FanDuel Over lines from player_props for those games,
         -- both standard and alternate, deduped to most recent snap per line value
         all_lines AS (
           SELECT
             gg.game_id       AS gameId,
             pp.market_key    AS marketKey,
             pp.outcome_point AS lineValue,
             pp.outcome_price AS overPrice,
             CASE WHEN pp.market_key LIKE '%_alternate' THEN 'alternate' ELSE 'standard' END AS lineType,
             ROW_NUMBER() OVER (
               PARTITION BY gg.game_id, pp.market_key, pp.outcome_point
               ORDER BY pp.snap_ts DESC
             ) AS rn
           FROM graded_games gg
           JOIN player p ON 1=1
           JOIN odds.player_props pp
             ON pp.event_id      = gg.event_id
            AND pp.player_name   = p.odds_player_name
            AND pp.bookmaker_key = 'fanduel'
            AND pp.outcome_name  = 'Over'
            AND pp.outcome_point IS NOT NULL
         )
         SELECT gameId, marketKey, lineValue, overPrice, lineType
         FROM all_lines
         WHERE rn = 1
         ORDER BY gameId, marketKey, lineValue`
      );
    return NextResponse.json({ grades: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
