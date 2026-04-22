import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns the canonical FanDuel posted line per game per market for a player.
// Used by the player game log to colour-code stat values vs prop lines and to
// populate the per-game prop expand panel.
//
// Source of truth: odds.player_props (raw API data), not common.daily_grades.
// daily_grades historically stored multiple line values per game/market due to
// bracket extrapolation (now removed), making grade_id ordering unreliable for
// identifying the actual posted line. player_props contains exactly what FanDuel
// posted, so it is unambiguous.
//
// Returns one row per (gameId, marketKey): standard (non-alternate) Over lines
// only. When no player_props row exists for a graded game, no row is returned
// for that market — the game log shows neutral gray rather than a potentially
// wrong value.
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
        `WITH graded_games AS (
           SELECT DISTINCT
             egm.game_id   AS gameId,
             dg.event_id,
             dg.market_key AS marketKey,
             pm.odds_player_name
           FROM common.daily_grades dg
           JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
           JOIN odds.player_map pm
             ON pm.player_id = dg.player_id
            AND pm.sport_key = 'basketball_nba'
           WHERE dg.player_id     = @playerId
             AND dg.bookmaker_key = 'fanduel'
             AND dg.market_key NOT LIKE '%_alternate'
         )
         SELECT gameId, marketKey, lineValue, outcomeName
         FROM (
           SELECT
             gg.gameId,
             gg.marketKey,
             pp.outcome_point AS lineValue,
             pp.outcome_name  AS outcomeName,
             ROW_NUMBER() OVER (
               PARTITION BY gg.gameId, gg.marketKey
               ORDER BY pp.snap_ts DESC
             ) AS rn
           FROM graded_games gg
           JOIN odds.player_props pp
             ON pp.event_id      = gg.event_id
            AND pp.market_key    = gg.marketKey
            AND pp.player_name   = gg.odds_player_name
            AND pp.bookmaker_key = 'fanduel'
            AND pp.outcome_name  = 'Over'
            AND pp.outcome_point IS NOT NULL
         ) ranked
         WHERE rn = 1
         ORDER BY gameId, marketKey`
      );
    return NextResponse.json({ grades: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
