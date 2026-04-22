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
// One row per (gameId, marketKey): standard (non-alternate) Over lines only.
// When player_props has no matching row for a graded game (e.g. pre-backfill
// history), falls back to the daily_grades row with the lowest line_value for
// that game/market, since the standard line is typically the lowest graded line
// (bracket lines were extrapolated upward from the posted line).
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
        `-- Primary: join daily_grades to odds.player_props to get the actual
         -- posted line. player_props has one canonical line per event/market
         -- so this is the correct reference for game log coloring.
         WITH graded_games AS (
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
         ),
         -- Look up the actual posted line from odds.player_props
         posted AS (
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
             ON pp.event_id    = gg.event_id
            AND pp.market_key  = gg.marketKey
            AND pp.player_name = gg.odds_player_name
            AND pp.bookmaker_key = 'fanduel'
            AND pp.outcome_name  = 'Over'
            AND pp.outcome_point IS NOT NULL
         ),
         -- Fallback: when no player_props row exists, use the lowest graded
         -- line from daily_grades (bracket lines were extrapolated upward, so
         -- the lowest is closest to the actual posted line).
         fallback AS (
           SELECT
             egm.game_id   AS gameId,
             dg.market_key AS marketKey,
             MIN(dg.line_value) AS lineValue,
             'Over'             AS outcomeName
           FROM common.daily_grades dg
           JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
           WHERE dg.player_id     = @playerId
             AND dg.bookmaker_key = 'fanduel'
             AND dg.outcome_name  = 'Over'
             AND dg.market_key NOT LIKE '%_alternate'
           GROUP BY egm.game_id, dg.market_key
         )
         SELECT
           COALESCE(p.gameId,    f.gameId)    AS gameId,
           COALESCE(p.marketKey, f.marketKey) AS marketKey,
           COALESCE(p.lineValue, f.lineValue) AS lineValue,
           'Over'                             AS outcomeName
         FROM fallback f
         LEFT JOIN (SELECT * FROM posted WHERE rn = 1) p
           ON p.gameId    = f.gameId
          AND p.marketKey = f.marketKey
         ORDER BY gameId, marketKey`
      );
    return NextResponse.json({ grades: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
