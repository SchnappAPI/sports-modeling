import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

/**
 * Lineup-wide career batter-vs-pitcher matchup for a given game.
 *
 * Reads:
 *   mlb.pitching_stats (note = 'SP') for each team's starting pitcher
 *   mlb.batting_stats  (batting_order % 100 = 0) for each team's starting
 *                      lineup in slot order
 *   mlb.career_batter_vs_pitcher for the lifetime stats of each (batter,
 *                                opposing SP) pair
 *
 * No runtime aggregation of mlb.player_at_bats or mlb.play_by_play. The
 * career-BvP table is materialized in-lockstep with the PBP loader; see
 * ADR-0019 in /docs/DECISIONS.md.
 *
 * Shape of response:
 *   {
 *     gamePk, awaySP { ... }, homeSP { ... },
 *     awayLineup: [ { batter, bvp } ... 9 rows ],
 *     homeLineup: [ { batter, bvp } ... 9 rows ],
 *     earliestDataDate: 'YYYY-MM-DD' | null   // earliest game_date across
 *                                              // all returned BvP rows
 *   }
 *
 * bvp is null when no career history exists (new matchup).
 */

interface Starter {
  playerId: number;
  playerName: string | null;
  teamId: number;
  handCode: string | null;
}

interface LineupSpot {
  batter: {
    playerId: number;
    playerName: string | null;
    teamId: number;
    battingOrder: number;
    position: string | null;
    handCode: string | null;
  };
  bvp: {
    pa: number;
    ab: number;
    h: number;
    hr: number;
    rbi: number;
    bb: number;
    k: number;
    avg: number | null;
    obp: number | null;
    slg: number | null;
    ops: number | null;
    lastFacedDate: string | null;
  } | null;
}

export async function GET(req: NextRequest) {
  const gamePkStr = req.nextUrl.searchParams.get('gamePk');
  if (!gamePkStr) return NextResponse.json({ error: 'gamePk required' }, { status: 400 });
  const gamePk = parseInt(gamePkStr);
  if (isNaN(gamePk)) return NextResponse.json({ error: 'gamePk must be an integer' }, { status: 400 });

  const pool = await getPool();

  // One round trip that pulls starters + lineup in a single resultset per
  // team using UNION ALL. Fewer roundtrips than four separate queries at
  // the cost of one WHERE clause that covers both tables.
  const startersAndBatters = await pool
    .request()
    .input('gamePk', mssql.Int, gamePk)
    .query(
      `-- Starting pitchers
       SELECT
         'SP'                  AS rowKind,
         p.player_id           AS playerId,
         pl.player_name        AS playerName,
         p.team_id             AS teamId,
         p.side                AS side,
         NULL                  AS battingOrder,
         NULL                  AS position,
         pl.pitch_hand         AS handCode
       FROM mlb.pitching_stats p
       LEFT JOIN mlb.players pl ON pl.player_id = p.player_id
       WHERE p.game_pk = @gamePk AND p.note = 'SP'
       UNION ALL
       -- Starting batters (batting_order % 100 = 0 drops subs; keeps slot-1
       -- through slot-9 starters in order)
       SELECT
         'BAT'                 AS rowKind,
         b.player_id           AS playerId,
         pl.player_name        AS playerName,
         b.team_id             AS teamId,
         b.side                AS side,
         b.batting_order       AS battingOrder,
         b.position            AS position,
         pl.bat_side           AS handCode
       FROM mlb.batting_stats b
       LEFT JOIN mlb.players pl ON pl.player_id = b.player_id
       WHERE b.game_pk = @gamePk AND b.batting_order % 100 = 0
       ORDER BY rowKind, side, battingOrder`
    );

  const rows = startersAndBatters.recordset as Array<{
    rowKind: 'SP' | 'BAT';
    playerId: number;
    playerName: string | null;
    teamId: number;
    side: 'A' | 'H';
    battingOrder: number | null;
    position: string | null;
    handCode: string | null;
  }>;

  const awaySP = rows.find((r) => r.rowKind === 'SP' && r.side === 'A');
  const homeSP = rows.find((r) => r.rowKind === 'SP' && r.side === 'H');

  const awayBatters = rows.filter((r) => r.rowKind === 'BAT' && r.side === 'A');
  const homeBatters = rows.filter((r) => r.rowKind === 'BAT' && r.side === 'H');

  // If either team has no SP row we bail early. The page falls back to a
  // "data not available" message. This can happen pre-game (mlb.pitching_stats
  // is populated at Final, not at game time) or for non-Final games.
  if (!awaySP || !homeSP) {
    return NextResponse.json({
      gamePk,
      awaySP: awaySP ? formatStarter(awaySP) : null,
      homeSP: homeSP ? formatStarter(homeSP) : null,
      awayLineup: [],
      homeLineup: [],
      earliestDataDate: null,
      available: false,
    });
  }

  // Collect the IDs we need BvP rows for. Each away batter vs the home SP;
  // each home batter vs the away SP. One query with both halves; the
  // (pitcher_id, batter_id) index covers it cleanly.
  const awayBatterIds = awayBatters.map((b) => b.playerId);
  const homeBatterIds = homeBatters.map((b) => b.playerId);

  let bvpRows: Array<{
    batterId: number;
    pitcherId: number;
    pa: number;
    ab: number;
    h: number;
    hr: number;
    rbi: number;
    bb: number;
    k: number;
    avg: number | null;
    obp: number | null;
    slg: number | null;
    ops: number | null;
    lastFacedDate: string | null;
  }> = [];

  if (awayBatterIds.length > 0 || homeBatterIds.length > 0) {
    // Build a dynamic WHERE clause with parameterized IDs. mssql requires
    // distinct parameter names per binding.
    const bvpReq = pool.request()
      .input('homeSP', mssql.Int, homeSP.playerId)
      .input('awaySP', mssql.Int, awaySP.playerId);

    const awayParams = awayBatterIds.map((id, i) => {
      const name = `awayB${i}`;
      bvpReq.input(name, mssql.Int, id);
      return `@${name}`;
    });
    const homeParams = homeBatterIds.map((id, i) => {
      const name = `homeB${i}`;
      bvpReq.input(name, mssql.Int, id);
      return `@${name}`;
    });

    // Two OR'd clauses: away batters vs home SP, home batters vs away SP.
    // Each half uses a matching PK half so both hit an index.
    const awayClause = awayParams.length > 0
      ? `(bvp.pitcher_id = @homeSP AND bvp.batter_id IN (${awayParams.join(', ')}))`
      : null;
    const homeClause = homeParams.length > 0
      ? `(bvp.pitcher_id = @awaySP AND bvp.batter_id IN (${homeParams.join(', ')}))`
      : null;
    const whereClause = [awayClause, homeClause].filter(Boolean).join(' OR ');

    const bvpResult = await bvpReq.query(
      `SELECT
         bvp.batter_id           AS batterId,
         bvp.pitcher_id          AS pitcherId,
         bvp.plate_appearances   AS pa,
         bvp.at_bats             AS ab,
         bvp.hits                AS h,
         bvp.home_runs           AS hr,
         bvp.rbi                 AS rbi,
         bvp.walks               AS bb,
         bvp.strikeouts          AS k,
         bvp.batting_avg         AS avg,
         bvp.obp                 AS obp,
         bvp.slg                 AS slg,
         bvp.ops                 AS ops,
         bvp.last_faced_date     AS lastFacedDate
       FROM mlb.career_batter_vs_pitcher bvp
       WHERE ${whereClause}`
    );
    bvpRows = bvpResult.recordset;
  }

  // Index BvP rows by (batterId, pitcherId) for O(1) lookup in the join loop
  const bvpIndex = new Map<string, typeof bvpRows[number]>();
  for (const r of bvpRows) {
    bvpIndex.set(`${r.batterId}:${r.pitcherId}`, r);
  }

  const awayLineup: LineupSpot[] = awayBatters.map((b) => ({
    batter: {
      playerId: b.playerId,
      playerName: b.playerName,
      teamId: b.teamId,
      battingOrder: b.battingOrder ?? 0,
      position: b.position,
      handCode: b.handCode,
    },
    bvp: lookupBvp(bvpIndex, b.playerId, homeSP.playerId),
  }));

  const homeLineup: LineupSpot[] = homeBatters.map((b) => ({
    batter: {
      playerId: b.playerId,
      playerName: b.playerName,
      teamId: b.teamId,
      battingOrder: b.battingOrder ?? 0,
      position: b.position,
      handCode: b.handCode,
    },
    bvp: lookupBvp(bvpIndex, b.playerId, awaySP.playerId),
  }));

  // Earliest career_batter_vs_pitcher.last_faced_date in the returned set
  // isn't quite right for the "data since X" footer — last_faced_date is
  // the MAX per pair, not the MIN across history. But it's a reasonable
  // proxy for "at least this pair's data is current as of X." For a true
  // earliest-data indicator we'd need a separate query against
  // mlb.play_by_play.MIN(game_date) which is fine to add later.
  const allDates = bvpRows.map((r) => r.lastFacedDate).filter((d): d is string => d != null);
  const earliestDataDate = allDates.length > 0
    ? allDates.sort()[0]
    : null;

  return NextResponse.json({
    gamePk,
    awaySP: formatStarter(awaySP),
    homeSP: formatStarter(homeSP),
    awayLineup,
    homeLineup,
    earliestDataDate,
    available: true,
  });
}

function formatStarter(r: {
  playerId: number;
  playerName: string | null;
  teamId: number;
  handCode: string | null;
}): Starter {
  return {
    playerId: r.playerId,
    playerName: r.playerName,
    teamId: r.teamId,
    handCode: r.handCode,
  };
}

function lookupBvp(
  index: Map<string, {
    pa: number;
    ab: number;
    h: number;
    hr: number;
    rbi: number;
    bb: number;
    k: number;
    avg: number | null;
    obp: number | null;
    slg: number | null;
    ops: number | null;
    lastFacedDate: string | null;
  }>,
  batterId: number,
  pitcherId: number,
): LineupSpot['bvp'] {
  const row = index.get(`${batterId}:${pitcherId}`);
  if (!row) return null;
  return {
    pa: row.pa,
    ab: row.ab,
    h: row.h,
    hr: row.hr,
    rbi: row.rbi,
    bb: row.bb,
    k: row.k,
    avg: row.avg,
    obp: row.obp,
    slg: row.slg,
    ops: row.ops,
    lastFacedDate: row.lastFacedDate,
  };
}
