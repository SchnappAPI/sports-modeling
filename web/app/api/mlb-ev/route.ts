import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Team-wide exit velocity summary for both teams in a given game.
//
// Scope:
//   - Season-to-date, excluding the selected game
//   - Starters: from mlb.batting_stats (batting_order % 100 = 0) when the
//     game has been played. Pre-game, fall back to the nine batters with
//     the most PAs in the last 14 days for each team ("projected")
//   - One summary row per batter plus the full per-at-bat detail list
//     (client-side expandable)
//
// Query plan:
//   1. Load starters for both teams. One UNION ALL covers the actual case;
//      a second query covers the projected fallback if either team has no
//      actual starters yet.
//   2. Aggregate mlb.player_at_bats over IX_player_at_bats_batter for the
//      <=18 batter_ids, filtered to season_year and game_pk != selected
//   3. Pull per-at-bat detail for the same batter_ids for tap-to-expand
//
// Why no materialization: at ~30-50 BBE per batter across 18 batters, this
// is a single-digit-ms indexed scan. ADR-0019 establishes no-runtime-agg
// as an invariant for lifetime BvP, but season-to-date EV does not need a
// matching rollup table until data volume justifies one.
//
// Column note: mlb.players.bat_side is the batter-hand column (L/R/S),
// matching the VS route. There is no `bats` column.

interface StarterRow {
  playerId: number;
  playerName: string;
  teamId: number;
  side: 'A' | 'H';
  position: string | null;
  bats: string | null;
  battingOrder: number | null;
  projected: boolean;
}

interface EvSummaryRow {
  playerId: number;
  bbe: number;
  avgEv: number | null;
  maxEv: number | null;
  hardHitPct: number | null;
  avgLa: number | null;
  sweetSpotPct: number | null;
  barrelPct: number | null;
  hrCount: number;
  avgXba: number | null;
}

interface EvAtBatRow {
  playerId: number;
  gamePk: number;
  gameDate: string;
  inning: number;
  pitcherId: number;
  pitcherName: string | null;
  resultType: string | null;
  exitVelo: number | null;
  launchAngle: number | null;
  distance: number | null;
  hitProb: number | null;
}

export async function GET(req: NextRequest) {
  const gamePkRaw = req.nextUrl.searchParams.get('gamePk');
  if (!gamePkRaw) return NextResponse.json({ error: 'gamePk required' }, { status: 400 });
  const gamePk = parseInt(gamePkRaw);
  if (Number.isNaN(gamePk)) return NextResponse.json({ error: 'gamePk invalid' }, { status: 400 });

  const pool = await getPool();

  // --- Step 1: resolve selected game to (away_team_id, home_team_id, game_date, season) ---
  const gameInfo = await pool
    .request()
    .input('gamePk', mssql.Int, gamePk)
    .query(
      `SELECT
         game_pk       AS gamePk,
         away_team_id  AS awayTeamId,
         home_team_id  AS homeTeamId,
         game_date     AS gameDate,
         YEAR(game_date) AS seasonYear
       FROM mlb.games
       WHERE game_pk = @gamePk`
    );

  if (gameInfo.recordset.length === 0) {
    return NextResponse.json({ error: 'game not found' }, { status: 404 });
  }
  const { awayTeamId, homeTeamId, gameDate, seasonYear } = gameInfo.recordset[0] as {
    awayTeamId: number;
    homeTeamId: number;
    gameDate: Date;
    seasonYear: number;
  };
  const seasonStart = `${seasonYear}-01-01`;

  // --- Step 2: actual starters from mlb.batting_stats ---
  const actualStarters = await pool
    .request()
    .input('gamePk', mssql.Int, gamePk)
    .query(
      `SELECT
         bs.player_id      AS playerId,
         p.player_name     AS playerName,
         bs.team_id        AS teamId,
         bs.side           AS side,
         bs.position       AS position,
         p.bat_side        AS bats,
         bs.batting_order  AS battingOrder
       FROM mlb.batting_stats bs
       LEFT JOIN mlb.players p ON p.player_id = bs.player_id
       WHERE bs.game_pk = @gamePk
         AND bs.batting_order IS NOT NULL
         AND bs.batting_order % 100 = 0
       ORDER BY bs.side, bs.batting_order`
    );

  const actualBySide = { A: [] as StarterRow[], H: [] as StarterRow[] };
  for (const r of actualStarters.recordset) {
    const row: StarterRow = { ...(r as StarterRow), projected: false };
    if (row.side === 'A') actualBySide.A.push(row);
    else if (row.side === 'H') actualBySide.H.push(row);
  }

  // --- Step 3: projected fallback for any team missing actual starters ---
  // Top 9 batters by PA in the last 14 days for that team. Position is
  // dropped on the projected path because bs.position varies per game and
  // there is no stable per-player position column to GROUP BY on.
  async function projectedForTeam(teamId: number, side: 'A' | 'H'): Promise<StarterRow[]> {
    const res = await pool
      .request()
      .input('teamId', mssql.Int, teamId)
      .input('gameDate', mssql.Date, gameDate)
      .query(
        `SELECT TOP 9
           bs.player_id     AS playerId,
           p.player_name    AS playerName,
           bs.team_id       AS teamId,
           p.bat_side       AS bats,
           SUM(bs.pa)       AS paTotal
         FROM mlb.batting_stats bs
         LEFT JOIN mlb.players p ON p.player_id = bs.player_id
         INNER JOIN mlb.games g ON g.game_pk = bs.game_pk
         WHERE bs.team_id = @teamId
           AND g.game_date >= DATEADD(day, -14, @gameDate)
           AND g.game_date <  @gameDate
           AND bs.pa IS NOT NULL
         GROUP BY bs.player_id, p.player_name, bs.team_id, p.bat_side
         ORDER BY paTotal DESC`
      );
    return res.recordset.map((r: any, idx: number) => ({
      playerId: r.playerId,
      playerName: r.playerName,
      teamId: r.teamId,
      side,
      position: null,
      bats: r.bats,
      battingOrder: (idx + 1) * 100,
      projected: true,
    }));
  }

  let awayStarters = actualBySide.A;
  let homeStarters = actualBySide.H;
  let awayProjected = false;
  let homeProjected = false;
  if (awayStarters.length === 0) {
    awayStarters = await projectedForTeam(awayTeamId, 'A');
    awayProjected = true;
  }
  if (homeStarters.length === 0) {
    homeStarters = await projectedForTeam(homeTeamId, 'H');
    homeProjected = true;
  }

  const starters: StarterRow[] = [...awayStarters, ...homeStarters];
  const playerIds = starters.map((s) => s.playerId);

  if (playerIds.length === 0) {
    return NextResponse.json({
      gamePk,
      awayTeamId,
      homeTeamId,
      seasonYear,
      awayProjected,
      homeProjected,
      starters: [],
      summary: [],
      atBats: [],
    });
  }

  // --- Step 4: aggregate season-to-date EV per batter, excluding current game ---
  // SQL Server doesn't accept arrays; use a comma-joined IN list. Safe because
  // playerIds are ints we just pulled from the DB.
  const idList = playerIds.join(',');

  const summary = await pool
    .request()
    .input('seasonStart', mssql.Date, seasonStart)
    .input('gamePk', mssql.Int, gamePk)
    .query(
      `SELECT
         batter_id AS playerId,
         COUNT(*)  AS bbe,
         AVG(CAST(hit_launch_speed AS FLOAT))    AS avgEv,
         MAX(hit_launch_speed)                    AS maxEv,
         SUM(CASE WHEN hit_launch_speed >= 95 THEN 1 ELSE 0 END) AS hardHitCount,
         AVG(CAST(hit_launch_angle AS FLOAT))     AS avgLa,
         SUM(CASE WHEN hit_launch_angle BETWEEN 8 AND 32 THEN 1 ELSE 0 END) AS sweetSpotCount,
         SUM(CASE WHEN hit_launch_speed >= 95 AND hit_launch_angle BETWEEN 8 AND 32 THEN 1 ELSE 0 END) AS barrelCount,
         SUM(CASE WHEN result_event_type = 'home_run' THEN 1 ELSE 0 END) AS hrCount,
         AVG(CAST(hit_probability AS FLOAT))      AS avgXba
       FROM mlb.player_at_bats
       WHERE batter_id IN (${idList})
         AND game_date >= @seasonStart
         AND game_pk   <> @gamePk
         AND hit_launch_speed IS NOT NULL
       GROUP BY batter_id`
    );

  const summaryRows: EvSummaryRow[] = summary.recordset.map((r: any) => {
    const bbe: number = r.bbe ?? 0;
    return {
      playerId: r.playerId,
      bbe,
      avgEv: r.avgEv,
      maxEv: r.maxEv,
      hardHitPct: bbe > 0 ? r.hardHitCount / bbe : null,
      avgLa: r.avgLa,
      sweetSpotPct: bbe > 0 ? r.sweetSpotCount / bbe : null,
      barrelPct: bbe > 0 ? r.barrelCount / bbe : null,
      hrCount: r.hrCount ?? 0,
      avgXba: r.avgXba,
    };
  });

  // --- Step 5: per-at-bat detail for tap-to-expand ---
  const detail = await pool
    .request()
    .input('seasonStart', mssql.Date, seasonStart)
    .input('gamePk', mssql.Int, gamePk)
    .query(
      `SELECT
         a.batter_id           AS playerId,
         a.game_pk             AS gamePk,
         a.game_date           AS gameDate,
         a.inning              AS inning,
         a.pitcher_id          AS pitcherId,
         pp.player_name        AS pitcherName,
         a.result_event_type   AS resultType,
         a.hit_launch_speed    AS exitVelo,
         a.hit_launch_angle    AS launchAngle,
         a.hit_total_distance  AS distance,
         a.hit_probability     AS hitProb
       FROM mlb.player_at_bats a
       LEFT JOIN mlb.players pp ON pp.player_id = a.pitcher_id
       WHERE a.batter_id IN (${idList})
         AND a.game_date >= @seasonStart
         AND a.game_pk   <> @gamePk
         AND a.hit_launch_speed IS NOT NULL
       ORDER BY a.batter_id, a.game_date DESC, a.at_bat_number DESC`
    );

  const atBats: EvAtBatRow[] = detail.recordset;

  return NextResponse.json({
    gamePk,
    awayTeamId,
    homeTeamId,
    seasonYear,
    awayProjected,
    homeProjected,
    starters,
    summary: summaryRows,
    atBats,
  });
}
