import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Trends Grid feed: per-game tier lines from common.player_tier_lines,
// plus today's FanDuel standard line, plus per-player game log hit/miss
// against that standard line.
//
// Roadmap: /docs/ROADMAP.md :: NBA Trends Grid.
// ADR-20260423-1 is the source of the tier system being surfaced here.
//
// Query params:
//   gameId   (required) - NBA game_id, e.g. '0042500123'
//   market   (optional) - one of the standard markets. Default 'player_points'.
//                         Alternate markets (with _alternate suffix) are not
//                         grid-rendered; tier rows exist for them but the
//                         grid toggles between standard stat categories.
//   window   (optional) - 10 | 30 | 82 (or 'all'). Default 30. Caps the
//                         game-log array length per player.
//
// Response shape:
// {
//   gameId, marketKey, gameDate,
//   home: { teamId, teamAbbr },
//   away: { teamId, teamAbbr },
//   players: [
//     {
//       playerId, playerName, teamTricode, position,
//       lineupStatus,      // 'Confirmed' | 'Projected' | null
//       starterStatus,     // 'Starter' | 'Bench' | 'Inactive' | null
//       compositeGrade,    // may be null if not graded
//       kdeWindow,         // 15 | 30 | 82 | null
//       blowoutDampened,   // boolean
//       safeLine, safeProb,
//       valueLine, valueProb,
//       highriskLine, highriskProb, highriskPrice,
//       lottoLine, lottoProb, lottoPrice,
//       standardLine,      // today's FanDuel standard line for this player-market
//       standardPrice,     // today's FanDuel Over price
//       gameLog: [
//         { gameId, gameDate, oppTricode, minutes, stat, hit }
//       ]
//     }
//   ]
// }

const DEFAULT_MARKET = 'player_points';
const ALLOWED_MARKETS = new Set([
  'player_points',
  'player_rebounds',
  'player_assists',
  'player_threes',
  'player_points_rebounds_assists',
  'player_points_rebounds',
  'player_points_assists',
  'player_rebounds_assists',
]);

// Maps market_key to the box-score column expression used for game-log stat
// computation. Combo markets sum the components.
const MARKET_STAT_SQL: Record<string, string> = {
  player_points:                    'pbs.pts',
  player_rebounds:                  'pbs.reb',
  player_assists:                   'pbs.ast',
  player_threes:                    'pbs.fg3m',
  player_points_rebounds_assists:   'pbs.pts + pbs.reb + pbs.ast',
  player_points_rebounds:           'pbs.pts + pbs.reb',
  player_points_assists:            'pbs.pts + pbs.ast',
  player_rebounds_assists:          'pbs.reb + pbs.ast',
};

interface TierRow {
  playerId: number;
  playerName: string;
  compositeGrade: number | null;
  kdeWindow: number | null;
  blowoutDampened: boolean;
  safeLine: number | null;
  safeProb: number | null;
  valueLine: number | null;
  valueProb: number | null;
  highriskLine: number | null;
  highriskProb: number | null;
  highriskPrice: number | null;
  lottoLine: number | null;
  lottoProb: number | null;
  lottoPrice: number | null;
}

interface LineupRow {
  playerId: number;
  playerName: string;
  teamTricode: string;
  position: string | null;
  starterStatus: string | null;
  lineupStatus: string | null;
}

interface StandardLineRow {
  playerId: number;
  standardLine: number;
  standardPrice: number | null;
}

interface GameLogRow {
  playerId: number;
  gameId: string;
  gameDate: string;
  oppTricode: string;
  minutes: number | null;
  stat: number;
}

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  const marketParam = req.nextUrl.searchParams.get('market') || DEFAULT_MARKET;
  const windowParam = req.nextUrl.searchParams.get('window') || '30';

  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }
  if (!ALLOWED_MARKETS.has(marketParam)) {
    return NextResponse.json({ error: `market must be one of: ${[...ALLOWED_MARKETS].join(', ')}` }, { status: 400 });
  }

  let logWindow: number | null;
  if (windowParam === 'all') {
    logWindow = null;
  } else {
    const n = parseInt(windowParam, 10);
    if (isNaN(n) || n < 1 || n > 200) {
      return NextResponse.json({ error: 'window must be 10, 30, 82, or all' }, { status: 400 });
    }
    logWindow = n;
  }

  const statExpr = MARKET_STAT_SQL[marketParam];

  try {
    const pool = await getPool();

    // 1. Game metadata: teams and date.
    const gameRes = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .query<{
        gameDate: string;
        homeTeamId: number;
        awayTeamId: number;
        homeAbbr: string;
        awayAbbr: string;
        eventId: string | null;
      }>(`
        SELECT
          CONVERT(VARCHAR(10), s.game_date, 23) AS gameDate,
          s.home_team_id             AS homeTeamId,
          s.away_team_id             AS awayTeamId,
          ht.team_tricode            AS homeAbbr,
          at.team_tricode            AS awayAbbr,
          (SELECT TOP 1 egm.event_id FROM odds.event_game_map egm WHERE egm.game_id = @gameId) AS eventId
        FROM nba.schedule s
        JOIN nba.teams ht ON ht.team_id = s.home_team_id
        JOIN nba.teams at ON at.team_id = s.away_team_id
        WHERE s.game_id = @gameId
      `);
    if (gameRes.recordset.length === 0) {
      return NextResponse.json({ error: 'game not found' }, { status: 404 });
    }
    const { gameDate, homeTeamId, awayTeamId, homeAbbr, awayAbbr, eventId } = gameRes.recordset[0];

    // 2. Tier lines for this game and market. Already keyed by (grade_date, game_id, player_id, market_key)
    //    with one row per player.
    const tierRes = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .input('gameDate', mssql.Date, gameDate)
      .input('marketKey', mssql.VarChar, marketParam)
      .query<TierRow>(`
        SELECT
          ptl.player_id       AS playerId,
          ptl.player_name     AS playerName,
          ptl.composite_grade AS compositeGrade,
          ptl.kde_window      AS kdeWindow,
          ptl.blowout_dampened AS blowoutDampened,
          ptl.safe_line       AS safeLine,
          ptl.safe_prob       AS safeProb,
          ptl.value_line      AS valueLine,
          ptl.value_prob      AS valueProb,
          ptl.highrisk_line   AS highriskLine,
          ptl.highrisk_prob   AS highriskProb,
          ptl.highrisk_price  AS highriskPrice,
          ptl.lotto_line      AS lottoLine,
          ptl.lotto_prob      AS lottoProb,
          ptl.lotto_price     AS lottoPrice
        FROM common.player_tier_lines ptl
        WHERE ptl.grade_date = @gameDate
          AND ptl.game_id    = @gameId
          AND ptl.market_key = @marketKey
      `);

    // 3. Lineup: player / team / position / starter status for every player in the game.
    const lineupRes = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .query<LineupRow>(`
        SELECT
          p.player_id        AS playerId,
          dl.player_name     AS playerName,
          dl.team_tricode    AS teamTricode,
          CASE
            WHEN dl.starter_status = 'Starter' AND dl.position IS NOT NULL
              THEN dl.position
            ELSE COALESCE(p.position, dl.position)
          END                AS position,
          dl.starter_status  AS starterStatus,
          dl.lineup_status   AS lineupStatus
        FROM nba.daily_lineups dl
        LEFT JOIN nba.players p ON p.player_name = dl.player_name
        WHERE dl.game_id = @gameId
      `);

    // 4. Standard line per (player, market) for today from odds.upcoming_player_props.
    //    Use the most recent snap_ts per player.
    let standardRows: StandardLineRow[] = [];
    if (eventId) {
      const r = await pool.request()
        .input('eventId', mssql.VarChar, eventId)
        .input('marketKey', mssql.VarChar, marketParam)
        .query<StandardLineRow>(`
          WITH ranked AS (
            SELECT
              pm.player_id        AS playerId,
              pp.outcome_point    AS standardLine,
              pp.outcome_price    AS standardPrice,
              ROW_NUMBER() OVER (PARTITION BY pm.player_id ORDER BY pp.snap_ts DESC) AS rn
            FROM odds.upcoming_player_props pp
            JOIN odds.player_map pm
              ON pm.odds_player_name = pp.player_name
             AND pm.sport_key        = 'basketball_nba'
            WHERE pp.event_id        = @eventId
              AND pp.market_key      = @marketKey
              AND pp.bookmaker_key   = 'fanduel'
              AND pp.outcome_name    = 'Over'
              AND pp.outcome_point IS NOT NULL
          )
          SELECT playerId, standardLine, standardPrice FROM ranked WHERE rn = 1
        `);
      standardRows = r.recordset;
    }

    // 5. Game log per player: last N games of the stat computed per market.
    //    Excludes the current game (don't show future self-prediction).
    const playerIds = Array.from(new Set([
      ...tierRes.recordset.map(r => r.playerId),
      ...lineupRes.recordset.map(r => r.playerId).filter((x): x is number => x !== null),
    ]));

    let gameLogRows: GameLogRow[] = [];
    if (playerIds.length > 0) {
      // Build the player-id IN () list server-side.
      // mssql doesn't support array parameter binding, so we inline the integer ids
      // (they're sourced from DB results, safe to concatenate as integers).
      const idList = playerIds.map(n => String(n)).join(',');
      const r = await pool.request()
        .input('gameId', mssql.VarChar, gameId)
        .query<GameLogRow>(`
          WITH player_games AS (
            SELECT
              pbs.player_id                         AS playerId,
              pbs.game_id                           AS gameId,
              CONVERT(VARCHAR(10), s.game_date, 23)   AS gameDate,
              CASE
                WHEN pbs.team_id = s.home_team_id THEN at2.team_tricode
                ELSE ht2.team_tricode
              END                                   AS oppTricode,
              SUM(CAST(pbs.minutes AS FLOAT))       AS minutes,
              SUM(${statExpr})                      AS stat,
              ROW_NUMBER() OVER (PARTITION BY pbs.player_id ORDER BY s.game_date DESC) AS rn
            FROM nba.player_box_score_stats pbs
            JOIN nba.schedule s  ON s.game_id = pbs.game_id
            JOIN nba.teams ht2   ON ht2.team_id = s.home_team_id
            JOIN nba.teams at2   ON at2.team_id = s.away_team_id
            WHERE pbs.player_id IN (${idList})
              AND pbs.game_id <> @gameId
            GROUP BY pbs.player_id, pbs.game_id, s.game_date,
                     pbs.team_id, s.home_team_id,
                     ht2.team_tricode, at2.team_tricode
          )
          SELECT playerId, gameId, gameDate, oppTricode, minutes, stat
          FROM player_games
          ${logWindow !== null ? `WHERE rn <= ${logWindow}` : ''}
          ORDER BY playerId, gameDate DESC
        `);
      gameLogRows = r.recordset;
    }

    // Assemble response: merge by playerId.
    const tierByPlayer = new Map<number, TierRow>();
    for (const row of tierRes.recordset) tierByPlayer.set(row.playerId, row);
    const standardByPlayer = new Map<number, StandardLineRow>();
    for (const row of standardRows) standardByPlayer.set(row.playerId, row);
    const logsByPlayer = new Map<number, Array<Omit<GameLogRow, 'playerId'> & { hit: boolean | null }>>();
    for (const row of gameLogRows) {
      if (!logsByPlayer.has(row.playerId)) logsByPlayer.set(row.playerId, []);
      const standardLine = standardByPlayer.get(row.playerId)?.standardLine ?? null;
      const hit = standardLine === null ? null : row.stat > standardLine;
      logsByPlayer.get(row.playerId)!.push({
        gameId:      row.gameId,
        gameDate:    row.gameDate,
        oppTricode:  row.oppTricode,
        minutes:     row.minutes,
        stat:        row.stat,
        hit,
      });
    }

    // Build lineup lookup for quick merge.
    const lineupByPlayer = new Map<number, LineupRow>();
    for (const row of lineupRes.recordset) {
      if (row.playerId !== null) lineupByPlayer.set(row.playerId, row);
    }

    // Union: include every player that has EITHER a tier row OR a lineup row.
    // Tier-without-lineup happens on playoff days when Stage 2 boxscorepreviewv3
    // hasn't populated bench/inactive yet. Lineup-without-tier happens for
    // players who don't have enough game history to be graded.
    const unionIds = new Set<number>([
      ...tierByPlayer.keys(),
      ...lineupByPlayer.keys(),
    ]);

    const players = [...unionIds].map(pid => {
      const lineup = lineupByPlayer.get(pid);
      const tier = tierByPlayer.get(pid);
      const standard = standardByPlayer.get(pid);
      const gameLog = logsByPlayer.get(pid) ?? [];
      // Prefer tier player_name (always present when tier row exists); fall
      // back to lineup (may be missing diacritics handled differently). Tie
      // goes to lineup because it matches display usage elsewhere.
      const playerName = lineup?.playerName ?? tier?.playerName ?? 'Unknown';
      return {
        playerId:       pid,
        playerName,
        teamTricode:    lineup?.teamTricode ?? null,
        position:       lineup?.position ?? null,
        lineupStatus:   lineup?.lineupStatus ?? null,
        starterStatus:  lineup?.starterStatus ?? null,
        compositeGrade: tier?.compositeGrade ?? null,
        kdeWindow:      tier?.kdeWindow ?? null,
        blowoutDampened: tier?.blowoutDampened ?? false,
        safeLine:       tier?.safeLine ?? null,
        safeProb:       tier?.safeProb ?? null,
        valueLine:      tier?.valueLine ?? null,
        valueProb:      tier?.valueProb ?? null,
        highriskLine:   tier?.highriskLine ?? null,
        highriskProb:   tier?.highriskProb ?? null,
        highriskPrice:  tier?.highriskPrice ?? null,
        lottoLine:      tier?.lottoLine ?? null,
        lottoProb:      tier?.lottoProb ?? null,
        lottoPrice:     tier?.lottoPrice ?? null,
        standardLine:   standard?.standardLine ?? null,
        standardPrice:  standard?.standardPrice ?? null,
        gameLog,
      };
    });

    return NextResponse.json({
      gameId,
      marketKey: marketParam,
      gameDate,
      home: { teamId: homeTeamId, teamAbbr: homeAbbr },
      away: { teamId: awayTeamId, teamAbbr: awayAbbr },
      players,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
