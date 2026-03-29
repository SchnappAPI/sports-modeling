import mssql from 'mssql';
import { getPool } from './db';

// ---------------------------------------------------------------------------
// Ping
// ---------------------------------------------------------------------------

export async function ping(): Promise<void> {
  const pool = await getPool();
  await pool.request().query('SELECT 1');
}

// ---------------------------------------------------------------------------
// Games
// ---------------------------------------------------------------------------

export interface GameRow {
  gameId: string;
  gameDate: string;
  gameStatus: number | null;
  gameStatusText: string | null;
  homeTeamId: number;
  awayTeamId: number;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
  homeTeamName: string;
  awayTeamName: string;
  spread: number | null;
  total: number | null;
}

export async function getGames(sport: string, date: string): Promise<GameRow[]> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input('date', mssql.VarChar, date)
    .query<GameRow>(
      `WITH all_lines AS (
         SELECT
           ugl.event_id,
           ue.home_team,
           ugl.market_key,
           ugl.bookmaker_key,
           ugl.outcome_name,
           CAST(ugl.outcome_point AS FLOAT) AS outcome_point
         FROM odds.upcoming_game_lines ugl
         JOIN odds.upcoming_events ue ON ue.event_id = ugl.event_id

         UNION ALL

         SELECT
           gl.event_id,
           e.home_team,
           gl.market_key,
           gl.bookmaker_key,
           gl.outcome_name,
           CAST(gl.outcome_point AS FLOAT) AS outcome_point
         FROM odds.game_lines gl
         JOIN odds.events e ON e.event_id = gl.event_id
       ),
       best_lines AS (
         SELECT
           event_id,
           MAX(CASE WHEN market_key = 'spreads'
                    AND bookmaker_key = 'fanduel'
                    AND outcome_name = home_team
               THEN outcome_point END) AS spread,
           MAX(CASE WHEN market_key = 'totals'
                    AND bookmaker_key = 'fanduel'
                    AND outcome_name = 'Over'
               THEN outcome_point END) AS total
         FROM all_lines
         GROUP BY event_id
       )
       SELECT
         s.game_id          AS gameId,
         CONVERT(VARCHAR(10), s.game_date, 120) AS gameDate,
         s.game_status      AS gameStatus,
         s.game_status_text AS gameStatusText,
         s.home_team_id     AS homeTeamId,
         s.away_team_id     AS awayTeamId,
         ht.team_tricode    AS homeTeamAbbr,
         at.team_tricode    AS awayTeamAbbr,
         ht.team_name       AS homeTeamName,
         at.team_name       AS awayTeamName,
         bl.spread          AS spread,
         bl.total           AS total
       FROM nba.schedule s
       JOIN nba.teams ht ON ht.team_id = s.home_team_id
       JOIN nba.teams at ON at.team_id = s.away_team_id
       LEFT JOIN odds.event_game_map egm ON egm.game_id = s.game_id
       LEFT JOIN best_lines bl ON bl.event_id = egm.event_id
       WHERE CONVERT(VARCHAR(10), s.game_date, 120) = @date
       ORDER BY s.game_date`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Roster
// ---------------------------------------------------------------------------

export interface RosterRow {
  playerId: number | null;
  playerName: string;
  teamAbbr: string;
  position: string | null;
  isStarter: boolean;
  lineupStatus: string | null;  // 'Confirmed' | 'Projected' | null
}

export async function getRoster(gameId: string): Promise<RosterRow[]> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input('gameId', mssql.VarChar, gameId)
    .query<RosterRow>(
      `SELECT
         p.player_id                                    AS playerId,
         dl.player_name                                 AS playerName,
         dl.team_tricode                                AS teamAbbr,
         dl.position                                    AS position,
         CASE WHEN dl.starter_status = 'Starter' THEN 1 ELSE 0 END AS isStarter,
         dl.lineup_status                               AS lineupStatus
       FROM nba.daily_lineups dl
       LEFT JOIN nba.players p ON p.player_name = dl.player_name
       WHERE dl.game_id = @gameId
       ORDER BY dl.team_tricode,
                CASE WHEN dl.starter_status = 'Starter' THEN 0 ELSE 1 END,
                dl.player_name`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Player averages (lineup-anchored, used by /api/player-averages)
// ---------------------------------------------------------------------------

export interface PlayerAverageRow {
  playerId: number | null;
  playerName: string;
  games: number;
  avgPts: number | null;
  avgReb: number | null;
  avgAst: number | null;
  avgStl: number | null;
  avgBlk: number | null;
  avgTov: number | null;
  avgMin: number | null;
  avg3pm: number | null;
}

export async function getPlayerAverages(
  gameId: string,
  lastN: number
): Promise<PlayerAverageRow[]> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input('gameId', mssql.VarChar, gameId)
    .input('lastN', mssql.Int, lastN)
    .query<PlayerAverageRow>(
      `WITH lineup AS (
         SELECT dl.player_name, p.player_id
         FROM nba.daily_lineups dl
         LEFT JOIN nba.players p ON p.player_name = dl.player_name
         WHERE dl.game_id = @gameId
       ),
       game_totals AS (
         SELECT
           pbs.player_id, pbs.game_id, pbs.game_date,
           SUM(pbs.pts) AS pts, SUM(pbs.reb) AS reb, SUM(pbs.ast) AS ast,
           SUM(pbs.stl) AS stl, SUM(pbs.blk) AS blk, SUM(pbs.tov) AS tov,
           SUM(pbs.minutes) AS minutes, SUM(pbs.fg3m) AS fg3m
         FROM nba.player_box_score_stats pbs
         JOIN lineup l ON l.player_id = pbs.player_id
         GROUP BY pbs.player_id, pbs.game_id, pbs.game_date
       ),
       ranked AS (
         SELECT *, ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
         FROM game_totals
       )
       SELECT
         l.player_id AS playerId, l.player_name AS playerName,
         COUNT(r.game_id) AS games,
         AVG(CAST(r.pts AS FLOAT)) AS avgPts,
         AVG(CAST(r.reb AS FLOAT)) AS avgReb,
         AVG(CAST(r.ast AS FLOAT)) AS avgAst,
         AVG(CAST(r.stl AS FLOAT)) AS avgStl,
         AVG(CAST(r.blk AS FLOAT)) AS avgBlk,
         AVG(CAST(r.tov AS FLOAT)) AS avgTov,
         AVG(CAST(r.minutes AS FLOAT)) AS avgMin,
         AVG(CAST(r.fg3m AS FLOAT)) AS avg3pm
       FROM lineup l
       LEFT JOIN (SELECT * FROM ranked WHERE rn <= @lastN) r ON r.player_id = l.player_id
       GROUP BY l.player_id, l.player_name
       ORDER BY l.player_name`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Boxscore
// ---------------------------------------------------------------------------

export interface BoxscoreRow {
  playerId: number;
  playerName: string;
  teamId: number;
  period: string;
  starterStatus: string | null;
  pts: number | null;
  reb: number | null;
  ast: number | null;
  stl: number | null;
  blk: number | null;
  tov: number | null;
  min: number | null;
  fg3m: number | null;
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
}

export async function getBoxscore(gameId: string): Promise<BoxscoreRow[]> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input('gameId', mssql.VarChar, gameId)
    .query<BoxscoreRow>(
      `SELECT
         pbs.player_id        AS playerId,
         p.player_name        AS playerName,
         pbs.team_id          AS teamId,
         pbs.period           AS period,
         dl.starter_status    AS starterStatus,
         pbs.pts, pbs.reb, pbs.ast, pbs.stl, pbs.blk, pbs.tov,
         pbs.minutes AS min,
         pbs.fg3m, pbs.fgm, pbs.fga, pbs.ftm, pbs.fta
       FROM nba.player_box_score_stats pbs
       JOIN nba.players p ON p.player_id = pbs.player_id
       LEFT JOIN nba.daily_lineups dl
         ON dl.game_id = pbs.game_id
         AND dl.player_name = p.player_name
       WHERE pbs.game_id = @gameId
       ORDER BY
         CASE WHEN dl.starter_status = 'Starter' THEN 0 ELSE 1 END,
         pbs.player_id, pbs.period`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Player detail — per-quarter game log rows
// ---------------------------------------------------------------------------

export interface PlayerGameRow {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
  dnp: boolean;
  started: boolean | null;
  period: string;
  pts: number | null;
  reb: number | null;
  ast: number | null;
  stl: number | null;
  blk: number | null;
  tov: number | null;
  min: number | null;
  fg3m: number | null;
  fgm: number | null;
  fga: number | null;
  ftm: number | null;
  fta: number | null;
}

export async function getPlayerGames(
  playerId: number,
  lastN: number
): Promise<PlayerGameRow[]> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input('playerId', mssql.Int, playerId)
    .input('lastN', mssql.Int, lastN)
    .query<PlayerGameRow>(
      `WITH player_team AS (
         SELECT team_id FROM nba.players WHERE player_id = @playerId
       ),
       team_games AS (
         SELECT TOP (@lastN)
           g.game_id,
           g.game_date,
           g.home_team_id,
           ht.team_tricode AS home_tricode,
           at.team_tricode AS away_tricode
         FROM nba.games g
         JOIN nba.teams ht ON ht.team_id = g.home_team_id
         JOIN nba.teams at ON at.team_id = g.away_team_id
         WHERE g.home_team_id = (SELECT team_id FROM player_team)
            OR g.away_team_id = (SELECT team_id FROM player_team)
         ORDER BY g.game_date DESC
       ),
       player_quarters AS (
         SELECT
           pbs.game_id,
           pbs.period,
           pbs.pts, pbs.reb, pbs.ast, pbs.stl, pbs.blk, pbs.tov,
           pbs.minutes AS min,
           pbs.fg3m, pbs.fgm, pbs.fga, pbs.ftm, pbs.fta
         FROM nba.player_box_score_stats pbs
         WHERE pbs.player_id = @playerId
       ),
       played_games AS (
         SELECT DISTINCT game_id FROM player_quarters
       ),
       lineup_status AS (
         SELECT dl.game_id,
                CASE WHEN dl.starter_status = 'Starter' THEN 1 ELSE 0 END AS started
         FROM nba.daily_lineups dl
         JOIN nba.players p ON p.player_name = dl.player_name
         WHERE p.player_id = @playerId
       )
       SELECT
         tg.game_id                              AS gameId,
         CONVERT(VARCHAR(10), tg.game_date, 120) AS gameDate,
         CASE WHEN tg.home_team_id = (SELECT team_id FROM player_team)
              THEN tg.away_tricode ELSE tg.home_tricode END AS opponentAbbr,
         CASE WHEN tg.home_team_id = (SELECT team_id FROM player_team)
              THEN 1 ELSE 0 END                 AS isHome,
         0                                       AS dnp,
         ls.started                              AS started,
         pq.period,
         pq.pts, pq.reb, pq.ast, pq.stl, pq.blk, pq.tov,
         pq.min, pq.fg3m, pq.fgm, pq.fga, pq.ftm, pq.fta
       FROM team_games tg
       JOIN played_games pg ON pg.game_id = tg.game_id
       JOIN player_quarters pq ON pq.game_id = tg.game_id
       LEFT JOIN lineup_status ls ON ls.game_id = tg.game_id

       UNION ALL

       SELECT
         tg.game_id                              AS gameId,
         CONVERT(VARCHAR(10), tg.game_date, 120) AS gameDate,
         CASE WHEN tg.home_team_id = (SELECT team_id FROM player_team)
              THEN tg.away_tricode ELSE tg.home_tricode END AS opponentAbbr,
         CASE WHEN tg.home_team_id = (SELECT team_id FROM player_team)
              THEN 1 ELSE 0 END                 AS isHome,
         1                                       AS dnp,
         NULL                                    AS started,
         'FullGame'                              AS period,
         NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
       FROM team_games tg
       WHERE tg.game_id NOT IN (SELECT game_id FROM played_games)

       ORDER BY gameDate DESC, gameId, period`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Grades (At a Glance) — with opponent team ID and player position for
// contextual defense ranks
// ---------------------------------------------------------------------------

export interface GradeRow {
  gradeId: number;
  gradeDate: string;
  playerId: number;
  playerName: string;
  marketKey: string;
  lineValue: number;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  weightedHitRate: number | null;
  grade: number | null;
  // contextual fields — populated for today's grades when schedule data available
  oppTeamId: number | null;
  position: string | null;
}

export async function getGrades(
  gradeDate: string,
  gameId: string | null
): Promise<GradeRow[]> {
  const pool = await getPool();
  const req = pool.request().input('gradeDate', mssql.VarChar, gradeDate);
  const gameFilter = gameId != null ? `AND egm.game_id = @gameId` : '';
  if (gameId != null) req.input('gameId', mssql.VarChar, gameId);
  const result = await req.query<GradeRow>(
    `WITH prop_prices AS (
       SELECT event_id, market_key, player_id, MIN(outcome_price) AS over_price
       FROM odds.upcoming_player_props
       WHERE bookmaker_key = 'fanduel' AND outcome_name = 'Over' AND player_id IS NOT NULL
       GROUP BY event_id, market_key, player_id

       UNION ALL

       SELECT pp.event_id, pp.market_key, pm.player_id, MIN(pp.outcome_price) AS over_price
       FROM odds.player_props pp
       JOIN odds.player_map pm
         ON pm.odds_player_name = pp.player_name AND pm.sport_key = pp.sport_key
        AND pm.player_id IS NOT NULL
       WHERE pp.bookmaker_key = 'fanduel' AND pp.outcome_name = 'Over'
       GROUP BY pp.event_id, pp.market_key, pm.player_id
     ),
     best_price AS (
       SELECT event_id, market_key, player_id, MIN(over_price) AS over_price
       FROM prop_prices
       GROUP BY event_id, market_key, player_id
     )
     SELECT
       dg.grade_id          AS gradeId,
       CONVERT(VARCHAR(10), dg.grade_date, 120) AS gradeDate,
       dg.player_id         AS playerId,
       dg.player_name       AS playerName,
       dg.market_key        AS marketKey,
       dg.line_value        AS lineValue,
       bp.over_price        AS overPrice,
       dg.hit_rate_60       AS hitRate60,
       dg.hit_rate_20       AS hitRate20,
       dg.sample_size_60    AS sampleSize60,
       dg.sample_size_20    AS sampleSize20,
       dg.weighted_hit_rate AS weightedHitRate,
       dg.grade             AS grade,
       CASE
         WHEN p.team_id = s.home_team_id THEN s.away_team_id
         ELSE s.home_team_id
       END                  AS oppTeamId,
       p.position           AS position
     FROM common.daily_grades dg
     LEFT JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
     LEFT JOIN best_price bp
       ON bp.event_id = dg.event_id AND bp.market_key = dg.market_key AND bp.player_id = dg.player_id
     LEFT JOIN nba.players p ON p.player_id = dg.player_id
     LEFT JOIN nba.schedule s ON s.game_id = egm.game_id
     WHERE CONVERT(VARCHAR(10), dg.grade_date, 120) = @gradeDate
     ${gameFilter}
     ORDER BY dg.grade DESC`
  );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Player props — all graded markets for a single player across all dates
// ---------------------------------------------------------------------------

export interface PlayerPropRow {
  gradeId: number;
  gradeDate: string;
  marketKey: string;
  lineValue: number;
  overPrice: number | null;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  grade: number | null;
}

export async function getPlayerProps(playerId: number): Promise<PlayerPropRow[]> {
  const pool = await getPool();
  const result = await pool
    .request()
    .input('playerId', mssql.Int, playerId)
    .query<PlayerPropRow>(
      `WITH prop_prices AS (
         SELECT event_id, market_key, player_id, MIN(outcome_price) AS over_price
         FROM odds.upcoming_player_props
         WHERE bookmaker_key = 'fanduel' AND outcome_name = 'Over' AND player_id IS NOT NULL
         GROUP BY event_id, market_key, player_id

         UNION ALL

         SELECT pp.event_id, pp.market_key, pm.player_id, MIN(pp.outcome_price) AS over_price
         FROM odds.player_props pp
         JOIN odds.player_map pm
           ON pm.odds_player_name = pp.player_name AND pm.sport_key = pp.sport_key
          AND pm.player_id IS NOT NULL
         WHERE pp.bookmaker_key = 'fanduel' AND pp.outcome_name = 'Over'
         GROUP BY pp.event_id, pp.market_key, pm.player_id
       ),
       best_price AS (
         SELECT event_id, market_key, player_id, MIN(over_price) AS over_price
         FROM prop_prices
         GROUP BY event_id, market_key, player_id
       )
       SELECT
         dg.grade_id          AS gradeId,
         CONVERT(VARCHAR(10), dg.grade_date, 120) AS gradeDate,
         dg.market_key        AS marketKey,
         dg.line_value        AS lineValue,
         bp.over_price        AS overPrice,
         dg.hit_rate_60       AS hitRate60,
         dg.hit_rate_20       AS hitRate20,
         dg.sample_size_60    AS sampleSize60,
         dg.sample_size_20    AS sampleSize20,
         dg.grade             AS grade
       FROM common.daily_grades dg
       LEFT JOIN best_price bp
         ON bp.event_id = dg.event_id AND bp.market_key = dg.market_key AND bp.player_id = dg.player_id
       WHERE dg.player_id = @playerId
         AND dg.bookmaker_key = 'fanduel'
       ORDER BY dg.grade_date DESC, dg.market_key, dg.line_value
    `
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Matchup defense — how a team defends each stat category at a given position
//
// Returns per-stat averages allowed and ranks across all 30 teams.
// Rank 1 = most allowed (best matchup for overs). Rank 30 = fewest allowed.
// Position matching is loose: G covers PG/SG/G, F covers SF/PF/F, C is exact.
// ---------------------------------------------------------------------------

export interface MatchupStatLine {
  avg: number;
  rank: number;   // 1 = most allowed
  gamesDefended: number;
}

export interface MatchupDefenseRow {
  oppTeamId: number;
  oppTeamAbbr: string;
  position: string;
  pts: MatchupStatLine;
  reb: MatchupStatLine;
  ast: MatchupStatLine;
  stl: MatchupStatLine;
  blk: MatchupStatLine;
  fg3m: MatchupStatLine;
  tov: MatchupStatLine;
}

export async function getMatchupDefense(
  oppTeamId: number,
  position: string
): Promise<MatchupDefenseRow | null> {
  const pool = await getPool();

  // Normalize position to broad group for matching.
  // NBA positions in nba.players are typically: G, F, C, G-F, F-G, F-C, C-F.
  // We match on the primary position character.
  const posGroup =
    position.startsWith('G') ? 'G' :
    position.startsWith('F') ? 'F' :
    position.startsWith('C') ? 'C' : null;

  if (!posGroup) return null;

  const result = await pool
    .request()
    .input('oppTeamId', mssql.Int, oppTeamId)
    .input('posGroup', mssql.VarChar, posGroup)
    .query(
      `-- Season start: October 1 of the current NBA season year.
       -- If today is before October, the season started in the prior calendar year.
       WITH season_start AS (
         SELECT CAST(
           CAST(
             CASE WHEN MONTH(GETUTCDATE()) < 10
               THEN YEAR(GETUTCDATE()) - 1
               ELSE YEAR(GETUTCDATE())
             END
           AS VARCHAR(4)) + '-10-01'
         AS DATE) AS dt
       ),
       game_totals AS (
         SELECT
           pbs.player_id,
           pbs.game_id,
           CASE
             WHEN pbs.team_id = s.home_team_id THEN s.away_team_id
             ELSE s.home_team_id
           END AS opp_team_id,
           SUM(pbs.pts)    AS pts,
           SUM(pbs.reb)    AS reb,
           SUM(pbs.ast)    AS ast,
           SUM(pbs.stl)    AS stl,
           SUM(pbs.blk)    AS blk,
           SUM(pbs.fg3m)   AS fg3m,
           SUM(pbs.tov)    AS tov
         FROM nba.player_box_score_stats pbs
         JOIN nba.schedule s ON s.game_id = pbs.game_id
         WHERE s.game_date >= (SELECT dt FROM season_start)
         GROUP BY pbs.player_id, pbs.game_id, pbs.team_id, s.home_team_id, s.away_team_id
       ),
       pos_filtered AS (
         SELECT gt.*
         FROM game_totals gt
         JOIN nba.players p ON p.player_id = gt.player_id
         WHERE LEFT(p.position, 1) = @posGroup
       ),
       team_defense AS (
         SELECT
           opp_team_id,
           COUNT(*)                    AS games_defended,
           AVG(CAST(pts  AS FLOAT))    AS avg_pts,
           AVG(CAST(reb  AS FLOAT))    AS avg_reb,
           AVG(CAST(ast  AS FLOAT))    AS avg_ast,
           AVG(CAST(stl  AS FLOAT))    AS avg_stl,
           AVG(CAST(blk  AS FLOAT))    AS avg_blk,
           AVG(CAST(fg3m AS FLOAT))    AS avg_fg3m,
           AVG(CAST(tov  AS FLOAT))    AS avg_tov
         FROM pos_filtered
         GROUP BY opp_team_id
       ),
       ranked AS (
         SELECT
           opp_team_id,
           games_defended,
           avg_pts,  RANK() OVER (ORDER BY avg_pts  DESC) AS rank_pts,
           avg_reb,  RANK() OVER (ORDER BY avg_reb  DESC) AS rank_reb,
           avg_ast,  RANK() OVER (ORDER BY avg_ast  DESC) AS rank_ast,
           avg_stl,  RANK() OVER (ORDER BY avg_stl  DESC) AS rank_stl,
           avg_blk,  RANK() OVER (ORDER BY avg_blk  DESC) AS rank_blk,
           avg_fg3m, RANK() OVER (ORDER BY avg_fg3m DESC) AS rank_fg3m,
           avg_tov,  RANK() OVER (ORDER BY avg_tov  DESC) AS rank_tov
         FROM team_defense
       )
       SELECT
         r.opp_team_id    AS oppTeamId,
         t.team_tricode   AS oppTeamAbbr,
         r.games_defended AS gamesDefended,
         r.avg_pts,  r.rank_pts,
         r.avg_reb,  r.rank_reb,
         r.avg_ast,  r.rank_ast,
         r.avg_stl,  r.rank_stl,
         r.avg_blk,  r.rank_blk,
         r.avg_fg3m, r.rank_fg3m,
         r.avg_tov,  r.rank_tov
       FROM ranked r
       JOIN nba.teams t ON t.team_id = r.opp_team_id
       WHERE r.opp_team_id = @oppTeamId`
    );

  if (result.recordset.length === 0) return null;
  const row = result.recordset[0];

  const line = (avg: number, rank: number, gd: number): MatchupStatLine => ({ avg, rank, gamesDefended: gd });
  return {
    oppTeamId:   row.oppTeamId,
    oppTeamAbbr: row.oppTeamAbbr,
    position,
    gamesDefended: row.gamesDefended,
    pts:  line(row.avg_pts,  row.rank_pts,  row.gamesDefended),
    reb:  line(row.avg_reb,  row.rank_reb,  row.gamesDefended),
    ast:  line(row.avg_ast,  row.rank_ast,  row.gamesDefended),
    stl:  line(row.avg_stl,  row.rank_stl,  row.gamesDefended),
    blk:  line(row.avg_blk,  row.rank_blk,  row.gamesDefended),
    fg3m: line(row.avg_fg3m, row.rank_fg3m, row.gamesDefended),
    tov:  line(row.avg_tov,  row.rank_tov,  row.gamesDefended),
  } as MatchupDefenseRow;
}
