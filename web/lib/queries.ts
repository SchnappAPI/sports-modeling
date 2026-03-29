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
         CASE WHEN dl.starter_status = 'Starter' THEN 1 ELSE 0 END AS isStarter
       FROM nba.daily_lineups dl
       LEFT JOIN nba.players p ON p.player_name = dl.player_name
       WHERE dl.game_id = @gameId
       ORDER BY dl.team_tricode, dl.starter_status, dl.player_name`
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
         pbs.player_id AS playerId,
         p.player_name AS playerName,
         pbs.team_id   AS teamId,
         pbs.period    AS period,
         pbs.pts, pbs.reb, pbs.ast, pbs.stl, pbs.blk, pbs.tov,
         pbs.minutes AS min,
         pbs.fg3m, pbs.fgm, pbs.fga, pbs.ftm, pbs.fta
       FROM nba.player_box_score_stats pbs
       JOIN nba.players p ON p.player_id = pbs.player_id
       WHERE pbs.game_id = @gameId
       ORDER BY pbs.player_id, pbs.period`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Player detail — full season game log with DNP rows
// ---------------------------------------------------------------------------

export interface PlayerGameRow {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
  dnp: boolean;
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

// Returns every completed game the player's team played this season.
// Games where the player has no box score rows are returned with dnp=true.
// Stats are summed across all quarters (no FullGame period exists).
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
      `-- Find the player's current team
       WITH player_team AS (
         SELECT team_id
         FROM nba.players
         WHERE player_id = @playerId
       ),
       -- All completed games for that team, most recent first
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
       -- Sum box score quarters for this player per game
       player_totals AS (
         SELECT
           pbs.game_id,
           SUM(pbs.pts)     AS pts,
           SUM(pbs.reb)     AS reb,
           SUM(pbs.ast)     AS ast,
           SUM(pbs.stl)     AS stl,
           SUM(pbs.blk)     AS blk,
           SUM(pbs.tov)     AS tov,
           SUM(pbs.minutes) AS min,
           SUM(pbs.fg3m)    AS fg3m,
           SUM(pbs.fgm)     AS fgm,
           SUM(pbs.fga)     AS fga,
           SUM(pbs.ftm)     AS ftm,
           SUM(pbs.fta)     AS fta
         FROM nba.player_box_score_stats pbs
         WHERE pbs.player_id = @playerId
         GROUP BY pbs.game_id
       )
       SELECT
         tg.game_id                              AS gameId,
         CONVERT(VARCHAR(10), tg.game_date, 120) AS gameDate,
         CASE
           WHEN tg.home_team_id = (SELECT team_id FROM player_team)
           THEN tg.away_tricode
           ELSE tg.home_tricode
         END                                     AS opponentAbbr,
         CASE
           WHEN tg.home_team_id = (SELECT team_id FROM player_team)
           THEN 1 ELSE 0
         END                                     AS isHome,
         CASE WHEN pt.game_id IS NULL THEN 1 ELSE 0 END AS dnp,
         pt.pts, pt.reb, pt.ast, pt.stl, pt.blk, pt.tov,
         pt.min, pt.fg3m, pt.fgm, pt.fga, pt.ftm, pt.fta
       FROM team_games tg
       LEFT JOIN player_totals pt ON pt.game_id = tg.game_id
       ORDER BY tg.game_date DESC`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Grades
// ---------------------------------------------------------------------------

export interface GradeRow {
  gradeId: number;
  gradeDate: string;
  playerId: number;
  playerName: string;
  marketKey: string;
  lineValue: number;
  hitRate60: number | null;
  hitRate20: number | null;
  sampleSize60: number | null;
  sampleSize20: number | null;
  weightedHitRate: number | null;
  grade: number | null;
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
    `SELECT
       dg.grade_id          AS gradeId,
       CONVERT(VARCHAR(10), dg.grade_date, 120) AS gradeDate,
       dg.player_id         AS playerId,
       dg.player_name       AS playerName,
       dg.market_key        AS marketKey,
       dg.line_value        AS lineValue,
       dg.hit_rate_60       AS hitRate60,
       dg.hit_rate_20       AS hitRate20,
       dg.sample_size_60    AS sampleSize60,
       dg.sample_size_20    AS sampleSize20,
       dg.weighted_hit_rate AS weightedHitRate,
       dg.grade             AS grade
     FROM common.daily_grades dg
     LEFT JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
     WHERE CONVERT(VARCHAR(10), dg.grade_date, 120) = @gradeDate
     ${gameFilter}
     ORDER BY dg.grade DESC`
  );
  return result.recordset;
}
