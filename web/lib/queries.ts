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

// Source is nba.schedule, not nba.games. nba.games only contains completed games
// (populated by box score ETL). nba.schedule contains all games regardless of status.
// game_status: 1 = upcoming, 2 = in progress, 3 = final.
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

// nba.daily_lineups has no player_id or team_id — identified by player_name and
// team_tricode. Join to nba.players on name to get player_id; LEFT JOIN because
// a player may exist in lineups before the players table is updated.
// starter_status values: 'Starter' | 'Bench'
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
// Player averages
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

// daily_lineups has no player_id — join to players on name to get IDs,
// then use those IDs to look up box score history.
// Players with no box score history appear with NULL averages and games = 0.
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
         SELECT
           dl.player_name,
           p.player_id
         FROM nba.daily_lineups dl
         LEFT JOIN nba.players p ON p.player_name = dl.player_name
         WHERE dl.game_id = @gameId
       ),
       ranked AS (
         SELECT
           pbs.player_id,
           pbs.game_id,
           ROW_NUMBER() OVER (PARTITION BY pbs.player_id ORDER BY g.game_date DESC) AS rn
         FROM nba.player_box_score_stats pbs
         JOIN nba.games g ON g.game_id = pbs.game_id
         JOIN lineup l ON l.player_id = pbs.player_id
         WHERE pbs.period = 'FullGame'
       ),
       recent AS (
         SELECT player_id, game_id FROM ranked WHERE rn <= @lastN
       )
       SELECT
         l.player_id                          AS playerId,
         l.player_name                        AS playerName,
         COUNT(DISTINCT pbs.game_id)          AS games,
         AVG(CAST(pbs.pts  AS FLOAT))         AS avgPts,
         AVG(CAST(pbs.reb  AS FLOAT))         AS avgReb,
         AVG(CAST(pbs.ast  AS FLOAT))         AS avgAst,
         AVG(CAST(pbs.stl  AS FLOAT))         AS avgStl,
         AVG(CAST(pbs.blk  AS FLOAT))         AS avgBlk,
         AVG(CAST(pbs.tov  AS FLOAT))         AS avgTov,
         AVG(CAST(pbs.min  AS FLOAT))         AS avgMin,
         AVG(CAST(pbs.fg3m AS FLOAT))         AS avg3pm
       FROM lineup l
       LEFT JOIN nba.player_box_score_stats pbs
         ON pbs.player_id = l.player_id
         AND pbs.game_id IN (SELECT game_id FROM recent WHERE player_id = l.player_id)
         AND pbs.period = 'FullGame'
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
         pbs.player_id  AS playerId,
         p.player_name  AS playerName,
         pbs.team_id    AS teamId,
         pbs.period     AS period,
         pbs.pts, pbs.reb, pbs.ast, pbs.stl, pbs.blk,
         pbs.tov, pbs.min, pbs.fg3m, pbs.fgm, pbs.fga,
         pbs.ftm, pbs.fta
       FROM nba.player_box_score_stats pbs
       JOIN nba.players p ON p.player_id = pbs.player_id
       WHERE pbs.game_id = @gameId
       ORDER BY pbs.player_id, pbs.period`
    );
  return result.recordset;
}

// ---------------------------------------------------------------------------
// Player detail
// ---------------------------------------------------------------------------

export interface PlayerGameRow {
  gameId: string;
  gameDate: string;
  opponentAbbr: string;
  isHome: boolean;
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
      `SELECT TOP (@lastN)
         pbs.game_id       AS gameId,
         CONVERT(VARCHAR(10), g.game_date, 120) AS gameDate,
         CASE WHEN g.home_team_id = pbs.team_id
              THEN at.team_tricode
              ELSE ht.team_tricode
         END               AS opponentAbbr,
         CASE WHEN g.home_team_id = pbs.team_id THEN 1 ELSE 0 END AS isHome,
         pbs.pts, pbs.reb, pbs.ast, pbs.stl, pbs.blk,
         pbs.tov, pbs.min, pbs.fg3m, pbs.fgm, pbs.fga,
         pbs.ftm, pbs.fta
       FROM nba.player_box_score_stats pbs
       JOIN nba.games g  ON g.game_id  = pbs.game_id
       JOIN nba.teams ht ON ht.team_id = g.home_team_id
       JOIN nba.teams at ON at.team_id = g.away_team_id
       WHERE pbs.player_id = @playerId
         AND pbs.period = 'FullGame'
       ORDER BY g.game_date DESC`
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
  const gameFilter =
    gameId != null
      ? `AND egm.game_id = @gameId`
      : '';
  if (gameId != null) {
    req.input('gameId', mssql.VarChar, gameId);
  }
  const result = await req.query<GradeRow>(
    `SELECT
       dg.grade_id           AS gradeId,
       CONVERT(VARCHAR(10), dg.grade_date, 120) AS gradeDate,
       dg.player_id          AS playerId,
       dg.player_name        AS playerName,
       dg.market_key         AS marketKey,
       dg.line_value         AS lineValue,
       dg.hit_rate_60        AS hitRate60,
       dg.hit_rate_20        AS hitRate20,
       dg.sample_size_60     AS sampleSize60,
       dg.sample_size_20     AS sampleSize20,
       dg.weighted_hit_rate  AS weightedHitRate,
       dg.grade              AS grade
     FROM common.daily_grades dg
     LEFT JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
     WHERE CONVERT(VARCHAR(10), dg.grade_date, 120) = @gradeDate
     ${gameFilter}
     ORDER BY dg.grade DESC`
  );
  return result.recordset;
}
