import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';
import mssql from 'mssql';

const POS_GROUPS = ['G', 'F', 'C'] as const;
type PosGroup = typeof POS_GROUPS[number];

// Map full position strings to canonical group.
// Falls back to 'G' for null or unrecognized values so no active player is silently dropped.
function posToGroup(pos: string | null): PosGroup {
  if (!pos) return 'G';
  const p = pos.toUpperCase().trim();
  if (p === 'PG' || p === 'SG' || p === 'G') return 'G';
  if (p === 'SF' || p === 'PF' || p === 'F') return 'F';
  if (p === 'C') return 'C';
  // Handle compound positions like 'G-F', 'F-C' — use first component
  const first = p.split('-')[0].trim();
  if (first === 'PG' || first === 'SG' || first === 'G') return 'G';
  if (first === 'SF' || first === 'PF' || first === 'F') return 'F';
  if (first === 'C') return 'C';
  return 'G'; // fallback — better to show under G than to drop silently
}

interface StatLine {
  avg: number;
  rank: number;
  gamesDefended: number;
}

interface TeamMatchup {
  teamId: number;
  teamAbbr: string;
  positions: Partial<Record<PosGroup, {
    pts:  StatLine;
    reb:  StatLine;
    ast:  StatLine;
    fg3m: StatLine;
    stl:  StatLine;
    blk:  StatLine;
    tov:  StatLine;
    gamesDefended: number;
  }>>;
}

interface LineupPlayer {
  playerId: number | null;
  playerName: string;
  position: string | null;
  starterStatus: string | null;
  lineupStatus: string | null;
}

interface DefRow {
  oppTeamId: number;
  posGroup: string;
  gamesDefended: number;
  avg_pts: number;  rank_pts: number;
  avg_reb: number;  rank_reb: number;
  avg_ast: number;  rank_ast: number;
  avg_fg3m: number; rank_fg3m: number;
  avg_stl: number;  rank_stl: number;
  avg_blk: number;  rank_blk: number;
  avg_tov: number;  rank_tov: number;
}

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  try {
    const pool = await getPool();

    const gameRes = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .query<{ homeTeamId: number; awayTeamId: number; homeAbbr: string; awayAbbr: string }>(`
        SELECT
          s.home_team_id  AS homeTeamId,
          s.away_team_id  AS awayTeamId,
          ht.team_tricode AS homeAbbr,
          at.team_tricode AS awayAbbr
        FROM nba.schedule s
        JOIN nba.teams ht ON ht.team_id = s.home_team_id
        JOIN nba.teams at ON at.team_id = s.away_team_id
        WHERE s.game_id = @gameId
      `);

    if (gameRes.recordset.length === 0) {
      return NextResponse.json({ error: 'game not found' }, { status: 404 });
    }

    const { homeTeamId, awayTeamId, homeAbbr, awayAbbr } = gameRes.recordset[0];

    const defRes = await pool.request()
      .input('homeTeamId', mssql.BigInt, homeTeamId)
      .input('awayTeamId', mssql.BigInt, awayTeamId)
      .query<DefRow>(`
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
            SUM(pbs.pts)  AS pts,
            SUM(pbs.reb)  AS reb,
            SUM(pbs.ast)  AS ast,
            SUM(pbs.stl)  AS stl,
            SUM(pbs.blk)  AS blk,
            SUM(pbs.fg3m) AS fg3m,
            SUM(pbs.tov)  AS tov
          FROM nba.player_box_score_stats pbs
          JOIN nba.schedule s ON s.game_id = pbs.game_id
          WHERE s.game_date >= (SELECT dt FROM season_start)
          GROUP BY pbs.player_id, pbs.game_id, pbs.team_id, s.home_team_id, s.away_team_id
        ),
        pos_filtered AS (
          SELECT gt.*, LEFT(p.position, 1) AS pos_group
          FROM game_totals gt
          JOIN nba.players p ON p.player_id = gt.player_id
          WHERE p.position IS NOT NULL
            AND gt.opp_team_id IN (@homeTeamId, @awayTeamId)
        ),
        team_pos_defense AS (
          SELECT
            opp_team_id,
            pos_group,
            COUNT(*)                 AS games_defended,
            AVG(CAST(pts  AS FLOAT)) AS avg_pts,
            AVG(CAST(reb  AS FLOAT)) AS avg_reb,
            AVG(CAST(ast  AS FLOAT)) AS avg_ast,
            AVG(CAST(stl  AS FLOAT)) AS avg_stl,
            AVG(CAST(blk  AS FLOAT)) AS avg_blk,
            AVG(CAST(fg3m AS FLOAT)) AS avg_fg3m,
            AVG(CAST(tov  AS FLOAT)) AS avg_tov
          FROM pos_filtered
          GROUP BY opp_team_id, pos_group
        ),
        all_teams_defense AS (
          SELECT
            gt2.opp_team_id,
            LEFT(p2.position, 1) AS pos_group,
            AVG(CAST(gt2.pts  AS FLOAT)) AS avg_pts,
            AVG(CAST(gt2.reb  AS FLOAT)) AS avg_reb,
            AVG(CAST(gt2.ast  AS FLOAT)) AS avg_ast,
            AVG(CAST(gt2.stl  AS FLOAT)) AS avg_stl,
            AVG(CAST(gt2.blk  AS FLOAT)) AS avg_blk,
            AVG(CAST(gt2.fg3m AS FLOAT)) AS avg_fg3m,
            AVG(CAST(gt2.tov  AS FLOAT)) AS avg_tov
          FROM (
            SELECT
              pbs2.player_id,
              CASE
                WHEN pbs2.team_id = s2.home_team_id THEN s2.away_team_id
                ELSE s2.home_team_id
              END AS opp_team_id,
              SUM(pbs2.pts)  AS pts,
              SUM(pbs2.reb)  AS reb,
              SUM(pbs2.ast)  AS ast,
              SUM(pbs2.stl)  AS stl,
              SUM(pbs2.blk)  AS blk,
              SUM(pbs2.fg3m) AS fg3m,
              SUM(pbs2.tov)  AS tov
            FROM nba.player_box_score_stats pbs2
            JOIN nba.schedule s2 ON s2.game_id = pbs2.game_id
            WHERE s2.game_date >= (SELECT dt FROM season_start)
            GROUP BY pbs2.player_id, pbs2.game_id, pbs2.team_id, s2.home_team_id, s2.away_team_id
          ) gt2
          JOIN nba.players p2 ON p2.player_id = gt2.player_id
          WHERE p2.position IS NOT NULL
          GROUP BY gt2.opp_team_id, LEFT(p2.position, 1)
        ),
        ranked AS (
          SELECT
            a.opp_team_id,
            a.pos_group,
            a.avg_pts,  RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_pts  DESC) AS rank_pts,
            a.avg_reb,  RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_reb  DESC) AS rank_reb,
            a.avg_ast,  RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_ast  DESC) AS rank_ast,
            a.avg_stl,  RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_stl  DESC) AS rank_stl,
            a.avg_blk,  RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_blk  DESC) AS rank_blk,
            a.avg_fg3m, RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_fg3m DESC) AS rank_fg3m,
            a.avg_tov,  RANK() OVER (PARTITION BY a.pos_group ORDER BY a.avg_tov  DESC) AS rank_tov
          FROM all_teams_defense a
        )
        SELECT
          tpd.opp_team_id   AS oppTeamId,
          tpd.pos_group     AS posGroup,
          tpd.games_defended AS gamesDefended,
          tpd.avg_pts,  r.rank_pts,
          tpd.avg_reb,  r.rank_reb,
          tpd.avg_ast,  r.rank_ast,
          tpd.avg_fg3m, r.rank_fg3m,
          tpd.avg_stl,  r.rank_stl,
          tpd.avg_blk,  r.rank_blk,
          tpd.avg_tov,  r.rank_tov
        FROM team_pos_defense tpd
        JOIN ranked r
          ON r.opp_team_id = tpd.opp_team_id
         AND r.pos_group   = tpd.pos_group
        WHERE tpd.pos_group IN ('G', 'F', 'C')
      `);

    const lineupRes = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .query<LineupPlayer & { teamTricode: string }>(`
        SELECT
          p.player_id       AS playerId,
          dl.player_name    AS playerName,
          dl.position       AS position,
          dl.starter_status AS starterStatus,
          dl.lineup_status  AS lineupStatus,
          dl.team_tricode   AS teamTricode
        FROM nba.daily_lineups dl
        LEFT JOIN nba.players p ON p.player_name = dl.player_name
        WHERE dl.game_id = @gameId
          AND dl.starter_status != 'Inactive'
        ORDER BY
          dl.team_tricode,
          CASE dl.starter_status WHEN 'Starter' THEN 0 ELSE 1 END,
          dl.player_name
      `);

    function buildTeam(teamId: number, abbr: string): TeamMatchup {
      const positions: TeamMatchup['positions'] = {};
      for (const row of defRes.recordset) {
        if (row.oppTeamId !== teamId) continue;
        const pg = row.posGroup as PosGroup;
        if (!POS_GROUPS.includes(pg)) continue;
        const sl = (avg: number, rank: number): StatLine => ({ avg, rank, gamesDefended: row.gamesDefended });
        positions[pg] = {
          pts:  sl(row.avg_pts,  row.rank_pts),
          reb:  sl(row.avg_reb,  row.rank_reb),
          ast:  sl(row.avg_ast,  row.rank_ast),
          fg3m: sl(row.avg_fg3m, row.rank_fg3m),
          stl:  sl(row.avg_stl,  row.rank_stl),
          blk:  sl(row.avg_blk,  row.rank_blk),
          tov:  sl(row.avg_tov,  row.rank_tov),
          gamesDefended: row.gamesDefended,
        };
      }
      return { teamId, teamAbbr: abbr, positions };
    }

    // Build lineup keyed by teamTricode -> posGroup -> players[]
    // posToGroup() never returns null — unknown positions fall back to 'G'
    const lineupByTeam: Record<string, Record<string, LineupPlayer[]>> = {};
    for (const row of lineupRes.recordset) {
      const tc = row.teamTricode;
      const pg = posToGroup(row.position);
      if (!lineupByTeam[tc]) lineupByTeam[tc] = {};
      if (!lineupByTeam[tc][pg]) lineupByTeam[tc][pg] = [];
      lineupByTeam[tc][pg].push({
        playerId:      row.playerId,
        playerName:    row.playerName,
        position:      row.position,
        starterStatus: row.starterStatus,
        lineupStatus:  row.lineupStatus,
      });
    }

    return NextResponse.json({
      home:   buildTeam(homeTeamId, homeAbbr),
      away:   buildTeam(awayTeamId, awayAbbr),
      lineup: lineupByTeam,
      gameId,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
