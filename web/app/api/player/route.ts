import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPlayerGames } from '@/lib/queries';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const playerId = req.nextUrl.searchParams.get('playerId');
  const games    = req.nextUrl.searchParams.get('games') ?? '100';
  const sport    = req.nextUrl.searchParams.get('sport') ?? 'nba';
  const gameId   = req.nextUrl.searchParams.get('gameId');

  if (!playerId) {
    return NextResponse.json({ error: 'playerId required' }, { status: 400 });
  }

  const lastN = Math.max(1, parseInt(games, 10) || 100);
  const pid   = parseInt(playerId, 10);
  if (isNaN(pid)) {
    return NextResponse.json({ error: 'playerId must be an integer' }, { status: 400 });
  }

  try {
    const pool = await getPool();
    const [log, playerResult] = await Promise.all([
      getPlayerGames(pid, lastN),
      pool
        .request()
        .input('playerId', mssql.Int, pid)
        .query(`
          SELECT
            p.player_name  AS playerName,
            p.team_id      AS teamId,
            p.team_tricode AS teamAbbr,
            p.position     AS position
          FROM nba.players p
          WHERE p.player_id = @playerId
        `),
    ]);

    const playerInfo = playerResult.recordset[0] ?? null;

    // Derive the most recent opponent team ID from the schedule so the matchup
    // defense section can render even without a gameId in the URL.
    let lastOppTeamId: number | null = null;
    const recentGame = log.find((r) => !r.dnp);
    if (recentGame && playerInfo?.teamId) {
      const schedResult = await pool
        .request()
        .input('gameId', mssql.VarChar, recentGame.gameId)
        .input('teamId', mssql.Int, playerInfo.teamId)
        .query(`
          SELECT
            CASE
              WHEN home_team_id = @teamId THEN away_team_id
              ELSE home_team_id
            END AS oppTeamId
          FROM nba.schedule
          WHERE game_id = @gameId
        `);
      lastOppTeamId = schedResult.recordset[0]?.oppTeamId ?? null;
    }

    // If a gameId was provided, look up this player's lineup status for that game.
    let gameLineupStatus: string | null = null;
    let gameStarterStatus: string | null = null;
    if (gameId && playerInfo?.playerName) {
      const lineupResult = await pool
        .request()
        .input('gameId',     mssql.VarChar, gameId)
        .input('playerName', mssql.VarChar, playerInfo.playerName)
        .query(`
          SELECT
            lineup_status  AS lineupStatus,
            starter_status AS starterStatus
          FROM nba.daily_lineups
          WHERE game_id    = @gameId
            AND player_name = @playerName
        `);
      const lineupRow = lineupResult.recordset[0] ?? null;
      gameLineupStatus   = lineupRow?.lineupStatus   ?? null;
      gameStarterStatus  = lineupRow?.starterStatus  ?? null;
    }

    return NextResponse.json({
      playerId:         pid,
      lastN,
      sport,
      log,
      playerName:       playerInfo?.playerName   ?? null,
      teamId:           playerInfo?.teamId        ?? null,
      teamAbbr:         playerInfo?.teamAbbr      ?? null,
      position:         playerInfo?.position      ?? null,
      lastOppTeamId,
      gameLineupStatus,
      gameStarterStatus,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
