import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPlayerGames } from '@/lib/queries';
import { getPool } from '@/lib/db';

export async function GET(req: NextRequest) {
  const playerId = req.nextUrl.searchParams.get('playerId');
  const games    = req.nextUrl.searchParams.get('games') ?? '100';
  const sport    = req.nextUrl.searchParams.get('sport') ?? 'nba';
  if (!playerId) {
    return NextResponse.json({ error: 'playerId required' }, { status: 400 });
  }

  const lastN = Math.max(1, parseInt(games, 10) || 100);
  const pid   = parseInt(playerId, 10);
  if (isNaN(pid)) {
    return NextResponse.json({ error: 'playerId must be an integer' }, { status: 400 });
  }

  try {
    // Fetch team info alongside game log so the client has teamId for the player switcher.
    const pool = await getPool();
    const [log, teamResult] = await Promise.all([
      getPlayerGames(pid, lastN),
      pool
        .request()
        .input('playerId', mssql.Int, pid)
        .query(`SELECT player_name AS playerName, team_id AS teamId, team_tricode AS teamAbbr
                FROM nba.players WHERE player_id = @playerId`),
    ]);
    const playerInfo = teamResult.recordset[0] ?? null;
    return NextResponse.json({
      playerId: pid, lastN, sport, log,
      playerName: playerInfo?.playerName ?? null,
      teamId: playerInfo?.teamId ?? null,
      teamAbbr: playerInfo?.teamAbbr ?? null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
