import { NextRequest, NextResponse } from 'next/server';
import mssql from 'mssql';
import { getPool } from '@/lib/db';

// Returns active players for a team, sorted by name.
// Used by the player game log page for the player switcher dropdown.
export async function GET(req: NextRequest) {
  const teamIdRaw = req.nextUrl.searchParams.get('teamId');
  if (!teamIdRaw) {
    return NextResponse.json({ error: 'teamId required' }, { status: 400 });
  }
  const teamId = parseInt(teamIdRaw, 10);
  if (isNaN(teamId)) {
    return NextResponse.json({ error: 'teamId must be an integer' }, { status: 400 });
  }
  try {
    const pool = await getPool();
    const result = await pool
      .request()
      .input('teamId', mssql.BigInt, teamId)
      .query(
        `SELECT
           player_id   AS playerId,
           player_name AS playerName
         FROM nba.players
         WHERE team_id = @teamId
           AND roster_status = 1
         ORDER BY player_name`
      );
    return NextResponse.json({ teamId, players: result.recordset });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
