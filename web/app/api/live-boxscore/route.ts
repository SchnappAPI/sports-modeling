import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';
import mssql from 'mssql';

// Live box score reads from nba.player_box_score_stats (written by nba_live.py every 5 min).
// We no longer call stats.nba.com directly from SWA — that endpoint blocks Azure SWA IPs.

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

interface LivePlayer {
  playerId: number;
  playerName: string;
  teamId: number;
  teamAbbr: string;
  pts: number;
  reb: number;
  ast: number;
  stl: number;
  blk: number;
  tov: number;
  min: number;
  fg3m: number;
  fg3a: number;
  fgm: number;
  fga: number;
  ftm: number;
  fta: number;
  starterStatus: string | null;
}

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  try {
    const pool = await getPool();

    // Get game status text from schedule
    const schedRes = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .query<{ gameStatusText: string; gameStatus: number }>(`
        SELECT game_status_text AS gameStatusText, game_status AS gameStatus
        FROM nba.schedule
        WHERE game_id = @gameId
      `);

    const gameStatusText = schedRes.recordset[0]?.gameStatusText ?? '';

    // Sum all quarters for each player in this game
    const res = await pool.request()
      .input('gameId', mssql.VarChar, gameId)
      .query<LivePlayer>(`
        SELECT
          pbs.player_id                    AS playerId,
          p.player_name                    AS playerName,
          pbs.team_id                      AS teamId,
          COALESCE(t.team_tricode, '')      AS teamAbbr,
          SUM(COALESCE(pbs.pts,  0))       AS pts,
          SUM(COALESCE(pbs.reb,  0))       AS reb,
          SUM(COALESCE(pbs.ast,  0))       AS ast,
          SUM(COALESCE(pbs.stl,  0))       AS stl,
          SUM(COALESCE(pbs.blk,  0))       AS blk,
          SUM(COALESCE(pbs.tov,  0))       AS tov,
          SUM(COALESCE(CAST(pbs.minutes AS FLOAT), 0)) AS min,
          SUM(COALESCE(pbs.fg3m, 0))       AS fg3m,
          SUM(COALESCE(pbs.fg3a, 0))       AS fg3a,
          SUM(COALESCE(pbs.fgm,  0))       AS fgm,
          SUM(COALESCE(pbs.fga,  0))       AS fga,
          SUM(COALESCE(pbs.ftm,  0))       AS ftm,
          SUM(COALESCE(pbs.fta,  0))       AS fta,
          dl.starter_status                AS starterStatus
        FROM nba.player_box_score_stats pbs
        JOIN nba.players p   ON p.player_id  = pbs.player_id
        JOIN nba.teams t     ON t.team_id    = pbs.team_id
        LEFT JOIN nba.daily_lineups dl
          ON dl.player_name   = p.player_name
         AND dl.team_tricode  = t.team_tricode
         AND dl.game_id       = pbs.game_id
        WHERE pbs.game_id = @gameId
          AND pbs.period != 'OT'
        GROUP BY pbs.player_id, p.player_name, pbs.team_id, t.team_tricode, dl.starter_status
        ORDER BY
          pbs.team_id,
          CASE dl.starter_status WHEN 'Starter' THEN 0 WHEN 'Bench' THEN 1 ELSE 2 END,
          SUM(COALESCE(CAST(pbs.minutes AS FLOAT), 0)) DESC
      `);

    if (res.recordset.length === 0) {
      return NextResponse.json(
        { error: 'No box score data in database yet. The live ETL runs every 5 minutes.' },
        { status: 404 }
      );
    }

    return NextResponse.json({
      gameId,
      gameStatusText,
      players: res.recordset,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
