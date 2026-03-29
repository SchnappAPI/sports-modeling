import { NextRequest, NextResponse } from 'next/server';

// Proxy live box score data from stats.nba.com server-side.
// Returns game totals only (all quarters summed) — no period breakdown.
//
// BoxScoreTraditionalV3 returns each player's stats as an ARRAY of per-period
// objects (each with a "period" integer key). This route sums them to produce
// game-level totals, mirroring the logic in nba_live.py.

const NBA_HEADERS = {
  'User-Agent':         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Accept':             'application/json, text/plain, */*',
  'Accept-Language':    'en-US,en;q=0.9',
  'Accept-Encoding':    'gzip, deflate, br',
  'x-nba-stats-origin': 'stats',
  'x-nba-stats-token':  'true',
  'Origin':             'https://www.nba.com',
  'Referer':            'https://www.nba.com/',
  'Connection':         'keep-alive',
};

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function parseMinutes(clock: string | undefined | null): number {
  if (!clock) return 0;
  const m = clock.match(/PT(\d+)M([\d.]+)S/);
  if (m) return parseInt(m[1], 10) + parseFloat(m[2]) / 60;
  const plain = parseFloat(String(clock));
  return isNaN(plain) ? 0 : plain;
}

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  const url = `https://stats.nba.com/stats/boxscoretraditionalv3?GameID=${gameId}&StartPeriod=0&EndPeriod=0&StartRange=0&EndRange=0&RangeType=0`;

  try {
    const resp = await fetch(url, {
      headers: NBA_HEADERS,
      cache: 'no-store',
    });

    if (!resp.ok) {
      const body = await resp.text().catch(() => '');
      return NextResponse.json(
        { error: `NBA API returned ${resp.status}`, detail: body.slice(0, 200) },
        { status: 502 }
      );
    }

    const data = await resp.json();
    const game = data?.boxScoreTraditional;
    if (!game) {
      return NextResponse.json({ error: 'Unexpected NBA API shape', keys: Object.keys(data ?? {}) }, { status: 502 });
    }

    type PlayerRow = {
      playerId: number;
      playerName: string;
      teamId: number;
      teamAbbr: string;
      pts: number; reb: number; ast: number; stl: number; blk: number; tov: number;
      min: number; fg3m: number; fgm: number; fga: number; ftm: number; fta: number;
    };

    const players: PlayerRow[] = [];

    for (const team of [game.homeTeam, game.awayTeam]) {
      if (!team) continue;
      const teamId   = Number(team.teamId);
      const teamAbbr = String(team.teamTricode ?? '');

      for (const player of (team.players ?? [])) {
        // statistics is an array of per-period objects in V3.
        // Each element has: period (int), clock, assists, points, etc.
        const statsArr: any[] = Array.isArray(player.statistics)
          ? player.statistics
          : player.statistics != null ? [player.statistics] : [];

        let pts = 0, reb = 0, ast = 0, stl = 0, blk = 0, tov = 0;
        let min = 0, fg3m = 0, fgm = 0, fga = 0, ftm = 0, fta = 0;

        for (const s of statsArr) {
          pts  += Number(s.points           ?? s.pts  ?? 0);
          reb  += Number(s.reboundsTotal    ?? s.reb  ?? 0);
          ast  += Number(s.assists          ?? s.ast  ?? 0);
          stl  += Number(s.steals           ?? s.stl  ?? 0);
          blk  += Number(s.blocks           ?? s.blk  ?? 0);
          tov  += Number(s.turnovers        ?? s.tov  ?? 0);
          fg3m += Number(s.threePointersMade     ?? s.fg3m ?? 0);
          fgm  += Number(s.fieldGoalsMade        ?? s.fgm  ?? 0);
          fga  += Number(s.fieldGoalsAttempted   ?? s.fga  ?? 0);
          ftm  += Number(s.freeThrowsMade        ?? s.ftm  ?? 0);
          fta  += Number(s.freeThrowsAttempted   ?? s.fta  ?? 0);
          min  += parseMinutes(s.clock ?? s.minutesCalculated);
        }

        players.push({
          playerId:   Number(player.personId),
          playerName: String(player.name ?? ''),
          teamId,
          teamAbbr,
          pts, reb, ast, stl, blk, tov,
          min: Math.round(min * 10) / 10,
          fg3m, fgm, fga, ftm, fta,
        });
      }
    }

    return NextResponse.json({
      gameId,
      gameStatusText: String(game.gameStatusText ?? ''),
      players,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
