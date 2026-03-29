import { NextRequest, NextResponse } from 'next/server';

// Proxy live box score data from stats.nba.com server-side.
// Returns game totals only (all quarters summed) — no period breakdown.
// This avoids a DB round-trip during live games; the nightly ETL handles
// the authoritative per-quarter write to nba.player_box_score_stats.
//
// The NBA API requires specific headers and blocks datacenter IPs without them.
// Called every 30 seconds from LiveBoxScore.tsx while the Live tab is active.

const NBA_HEADERS = {
  'User-Agent':         'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
  'Accept':             'application/json, text/plain, */*',
  'Accept-Language':    'en-US,en;q=0.9',
  'x-nba-stats-origin': 'stats',
  'x-nba-stats-token':  'true',
  'Origin':             'https://www.nba.com',
  'Referer':            'https://www.nba.com/',
};

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  const url = `https://stats.nba.com/stats/boxscoretraditionalv3?GameID=${gameId}&StartPeriod=0&EndPeriod=0&StartRange=0&EndRange=0&RangeType=0`;

  try {
    const resp = await fetch(url, { headers: NBA_HEADERS, next: { revalidate: 0 } });
    if (!resp.ok) {
      return NextResponse.json(
        { error: `NBA API returned ${resp.status}` },
        { status: 502 }
      );
    }

    const data = await resp.json();
    const game = data?.boxScoreTraditional;
    if (!game) {
      return NextResponse.json({ error: 'Unexpected NBA API shape' }, { status: 502 });
    }

    const players: {
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
      fgm: number;
      fga: number;
      ftm: number;
      fta: number;
    }[] = [];

    for (const team of [game.homeTeam, game.awayTeam]) {
      if (!team) continue;
      const teamId   = Number(team.teamId);
      const teamAbbr = String(team.teamTricode ?? '');

      // Each player has a top-level statistics object with game totals
      // and an array of per-period statistics. We use the top-level totals.
      for (const player of (team.players ?? [])) {
        const s = player.statistics ?? {};
        const minutesRaw: string = s.minutesCalculated ?? s.clock ?? '';
        let min = 0;
        const m = minutesRaw.match(/PT(\d+)M([\d.]+)S/);
        if (m) {
          min = parseInt(m[1], 10) + parseFloat(m[2]) / 60;
        } else {
          const plain = parseFloat(minutesRaw);
          if (!isNaN(plain)) min = plain;
        }

        players.push({
          playerId:   Number(player.personId),
          playerName: String(player.name ?? ''),
          teamId,
          teamAbbr,
          pts:  Number(s.points ?? 0),
          reb:  Number(s.reboundsTotal ?? 0),
          ast:  Number(s.assists ?? 0),
          stl:  Number(s.steals ?? 0),
          blk:  Number(s.blocks ?? 0),
          tov:  Number(s.turnovers ?? 0),
          min:  Math.round(min * 10) / 10,
          fg3m: Number(s.threePointersMade ?? 0),
          fgm:  Number(s.fieldGoalsMade ?? 0),
          fga:  Number(s.fieldGoalsAttempted ?? 0),
          ftm:  Number(s.freeThrowsMade ?? 0),
          fta:  Number(s.freeThrowsAttempted ?? 0),
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
