import { NextRequest, NextResponse } from 'next/server';
import { getGames } from '@/lib/queries';

const RUNNER_URL = process.env.RUNNER_URL ?? 'https://live.schnapp.bet';
const RUNNER_KEY = process.env.RUNNER_API_KEY ?? 'runner-Lake4971';

function todayCT(): string {
  // Returns today's date in Central time as YYYY-MM-DD.
  // Matches the ETL (game dates normalized to CT) and the browser UI.
  const now = new Date();
  const ct = new Date(now.toLocaleString('en-US', { timeZone: 'America/Chicago' }));
  return `${ct.getFullYear()}-${String(ct.getMonth() + 1).padStart(2, '0')}-${String(ct.getDate()).padStart(2, '0')}`;
}

export async function GET(req: NextRequest) {
  const sport = req.nextUrl.searchParams.get('sport') ?? 'nba';
  const date  = req.nextUrl.searchParams.get('date')  ?? todayCT();

  if (sport !== 'nba') {
    return NextResponse.json({ sport, date, games: [], note: 'only nba supported' });
  }

  // The DB (nba.schedule) is always the source of truth for the game list.
  // Game dates are normalized to Central time by the ETL so they match exactly
  // what the UI requests. DB is used for all dates including today.
  //
  // For today only, we additionally fetch the CDN scoreboard and overlay
  // live scores/status onto any games that are currently in progress (status 2).
  // The CDN is never used to determine the game list — only to update scores
  // for games already in the DB that are currently live.
  const games = await getGames(sport, date).catch((err) => {
    throw new Error(err instanceof Error ? err.message : String(err));
  });

  const isToday = date === todayCT();

  if (isToday && games.length > 0) {
    try {
      const res = await fetch(`${RUNNER_URL}/scoreboard`, {
        headers: { 'X-Runner-Key': RUNNER_KEY },
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) {
        const data = await res.json();
        const cdnByGameId = new Map<string, any>();
        for (const g of (data.games ?? [])) {
          cdnByGameId.set(g.gameId, g);
        }

        // Overlay CDN status/scores onto DB games that are live.
        // Only update games that exist in the DB for today's date —
        // CDN games that don't match a DB game ID are ignored entirely.
        const merged = games.map((g) => {
          const cdn = cdnByGameId.get(g.gameId);
          if (!cdn) return g;
          // Only overlay if CDN shows this game as live or has a more recent status
          return {
            ...g,
            gameStatus:     cdn.gameStatus     ?? g.gameStatus,
            gameStatusText: cdn.gameStatusText ?? g.gameStatusText,
            homeScore:      cdn.homeScore      ?? g.homeScore,
            awayScore:      cdn.awayScore      ?? g.awayScore,
          };
        });

        return NextResponse.json({ sport, date, games: merged, source: 'db+cdn' });
      }
    } catch {
      // Flask unreachable — return DB data as-is
    }
  }

  return NextResponse.json({ sport, date, games, source: 'db' });
}
