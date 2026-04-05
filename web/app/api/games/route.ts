import { NextRequest, NextResponse } from 'next/server';
import { getGames } from '@/lib/queries';

const RUNNER_URL = 'http://20.109.181.21:5000';
const RUNNER_KEY = 'runner-Lake4971';

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

  // For today's date, try the CDN via Flask for live scores and status.
  // Only use CDN data when it has at least one live or upcoming game —
  // if everything is Final the NBA game-day file has not rolled over yet
  // and the DB has the correct upcoming schedule for tonight.
  // For historical dates always use the DB.
  const isToday = date === todayCT();

  if (isToday) {
    try {
      const res = await fetch(`${RUNNER_URL}/scoreboard`, {
        headers: { 'X-Runner-Key': RUNNER_KEY },
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) {
        const data = await res.json();
        const cdnGames: any[] = data.games ?? [];

        // Gate: only trust CDN when it has live (2) or upcoming (1) games.
        // All-Final means the CDN is still showing yesterday's completed games
        // and has not yet published tonight's schedule.
        const hasActive = cdnGames.some(
          (g: any) => g.gameStatus === 1 || g.gameStatus === 2
        );

        if (hasActive) {
          const games = cdnGames.map((g: any) => ({
            gameId:         g.gameId,
            gameDate:       date,
            gameStatus:     g.gameStatus,
            gameStatusText: g.gameStatusText,
            homeTeamId:     g.homeTeamId,
            awayTeamId:     g.awayTeamId,
            homeTeamAbbr:   g.homeTeamAbbr,
            awayTeamAbbr:   g.awayTeamAbbr,
            homeTeamName:   g.homeTeamAbbr,
            awayTeamName:   g.awayTeamAbbr,
            homeScore:      g.homeScore,
            awayScore:      g.awayScore,
            spread:         null,
            total:          null,
          }));
          return NextResponse.json({ sport, date, games, source: 'cdn' });
        }
        // CDN is all-Final — fall through to DB
      }
    } catch {
      // Flask unreachable — fall through to DB
    }
  }

  // DB path: historical dates, CDN all-Final, or Flask unreachable.
  try {
    const games = await getGames(sport, date);
    return NextResponse.json({ sport, date, games, source: 'db' });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
