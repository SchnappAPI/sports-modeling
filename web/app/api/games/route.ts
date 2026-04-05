import { NextRequest, NextResponse } from 'next/server';
import { getGames } from '@/lib/queries';

const RUNNER_URL = 'http://20.109.181.21:5000';
const RUNNER_KEY = 'runner-Lake4971';

function todayET(): string {
  // Returns today's date in ET as YYYY-MM-DD.
  // The CDN scoreboard is always for the current NBA calendar day (ET-based).
  const now = new Date();
  const et = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  return `${et.getFullYear()}-${String(et.getMonth() + 1).padStart(2, '0')}-${String(et.getDate()).padStart(2, '0')}`;
}

export async function GET(req: NextRequest) {
  const sport = req.nextUrl.searchParams.get('sport') ?? 'nba';
  const date  = req.nextUrl.searchParams.get('date')  ?? todayET();

  if (sport !== 'nba') {
    return NextResponse.json({ sport, date, games: [], note: 'only nba supported' });
  }

  // For today's date use Flask -> CDN scoreboard (live, no DB round trip, no cron dependency).
  // For any other date fall back to DB which has historical scores from nba.schedule.
  const isToday = date === todayET();

  if (isToday) {
    try {
      const res = await fetch(`${RUNNER_URL}/scoreboard`, {
        headers: { 'X-Runner-Key': RUNNER_KEY },
        // Short timeout — if Flask is unreachable fall back to DB
        signal: AbortSignal.timeout(5000),
      });
      if (res.ok) {
        const data = await res.json();
        // Map Flask scoreboard shape to the GameRow shape the app expects.
        // Flask does not have team names or odds — those come from the DB for today
        // only when explicitly needed. Game strip only needs abbr + status + scores.
        const games = (data.games ?? []).map((g: any) => ({
          gameId:        g.gameId,
          gameDate:      date,
          gameStatus:    g.gameStatus,
          gameStatusText:g.gameStatusText,
          homeTeamId:    g.homeTeamId,
          awayTeamId:    g.awayTeamId,
          homeTeamAbbr:  g.homeTeamAbbr,
          awayTeamAbbr:  g.awayTeamAbbr,
          homeTeamName:  g.homeTeamAbbr,
          awayTeamName:  g.awayTeamAbbr,
          homeScore:     g.homeScore,
          awayScore:     g.awayScore,
          period:        g.period,
          gameClock:     g.gameClock,
          spread:        null,
          total:         null,
        }));
        return NextResponse.json({ sport, date, games, source: 'cdn' });
      }
    } catch {
      // Flask unreachable — fall through to DB
    }
  }

  // DB path: historical dates, or today if Flask failed.
  try {
    const games = await getGames(sport, date);
    return NextResponse.json({ sport, date, games, source: 'db' });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
