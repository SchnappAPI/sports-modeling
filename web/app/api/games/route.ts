import { NextRequest, NextResponse } from 'next/server';
import { getGames } from '@/lib/queries';

export async function GET(req: NextRequest) {
  const sport = req.nextUrl.searchParams.get('sport') ?? 'nba';
  const date  = req.nextUrl.searchParams.get('date')  ?? new Date().toISOString().slice(0, 10);

  if (sport !== 'nba') {
    return NextResponse.json({ sport, date, games: [], note: 'only nba supported' });
  }

  try {
    const games = await getGames(sport, date);
    return NextResponse.json({ sport, date, games });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
