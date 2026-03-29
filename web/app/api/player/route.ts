import { NextRequest, NextResponse } from 'next/server';
import { getPlayerGames } from '@/lib/queries';

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
    const log = await getPlayerGames(pid, lastN);
    return NextResponse.json({ playerId: pid, lastN, sport, log });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
