import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const playerId = req.nextUrl.searchParams.get('playerId');
  const games    = req.nextUrl.searchParams.get('games') ?? '20';
  const sport    = req.nextUrl.searchParams.get('sport') ?? 'nba';
  if (!playerId) {
    return NextResponse.json({ error: 'playerId required' }, { status: 400 });
  }

  // TODO step 6: replace with real query
  return NextResponse.json({ playerId, games, sport, log: [] });
}
