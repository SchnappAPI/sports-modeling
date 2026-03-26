import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  const sport  = req.nextUrl.searchParams.get('sport') ?? 'nba';
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  // TODO step 6: live data source not yet wired
  return NextResponse.json({ gameId, sport, live: null });
}
