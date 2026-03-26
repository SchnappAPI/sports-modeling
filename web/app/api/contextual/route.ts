import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const playerId = req.nextUrl.searchParams.get('playerId');
  const gameId   = req.nextUrl.searchParams.get('gameId');
  const quarter  = req.nextUrl.searchParams.get('quarter');
  const stat     = req.nextUrl.searchParams.get('stat');
  const sport    = req.nextUrl.searchParams.get('sport') ?? 'nba';
  if (!playerId || !gameId || !quarter || !stat) {
    return NextResponse.json(
      { error: 'playerId, gameId, quarter, stat required' },
      { status: 400 }
    );
  }

  // TODO step 6: contextual similarity not yet implemented
  return NextResponse.json({ playerId, gameId, quarter, stat, sport, results: [] });
}
