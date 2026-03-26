import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const gameId  = req.nextUrl.searchParams.get('gameId');
  const context = req.nextUrl.searchParams.get('context') ?? '20';
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  // TODO step 6: replace with real query
  return NextResponse.json({ gameId, context, players: [] });
}
