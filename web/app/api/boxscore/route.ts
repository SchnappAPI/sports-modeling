import { NextRequest, NextResponse } from 'next/server';
import { getBoxscore } from '@/lib/queries';

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  try {
    const rows = await getBoxscore(gameId);
    return NextResponse.json({ gameId, rows });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
