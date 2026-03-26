import { NextRequest, NextResponse } from 'next/server';
import { getPlayerAverages } from '@/lib/queries';

export async function GET(req: NextRequest) {
  const gameId  = req.nextUrl.searchParams.get('gameId');
  const context = req.nextUrl.searchParams.get('context') ?? '20';
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  const lastN = Math.max(1, parseInt(context, 10) || 20);

  try {
    const players = await getPlayerAverages(gameId, lastN);
    return NextResponse.json({ gameId, lastN, players });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
