import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const date   = req.nextUrl.searchParams.get('date')   ?? new Date().toISOString().slice(0, 10);
  const sport  = req.nextUrl.searchParams.get('sport')  ?? 'nba';
  const gameId = req.nextUrl.searchParams.get('gameId') ?? null;

  // TODO step 6: replace with real query
  return NextResponse.json({ date, sport, gameId, grades: [] });
}
