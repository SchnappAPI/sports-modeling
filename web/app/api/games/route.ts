import { NextRequest, NextResponse } from 'next/server';

export async function GET(req: NextRequest) {
  const sport = req.nextUrl.searchParams.get('sport') ?? 'nba';
  const date  = req.nextUrl.searchParams.get('date')  ?? new Date().toISOString().slice(0, 10);

  // TODO step 6: replace with real query
  return NextResponse.json({
    sport,
    date,
    games: [],
  });
}
