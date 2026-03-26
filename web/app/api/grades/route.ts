import { NextRequest, NextResponse } from 'next/server';
import { getGrades } from '@/lib/queries';

export async function GET(req: NextRequest) {
  const date   = req.nextUrl.searchParams.get('date')   ?? new Date().toISOString().slice(0, 10);
  const gameId = req.nextUrl.searchParams.get('gameId') ?? null;

  try {
    const grades = await getGrades(date, gameId);
    return NextResponse.json({ date, gameId, grades });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
