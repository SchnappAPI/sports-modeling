import { NextResponse } from 'next/server';
import { ping } from '@/lib/queries';

export async function GET() {
  try {
    await ping();
    return NextResponse.json({ status: 'ok', ts: new Date().toISOString() });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ status: 'error', error: message }, { status: 500 });
  }
}
