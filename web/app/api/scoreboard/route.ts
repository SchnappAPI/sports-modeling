import { NextResponse } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const RUNNER_URL = process.env.RUNNER_URL ?? 'https://live.schnapp.bet';
const RUNNER_KEY = process.env.RUNNER_API_KEY ?? 'runner-Lake4971';
const TIMEOUT_MS = 10_000;

export async function GET() {
  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
    const resp = await fetch(`${RUNNER_URL}/scoreboard`, {
      headers: { 'X-Runner-Key': RUNNER_KEY },
      signal: controller.signal,
      cache: 'no-store',
    });
    clearTimeout(timer);
    if (!resp.ok) {
      return NextResponse.json({ error: `Runner returned ${resp.status}` }, { status: 502 });
    }
    const data = await resp.json();
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: `Runner unavailable: ${message}` }, { status: 503 });
  }
}
