import { NextRequest, NextResponse } from 'next/server';

// Proxies live box score requests through the schnapp-runner VM Flask service.
// The VM calls stats.nba.com via the Webshare residential proxy, bypassing
// the IP block that affects Azure SWA and GitHub Actions datacenter IPs.
//
// Falls back to the DB if the VM is unreachable.

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const RUNNER_URL = 'https://live.schnapp.bet';
const RUNNER_KEY = 'runner-Lake4971';
const TIMEOUT_MS = 10_000;

export async function GET(req: NextRequest) {
  const gameId = req.nextUrl.searchParams.get('gameId');
  if (!gameId) {
    return NextResponse.json({ error: 'gameId required' }, { status: 400 });
  }

  try {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);

    const resp = await fetch(
      `${RUNNER_URL}/boxscore?gameId=${encodeURIComponent(gameId)}`,
      {
        headers: { 'X-Runner-Key': RUNNER_KEY },
        signal: controller.signal,
        cache: 'no-store',
      }
    );
    clearTimeout(timer);

    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      return NextResponse.json(
        { error: body.error ?? `Runner returned ${resp.status}` },
        { status: 502 }
      );
    }

    const data = await resp.json();
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: `Runner unavailable: ${message}` }, { status: 503 });
  }
}
