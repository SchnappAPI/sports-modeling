import { NextResponse, type NextRequest } from 'next/server';

// Site-wide gates driven by the `common.feature_flags` table. Flags are
// fetched via /api/flags and cached in module memory for CACHE_MS so the
// DB sees at most one read per minute per function instance. Failing
// open on any error is deliberate — the gate exists to discourage
// casual visitors during work, not to enforce security.

const COOKIE_NAME = 'sb_unlock';
const COOKIE_MAX_AGE = 60 * 60 * 24 * 30; // 30 days
const UNLOCK_CODE = 'go';
const CACHE_MS = 60_000;

let cachedFlags: Record<string, boolean> | null = null;
let cachedAt = 0;

async function getFlags(req: NextRequest): Promise<Record<string, boolean>> {
  const now = Date.now();
  if (cachedFlags && now - cachedAt < CACHE_MS) return cachedFlags;
  try {
    const url = new URL('/api/flags', req.url);
    const res = await fetch(url, { cache: 'no-store' });
    if (!res.ok) throw new Error(`flags fetch ${res.status}`);
    const data = (await res.json()) as Record<string, boolean>;
    cachedFlags = data;
    cachedAt = now;
    return data;
  } catch {
    // Fail open: return last good cache if we have one, else empty map.
    return cachedFlags ?? {};
  }
}

const MAINTENANCE_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<meta name="robots" content="noindex,nofollow" />
<title>Scheduled Maintenance</title>
<style>
  html,body{margin:0;padding:0;height:100%;background:#0b0b0c;}
  body{
    color:#a8a8ad;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;
    display:flex;align-items:center;justify-content:center;
    text-align:center;padding:24px;
  }
  .wrap{max-width:420px;}
  h1{font-size:18px;font-weight:500;margin:0 0 10px;color:#d6d6d9;letter-spacing:.2px;}
  p{font-size:14px;line-height:1.6;margin:0;}
</style>
</head>
<body>
  <div class="wrap">
    <h1>Scheduled maintenance</h1>
    <p>We are performing routine maintenance and will be back shortly. Thanks for your patience.</p>
  </div>
</body>
</html>`;

export async function middleware(request: NextRequest) {
  const { pathname, searchParams } = request.nextUrl;

  // Bypass list for paths that must always be reachable, even during
  // maintenance: keep-alive ping, the flags endpoint itself (middleware
  // calls it), and /admin + /api/admin/* so the operator can always
  // sign in to flip the toggle back off.
  if (
    pathname === '/api/ping' ||
    pathname === '/api/flags' ||
    pathname === '/admin' ||
    pathname.startsWith('/admin/') ||
    pathname.startsWith('/api/admin/')
  ) {
    return NextResponse.next();
  }

  // Unlock attempt via query string. Always honored, even if maintenance
  // is off — sets the bypass cookie so future locks let you through.
  if (searchParams.get('unlock') === UNLOCK_CODE) {
    const cleanUrl = request.nextUrl.clone();
    cleanUrl.searchParams.delete('unlock');
    const res = NextResponse.redirect(cleanUrl);
    res.cookies.set({
      name: COOKIE_NAME,
      value: UNLOCK_CODE,
      httpOnly: true,
      secure: true,
      sameSite: 'lax',
      maxAge: COOKIE_MAX_AGE,
      path: '/',
    });
    return res;
  }

  // Cookie present and matches: pass through regardless of maintenance.
  if (request.cookies.get(COOKIE_NAME)?.value === UNLOCK_CODE) {
    return NextResponse.next();
  }

  const flags = await getFlags(request);
  if (!flags['maintenance_mode']) return NextResponse.next();

  // Status 200, not 503. 503 was the technically-correct semantic for
  // "service unavailable, retry later", but Azure SWA's deployment warmup
  // probes anonymous traffic against the new revision and treats any 5xx
  // as unhealthy, retrying until a ~10 minute timeout and then failing
  // the deploy. With maintenance_mode on at deploy time, every SWA deploy
  // gets bricked. See ADR-20260426-1. The maintenance HTML has
  // noindex,nofollow so 200 does not cause SEO issues.
  return new NextResponse(MAINTENANCE_HTML, {
    status: 200,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
    },
  });
}

export const config = {
  matcher: [
    '/((?!_next/static|_next/image|favicon.ico|icon.svg|icon-192\\.png|icon-512\\.png|manifest\\.json|sw\\.js|robots\\.txt|sitemap\\.xml).*)',
  ],
};
