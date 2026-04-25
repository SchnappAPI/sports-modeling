import { NextResponse, type NextRequest } from 'next/server';

// Site-wide maintenance gate. When MAINTENANCE_MODE=1, every request is
// served a generic maintenance page UNLESS the visitor presents a valid
// bypass cookie. Bypass is granted by visiting any URL with
// ?unlock=<MAINTENANCE_BYPASS_CODE>; the cookie is then set for 30 days
// and the URL is cleaned via redirect so the code never appears in the
// address bar after the first hop.
//
// Toggle in Azure SWA env vars:
//   MAINTENANCE_MODE         "1" to enable, anything else (or unset) = off
//   MAINTENANCE_BYPASS_CODE  the unlock phrase, e.g. "go"
// Flipping the env var takes effect on the next cold start of the SWA
// function host (typically within a minute) without redeploy.

const COOKIE_NAME = 'sb_unlock';
const COOKIE_MAX_AGE = 60 * 60 * 24 * 30; // 30 days

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

export function middleware(request: NextRequest) {
  const enabled = process.env.MAINTENANCE_MODE === '1';
  if (!enabled) return NextResponse.next();

  const code = process.env.MAINTENANCE_BYPASS_CODE;
  // Misconfiguration safety: if the env var pair is half-set, fail OPEN
  // so a typo cannot lock everyone (including the operator) out.
  if (!code) return NextResponse.next();

  const { pathname, searchParams } = request.nextUrl;

  // Always allow the keep-alive ping. Uptime Robot pings this to keep
  // the Azure SQL serverless DB warm; gating it would defeat the purpose.
  if (pathname === '/api/ping') return NextResponse.next();

  // Cookie present and matches: pass through.
  const cookieVal = request.cookies.get(COOKIE_NAME)?.value;
  if (cookieVal && cookieVal === code) return NextResponse.next();

  // Unlock attempt via query string.
  const unlock = searchParams.get('unlock');
  if (unlock && unlock === code) {
    const cleanUrl = request.nextUrl.clone();
    cleanUrl.searchParams.delete('unlock');
    const res = NextResponse.redirect(cleanUrl);
    res.cookies.set({
      name: COOKIE_NAME,
      value: code,
      httpOnly: true,
      secure: true,
      sameSite: 'lax',
      maxAge: COOKIE_MAX_AGE,
      path: '/',
    });
    return res;
  }

  // Locked. Serve the maintenance HTML directly from middleware so no
  // app route, layout, or branding ever renders.
  return new NextResponse(MAINTENANCE_HTML, {
    status: 503,
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
      'Retry-After': '3600',
    },
  });
}

export const config = {
  // Run on every path EXCEPT Next internals and static asset files.
  // The matcher is the cheapest way to keep middleware off the hot path
  // for asset requests; the function body still re-checks the env flag.
  matcher: [
    '/((?!_next/static|_next/image|favicon.ico|icon.svg|icon-192\\.png|icon-512\\.png|manifest\\.json|sw\\.js|robots\\.txt|sitemap\\.xml).*)',
  ],
};
