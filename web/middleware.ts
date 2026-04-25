import { NextResponse, type NextRequest } from 'next/server';

// Site-wide maintenance gate. Flip MAINTENANCE_ON to true, commit, push.
// SWA redeploys in ~90s and the site is locked for everyone except
// visitors who hit any URL with ?unlock=<UNLOCK_CODE> (sets a 30-day
// cookie, then redirects to the clean URL). Flip back to false to
// disable.

const MAINTENANCE_ON = true;
const UNLOCK_CODE = 'go';

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
  if (!MAINTENANCE_ON) return NextResponse.next();

  const { pathname, searchParams } = request.nextUrl;

  // Always allow the keep-alive ping.
  if (pathname === '/api/ping') return NextResponse.next();

  // Cookie present and matches: pass through.
  if (request.cookies.get(COOKIE_NAME)?.value === UNLOCK_CODE) {
    return NextResponse.next();
  }

  // Unlock attempt via query string.
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
  matcher: [
    '/((?!_next/static|_next/image|favicon.ico|icon.svg|icon-192\\.png|icon-512\\.png|manifest\\.json|sw\\.js|robots\\.txt|sitemap\\.xml).*)',
  ],
};
