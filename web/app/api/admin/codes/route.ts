import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

// Authed admin requests also receive the sb_unlock cookie so the admin
// can browse the gated site after signing in. 30-day cookie, refreshed
// on every authed call.
function withUnlock(res: NextResponse): NextResponse {
  res.cookies.set({
    name: 'sb_unlock',
    value: 'go',
    httpOnly: true,
    secure: true,
    sameSite: 'lax',
    maxAge: 60 * 60 * 24 * 30,
    path: '/',
  });
  return res;
}

const ADMIN_PASSCODE = process.env.ADMIN_PASSCODE ?? '';

function checkAdmin(req: NextRequest): boolean {
  const token = req.headers.get('x-admin-token') ?? '';
  return token === ADMIN_PASSCODE;
}

// GET — list all codes
export async function GET(req: NextRequest) {
  if (!checkAdmin(req)) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  try {
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT code, name, active, activated, activated_at, last_seen_at, created_at
      FROM common.user_codes
      ORDER BY created_at DESC
    `);
    return withUnlock(NextResponse.json({ codes: result.recordset }));
  } catch (err) {
    console.error('Admin GET error:', err);
    return NextResponse.json({ error: 'Server error' }, { status: 500 });
  }
}

// POST — add a new code
export async function POST(req: NextRequest) {
  if (!checkAdmin(req)) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  try {
    const { code, name } = await req.json();
    if (!code || !name) return NextResponse.json({ error: 'code and name required' }, { status: 400 });
    const normalized = code.trim().toUpperCase();
    const pool = await getPool();
    await pool.request()
      .input('code', normalized)
      .input('name', name.trim())
      .query(`
        INSERT INTO common.user_codes (code, name, active, activated, created_at)
        VALUES (@code, @name, 1, 0, GETUTCDATE())
      `);
    return withUnlock(NextResponse.json({ ok: true }));
  } catch (err) {
    console.error('Admin POST error:', err);
    return NextResponse.json({ error: 'Server error' }, { status: 500 });
  }
}

// PATCH — toggle active status
export async function PATCH(req: NextRequest) {
  if (!checkAdmin(req)) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  try {
    const { code, active } = await req.json();
    if (!code || typeof active !== 'boolean') return NextResponse.json({ error: 'code and active required' }, { status: 400 });
    const pool = await getPool();
    await pool.request()
      .input('code', code.trim().toUpperCase())
      .input('active', active ? 1 : 0)
      .query(`UPDATE common.user_codes SET active = @active WHERE code = @code`);
    return withUnlock(NextResponse.json({ ok: true }));
  } catch (err) {
    console.error('Admin PATCH error:', err);
    return NextResponse.json({ error: 'Server error' }, { status: 500 });
  }
}
