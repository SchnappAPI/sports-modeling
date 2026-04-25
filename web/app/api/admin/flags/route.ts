import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

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
  return !!ADMIN_PASSCODE && token === ADMIN_PASSCODE;
}

// GET — list all flags with metadata.
export async function GET(req: NextRequest) {
  if (!checkAdmin(req)) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  try {
    const pool = await getPool();
    const result = await pool.request().query(
      `SELECT flag_key, enabled, updated_at FROM common.feature_flags ORDER BY flag_key`
    );
    return withUnlock(NextResponse.json({ flags: result.recordset }));
  } catch (err) {
    console.error('admin flags GET error:', err);
    return NextResponse.json({ error: 'Server error' }, { status: 500 });
  }
}

// PATCH — toggle a single flag. Body: { flag_key: string, enabled: boolean }.
export async function PATCH(req: NextRequest) {
  if (!checkAdmin(req)) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  try {
    const { flag_key, enabled } = await req.json();
    if (typeof flag_key !== 'string' || typeof enabled !== 'boolean') {
      return NextResponse.json({ error: 'flag_key (string) and enabled (boolean) required' }, { status: 400 });
    }
    const pool = await getPool();
    const result = await pool.request()
      .input('flag_key', flag_key)
      .input('enabled', enabled ? 1 : 0)
      .query(`
        UPDATE common.feature_flags
        SET enabled = @enabled, updated_at = SYSUTCDATETIME()
        WHERE flag_key = @flag_key
      `);
    if (result.rowsAffected[0] === 0) {
      return NextResponse.json({ error: 'unknown flag_key' }, { status: 404 });
    }
    return withUnlock(NextResponse.json({ ok: true }));
  } catch (err) {
    console.error('admin flags PATCH error:', err);
    return NextResponse.json({ error: 'Server error' }, { status: 500 });
  }
}
