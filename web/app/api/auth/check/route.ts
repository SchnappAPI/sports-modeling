import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';
import { createHmac } from 'crypto';

const SECRET = process.env.AUTH_TOKEN_SECRET ?? 'fallback-dev-secret-change-me';

function verifyToken(token: string): string | null {
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [payload, sig] = parts;
  const expected = createHmac('sha256', SECRET).update(payload).digest('base64url');
  if (sig !== expected) return null;
  try {
    const { code } = JSON.parse(Buffer.from(payload, 'base64url').toString());
    return code ?? null;
  } catch {
    return null;
  }
}

export async function GET(req: NextRequest) {
  try {
    const token = req.headers.get('x-auth-token') ?? '';
    const code = verifyToken(token);
    if (!code) {
      return NextResponse.json({ valid: false }, { status: 401 });
    }

    const pool = await getPool();
    const result = await pool.request()
      .input('code', code)
      .query(`SELECT active, name FROM common.user_codes WHERE code = @code`);

    if (result.recordset.length === 0 || !result.recordset[0].active) {
      return NextResponse.json({ valid: false }, { status: 401 });
    }

    // Update last seen
    await pool.request()
      .input('code', code)
      .input('now', new Date())
      .query(`UPDATE common.user_codes SET last_seen_at = @now WHERE code = @code`);

    return NextResponse.json({ valid: true, name: result.recordset[0].name });
  } catch (err) {
    console.error('Auth check error:', err);
    return NextResponse.json({ valid: false }, { status: 500 });
  }
}
