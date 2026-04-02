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
      .query(`
        SELECT active, name, is_demo, demo_date_nba
        FROM common.user_codes
        WHERE code = @code
      `);

    if (result.recordset.length === 0 || !result.recordset[0].active) {
      return NextResponse.json({ valid: false }, { status: 401 });
    }

    const row = result.recordset[0];

    await pool.request()
      .input('code', code)
      .input('now', new Date())
      .query(`UPDATE common.user_codes SET last_seen_at = @now WHERE code = @code`);

    const isDemo = !!row.is_demo;
    const demoDates = {
      nba: row.demo_date_nba
        ? new Date(row.demo_date_nba).toISOString().slice(0, 10)
        : null,
    };

    return NextResponse.json({
      valid: true,
      name: row.name,
      mode: isDemo ? 'demo' : 'live',
      demoDates,
    });
  } catch (err) {
    console.error('Auth check error:', err);
    return NextResponse.json({ valid: false }, { status: 500 });
  }
}
