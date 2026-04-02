import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';
import { createHmac } from 'crypto';

const SECRET = process.env.AUTH_TOKEN_SECRET ?? 'fallback-dev-secret-change-me';

function makeToken(code: string): string {
  const payload = Buffer.from(JSON.stringify({ code, ts: Date.now() })).toString('base64url');
  const sig = createHmac('sha256', SECRET).update(payload).digest('base64url');
  return `${payload}.${sig}`;
}

export async function POST(req: NextRequest) {
  try {
    const { code } = await req.json();
    if (!code || typeof code !== 'string') {
      return NextResponse.json({ error: 'No code provided.' }, { status: 400 });
    }

    const normalized = code.trim().toUpperCase();
    const pool = await getPool();
    const result = await pool.request()
      .input('code', normalized)
      .query(`
        SELECT code, name, active, activated, is_demo, demo_date_nba
        FROM common.user_codes
        WHERE code = @code
      `);

    if (result.recordset.length === 0) {
      return NextResponse.json({ error: 'That code does not exist. Double-check and try again.' }, { status: 401 });
    }

    const row = result.recordset[0];

    if (!row.active) {
      return NextResponse.json({ error: 'This code has been deactivated. Contact the admin.' }, { status: 403 });
    }

    const now = new Date();

    if (!row.activated) {
      await pool.request()
        .input('code', normalized)
        .input('now', now)
        .query(`UPDATE common.user_codes SET activated = 1, activated_at = @now, last_seen_at = @now WHERE code = @code`);
    } else {
      await pool.request()
        .input('code', normalized)
        .input('now', now)
        .query(`UPDATE common.user_codes SET last_seen_at = @now WHERE code = @code`);
    }

    const isDemo = !!row.is_demo;
    const demoDates = {
      nba: row.demo_date_nba
        ? new Date(row.demo_date_nba).toISOString().slice(0, 10)
        : null,
    };

    const token = makeToken(normalized);
    return NextResponse.json({
      token,
      name: row.name,
      mode: isDemo ? 'demo' : 'live',
      demoDates,
    });
  } catch (err) {
    console.error('Auth validate error:', err);
    return NextResponse.json({ error: 'Server error.' }, { status: 500 });
  }
}
