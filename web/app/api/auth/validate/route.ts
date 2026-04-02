import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';
import { createHmac } from 'crypto';

const SECRET = process.env.AUTH_TOKEN_SECRET ?? 'fallback-dev-secret-change-me';

interface DemoDates {
  nba?: string;
  nfl?: string;
  mlb?: string;
}

function makeToken(code: string, mode: 'live' | 'demo', demoDates?: DemoDates): string {
  const payload = Buffer.from(
    JSON.stringify({ code, ts: Date.now(), mode, ...(demoDates ? { demoDates } : {}) })
  ).toString('base64url');
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
      .query(`SELECT code, name, active, activated, mode, max_activations FROM common.user_codes WHERE code = @code`);

    if (result.recordset.length === 0) {
      return NextResponse.json({ error: 'That code does not exist. Double-check and try again.' }, { status: 401 });
    }

    const row = result.recordset[0];

    if (!row.active) {
      return NextResponse.json({ error: 'This code has been deactivated. Contact the admin.' }, { status: 403 });
    }

    // Check activation count against limit
    const countResult = await pool.request()
      .input('code', normalized)
      .query(`SELECT COUNT(*) AS activation_count FROM common.user_activations WHERE code = @code`);
    const activationCount = countResult.recordset[0].activation_count as number;

    if (activationCount >= row.max_activations) {
      return NextResponse.json({ error: 'This code has reached its maximum number of uses. Contact the admin.' }, { status: 403 });
    }

    const userMode: 'live' | 'demo' = row.mode === 'demo' ? 'demo' : 'live';
    let demoDates: DemoDates | undefined;

    if (userMode === 'demo') {
      const demoResult = await pool.request().query(
        `SELECT sport, demo_date FROM common.demo_config`
      );
      demoDates = {};
      for (const r of demoResult.recordset) {
        const sport = r.sport as 'nba' | 'nfl' | 'mlb';
        demoDates[sport] = r.demo_date instanceof Date
          ? r.demo_date.toISOString().slice(0, 10)
          : String(r.demo_date).slice(0, 10);
      }
    }

    const now = new Date();

    // Log this activation
    await pool.request()
      .input('code', normalized)
      .input('now', now)
      .query(`INSERT INTO common.user_activations (code, activated_at) VALUES (@code, @now)`);

    // Update user_codes timestamps
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

    const token = makeToken(normalized, userMode, demoDates);
    return NextResponse.json({ token, name: row.name, mode: userMode, demoDates });
  } catch (err) {
    console.error('Auth validate error:', err);
    return NextResponse.json({ error: 'Server error.' }, { status: 500 });
  }
}
