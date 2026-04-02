import { NextRequest, NextResponse } from 'next/server';
import { getPool } from '@/lib/db';
import { createHmac } from 'crypto';

const SECRET = process.env.AUTH_TOKEN_SECRET ?? 'fallback-dev-secret-change-me';

interface DemoDates {
  nba?: string;
  nfl?: string;
  mlb?: string;
}

interface TokenPayload {
  code: string;
  ts: number;
  mode?: 'live' | 'demo';
  demoDates?: DemoDates;
}

function verifyToken(token: string): TokenPayload | null {
  const parts = token.split('.');
  if (parts.length !== 2) return null;
  const [payload, sig] = parts;
  const expected = createHmac('sha256', SECRET).update(payload).digest('base64url');
  if (sig !== expected) return null;
  try {
    return JSON.parse(Buffer.from(payload, 'base64url').toString());
  } catch {
    return null;
  }
}

export async function GET(req: NextRequest) {
  try {
    const token = req.headers.get('x-auth-token') ?? '';
    const parsed = verifyToken(token);
    if (!parsed?.code) {
      return NextResponse.json({ valid: false }, { status: 401 });
    }

    const { code } = parsed;

    const pool = await getPool();
    const result = await pool.request()
      .input('code', code)
      .query(`SELECT active, name, mode FROM common.user_codes WHERE code = @code`);

    if (result.recordset.length === 0 || !result.recordset[0].active) {
      return NextResponse.json({ valid: false }, { status: 401 });
    }

    const row = result.recordset[0];

    await pool.request()
      .input('code', code)
      .input('now', new Date())
      .query(`UPDATE common.user_codes SET last_seen_at = @now WHERE code = @code`);

    // Re-read demoDates from demo_config so they stay fresh if the admin
    // changes the demo date without requiring all users to re-login.
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

    return NextResponse.json({
      valid: true,
      name: row.name,
      mode: userMode,
      demoDates,
    });
  } catch (err) {
    console.error('Auth check error:', err);
    return NextResponse.json({ valid: false }, { status: 500 });
  }
}
