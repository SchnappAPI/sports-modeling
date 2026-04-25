import { NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

// Public GET. Returns the full flag map as { [flag_key]: boolean }.
// Consumed by middleware (with in-memory caching) and by server-side
// page gates. Do NOT add auth here — middleware needs to call this
// before it can check any cookie.
export async function GET() {
  try {
    const pool = await getPool();
    const result = await pool.request().query(
      `SELECT flag_key, enabled FROM common.feature_flags`
    );
    const flags: Record<string, boolean> = {};
    for (const row of result.recordset) {
      flags[row.flag_key] = !!row.enabled;
    }
    return NextResponse.json(flags, {
      headers: { 'Cache-Control': 'no-store' },
    });
  } catch (err) {
    console.error('flags GET error:', err);
    // Fail open: empty map means every gate sees `false` for its flag,
    // which for sport visibility means hidden, BUT middleware treats
    // missing maintenance_mode as off. Caller-side handles the
    // semantics; this route just reports the truth from the DB.
    return NextResponse.json({}, { status: 500 });
  }
}
