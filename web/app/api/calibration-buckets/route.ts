import { NextResponse } from 'next/server';
import { getPool } from '@/lib/db';

// Returns the latest snapshot of calibration buckets for the active model.
// Reads common.grade_calibration (the live, daily-recomputed table). When
// the weekly-snapshot architecture lands (ADR-20260425-3 follow-on), this
// route will be extended to read from grade_calibration_history.

export const dynamic = 'force-dynamic';

type Bucket = {
  bucket_min: number;
  bucket_max: number;
  sample_size: number;
  empirical_hit_rate: number;
  isotonic_hit_rate: number;
  max_well_sampled_rate: number | null;
};

export async function GET() {
  try {
    const pool = await getPool();
    const result = await pool.request().query(`
      SELECT bucket_min, bucket_max, sample_size,
             empirical_hit_rate, isotonic_hit_rate,
             max_well_sampled_rate, last_updated
        FROM common.grade_calibration
       ORDER BY bucket_min ASC
    `);
    const buckets: Bucket[] = result.recordset.map((r: Record<string, unknown>) => ({
      bucket_min: Number(r.bucket_min),
      bucket_max: Number(r.bucket_max),
      sample_size: Number(r.sample_size),
      empirical_hit_rate: Number(r.empirical_hit_rate),
      isotonic_hit_rate: Number(r.isotonic_hit_rate),
      max_well_sampled_rate: r.max_well_sampled_rate == null ? null : Number(r.max_well_sampled_rate),
    }));
    const lastUpdated = result.recordset[0]?.last_updated ?? null;
    const cap = buckets[0]?.max_well_sampled_rate ?? null;
    return NextResponse.json({ buckets, last_updated: lastUpdated, max_well_sampled_rate: cap });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
