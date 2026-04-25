import { cookies } from 'next/headers';
import { getPool } from './db';

// Server-only feature flag lookups for page-level gates.
//
// Cache: in-process map with a 60s TTL, shared across all flag checks
// in a single request and across requests on the same function instance.
// This matches the middleware cache so the DB sees at most ~1 read per
// minute per instance.
//
// Admin bypass: any visitor with the sb_unlock cookie set to 'go' is
// allowed through every gate. The cookie is set by /api/admin/* on
// successful auth, so signing into /admin grants full site access.
//
// Cascading: page.<sport>.<x> flags are implicitly gated by sport.<sport>.
// If the sport is off, the page is off too, regardless of its own flag.

let cache: { flags: Record<string, boolean>; at: number } | null = null;
const TTL_MS = 60_000;

async function loadFlags(): Promise<Record<string, boolean>> {
  if (cache && Date.now() - cache.at < TTL_MS) return cache.flags;
  try {
    const pool = await getPool();
    const result = await pool.request().query(
      `SELECT flag_key, enabled FROM common.feature_flags`
    );
    const flags: Record<string, boolean> = {};
    for (const row of result.recordset) flags[row.flag_key] = !!row.enabled;
    cache = { flags, at: Date.now() };
    return flags;
  } catch {
    return cache?.flags ?? {};
  }
}

async function hasAdminCookie(): Promise<boolean> {
  const c = await cookies();
  return c.get('sb_unlock')?.value === 'go';
}

// True if the page identified by `flagKey` should be rendered.
// Treats missing flags as enabled (don't surprise-hide pages on a fresh DB).
export async function isPageVisible(flagKey: string): Promise<boolean> {
  if (await hasAdminCookie()) return true;
  const flags = await loadFlags();

  // Cascade from sport to page: if the parent sport flag is explicitly
  // disabled, all its sub-pages are also disabled.
  if (flagKey.startsWith('page.')) {
    const parts = flagKey.split('.');
    if (parts.length >= 2) {
      const parentKey = `sport.${parts[1]}`;
      if (parentKey in flags && !flags[parentKey]) return false;
    }
  }

  if (!(flagKey in flags)) return true;
  return flags[flagKey];
}
