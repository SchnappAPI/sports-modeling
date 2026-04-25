import { NextRequest, NextResponse } from 'next/server';

const OWNER    = 'SchnappAPI';
const REPO     = 'sports-modeling';
const WORKFLOW = 'refresh-data.yml';

export async function POST(req: NextRequest) {
  // Two auth paths:
  //   1. Admin session token in `x-admin-token` header (matches ADMIN_PASSCODE).
  //      Used by the admin Tools tab so you don't need a separate code.
  //   2. Body `{ code: string }` matching ADMIN_REFRESH_CODE.
  //      Used by the legacy in-page Refresh Data button.
  const adminToken = req.headers.get('x-admin-token') ?? '';
  const adminPasscode = process.env.ADMIN_PASSCODE ?? '';
  const adminAuthed = !!adminPasscode && adminToken === adminPasscode;

  let bodyAuthed = false;
  if (!adminAuthed) {
    const body = await req.json().catch(() => ({}));
    const adminCode = (body.code ?? '').trim().toUpperCase();
    const expected  = (process.env.ADMIN_REFRESH_CODE ?? '').trim().toUpperCase();
    if (!expected) {
      return NextResponse.json({ error: 'ADMIN_REFRESH_CODE not configured' }, { status: 500 });
    }
    if (adminCode && adminCode === expected) {
      bodyAuthed = true;
    }
  }

  if (!adminAuthed && !bodyAuthed) {
    return NextResponse.json({ error: 'Invalid code.' }, { status: 401 });
  }

  const token = process.env.GITHUB_PAT;
  if (!token) {
    return NextResponse.json({ error: 'GITHUB_PAT not configured' }, { status: 500 });
  }

  // Dispatch the workflow
  const dispatchRes = await fetch(
    `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/dispatches`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ref: 'main' }),
    }
  );

  if (!dispatchRes.ok) {
    const text = await dispatchRes.text();
    return NextResponse.json({ error: `GitHub dispatch failed: ${text}` }, { status: 500 });
  }

  // Wait briefly then return the run ID for polling
  await new Promise((r) => setTimeout(r, 3000));

  const runsRes = await fetch(
    `https://api.github.com/repos/${OWNER}/${REPO}/actions/workflows/${WORKFLOW}/runs?per_page=1&event=workflow_dispatch`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
      },
    }
  );

  const data  = runsRes.ok ? await runsRes.json() : {};
  const runId = data.workflow_runs?.[0]?.id ?? null;
  return NextResponse.json({ runId });
}
