import { NextRequest, NextResponse } from 'next/server';

const OWNER = 'SchnappAPI';
const REPO  = 'sports-modeling';
const WORKFLOW = 'refresh-lines.yml';

export async function POST(_req: NextRequest) {
  const token = process.env.GITHUB_PAT;
  if (!token) {
    return NextResponse.json({ error: 'GITHUB_PAT not configured' }, { status: 500 });
  }

  // Trigger workflow_dispatch
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

  // GitHub returns 204 with no body. Wait briefly then fetch the run ID
  // of the most recently queued refresh-lines run so the client can poll.
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

  if (!runsRes.ok) {
    // Dispatch succeeded even if we can't get the run ID yet.
    return NextResponse.json({ runId: null });
  }

  const data = await runsRes.json();
  const runId = data.workflow_runs?.[0]?.id ?? null;
  return NextResponse.json({ runId });
}
