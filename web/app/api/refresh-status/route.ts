import { NextRequest, NextResponse } from 'next/server';

const OWNER = 'SchnappAPI';
const REPO  = 'sports-modeling';

export async function GET(req: NextRequest) {
  const runId = req.nextUrl.searchParams.get('runId');
  if (!runId) {
    return NextResponse.json({ error: 'runId required' }, { status: 400 });
  }

  const token = process.env.GITHUB_PAT;
  if (!token) {
    return NextResponse.json({ error: 'GITHUB_PAT not configured' }, { status: 500 });
  }

  const res = await fetch(
    `https://api.github.com/repos/${OWNER}/${REPO}/actions/runs/${runId}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github+json',
      },
    }
  );

  if (!res.ok) {
    return NextResponse.json({ error: `GitHub API error: ${res.status}` }, { status: 500 });
  }

  const data = await res.json();
  return NextResponse.json({
    status:     data.status,      // queued | in_progress | completed
    conclusion: data.conclusion,  // success | failure | null
  });
}
