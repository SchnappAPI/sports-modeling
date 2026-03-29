import { NextRequest, NextResponse } from 'next/server';
import { getMatchupDefense } from '@/lib/queries';

export async function GET(req: NextRequest) {
  const oppTeamIdParam = req.nextUrl.searchParams.get('oppTeamId');
  const position       = req.nextUrl.searchParams.get('position');

  if (!oppTeamIdParam || !position) {
    return NextResponse.json(
      { error: 'oppTeamId and position are required' },
      { status: 400 }
    );
  }

  const oppTeamId = parseInt(oppTeamIdParam, 10);
  if (isNaN(oppTeamId)) {
    return NextResponse.json({ error: 'oppTeamId must be an integer' }, { status: 400 });
  }

  try {
    const data = await getMatchupDefense(oppTeamId, position);
    if (!data) {
      return NextResponse.json(
        { error: 'No defense data for this team and position' },
        { status: 404 }
      );
    }
    return NextResponse.json(data);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
