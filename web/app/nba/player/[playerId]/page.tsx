import { Suspense } from 'react';
import PlayerPageInner from './PlayerPageInner';
import ComingSoon from '@/components/ComingSoon';
import { isPageVisible } from '@/lib/feature-flags';

type Props = { params: Promise<{ playerId: string }> };

export default async function PlayerPage({ params }: Props) {
  if (!(await isPageVisible('page.nba.player'))) return <ComingSoon label="Player" />;
  const { playerId } = await params;
  return (
    <Suspense fallback={<div className="p-4 text-sm text-gray-500">Loading...</div>}>
      <PlayerPageInner playerId={playerId} />
    </Suspense>
  );
}
