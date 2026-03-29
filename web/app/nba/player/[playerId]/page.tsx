import { Suspense } from 'react';
import PlayerPageInner from './PlayerPageInner';

type Props = { params: Promise<{ playerId: string }> };

export default async function PlayerPage({ params }: Props) {
  const { playerId } = await params;
  return (
    <Suspense fallback={<div className="p-4 text-sm text-gray-500">Loading...</div>}>
      <PlayerPageInner playerId={playerId} />
    </Suspense>
  );
}
