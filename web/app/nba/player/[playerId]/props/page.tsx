import { Suspense } from 'react';
import PlayerPropsInner from './PlayerPropsInner';

export default async function PlayerPropsPage({
  params,
}: {
  params: Promise<{ playerId: string }>;
}) {
  const { playerId } = await params;
  return (
    <Suspense fallback={<div className="px-4 py-6 text-sm text-gray-500">Loading...</div>}>
      <PlayerPropsInner playerId={playerId} />
    </Suspense>
  );
}
