'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import GameStrip, { type Game } from '@/components/GameStrip';
import GameTabs from '@/components/GameTabs';

export default function NbaPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [games, setGames] = useState<Game[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const activeGameId = searchParams.get('gameId');

  useEffect(() => {
    const today = new Date().toISOString().slice(0, 10);
    fetch(`/api/games?sport=nba&date=${today}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: { games: Game[] }) => {
        const list = data.games ?? [];
        setGames(list);
        if (list.length > 0 && !searchParams.get('gameId')) {
          router.replace(`/nba?gameId=${list[0].gameId}`);
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, []);

  function handleSelectGame(gameId: string) {
    const params = new URLSearchParams();
    params.set('gameId', gameId);
    const currentTab = searchParams.get('tab');
    if (currentTab) params.set('tab', currentTab);
    router.replace(`/nba?${params.toString()}`);
  }

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800">
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">NBA</span>
      </div>

      {loading && <div className="px-4 py-3 text-sm text-gray-500">Loading games...</div>}
      {error && <div className="px-4 py-3 text-sm text-red-400">Error: {error}</div>}
      {!loading && !error && (
        <GameStrip
          games={games}
          activeGameId={activeGameId}
          onSelect={handleSelectGame}
        />
      )}

      <div className="flex-1 px-4">
        {activeGameId ? (
          <GameTabs gameId={activeGameId} />
        ) : (
          !loading && <div className="py-6 text-sm text-gray-500">Select a game above.</div>
        )}
      </div>
    </div>
  );
}
