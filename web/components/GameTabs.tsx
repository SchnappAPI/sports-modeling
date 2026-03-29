'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import RosterTable from './RosterTable';
import StatsTable from './StatsTable';

interface Props {
  gameId: string;
}

const TABS = ['roster', 'stats', 'boxscore'] as const;
type Tab = typeof TABS[number];

const TAB_LABELS: Record<Tab, string> = {
  roster: 'Roster',
  stats: 'Stats',
  boxscore: 'Box Score',
};

export default function GameTabs({ gameId }: Props) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const activeTab = (searchParams.get('tab') as Tab) ?? 'roster';

  function selectTab(tab: Tab) {
    const params = new URLSearchParams(searchParams.toString());
    params.set('tab', tab);
    router.replace(`/nba?${params.toString()}`);
  }

  return (
    <div className="mt-4">
      <div className="flex gap-1 border-b border-gray-800 mb-4">
        {TABS.map((tab) => (
          <button
            key={tab}
            onClick={() => selectTab(tab)}
            className={[
              'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
              activeTab === tab
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-500 hover:text-gray-300',
            ].join(' ')}
          >
            {TAB_LABELS[tab]}
          </button>
        ))}
      </div>

      {activeTab === 'roster' && <RosterTable gameId={gameId} />}
      {activeTab === 'stats' && <StatsTable gameId={gameId} />}
      {activeTab === 'boxscore' && <div className="text-sm text-gray-500">Box score coming soon.</div>}
    </div>
  );
}
