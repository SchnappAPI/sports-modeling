'use client';

import { useRouter, useSearchParams } from 'next/navigation';
import RosterTable from './RosterTable';
import StatsTable from './StatsTable';
import BoxScoreTable from './BoxScoreTable';
import LiveBoxScore from './LiveBoxScore';

interface Props {
  gameId: string;
  homeTeamId: number;
  awayTeamId: number;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
  selectedDate: string;
  gameStatus: number | null;
}

// Live tab only appears when game is in progress.
type Tab = 'roster' | 'stats' | 'boxscore' | 'live';

function getTabs(isLive: boolean): Tab[] {
  return isLive
    ? ['live', 'roster', 'stats', 'boxscore']
    : ['roster', 'stats', 'boxscore'];
}

const TAB_LABELS: Record<Tab, string> = {
  live:     'Live',
  roster:   'Roster',
  stats:    'Stats',
  boxscore: 'Box Score',
};

export default function GameTabs({
  gameId, homeTeamId, awayTeamId, homeTeamAbbr, awayTeamAbbr, selectedDate, gameStatus,
}: Props) {
  const router      = useRouter();
  const searchParams = useSearchParams();
  const isLive      = gameStatus === 2;
  const tabs        = getTabs(isLive);

  const rawTab    = searchParams.get('tab') as Tab | null;
  const activeTab = rawTab && tabs.includes(rawTab) ? rawTab : (isLive ? 'live' : 'roster');

  function selectTab(tab: Tab) {
    const params = new URLSearchParams(searchParams.toString());
    params.set('tab', tab);
    router.replace(`/nba?${params.toString()}`);
  }

  return (
    <div className="mt-4">
      <div className="flex gap-1 border-b border-gray-800 mb-4">
        {tabs.map((tab) => (
          <button
            key={tab}
            onClick={() => selectTab(tab)}
            className={[
              'px-4 py-2 text-sm font-medium border-b-2 transition-colors flex items-center gap-1.5',
              activeTab === tab
                ? tab === 'live'
                  ? 'border-red-500 text-red-400'
                  : 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-500 hover:text-gray-300',
            ].join(' ')}
          >
            {tab === 'live' && (
              <span className="relative flex h-1.5 w-1.5">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
                <span className="relative inline-flex rounded-full h-1.5 w-1.5 bg-red-500" />
              </span>
            )}
            {TAB_LABELS[tab]}
          </button>
        ))}
      </div>

      {activeTab === 'live' && isLive && (
        <LiveBoxScore gameId={gameId} selectedDate={selectedDate} />
      )}
      {activeTab === 'roster' && (
        <RosterTable gameId={gameId} selectedDate={selectedDate} />
      )}
      {activeTab === 'stats' && (
        <StatsTable
          gameId={gameId}
          homeTeamId={homeTeamId}
          awayTeamId={awayTeamId}
          homeTeamAbbr={homeTeamAbbr}
          awayTeamAbbr={awayTeamAbbr}
          selectedDate={selectedDate}
        />
      )}
      {activeTab === 'boxscore' && (
        <BoxScoreTable
          gameId={gameId}
          selectedDate={selectedDate}
        />
      )}
    </div>
  );
}
