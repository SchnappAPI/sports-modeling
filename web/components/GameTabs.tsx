'use client';

import React from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import RosterTable from './RosterTable';
import StatsTable from './StatsTable';
import BoxScoreTable from './BoxScoreTable';
import LiveBoxScore from './LiveBoxScore';
import MatchupGrid from './MatchupGrid';
import TrendsGrid from './TrendsGrid';
import PropMatrix, { type MatrixRow } from './PropMatrix';

interface Props {
  gameId: string;
  homeTeamId: number;
  awayTeamId: number;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
  selectedDate: string;
  gameStatus: number | null;
}

type Tab = 'roster' | 'stats' | 'boxscore' | 'live' | 'matchups' | 'props' | 'trends';

function getTabs(isLive: boolean): Tab[] {
  return isLive
    ? ['live', 'roster', 'matchups', 'trends', 'props', 'stats', 'boxscore']
    : ['roster', 'matchups', 'trends', 'props', 'stats', 'boxscore'];
}

const TAB_LABELS: Record<Tab, string> = {
  live:     'Live',
  roster:   'Roster',
  matchups: 'Matchups',
  trends:   'Trends',
  props:    'Props',
  stats:    'Stats',
  boxscore: 'Box Score',
};

function PropsTab({ gameId, selectedDate }: { gameId: string; selectedDate: string }) {
  const [rows, setRows]       = React.useState<MatrixRow[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError]     = React.useState<string | null>(null);

  React.useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/grades?date=${selectedDate}&gameId=${gameId}`)
      .then((r) => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then((data) => setRows(data.grades ?? []))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [gameId, selectedDate]);

  if (loading) return <div className="px-4 py-6 text-sm text-gray-500">Loading...</div>;
  if (error)   return <div className="px-4 py-6 text-sm text-red-400">Error: {error}</div>;
  if (rows.length === 0) return <div className="px-4 py-6 text-sm text-gray-500">No props graded for this game.</div>;

  return <PropMatrix rows={rows} gradeDate={selectedDate} outcomeFilter="Over" />;
}

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
      {activeTab === 'matchups' && (
        <MatchupGrid
          gameId={gameId}
          homeTeamAbbr={homeTeamAbbr}
          awayTeamAbbr={awayTeamAbbr}
          selectedDate={selectedDate}
        />
      )}
      {activeTab === 'trends' && (
        <TrendsGrid
          gameId={gameId}
          homeTeamAbbr={homeTeamAbbr}
          awayTeamAbbr={awayTeamAbbr}
          selectedDate={selectedDate}
        />
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
      {activeTab === 'props' && (
        <PropsTab gameId={gameId} selectedDate={selectedDate} />
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
