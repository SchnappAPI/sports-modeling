'use client';

import { useEffect, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import Link from 'next/link';
import GameStrip, { type Game } from '@/components/GameStrip';
import GameTabs from '@/components/GameTabs';

// ---------------------------------------------------------------------------
// Sort games within a day by start time.
//
// game_status_text is "7:30 pm ET" for upcoming games, "Final" for completed,
// and a clock string like "Q3 4:22" for live. Sort order:
//   1. Upcoming games, ascending by parsed start time (earliest left)
//   2. Live games
//   3. Final games
//
// If the time cannot be parsed the game sorts after other upcoming games
// but before live/final.
// ---------------------------------------------------------------------------
function parseStartMinutes(text: string | null): number | null {
  if (!text) return null;
  const m = text.match(/(\d{1,2}):(\d{2})\s*(am|pm)/i);
  if (!m) return null;
  let h = parseInt(m[1], 10);
  const min = parseInt(m[2], 10);
  const ampm = m[3].toLowerCase();
  if (ampm === 'pm' && h !== 12) h += 12;
  if (ampm === 'am' && h === 12) h = 0;
  return h * 60 + min;
}

function sortGames(games: Game[]): Game[] {
  return [...games].sort((a, b) => {
    const statusA = a.gameStatus ?? 1;
    const statusB = b.gameStatus ?? 1;

    // Both upcoming (status 1 or null)
    const aUpcoming = statusA === 1 || a.gameStatus == null;
    const bUpcoming = statusB === 1 || b.gameStatus == null;

    if (aUpcoming && bUpcoming) {
      const tA = parseStartMinutes(a.gameStatusText);
      const tB = parseStartMinutes(b.gameStatusText);
      if (tA != null && tB != null) return tA - tB;
      if (tA != null) return -1;
      if (tB != null) return 1;
      return 0;
    }

    // Upcoming before live before final
    const bucket = (s: number | null) => (s == null || s === 1 ? 0 : s === 2 ? 1 : 2);
    return bucket(a.gameStatus) - bucket(b.gameStatus);
  });
}

function todayLocal(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

export default function NbaPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const [selectedDate, setSelectedDate] = useState<string>(todayLocal);
  const [games, setGames] = useState<Game[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const activeGameId = searchParams.get('gameId');
  const activeGame = games.find((g) => g.gameId === activeGameId) ?? null;

  // Fetch games whenever the selected date changes.
  useEffect(() => {
    setLoading(true);
    setError(null);
    setGames([]);

    fetch(`/api/games?sport=nba&date=${selectedDate}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: { games: Game[] }) => {
        const sorted = sortGames(data.games ?? []);
        setGames(sorted);
        // Auto-select first game only when no gameId is in the URL or
        // when navigating to a new date (the current gameId won't exist
        // in the new date's game list).
        const currentGameId = searchParams.get('gameId');
        const stillValid = sorted.some((g) => g.gameId === currentGameId);
        if (sorted.length > 0 && !stillValid) {
          router.replace(`/nba?gameId=${sorted[0].gameId}`);
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [selectedDate]);

  function handleSelectGame(gameId: string) {
    const params = new URLSearchParams();
    params.set('gameId', gameId);
    const currentTab = searchParams.get('tab');
    if (currentTab) params.set('tab', currentTab);
    router.replace(`/nba?${params.toString()}`);
  }

  function handleDateChange(e: React.ChangeEvent<HTMLInputElement>) {
    setSelectedDate(e.target.value);
    // Clear the active game so the tab area doesn't show stale data
    // while the new date loads.
    router.replace('/nba');
  }

  const gradesHref = activeGameId
    ? `/nba/grades?gameId=${activeGameId}&date=${selectedDate}`
    : `/nba/grades?date=${selectedDate}`;

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between gap-3">
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">NBA</span>

        <input
          type="date"
          value={selectedDate}
          onChange={handleDateChange}
          className="text-sm bg-gray-900 border border-gray-700 rounded px-2 py-1 text-gray-300
                     focus:outline-none focus:border-gray-500 cursor-pointer"
        />

        <Link
          href={gradesHref}
          className="text-sm font-medium text-gray-400 hover:text-blue-400 transition-colors"
        >
          At a Glance
        </Link>
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
        {activeGame ? (
          <GameTabs
            gameId={activeGame.gameId}
            homeTeamId={activeGame.homeTeamId}
            awayTeamId={activeGame.awayTeamId}
            homeTeamAbbr={activeGame.homeTeamAbbr}
            awayTeamAbbr={activeGame.awayTeamAbbr}
          />
        ) : (
          !loading && <div className="py-6 text-sm text-gray-500">Select a game above.</div>
        )}
      </div>
    </div>
  );
}
