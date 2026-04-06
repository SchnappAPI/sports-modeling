'use client';

import { useEffect, useRef, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import MlbGameTabs from './MlbGameTabs';

interface MlbGame {
  gameId: number;
  gameDate: string;
  gameStatus: string | null;
  gameDisplay: string;
  awayTeamId: number;
  homeTeamId: number;
  awayTeamAbbr: string;
  homeTeamAbbr: string;
  awayTeamName: string;
  homeTeamName: string;
  awayScore: number | null;
  homeScore: number | null;
  gameDateTime: string | null;
  awayPitcher: string | null;
  homePitcher: string | null;
}

function todayLocal(): string {
  const d = new Date();
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

function shiftDate(dateStr: string, days: number): string {
  const [y, m, d] = dateStr.split('-').map(Number);
  const dt = new Date(y, m - 1, d + days);
  return `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}-${String(dt.getDate()).padStart(2, '0')}`;
}

function formatGameTime(isoStr: string | null): string {
  if (!isoStr) return '';
  try {
    const d = new Date(isoStr);
    return d.toLocaleTimeString('en-US', {
      timeZone: 'America/Chicago',
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
    }) + ' CT';
  } catch {
    return '';
  }
}

function statusLabel(game: MlbGame): string {
  if (game.gameStatus === 'F' || game.gameStatus === 'Final') return 'Final';
  if (game.gameStatus && game.gameStatus !== 'Preview') return game.gameStatus;
  return formatGameTime(game.gameDateTime);
}

export default function MlbPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();

  const urlDate = searchParams.get('date');
  const [selectedDate, setSelectedDate] = useState<string>(urlDate ?? todayLocal());
  const [games, setGames] = useState<MlbGame[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const isExplicitSelection = useRef(false);

  const activeGameId = searchParams.get('gameId');
  const activeGame = games.find((g) => String(g.gameId) === activeGameId) ?? null;

  async function loadGames() {
    setLoading(true);
    setError(null);
    setGames([]);
    try {
      const res = await fetch(`/api/mlb-games?date=${selectedDate}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const sorted: MlbGame[] = (data.games ?? []).sort((a: MlbGame, b: MlbGame) => {
        const aTime = a.gameDateTime ?? '';
        const bTime = b.gameDateTime ?? '';
        return aTime.localeCompare(bTime);
      });
      setGames(sorted);

      const currentId = searchParams.get('gameId');
      const currentValid = sorted.find((g) => String(g.gameId) === currentId);
      if (!currentValid && sorted.length > 0 && !isExplicitSelection.current) {
        const pick = sorted.find((g) => g.gameStatus !== 'F') ?? sorted[0];
        router.replace(`/mlb?gameId=${pick.gameId}&date=${selectedDate}`);
      }
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadGames(); }, [selectedDate]);

  function handleSelectGame(gameId: number) {
    isExplicitSelection.current = true;
    router.replace(`/mlb?gameId=${gameId}&date=${selectedDate}`);
  }

  function applyDate(newDate: string) {
    isExplicitSelection.current = false;
    setSelectedDate(newDate);
    router.replace(`/mlb?date=${newDate}`);
  }

  return (
    <div className="flex flex-col min-h-screen">
      {/* Header */}
      <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between gap-3">
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">MLB</span>
        <div className="flex items-center gap-1">
          <button
            onClick={() => applyDate(shiftDate(selectedDate, -1))}
            className="px-2 py-1 text-gray-400 hover:text-gray-200 text-base leading-none"
            aria-label="Previous day"
          >
            &#8249;
          </button>
          <input
            type="date"
            value={selectedDate}
            onChange={(e) => applyDate(e.target.value)}
            className="text-sm bg-gray-900 border border-gray-700 rounded px-2 py-1 text-gray-300 focus:outline-none focus:border-gray-500 cursor-pointer"
          />
          <button
            onClick={() => applyDate(shiftDate(selectedDate, 1))}
            className="px-2 py-1 text-gray-400 hover:text-gray-200 text-base leading-none"
            aria-label="Next day"
          >
            &#8250;
          </button>
        </div>
        <div className="w-20" />
      </div>

      {/* Game strip */}
      {loading && <div className="px-4 py-3 text-sm text-gray-500">Loading...</div>}
      {error && <div className="px-4 py-3 text-sm text-red-400">Error: {error}</div>}
      {!loading && !error && games.length > 0 && (
        <div className="flex overflow-x-auto border-b border-gray-800 bg-gray-950">
          {games.map((g) => {
            const isActive = String(g.gameId) === activeGameId;
            const isFinal = g.gameStatus === 'F' || g.gameStatus === 'Final';
            return (
              <button
                key={g.gameId}
                onClick={() => handleSelectGame(g.gameId)}
                className={`flex-shrink-0 px-4 py-2 text-left border-r border-gray-800 transition-colors ${
                  isActive ? 'bg-gray-800' : 'hover:bg-gray-900'
                }`}
              >
                <div className="flex items-center gap-2 text-sm">
                  <span className={isFinal && g.awayScore != null && g.homeScore != null && g.awayScore > g.homeScore ? 'font-semibold text-gray-100' : 'text-gray-400'}>
                    {g.awayTeamAbbr}
                  </span>
                  {isFinal && g.awayScore != null && (
                    <span className={g.awayScore > (g.homeScore ?? 0) ? 'font-semibold text-gray-100 text-xs' : 'text-gray-500 text-xs'}>{g.awayScore}</span>
                  )}
                </div>
                <div className="flex items-center gap-2 text-sm mt-0.5">
                  <span className={isFinal && g.awayScore != null && g.homeScore != null && g.homeScore > g.awayScore ? 'font-semibold text-gray-100' : 'text-gray-400'}>
                    {g.homeTeamAbbr}
                  </span>
                  {isFinal && g.homeScore != null && (
                    <span className={g.homeScore > (g.awayScore ?? 0) ? 'font-semibold text-gray-100 text-xs' : 'text-gray-500 text-xs'}>{g.homeScore}</span>
                  )}
                </div>
                <div className="text-xs text-gray-500 mt-1">{statusLabel(g)}</div>
              </button>
            );
          })}
        </div>
      )}
      {!loading && !error && games.length === 0 && (
        <div className="px-4 py-6 text-sm text-gray-500">No games scheduled for this date.</div>
      )}

      {/* Game detail */}
      <div className="flex-1 px-4">
        {activeGame ? (
          <MlbGameTabs game={activeGame} />
        ) : (
          !loading && games.length > 0 && (
            <div className="py-6 text-sm text-gray-500">Select a game above.</div>
          )
        )}
      </div>
    </div>
  );
}
