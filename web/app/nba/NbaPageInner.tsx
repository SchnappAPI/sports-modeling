'use client';

import { useEffect, useRef, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import Link from 'next/link';
import GameStrip, { type Game } from '@/components/GameStrip';
import GameTabs from '@/components/GameTabs';
import RefreshDataButton from '@/components/RefreshDataButton';
import { randomLoadingWord } from '@/lib/loadingWord';
import { useAuth } from '@/lib/auth-context';

// Convert an ET time string like "7:30 pm ET" to CT by subtracting 1 hour.
function convertEtToCt(text: string | null): string | null {
  if (!text) return text;
  const m = text.match(/^(\d{1,2}):(\d{2})\s*(am|pm)\s*ET$/i);
  if (!m) return text;
  let h = parseInt(m[1], 10);
  const min = m[2];
  const ampm = m[3].toLowerCase();
  if (ampm === 'pm' && h !== 12) h += 12;
  if (ampm === 'am' && h === 12) h = 0;
  h -= 1;
  if (h < 0) h += 24;
  let displayAmPm = h >= 12 ? 'pm' : 'am';
  let displayH = h % 12;
  if (displayH === 0) displayH = 12;
  return `${displayH}:${min} ${displayAmPm} CT`;
}

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
    const aUpcoming = a.gameStatus == null || a.gameStatus === 1;
    const bUpcoming = b.gameStatus == null || b.gameStatus === 1;
    if (aUpcoming && bUpcoming) {
      const tA = parseStartMinutes(a.gameStatusText);
      const tB = parseStartMinutes(b.gameStatusText);
      if (tA != null && tB != null) return tA - tB;
      if (tA != null) return -1;
      if (tB != null) return 1;
      return 0;
    }
    const bucket = (s: number | null) => (s == null || s === 1 ? 0 : s === 2 ? 1 : 2);
    return bucket(a.gameStatus) - bucket(b.gameStatus);
  });
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

async function fetchGames(date: string): Promise<Game[]> {
  const res = await fetch(`/api/games?sport=nba&date=${date}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data: { games: Game[] } = await res.json();
  return data.games ?? [];
}

// Pick the best game to auto-select from a sorted list.
// Prefers: live (status 2) > pre-game (status 1) > first finished (status 3).
// This prevents landing on a finished game when upcoming games exist.
function pickDefaultGame(sorted: Game[]): Game | undefined {
  return (
    sorted.find((g) => g.gameStatus === 2) ??
    sorted.find((g) => g.gameStatus == null || g.gameStatus === 1) ??
    sorted[0]
  );
}

export default function NbaPageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { mode, demoDates, logout } = useAuth();

  const isDemo   = mode === 'demo';
  const demoDate = demoDates.nba;

  const urlDate  = searchParams.get('date');
  const urlGameId = searchParams.get('gameId');
  const defaultDate = isDemo && demoDate ? demoDate : todayLocal();
  const [selectedDate, setSelectedDate] = useState<string>(urlDate ?? defaultDate);
  const [games, setGames] = useState<Game[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingWord] = useState(() => randomLoadingWord());
  const [error, setError] = useState<string | null>(null);

  // Only suppress the fallback + smart-select when the user has explicitly
  // navigated to a date AND a game via UI interaction (not initial page load).
  const isExplicitSelection = useRef<boolean>(false);

  const activeGameId = searchParams.get('gameId');
  const activeGame   = games.find((g) => g.gameId === activeGameId) ?? null;

  const effectiveDate = isDemo && demoDate ? demoDate : selectedDate;

  async function loadGames() {
    if (isDemo) {
      setLoading(true);
      setError(null);
      setGames([]);
      try {
        const raw = await fetchGames(effectiveDate);
        const sorted = sortGames(raw.map((g) => ({
          ...g,
          gameStatusText: (g.gameStatus == null || g.gameStatus === 1)
            ? convertEtToCt(g.gameStatusText)
            : g.gameStatusText,
        })));
        setGames(sorted);
        const currentGameId = searchParams.get('gameId');
        const stillValid = sorted.some((g) => g.gameId === currentGameId);
        if (sorted.length > 0 && !stillValid) {
          const pick = pickDefaultGame(sorted);
          if (pick) router.replace(`/nba?gameId=${pick.gameId}&date=${effectiveDate}`);
        }
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
      return;
    }

    setLoading(true);
    setError(null);
    setGames([]);

    try {
      let raw = await fetchGames(effectiveDate);
      let usedDate = effectiveDate;

      // If today has no games and this was not an explicit user navigation,
      // fall back to yesterday. Handles the window between local midnight and
      // when nba-etl populates today's schedule (runs at 9am UTC / 3am CT).
      if (raw.length === 0 && !isExplicitSelection.current) {
        const yesterday = shiftDate(effectiveDate, -1);
        const fallback = await fetchGames(yesterday);
        if (fallback.length > 0) {
          raw = fallback;
          usedDate = yesterday;
          setSelectedDate(yesterday);
          router.replace(`/nba?date=${yesterday}`);
        }
      }

      const sorted = sortGames(raw.map((g) => ({
        ...g,
        gameStatusText: (g.gameStatus == null || g.gameStatus === 1)
          ? convertEtToCt(g.gameStatusText)
          : g.gameStatusText,
      })));
      setGames(sorted);

      const currentGameId = searchParams.get('gameId');
      const currentGame   = sorted.find((g) => g.gameId === currentGameId);

      // Always replace if:
      // 1. No gameId in URL, or gameId not in today's games (stale from another date)
      // 2. Not an explicit user selection AND the current game is finished while
      //    pre-game or live games exist on this date (NBA stores late-night games
      //    under the next calendar date, so finished games appear alongside
      //    tonight's upcoming games — always prefer the upcoming ones)
      const hasUpcoming = sorted.some((g) => g.gameStatus == null || g.gameStatus === 1 || g.gameStatus === 2);
      const currentIsFinished = currentGame?.gameStatus === 3;
      const shouldReplace =
        !currentGame ||
        (!isExplicitSelection.current && currentIsFinished && hasUpcoming);

      if (sorted.length > 0 && shouldReplace) {
        const pick = pickDefaultGame(sorted);
        if (pick) router.replace(`/nba?gameId=${pick.gameId}&date=${usedDate}`);
      }
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadGames(); }, [effectiveDate]);

  function handleSelectGame(gameId: string) {
    isExplicitSelection.current = true;
    const params = new URLSearchParams();
    params.set('gameId', gameId);
    params.set('date', effectiveDate);
    const currentTab = searchParams.get('tab');
    if (currentTab) params.set('tab', currentTab);
    router.replace(`/nba?${params.toString()}`);
  }

  function applyDate(newDate: string) {
    if (isDemo) return;
    isExplicitSelection.current = true;
    setSelectedDate(newDate);
    router.replace(`/nba?date=${newDate}`);
  }

  function handleDateChange(e: React.ChangeEvent<HTMLInputElement>) {
    applyDate(e.target.value);
  }

  const gradesHref = `/nba/grades?date=${effectiveDate}`;

  return (
    <div className="flex flex-col min-h-screen">
      <div className="px-4 py-3 border-b border-gray-800 flex items-center justify-between gap-3">
        <span className="text-sm font-semibold text-gray-400 uppercase tracking-wider">NBA</span>

        <div className="flex items-center gap-1">
          {!isDemo && (
            <button
              onClick={() => applyDate(shiftDate(selectedDate, -1))}
              className="px-2 py-1 text-gray-400 hover:text-gray-200 text-base leading-none"
              aria-label="Previous day"
            >
              &#8249;
            </button>
          )}
          <input
            type="date"
            value={effectiveDate}
            onChange={handleDateChange}
            disabled={isDemo}
            className={`text-sm bg-gray-900 border border-gray-700 rounded px-2 py-1 text-gray-300
                       focus:outline-none focus:border-gray-500 ${
                         isDemo ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'
                       }`}
          />
          {!isDemo && (
            <button
              onClick={() => applyDate(shiftDate(selectedDate, 1))}
              className="px-2 py-1 text-gray-400 hover:text-gray-200 text-base leading-none"
              aria-label="Next day"
            >
              &#8250;
            </button>
          )}
        </div>

        <div className="flex items-center gap-3">
          <Link
            href={gradesHref}
            className="text-sm font-medium text-gray-400 hover:text-blue-400 transition-colors"
          >
            At a Glance
          </Link>
          {!isDemo && (
            <RefreshDataButton onComplete={loadGames} />
          )}
          {!isDemo && (
            <button
              onClick={logout}
              className="text-xs text-gray-600 hover:text-gray-400 transition-colors"
            >
              Log out
            </button>
          )}
        </div>
      </div>

      {loading && <div className="px-4 py-3 text-sm text-gray-500">{loadingWord}...</div>}
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
            selectedDate={effectiveDate}
            gameStatus={activeGame.gameStatus}
          />
        ) : (
          !loading && <div className="py-6 text-sm text-gray-500">Select a game above.</div>
        )}
      </div>
    </div>
  );
}
