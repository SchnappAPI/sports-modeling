'use client';

export interface Game {
  gameId: string;
  gameDate: string;
  gameStatus: number | null;
  gameStatusText: string | null;
  homeTeamId: number;
  awayTeamId: number;
  homeTeamAbbr: string;
  awayTeamAbbr: string;
  homeTeamName: string;
  awayTeamName: string;
  homeScore: number | null;
  awayScore: number | null;
  period?: number | null;
  gameClock?: string | null;
  spread: number | null;
  total: number | null;
}

interface Props {
  games: Game[];
  activeGameId: string | null;
  onSelect: (gameId: string) => void;
}

export default function GameStrip({ games, activeGameId, onSelect }: Props) {
  if (games.length === 0) {
    return (
      <div className="px-4 py-3 text-sm text-gray-500">
        No games today.
      </div>
    );
  }

  return (
    <div className="flex gap-2 overflow-x-auto px-4 py-3 border-b border-gray-800">
      {games.map((game) => {
        const isActive = game.gameId === activeGameId;
        const isLive   = game.gameStatus === 2;
        const isFinal  = game.gameStatus === 3;
        const hasScore = game.homeScore != null && game.awayScore != null;

        const statusLabel =
          isFinal ? 'Final' :
          isLive  ? game.gameStatusText ?? 'Live' :
          game.gameStatusText ?? 'Upcoming';

        const spreadLabel =
          game.spread != null
            ? (game.spread > 0 ? `+${game.spread}` : `${game.spread}`)
            : null;

        // Determine leading team for score emphasis (home wins ties)
        const homeLeads =
          hasScore && game.homeScore != null && game.awayScore != null
            ? game.homeScore >= game.awayScore
            : true;

        return (
          <button
            key={game.gameId}
            onClick={() => onSelect(game.gameId)}
            className={[
              'flex-shrink-0 rounded-lg border px-4 py-3 text-left transition-colors',
              isActive
                ? 'border-blue-500 bg-gray-800'
                : 'border-gray-700 bg-gray-900 hover:border-gray-500',
            ].join(' ')}
          >
            {/* Status row */}
            <div className="flex items-center gap-1.5 mb-1">
              {isLive && (
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-red-500" />
                </span>
              )}
              <span className={`text-xs font-semibold ${isLive ? 'text-red-400' : 'text-gray-400'}`}>
                {statusLabel}
              </span>
            </div>

            {/* Matchup row */}
            {(isLive || isFinal) && hasScore ? (
              // Score layout: AWY score @ HME score
              <div className="text-sm font-bold flex items-center gap-1.5">
                <span className={!homeLeads ? 'text-gray-100' : 'text-gray-500'}>
                  {game.awayScore}
                </span>
                <span className="text-xs text-gray-600">{game.awayTeamAbbr}</span>
                <span className="text-gray-600 text-xs">@</span>
                <span className="text-xs text-gray-600">{game.homeTeamAbbr}</span>
                <span className={homeLeads ? 'text-gray-100' : 'text-gray-500'}>
                  {game.homeScore}
                </span>
              </div>
            ) : (
              // Pre-game layout: AWY @ HME
              <div className="text-sm font-bold">
                {game.awayTeamAbbr} <span className="text-gray-500">@</span> {game.homeTeamAbbr}
              </div>
            )}

            {/* Bottom row: odds for upcoming, nothing for live/final with scores */}
            {!(isLive || isFinal) && (
              <div className="text-xs text-gray-400 mt-1">
                {spreadLabel != null && <span className="mr-2">{spreadLabel}</span>}
                {game.total != null && <span>O/U {game.total}</span>}
                {spreadLabel == null && game.total == null && <span className="text-gray-600">No line</span>}
              </div>
            )}
          </button>
        );
      })}
    </div>
  );
}
