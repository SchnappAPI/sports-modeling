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
        const statusLabel =
          game.gameStatus === 3
            ? 'Final'
            : game.gameStatus === 2
            ? 'Live'
            : game.gameStatusText ?? 'Upcoming';

        const spreadLabel =
          game.spread != null ? (game.spread > 0 ? `+${game.spread}` : `${game.spread}`) : null;

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
            <div className="text-xs font-semibold text-gray-400 mb-1">{statusLabel}</div>
            <div className="text-sm font-bold">
              {game.awayTeamAbbr} <span className="text-gray-500">@</span> {game.homeTeamAbbr}
            </div>
            <div className="text-xs text-gray-400 mt-1">
              {spreadLabel != null && <span className="mr-2">{spreadLabel}</span>}
              {game.total != null && <span>O/U {game.total}</span>}
              {spreadLabel == null && game.total == null && <span className="text-gray-600">No line</span>}
            </div>
          </button>
        );
      })}
    </div>
  );
}
