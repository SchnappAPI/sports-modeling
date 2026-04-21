'use client';

import { useEffect, useState } from 'react';

interface MlbGame {
  gameId: number;
  awayTeamId: number;
  homeTeamId: number;
  awayTeamAbbr: string;
  homeTeamAbbr: string;
  awayScore: number | null;
  homeScore: number | null;
  gameStatus: string | null;
  awayPitcher: string | null;
  homePitcher: string | null;
}

interface Starter {
  playerId: number;
  playerName: string | null;
  teamId: number;
  handCode: string | null;
}

interface LineupSpot {
  batter: {
    playerId: number;
    playerName: string | null;
    teamId: number;
    battingOrder: number;
    position: string | null;
    handCode: string | null;
  };
  bvp: {
    pa: number;
    ab: number;
    h: number;
    hr: number;
    rbi: number;
    bb: number;
    k: number;
    avg: number | null;
    obp: number | null;
    slg: number | null;
    ops: number | null;
    lastFacedDate: string | null;
  } | null;
}

interface BvpResponse {
  gamePk: number;
  awaySP: Starter | null;
  homeSP: Starter | null;
  awayLineup: LineupSpot[];
  homeLineup: LineupSpot[];
  earliestDataDate: string | null;
  available: boolean;
}

function fmt(val: number | null | undefined): string {
  if (val == null) return '-';
  return String(val);
}

function fmtAvg(val: number | null | undefined): string {
  if (val == null) return '-';
  return val.toFixed(3).replace(/^0/, '');
}

function pitcherHandLabel(code: string | null): string {
  if (code === 'L') return 'LHP';
  if (code === 'R') return 'RHP';
  return '';
}

function batterHandLabel(code: string | null): string {
  if (code === 'L') return 'L';
  if (code === 'R') return 'R';
  if (code === 'S') return 'S';
  return '';
}

function formatDate(iso: string | null): string {
  if (!iso) return '';
  // iso comes from SQL Server as 'YYYY-MM-DDTHH:MM:SS.000Z' or 'YYYY-MM-DD'.
  // Strip time portion and reformat as M/D/YY for a compact display.
  const datePart = iso.split('T')[0];
  const [y, m, d] = datePart.split('-').map(Number);
  if (!y || !m || !d) return '';
  return `${m}/${d}/${String(y).slice(2)}`;
}

function LineupTable({
  title,
  lineup,
  pitcher,
  battingTeamAbbr,
  pitchingTeamAbbr,
}: {
  title: string;
  lineup: LineupSpot[];
  pitcher: Starter;
  battingTeamAbbr: string;
  pitchingTeamAbbr: string;
}) {
  const pitcherHand = pitcherHandLabel(pitcher.handCode);
  return (
    <div className="mb-6">
      <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">
        {title}
      </div>
      <div className="text-sm text-gray-300 mb-2">
        <span className="text-gray-500">{battingTeamAbbr} batters vs </span>
        <span className="text-gray-200">{pitcher.playerName ?? `Pitcher ${pitcher.playerId}`}</span>
        {pitcherHand && (
          <span className="text-gray-600 ml-1.5 text-xs">{pitchingTeamAbbr} {pitcherHand}</span>
        )}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-600 border-b border-gray-800">
              <th className="text-left pb-1 pr-3 font-normal">Batter</th>
              <th className="text-center pb-1 px-1.5 font-normal">PA</th>
              <th className="text-center pb-1 px-1.5 font-normal">AB</th>
              <th className="text-center pb-1 px-1.5 font-normal">H</th>
              <th className="text-center pb-1 px-1.5 font-normal">HR</th>
              <th className="text-center pb-1 px-1.5 font-normal">RBI</th>
              <th className="text-center pb-1 px-1.5 font-normal">BB</th>
              <th className="text-center pb-1 px-1.5 font-normal">K</th>
              <th className="text-center pb-1 px-1.5 font-normal">AVG</th>
              <th className="text-center pb-1 px-1.5 font-normal">OBP</th>
              <th className="text-center pb-1 px-1.5 font-normal">SLG</th>
              <th className="text-center pb-1 px-1.5 font-normal">OPS</th>
              <th className="text-center pb-1 pl-2 font-normal">Last</th>
            </tr>
          </thead>
          <tbody>
            {lineup.map((spot) => {
              const slot = Math.floor((spot.batter.battingOrder ?? 0) / 100);
              const hasHistory = spot.bvp != null && spot.bvp.pa > 0;
              const bvp = spot.bvp;
              const batterHand = batterHandLabel(spot.batter.handCode);
              return (
                <tr
                  key={spot.batter.playerId}
                  className={`border-b border-gray-900 ${
                    hasHistory && (bvp?.hr ?? 0) > 0 ? 'bg-yellow-950/10' : ''
                  }`}
                >
                  <td className="py-1 pr-3 whitespace-nowrap">
                    <span className="text-gray-600 mr-1 text-xs">{slot || ''}</span>
                    <span className={hasHistory ? 'text-gray-200' : 'text-gray-500'}>
                      {spot.batter.playerName ?? `Batter ${spot.batter.playerId}`}
                    </span>
                    {batterHand && (
                      <span className="text-gray-600 ml-1 text-xs">{batterHand}</span>
                    )}
                    {spot.batter.position && (
                      <span className="text-gray-700 ml-1 text-xs">{spot.batter.position}</span>
                    )}
                  </td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(bvp?.pa)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(bvp?.ab)}</td>
                  <td
                    className={`text-center py-1 px-1.5 tabular-nums font-semibold ${
                      hasHistory && (bvp?.h ?? 0) > 0 ? 'text-gray-100' : 'text-gray-500'
                    }`}
                  >
                    {fmt(bvp?.h)}
                  </td>
                  <td
                    className={`text-center py-1 px-1.5 tabular-nums ${
                      hasHistory && (bvp?.hr ?? 0) > 0 ? 'text-yellow-400 font-semibold' : ''
                    }`}
                  >
                    {fmt(bvp?.hr)}
                  </td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(bvp?.rbi)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(bvp?.bb)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(bvp?.k)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">
                    {fmtAvg(bvp?.avg ?? null)}
                  </td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">
                    {fmtAvg(bvp?.obp ?? null)}
                  </td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">
                    {fmtAvg(bvp?.slg ?? null)}
                  </td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">
                    {fmtAvg(bvp?.ops ?? null)}
                  </td>
                  <td className="text-center py-1 pl-2 tabular-nums text-gray-600 text-xs">
                    {formatDate(bvp?.lastFacedDate ?? null)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function MlbVsView({ game }: { game: MlbGame }) {
  const [data, setData] = useState<BvpResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    setData(null);
    fetch(`/api/mlb-bvp?gamePk=${game.gameId}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((json: BvpResponse) => setData(json))
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [game.gameId]);

  const isFinal = game.gameStatus === 'F' || game.gameStatus === 'Final';

  return (
    <div className="py-4">
      {/* Score header — same layout as the Game view for continuity */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <div className="flex items-center gap-3">
            <span className="text-lg font-semibold text-gray-100">{game.awayTeamAbbr}</span>
            {isFinal && game.awayScore != null && (
              <span
                className={`text-2xl font-bold tabular-nums ${
                  game.awayScore > (game.homeScore ?? 0) ? 'text-gray-100' : 'text-gray-500'
                }`}
              >
                {game.awayScore}
              </span>
            )}
          </div>
          {game.awayPitcher && (
            <div className="text-xs text-gray-500 mt-0.5">{game.awayPitcher}</div>
          )}
        </div>
        <div className="text-xs text-gray-500 pt-2">{isFinal ? 'Final' : game.gameStatus ?? ''}</div>
        <div className="text-right">
          <div className="flex items-center gap-3 justify-end">
            {isFinal && game.homeScore != null && (
              <span
                className={`text-2xl font-bold tabular-nums ${
                  game.homeScore > (game.awayScore ?? 0) ? 'text-gray-100' : 'text-gray-500'
                }`}
              >
                {game.homeScore}
              </span>
            )}
            <span className="text-lg font-semibold text-gray-100">{game.homeTeamAbbr}</span>
          </div>
          {game.homePitcher && (
            <div className="text-xs text-gray-500 mt-0.5 text-right">{game.homePitcher}</div>
          )}
        </div>
      </div>

      {loading && <div className="text-sm text-gray-500">Loading...</div>}
      {error && <div className="text-sm text-red-400">Error: {error}</div>}

      {!loading && !error && data && !data.available && (
        <div className="text-sm text-gray-500">
          Career matchup data is not available for this game. This usually means the game has not
          gone Final yet, or starting pitchers have not been recorded in mlb.pitching_stats.
        </div>
      )}

      {!loading && !error && data && data.available && (
        <>
          {data.awayLineup.length === 0 && data.homeLineup.length === 0 && (
            <div className="text-sm text-gray-500">
              No starting lineup data available for this game.
            </div>
          )}

          {data.awayLineup.length > 0 && data.homeSP && (
            <LineupTable
              title="Away Lineup"
              lineup={data.awayLineup}
              pitcher={data.homeSP}
              battingTeamAbbr={game.awayTeamAbbr}
              pitchingTeamAbbr={game.homeTeamAbbr}
            />
          )}

          {data.homeLineup.length > 0 && data.awaySP && (
            <LineupTable
              title="Home Lineup"
              lineup={data.homeLineup}
              pitcher={data.awaySP}
              battingTeamAbbr={game.homeTeamAbbr}
              pitchingTeamAbbr={game.awayTeamAbbr}
            />
          )}

          <div className="text-xs text-gray-600 mt-4 pt-3 border-t border-gray-900">
            Career stats from play-by-play data loaded into the database. A hitter with zero PA
            against this pitcher may still have historical matchups that are not yet loaded — the
            play-by-play backfill is partial.
          </div>
        </>
      )}
    </div>
  );
}
