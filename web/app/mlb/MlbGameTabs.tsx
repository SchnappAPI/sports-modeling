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

interface Batter {
  playerId: number;
  playerName: string;
  teamId: number;
  side: string;
  position: string | null;
  battingOrder: number | null;
  ab: number | null;
  r: number | null;
  h: number | null;
  doubles: number | null;
  triples: number | null;
  hr: number | null;
  rbi: number | null;
  bb: number | null;
  k: number | null;
  sb: number | null;
  tb: number | null;
  avg: number | null;
  obp: number | null;
  slg: number | null;
  ops: number | null;
}

interface Pitcher {
  playerId: number;
  playerName: string;
  teamId: number;
  side: string;
  note: string | null;
  ip: number | null;
  h: number | null;
  r: number | null;
  er: number | null;
  bb: number | null;
  k: number | null;
  hr: number | null;
  era: number | null;
  pitches: number | null;
  strikes: number | null;
}

function fmt(val: number | null, decimals = 0): string {
  if (val == null) return '-';
  return decimals > 0 ? val.toFixed(decimals) : String(val);
}

function fmtAvg(val: number | null): string {
  if (val == null) return '-';
  return val.toFixed(3).replace(/^0/, '');
}

function fmtIp(val: number | null): string {
  if (val == null) return '-';
  const whole = Math.floor(val);
  const frac = val - whole;
  const outs = Math.round(frac * 3);
  return outs === 0 ? `${whole}.0` : `${whole}.${outs}`;
}

function BatterTable({ batters, teamAbbr }: { batters: Batter[]; teamAbbr: string }) {
  return (
    <div className="mb-6">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{teamAbbr} Batting</div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-500 border-b border-gray-800">
              <th className="text-left pb-1 pr-3 font-normal">Player</th>
              <th className="text-center pb-1 px-1 font-normal">AB</th>
              <th className="text-center pb-1 px-1 font-normal">R</th>
              <th className="text-center pb-1 px-1 font-normal">H</th>
              <th className="text-center pb-1 px-1 font-normal">2B</th>
              <th className="text-center pb-1 px-1 font-normal">3B</th>
              <th className="text-center pb-1 px-1 font-normal">HR</th>
              <th className="text-center pb-1 px-1 font-normal">RBI</th>
              <th className="text-center pb-1 px-1 font-normal">BB</th>
              <th className="text-center pb-1 px-1 font-normal">K</th>
              <th className="text-center pb-1 px-1 font-normal">SB</th>
              <th className="text-center pb-1 px-1 font-normal">AVG</th>
              <th className="text-center pb-1 px-1 font-normal">OBP</th>
              <th className="text-center pb-1 px-1 font-normal">SLG</th>
              <th className="text-center pb-1 px-1 font-normal">OPS</th>
            </tr>
          </thead>
          <tbody>
            {batters.map((b) => (
              <tr key={b.playerId} className="border-b border-gray-900 hover:bg-gray-900">
                <td className="py-1 pr-3 whitespace-nowrap">
                  <span className="text-gray-500 mr-1">{b.battingOrder ?? ''}</span>
                  {b.playerName}
                  {b.position && <span className="text-gray-600 ml-1 text-xs">{b.position}</span>}
                </td>
                <td className="text-center py-1 px-1">{fmt(b.ab)}</td>
                <td className="text-center py-1 px-1">{fmt(b.r)}</td>
                <td className="text-center py-1 px-1">{fmt(b.h)}</td>
                <td className="text-center py-1 px-1">{fmt(b.doubles)}</td>
                <td className="text-center py-1 px-1">{fmt(b.triples)}</td>
                <td className="text-center py-1 px-1">{fmt(b.hr)}</td>
                <td className="text-center py-1 px-1">{fmt(b.rbi)}</td>
                <td className="text-center py-1 px-1">{fmt(b.bb)}</td>
                <td className="text-center py-1 px-1">{fmt(b.k)}</td>
                <td className="text-center py-1 px-1">{fmt(b.sb)}</td>
                <td className="text-center py-1 px-1">{fmtAvg(b.avg)}</td>
                <td className="text-center py-1 px-1">{fmtAvg(b.obp)}</td>
                <td className="text-center py-1 px-1">{fmtAvg(b.slg)}</td>
                <td className="text-center py-1 px-1">{fmtAvg(b.ops)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PitcherTable({ pitchers, teamAbbr }: { pitchers: Pitcher[]; teamAbbr: string }) {
  return (
    <div className="mb-6">
      <div className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-2">{teamAbbr} Pitching</div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-500 border-b border-gray-800">
              <th className="text-left pb-1 pr-3 font-normal">Pitcher</th>
              <th className="text-center pb-1 px-1 font-normal">IP</th>
              <th className="text-center pb-1 px-1 font-normal">H</th>
              <th className="text-center pb-1 px-1 font-normal">R</th>
              <th className="text-center pb-1 px-1 font-normal">ER</th>
              <th className="text-center pb-1 px-1 font-normal">BB</th>
              <th className="text-center pb-1 px-1 font-normal">K</th>
              <th className="text-center pb-1 px-1 font-normal">HR</th>
              <th className="text-center pb-1 px-1 font-normal">ERA</th>
              <th className="text-center pb-1 px-1 font-normal">P</th>
              <th className="text-center pb-1 px-1 font-normal">S</th>
            </tr>
          </thead>
          <tbody>
            {pitchers.map((p) => (
              <tr key={p.playerId} className="border-b border-gray-900 hover:bg-gray-900">
                <td className="py-1 pr-3 whitespace-nowrap">
                  {p.playerName}
                  {p.note === 'SP' && <span className="text-gray-600 ml-1 text-xs">SP</span>}
                </td>
                <td className="text-center py-1 px-1">{fmtIp(p.ip)}</td>
                <td className="text-center py-1 px-1">{fmt(p.h)}</td>
                <td className="text-center py-1 px-1">{fmt(p.r)}</td>
                <td className="text-center py-1 px-1">{fmt(p.er)}</td>
                <td className="text-center py-1 px-1">{fmt(p.bb)}</td>
                <td className="text-center py-1 px-1">{fmt(p.k)}</td>
                <td className="text-center py-1 px-1">{fmt(p.hr)}</td>
                <td className="text-center py-1 px-1">{fmt(p.era, 2)}</td>
                <td className="text-center py-1 px-1">{fmt(p.pitches)}</td>
                <td className="text-center py-1 px-1">{fmt(p.strikes)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function MlbGameTabs({ game }: { game: MlbGame }) {
  const [batters, setBatters] = useState<Batter[]>([]);
  const [pitchers, setPitchers] = useState<Pitcher[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/mlb-boxscore?gamePk=${game.gameId}`)
      .then((r) => r.json())
      .then((data) => {
        setBatters(data.batters ?? []);
        setPitchers(data.pitchers ?? []);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [game.gameId]);

  const isFinal = game.gameStatus === 'F' || game.gameStatus === 'Final';

  const awayBatters = batters.filter((b) => b.side === 'A');
  const homeBatters = batters.filter((b) => b.side === 'H');
  const awayPitchers = pitchers.filter((p) => p.side === 'A');
  const homePitchers = pitchers.filter((p) => p.side === 'H');

  return (
    <div className="py-4">
      {/* Score header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <div className="flex items-center gap-3">
            <span className="text-lg font-semibold text-gray-100">{game.awayTeamAbbr}</span>
            {isFinal && game.awayScore != null && (
              <span className={`text-2xl font-bold ${
                game.awayScore > (game.homeScore ?? 0) ? 'text-gray-100' : 'text-gray-500'
              }`}>{game.awayScore}</span>
            )}
          </div>
          {game.awayPitcher && (
            <div className="text-xs text-gray-500 mt-0.5">{game.awayPitcher}</div>
          )}
        </div>
        <div className="text-xs text-gray-500">{isFinal ? 'Final' : (game.gameStatus ?? '')}</div>
        <div className="text-right">
          <div className="flex items-center gap-3 justify-end">
            {isFinal && game.homeScore != null && (
              <span className={`text-2xl font-bold ${
                game.homeScore > (game.awayScore ?? 0) ? 'text-gray-100' : 'text-gray-500'
              }`}>{game.homeScore}</span>
            )}
            <span className="text-lg font-semibold text-gray-100">{game.homeTeamAbbr}</span>
          </div>
          {game.homePitcher && (
            <div className="text-xs text-gray-500 mt-0.5 text-right">{game.homePitcher}</div>
          )}
        </div>
      </div>

      {loading && <div className="text-sm text-gray-500">Loading box score...</div>}
      {error && <div className="text-sm text-red-400">Error: {error}</div>}

      {!loading && !error && batters.length === 0 && (
        <div className="text-sm text-gray-500">Box score not yet available for this game.</div>
      )}

      {!loading && !error && batters.length > 0 && (
        <>
          <BatterTable batters={awayBatters} teamAbbr={game.awayTeamAbbr} />
          <BatterTable batters={homeBatters} teamAbbr={game.homeTeamAbbr} />
          {(awayPitchers.length > 0 || homePitchers.length > 0) && (
            <>
              <PitcherTable pitchers={awayPitchers} teamAbbr={game.awayTeamAbbr} />
              <PitcherTable pitchers={homePitchers} teamAbbr={game.homeTeamAbbr} />
            </>
          )}
        </>
      )}
    </div>
  );
}
