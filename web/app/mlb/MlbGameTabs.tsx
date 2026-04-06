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

interface InningLine {
  inning: number;
  isTop: boolean;
  runs: number;
}

interface Summary {
  runs: number;
  hits: number;
}

interface AtBat {
  atBatNumber: number;
  inning: number;
  isTop: boolean;
  batterId: number;
  batterName: string;
  pitcherId: number;
  pitcherName: string;
  resultType: string | null;
  resultDesc: string | null;
  rbi: number | null;
  exitVelo: number | null;
  launchAngle: number | null;
  distance: number | null;
  trajectory: string | null;
  hardness: string | null;
  hitProb: number | null;
  batSpeed: number | null;
  hrBallparks: number | null;
  awayTeamId: number;
  homeTeamId: number;
}

type TabKey = 'boxscore' | 'exitvelo';

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

function resultColor(resultType: string | null): string {
  if (!resultType) return 'text-gray-400';
  const t = resultType.toLowerCase();
  if (t.includes('home_run')) return 'text-yellow-400';
  if (t.includes('hit') || t === 'single' || t === 'double' || t === 'triple') return 'text-green-400';
  if (t.includes('strikeout')) return 'text-red-400';
  return 'text-gray-400';
}

function resultLabel(resultType: string | null): string {
  if (!resultType) return '-';
  return resultType
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function veloColor(velo: number | null): string {
  if (velo == null) return 'text-gray-400';
  if (velo >= 100) return 'text-red-400';
  if (velo >= 95) return 'text-orange-400';
  if (velo >= 90) return 'text-yellow-400';
  return 'text-gray-300';
}

// ---------------------------------------------------------------------------
// Linescore
// ---------------------------------------------------------------------------

function Linescore({
  innings,
  summary,
  awayAbbr,
  homeAbbr,
  awayScore,
  homeScore,
}: {
  innings: InningLine[];
  summary: Record<string, Summary>;
  awayAbbr: string;
  homeAbbr: string;
  awayScore: number | null;
  homeScore: number | null;
}) {
  const maxInning = Math.max(...innings.map((i) => i.inning), 9);
  const inningNums = Array.from({ length: maxInning }, (_, i) => i + 1);

  function getScore(isTop: boolean, inning: number): string {
    const row = innings.find((i) => i.inning === inning && i.isTop === isTop);
    return row != null ? String(row.runs) : '-';
  }

  const awayR = summary['A']?.runs ?? awayScore ?? 0;
  const homeR = summary['H']?.runs ?? homeScore ?? 0;
  const awayH = summary['A']?.hits ?? null;
  const homeH = summary['H']?.hits ?? null;

  return (
    <div className="overflow-x-auto mb-5">
      <table className="text-xs text-center text-gray-300">
        <thead>
          <tr className="text-gray-500 border-b border-gray-800">
            <th className="text-left pr-4 pb-1 font-normal w-12"></th>
            {inningNums.map((n) => (
              <th key={n} className="w-7 pb-1 font-normal">{n}</th>
            ))}
            <th className="pl-3 pb-1 font-semibold">R</th>
            <th className="pl-2 pb-1 font-normal">H</th>
          </tr>
        </thead>
        <tbody>
          <tr className="border-b border-gray-900">
            <td className="text-left pr-4 py-1.5 font-semibold text-gray-200">{awayAbbr}</td>
            {inningNums.map((n) => (
              <td key={n} className="py-1.5">{getScore(true, n)}</td>
            ))}
            <td className={`pl-3 py-1.5 font-bold ${awayR > homeR ? 'text-gray-100' : 'text-gray-500'}`}>{awayR}</td>
            <td className="pl-2 py-1.5 text-gray-400">{awayH != null ? awayH : '-'}</td>
          </tr>
          <tr>
            <td className="text-left pr-4 py-1.5 font-semibold text-gray-200">{homeAbbr}</td>
            {inningNums.map((n) => (
              <td key={n} className="py-1.5">{getScore(false, n)}</td>
            ))}
            <td className={`pl-3 py-1.5 font-bold ${homeR > awayR ? 'text-gray-100' : 'text-gray-500'}`}>{homeR}</td>
            <td className="pl-2 py-1.5 text-gray-400">{homeH != null ? homeH : '-'}</td>
          </tr>
        </tbody>
      </table>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Batter table
// ---------------------------------------------------------------------------

function BatterTable({ batters, teamAbbr }: { batters: Batter[]; teamAbbr: string }) {
  if (batters.length === 0) return null;
  return (
    <div className="mb-5">
      <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">{teamAbbr} Batting</div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-600 border-b border-gray-800">
              <th className="text-left pb-1 pr-3 font-normal">Batter</th>
              <th className="text-center pb-1 px-1.5 font-normal">AB</th>
              <th className="text-center pb-1 px-1.5 font-normal">R</th>
              <th className="text-center pb-1 px-1.5 font-normal">H</th>
              <th className="text-center pb-1 px-1.5 font-normal">2B</th>
              <th className="text-center pb-1 px-1.5 font-normal">3B</th>
              <th className="text-center pb-1 px-1.5 font-normal">HR</th>
              <th className="text-center pb-1 px-1.5 font-normal">RBI</th>
              <th className="text-center pb-1 px-1.5 font-normal">BB</th>
              <th className="text-center pb-1 px-1.5 font-normal">K</th>
              <th className="text-center pb-1 px-1.5 font-normal">SB</th>
              <th className="text-center pb-1 px-1.5 font-normal">AVG</th>
              <th className="text-center pb-1 px-1.5 font-normal">OBP</th>
              <th className="text-center pb-1 px-1.5 font-normal">SLG</th>
              <th className="text-center pb-1 px-1.5 font-normal">OPS</th>
            </tr>
          </thead>
          <tbody>
            {batters.map((b, idx) => {
              const isSubstitute = idx > 0 && b.battingOrder !== null &&
                batters[idx - 1].battingOrder !== null &&
                Math.floor((b.battingOrder ?? 0) / 100) === Math.floor((batters[idx - 1].battingOrder ?? 0) / 100) &&
                b.battingOrder !== batters[idx - 1].battingOrder;
              return (
                <tr key={b.playerId} className={`border-b border-gray-900 ${
                  (b.h ?? 0) > 0 ? 'bg-green-950/10' : ''
                }`}>
                  <td className="py-1 pr-3 whitespace-nowrap">
                    {isSubstitute && <span className="text-gray-700 mr-1">+</span>}
                    <span className="text-gray-600 mr-1 text-xs">{b.battingOrder != null ? Math.floor(b.battingOrder / 100) : ''}</span>
                    <span className={isSubstitute ? 'text-gray-500' : 'text-gray-200'}>{b.playerName}</span>
                    {b.position && <span className="text-gray-600 ml-1">{b.position}</span>}
                  </td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.ab)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.r)}</td>
                  <td className={`text-center py-1 px-1.5 tabular-nums font-semibold ${
                    (b.h ?? 0) > 0 ? 'text-gray-100' : 'text-gray-500'
                  }`}>{fmt(b.h)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.doubles)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.triples)}</td>
                  <td className={`text-center py-1 px-1.5 tabular-nums ${
                    (b.hr ?? 0) > 0 ? 'text-yellow-400 font-semibold' : ''
                  }`}>{fmt(b.hr)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.rbi)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.bb)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.k)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums">{fmt(b.sb)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">{fmtAvg(b.avg)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">{fmtAvg(b.obp)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">{fmtAvg(b.slg)}</td>
                  <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">{fmtAvg(b.ops)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Pitcher table
// ---------------------------------------------------------------------------

function PitcherTable({ pitchers, teamAbbr }: { pitchers: Pitcher[]; teamAbbr: string }) {
  if (pitchers.length === 0) return null;
  return (
    <div className="mb-5">
      <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">{teamAbbr} Pitching</div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-600 border-b border-gray-800">
              <th className="text-left pb-1 pr-3 font-normal">Pitcher</th>
              <th className="text-center pb-1 px-1.5 font-normal">IP</th>
              <th className="text-center pb-1 px-1.5 font-normal">H</th>
              <th className="text-center pb-1 px-1.5 font-normal">R</th>
              <th className="text-center pb-1 px-1.5 font-normal">ER</th>
              <th className="text-center pb-1 px-1.5 font-normal">BB</th>
              <th className="text-center pb-1 px-1.5 font-normal">K</th>
              <th className="text-center pb-1 px-1.5 font-normal">HR</th>
              <th className="text-center pb-1 px-1.5 font-normal">ERA</th>
              <th className="text-center pb-1 px-1.5 font-normal">P-S</th>
            </tr>
          </thead>
          <tbody>
            {pitchers.map((p) => (
              <tr key={p.playerId} className="border-b border-gray-900">
                <td className="py-1 pr-3 whitespace-nowrap">
                  <span className={p.note === 'SP' ? 'text-gray-200' : 'text-gray-400'}>{p.playerName}</span>
                  {p.note === 'SP' && <span className="text-gray-600 ml-1 text-xs">SP</span>}
                </td>
                <td className="text-center py-1 px-1.5 tabular-nums">{fmtIp(p.ip)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums">{fmt(p.h)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums">{fmt(p.r)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums">{fmt(p.er)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums">{fmt(p.bb)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums">{fmt(p.k)}</td>
                <td className={`text-center py-1 px-1.5 tabular-nums ${
                  (p.hr ?? 0) > 0 ? 'text-yellow-400' : ''
                }`}>{fmt(p.hr)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">{fmt(p.era, 2)}</td>
                <td className="text-center py-1 px-1.5 tabular-nums text-gray-500">
                  {p.pitches != null ? `${p.pitches}-${p.strikes ?? '-'}` : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Exit velo table
// ---------------------------------------------------------------------------

function ExitVeloTable({
  atBats,
  teamId,
  teamAbbr,
  awayTeamId,
}: {
  atBats: AtBat[];
  teamId: number;
  teamAbbr: string;
  awayTeamId: number;
}) {
  // Batters from this team are in the opposing half-inning
  // isTop=true means away team is batting; isTop=false means home team batting
  const isAway = teamId === awayTeamId;
  const teamAtBats = atBats.filter((ab) =>
    isAway ? ab.isTop : !ab.isTop
  );

  // Only show plate appearances with ball-in-play data
  const withData = teamAtBats.filter(
    (ab) => ab.exitVelo != null || ab.resultType != null
  );

  if (withData.length === 0) return null;

  return (
    <div className="mb-5">
      <div className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-1.5">
        {teamAbbr} At-Bats
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="text-gray-600 border-b border-gray-800">
              <th className="text-left pb-1 pr-3 font-normal">Batter</th>
              <th className="text-left pb-1 pr-3 font-normal">Pitcher</th>
              <th className="text-center pb-1 px-1.5 font-normal">Inn</th>
              <th className="text-left pb-1 px-1.5 font-normal">Result</th>
              <th className="text-center pb-1 px-1.5 font-normal">EV</th>
              <th className="text-center pb-1 px-1.5 font-normal">LA</th>
              <th className="text-center pb-1 px-1.5 font-normal">Dist</th>
              <th className="text-center pb-1 px-1.5 font-normal">xBA</th>
            </tr>
          </thead>
          <tbody>
            {withData.map((ab) => (
              <tr key={ab.atBatNumber} className="border-b border-gray-900">
                <td className="py-1 pr-3 whitespace-nowrap text-gray-200">{ab.batterName}</td>
                <td className="py-1 pr-3 whitespace-nowrap text-gray-400">{ab.pitcherName}</td>
                <td className="text-center py-1 px-1.5 tabular-nums text-gray-500">{ab.inning}</td>
                <td className={`py-1 px-1.5 whitespace-nowrap ${resultColor(ab.resultType)}`}>
                  {resultLabel(ab.resultType)}
                </td>
                <td className={`text-center py-1 px-1.5 tabular-nums font-semibold ${veloColor(ab.exitVelo)}`}>
                  {ab.exitVelo != null ? ab.exitVelo.toFixed(1) : '-'}
                </td>
                <td className="text-center py-1 px-1.5 tabular-nums">
                  {ab.launchAngle != null ? ab.launchAngle : '-'}
                </td>
                <td className="text-center py-1 px-1.5 tabular-nums">
                  {ab.distance != null ? ab.distance : '-'}
                </td>
                <td className="text-center py-1 px-1.5 tabular-nums text-gray-400">
                  {ab.hitProb != null ? ab.hitProb.toFixed(3).replace(/^0/, '') : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function MlbGameTabs({ game }: { game: MlbGame }) {
  const [activeTab, setActiveTab] = useState<TabKey>('boxscore');
  const [batters, setBatters] = useState<Batter[]>([]);
  const [pitchers, setPitchers] = useState<Pitcher[]>([]);
  const [innings, setInnings] = useState<InningLine[]>([]);
  const [summary, setSummary] = useState<Record<string, Summary>>({});
  const [hasPbp, setHasPbp] = useState(false);
  const [atBats, setAtBats] = useState<AtBat[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);

    Promise.all([
      fetch(`/api/mlb-boxscore?gamePk=${game.gameId}`).then((r) => r.json()),
      fetch(`/api/mlb-linescore?gamePk=${game.gameId}`).then((r) => r.json()),
      fetch(`/api/mlb-atbats?gamePk=${game.gameId}`).then((r) => r.json()),
    ])
      .then(([boxData, lineData, atBatData]) => {
        setBatters(boxData.batters ?? []);
        setPitchers(boxData.pitchers ?? []);
        setInnings(lineData.innings ?? []);
        setSummary(lineData.summary ?? {});
        setHasPbp(lineData.hasPbp ?? false);
        setAtBats(atBatData.atBats ?? []);
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false));
  }, [game.gameId]);

  const isFinal = game.gameStatus === 'F' || game.gameStatus === 'Final';

  const awayBatters = batters.filter((b) => b.side === 'A');
  const homeBatters = batters.filter((b) => b.side === 'H');
  const awayPitchers = pitchers.filter((p) => p.side === 'A');
  const homePitchers = pitchers.filter((p) => p.side === 'H');

  const tabs: { key: TabKey; label: string }[] = [
    { key: 'boxscore', label: 'Box Score' },
    { key: 'exitvelo', label: 'Exit Velo' },
  ];

  return (
    <div className="py-4">
      {/* Score header */}
      <div className="flex items-start justify-between mb-4">
        <div>
          <div className="flex items-center gap-3">
            <span className="text-lg font-semibold text-gray-100">{game.awayTeamAbbr}</span>
            {isFinal && game.awayScore != null && (
              <span className={`text-2xl font-bold tabular-nums ${
                game.awayScore > (game.homeScore ?? 0) ? 'text-gray-100' : 'text-gray-500'
              }`}>{game.awayScore}</span>
            )}
          </div>
          {game.awayPitcher && (
            <div className="text-xs text-gray-500 mt-0.5">{game.awayPitcher}</div>
          )}
        </div>
        <div className="text-xs text-gray-500 pt-2">{isFinal ? 'Final' : (game.gameStatus ?? '')}</div>
        <div className="text-right">
          <div className="flex items-center gap-3 justify-end">
            {isFinal && game.homeScore != null && (
              <span className={`text-2xl font-bold tabular-nums ${
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

      {loading && <div className="text-sm text-gray-500">Loading...</div>}
      {error && <div className="text-sm text-red-400">Error: {error}</div>}

      {!loading && !error && (
        <>
          {/* Linescore */}
          {hasPbp && innings.length > 0 && (
            <Linescore
              innings={innings}
              summary={summary}
              awayAbbr={game.awayTeamAbbr}
              homeAbbr={game.homeTeamAbbr}
              awayScore={game.awayScore}
              homeScore={game.homeScore}
            />
          )}

          {batters.length === 0 && (
            <div className="text-sm text-gray-500">Box score not yet available for this game.</div>
          )}

          {batters.length > 0 && (
            <>
              {/* Tabs */}
              <div className="flex gap-1 mb-4 border-b border-gray-800">
                {tabs.map((t) => (
                  <button
                    key={t.key}
                    onClick={() => setActiveTab(t.key)}
                    className={[
                      'px-4 py-2 text-sm font-medium transition-colors',
                      activeTab === t.key
                        ? 'text-gray-100 border-b-2 border-blue-500 -mb-px'
                        : 'text-gray-500 hover:text-gray-300',
                    ].join(' ')}
                  >
                    {t.label}
                  </button>
                ))}
              </div>

              {activeTab === 'boxscore' && (
                <>
                  <BatterTable batters={awayBatters} teamAbbr={game.awayTeamAbbr} />
                  <BatterTable batters={homeBatters} teamAbbr={game.homeTeamAbbr} />
                  <PitcherTable pitchers={awayPitchers} teamAbbr={game.awayTeamAbbr} />
                  <PitcherTable pitchers={homePitchers} teamAbbr={game.homeTeamAbbr} />
                </>
              )}

              {activeTab === 'exitvelo' && (
                atBats.length === 0 ? (
                  <div className="text-sm text-gray-500">
                    Exit velocity data not yet available for this game. Run the play-by-play ETL to load it.
                  </div>
                ) : (
                  <>
                    <ExitVeloTable
                      atBats={atBats}
                      teamId={game.awayTeamId}
                      teamAbbr={game.awayTeamAbbr}
                      awayTeamId={game.awayTeamId}
                    />
                    <ExitVeloTable
                      atBats={atBats}
                      teamId={game.homeTeamId}
                      teamAbbr={game.homeTeamAbbr}
                      awayTeamId={game.awayTeamId}
                    />
                  </>
                )
              )}
            </>
          )}
        </>
      )}
    </div>
  );
}
