import { NextResponse } from 'next/server';

const ODDS_API_KEY = process.env.ODDS_API_KEY ?? '';
const BOOKMAKER   = 'fanduel';
const SPORT       = 'basketball_nba';

// Player prop markets we grade — matches PLAYER_MARKETS in grade_props.py
const MARKETS = [
  'player_points', 'player_rebounds', 'player_assists', 'player_threes',
  'player_blocks', 'player_steals',
  'player_points_rebounds_assists', 'player_points_rebounds',
  'player_points_assists', 'player_rebounds_assists',
  'player_points_alternate', 'player_rebounds_alternate',
  'player_assists_alternate', 'player_threes_alternate',
  'player_blocks_alternate', 'player_steals_alternate',
  'player_points_assists_alternate', 'player_points_rebounds_alternate',
  'player_rebounds_assists_alternate', 'player_points_rebounds_assists_alternate',
].join(',');

/**
 * GET /api/live-props
 *
 * Fetches current FanDuel NBA player prop odds from The Odds API.
 * Returns a flat lookup object keyed by:
 *   "<eventId>|<playerName>|<marketKey>|<lineValue>|<outcomeName>"
 * with the current price as the value.
 *
 * The /v4/sports/{sport}/odds endpoint returns both live and upcoming events
 * in a single call. We return all of them so the UI can decide which to show
 * based on game status.
 *
 * Also returns liveEventIds: Set of event IDs where commence_time < now
 * (i.e. game has started).
 */
export async function GET() {
  if (!ODDS_API_KEY) {
    return NextResponse.json({ error: 'ODDS_API_KEY not configured' }, { status: 500 });
  }

  try {
    const url = new URL(`https://api.the-odds-api.com/v4/sports/${SPORT}/odds`);
    url.searchParams.set('apiKey', ODDS_API_KEY);
    url.searchParams.set('regions', 'us');
    url.searchParams.set('markets', MARKETS);
    url.searchParams.set('bookmakers', BOOKMAKER);
    url.searchParams.set('oddsFormat', 'american');

    const resp = await fetch(url.toString(), { next: { revalidate: 0 } });
    if (!resp.ok) {
      const text = await resp.text();
      return NextResponse.json({ error: `Odds API error ${resp.status}: ${text}` }, { status: 502 });
    }

    const events: OddsEvent[] = await resp.json();
    const now = Date.now();

    // Build flat lookup: key -> american odds price
    const prices: Record<string, number> = {};
    // Event IDs where the game has already started (commence_time < now)
    const liveEventIds: string[] = [];

    for (const event of events) {
      const commenceMs = new Date(event.commence_time).getTime();
      if (commenceMs < now) liveEventIds.push(event.id);

      for (const bookmaker of event.bookmakers ?? []) {
        if (bookmaker.key !== BOOKMAKER) continue;
        for (const market of bookmaker.markets ?? []) {
          for (const outcome of market.outcomes ?? []) {
            const key = `${event.id}|${outcome.description ?? outcome.name}|${market.key}|${outcome.point}|${outcome.name}`;
            prices[key] = outcome.price;
          }
        }
      }
    }

    return NextResponse.json({ prices, liveEventIds });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

// ---------------------------------------------------------------------------
// Odds API response types (minimal)
// ---------------------------------------------------------------------------

interface OddsEvent {
  id: string;
  sport_key: string;
  commence_time: string;
  home_team: string;
  away_team: string;
  bookmakers?: OddsBookmaker[];
}

interface OddsBookmaker {
  key: string;
  markets?: OddsMarket[];
}

interface OddsMarket {
  key: string;
  outcomes?: OddsOutcome[];
}

interface OddsOutcome {
  name: string;         // 'Over' | 'Under'
  description?: string; // player name for prop markets
  price: number;        // american odds
  point?: number;       // line value
}
