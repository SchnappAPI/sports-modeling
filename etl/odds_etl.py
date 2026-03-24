"""
odds_etl.py

Ingests historical odds data from The Odds API v4 into Azure SQL.
Schema: odds (events, game_lines, player_props, market_probe)

Modes:
  probe    -- Coverage discovery pass. Writes only to odds.market_probe. Exits without
              touching events, game_lines, or player_props.
  backfill -- Incremental ingestion of historical event odds. Processes oldest-first
              bounded by --games per run.

Usage examples:
  python etl/odds_etl.py --mode probe --sport nba
  python etl/odds_etl.py --mode backfill --sport all --games 20
  python etl/odds_etl.py --mode backfill --sport nfl --season 2024 --games 10 --quota-floor 100000
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

# Allow `from etl.db import ...` when the script is invoked as
# `python etl/odds_etl.py` from the repo root. In that case Python adds
# etl/ to sys.path but not the repo root, so the etl package is invisible.
_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

import pandas as pd
import requests
from sqlalchemy import text

from etl.db import get_engine, upsert

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.the-odds-api.com"

SPORT_KEYS = {
    "nfl": "americanfootball_nfl",
    "nba": "basketball_nba",
    "mlb": "baseball_mlb",
}

# Season month ranges (inclusive). Used to compute default season date ranges.
SEASON_MONTHS = {
    "nfl": (9, 2),   # September through February (wraps year)
    "nba": (10, 6),  # October through June (wraps year)
    "mlb": (3, 11),  # March through November (same year)
}

# Minimum date for additional market (prop) historical data per API docs.
PROPS_CUTOFF = datetime(2023, 5, 3, 5, 30, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Market constants
# ---------------------------------------------------------------------------

NFL_FEATURED = [
    "h2h", "spreads", "totals", "team_totals",
    "h2h_h1", "spreads_h1", "totals_h1",
    "h2h_q1", "spreads_q1", "totals_q1",
    "team_totals_h1",
]
NFL_PROPS = [
    "player_pass_yds", "player_pass_tds", "player_pass_attempts",
    "player_pass_completions", "player_pass_interceptions",
    "player_pass_longest_completion", "player_pass_yds_q1",
    "player_rush_yds", "player_rush_longest",
    "player_reception_yds", "player_receptions", "player_reception_longest",
    "player_pass_rush_yds", "player_rush_reception_yds",
    "player_1st_td", "player_anytime_td", "player_last_td",
]
NFL_ALT_PROPS = [
    "player_pass_yds_alternate", "player_pass_tds_alternate",
    "player_rush_yds_alternate", "player_reception_yds_alternate",
    "player_receptions_alternate", "player_pass_rush_yds_alternate",
    "player_rush_reception_yds_alternate",
]

NBA_FEATURED = [
    "h2h", "spreads", "totals", "team_totals",
    "h2h_h1", "spreads_h1", "totals_h1",
    "h2h_q1", "spreads_q1", "totals_q1",
    "team_totals_h1",
]
NBA_PROPS = [
    "player_points", "player_points_q1",
    "player_rebounds", "player_rebounds_q1",
    "player_assists", "player_assists_q1",
    "player_threes", "player_blocks", "player_steals",
    "player_points_rebounds_assists", "player_points_rebounds",
    "player_points_assists", "player_rebounds_assists",
    "player_first_basket", "player_first_team_basket",
    "player_double_double", "player_triple_double",
    "player_method_of_first_basket",
]
NBA_ALT_PROPS = [
    "player_points_alternate", "player_rebounds_alternate",
    "player_assists_alternate", "player_blocks_alternate",
    "player_steals_alternate", "player_threes_alternate",
    "player_points_assists_alternate", "player_points_rebounds_alternate",
    "player_rebounds_assists_alternate",
    "player_points_rebounds_assists_alternate",
]

MLB_FEATURED = [
    "h2h", "spreads", "totals", "team_totals",
    "h2h_1st_5_innings", "spreads_1st_5_innings", "totals_1st_5_innings",
    "totals_1st_1_innings",
]
MLB_PROPS = [
    "batter_home_runs", "batter_first_home_run",
    "batter_hits", "batter_total_bases", "batter_rbis",
    "batter_runs_scored", "batter_hits_runs_rbis",
    "batter_singles", "batter_doubles", "batter_triples",
    "batter_walks", "batter_strikeouts", "batter_stolen_bases",
    "pitcher_strikeouts", "pitcher_hits_allowed", "pitcher_walks",
    "pitcher_earned_runs", "pitcher_outs",
]
MLB_ALT_PROPS = [
    "batter_total_bases_alternate", "batter_home_runs_alternate",
    "batter_hits_alternate", "batter_rbis_alternate",
    "batter_runs_scored_alternate", "pitcher_strikeouts_alternate",
]

FEATURED_MARKETS = {"nfl": NFL_FEATURED, "nba": NBA_FEATURED, "mlb": MLB_FEATURED}
PROP_MARKETS = {"nfl": NFL_PROPS, "nba": NBA_PROPS, "mlb": MLB_PROPS}
ALT_PROP_MARKETS = {"nfl": NFL_ALT_PROPS, "nba": NBA_ALT_PROPS, "mlb": MLB_ALT_PROPS}

BOOKMAKERS = "fanduel,draftkings,betmgm,williamhill_us"

# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'odds') EXEC('CREATE SCHEMA odds')",

    """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'events'
    )
    CREATE TABLE odds.events (
        event_id        VARCHAR(50)  NOT NULL PRIMARY KEY,
        sport_key       VARCHAR(50)  NOT NULL,
        sport_title     VARCHAR(50)  NULL,
        commence_time   DATETIME2    NOT NULL,
        home_team       VARCHAR(100) NULL,
        away_team       VARCHAR(100) NULL,
        season_year     INT          NULL,
        created_at      DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,

    """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'game_lines'
    )
    CREATE TABLE odds.game_lines (
        event_id            VARCHAR(50)   NOT NULL,
        sport_key           VARCHAR(50)   NOT NULL,
        market_key          VARCHAR(100)  NOT NULL,
        bookmaker_key       VARCHAR(50)   NOT NULL,
        bookmaker_title     VARCHAR(100)  NULL,
        outcome_name        VARCHAR(100)  NOT NULL,
        outcome_price       INT           NULL,
        outcome_point       DECIMAL(6,1)  NULL,
        snapshot_timestamp  DATETIME2     NULL,
        created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
    )
    """,

    """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'player_props'
    )
    CREATE TABLE odds.player_props (
        event_id            VARCHAR(50)   NOT NULL,
        sport_key           VARCHAR(50)   NOT NULL,
        market_key          VARCHAR(100)  NOT NULL,
        bookmaker_key       VARCHAR(50)   NOT NULL,
        bookmaker_title     VARCHAR(100)  NULL,
        player_name         VARCHAR(100)  NOT NULL,
        outcome_name        VARCHAR(20)   NOT NULL,
        outcome_price       INT           NULL,
        outcome_point       DECIMAL(6,1)  NULL,
        snapshot_timestamp  DATETIME2     NULL,
        created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
    )
    """,

    """
    IF NOT EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'market_probe'
    )
    CREATE TABLE odds.market_probe (
        probe_id            INT IDENTITY PRIMARY KEY,
        sport_key           VARCHAR(50)   NOT NULL,
        market_key          VARCHAR(100)  NOT NULL,
        market_type         VARCHAR(20)   NULL,
        bookmaker_count     INT           NULL,
        outcome_count       INT           NULL,
        is_covered          BIT           NULL,
        covered_bookmakers  VARCHAR(200)  NULL,
        sample_event_ids    VARCHAR(500)  NULL,
        sample_dates        VARCHAR(200)  NULL,
        probe_timestamp     DATETIME2     NULL,
        created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
    )
    """,
]


def ensure_schema(engine):
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))


# ---------------------------------------------------------------------------
# DataFrame cleaning
# ---------------------------------------------------------------------------

def clean_dataframe(df):
    df = df.where(pd.notna(df), other=None)
    for col in df.select_dtypes(include=["int64", "float64"]).columns:
        df[col] = df[col].apply(
            lambda x: None if x is None
            else int(x) if isinstance(x, (int, float)) and not isinstance(x, bool) and x == int(x)
            else float(x)
        )
    return df


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

_remaining_credits = None  # updated after each call


def _request(url, params, quota_floor, retries=3):
    """
    Perform a GET request with retry logic.
    Returns (response_json, headers) or raises on unrecoverable failure.
    Skippable failures (non-200 non-retryable) return (None, None).
    """
    global _remaining_credits
    wait_times = [10, 30, 60]
    last_exc = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                wait = wait_times[attempt]
                print(f"    [retry {attempt+1}] Request exception: {exc}. Waiting {wait}s...")
                time.sleep(wait)
            continue

        # Update quota tracking from headers whenever present
        remaining_header = resp.headers.get("x-requests-remaining")
        used_header = resp.headers.get("x-requests-used")
        last_header = resp.headers.get("x-requests-last")
        if remaining_header is not None:
            _remaining_credits = int(remaining_header)
            print(
                f"    [quota] remaining={remaining_header}  used={used_header}  last={last_header}"
            )

        if resp.status_code == 200:
            # Quota floor check after successful call
            if _remaining_credits is not None and _remaining_credits < quota_floor:
                print(
                    f"WARNING: remaining credits ({_remaining_credits}) below quota floor "
                    f"({quota_floor}). Stopping."
                )
                sys.exit(1)
            return resp.json(), resp.headers

        if resp.status_code in (401, 403, 404):
            print(f"    [skip] HTTP {resp.status_code}: {resp.text[:200]}")
            return None, None

        if resp.status_code in (429,) or resp.status_code >= 500:
            wait = wait_times[min(attempt, len(wait_times) - 1)]
            print(
                f"    [retry {attempt+1}] HTTP {resp.status_code}: {resp.text[:200]}. "
                f"Waiting {wait}s..."
            )
            time.sleep(wait)
            continue

        print(f"    [skip] Unexpected HTTP {resp.status_code}: {resp.text[:200]}")
        return None, None

    if last_exc:
        print(f"    [skip] All retries exhausted. Last exception: {last_exc}")
    else:
        print("    [skip] All retries exhausted.")
    return None, None


def _check_quota_before_call(quota_floor):
    if _remaining_credits is not None and _remaining_credits < quota_floor:
        print(
            f"WARNING: remaining credits ({_remaining_credits}) below quota floor "
            f"({quota_floor}) before API call. Stopping."
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Season date range helpers
# ---------------------------------------------------------------------------

def _default_season(sport):
    """Return the most recently completed or active season year for a sport."""
    today = date.today()
    start_month, end_month = SEASON_MONTHS[sport]
    wraps = start_month > end_month  # season crosses calendar year boundary

    if wraps:
        # e.g., NFL: Sep-Feb. Season year is the year in which September falls.
        if today.month >= start_month:
            return today.year
        else:
            return today.year - 1
    else:
        # e.g., MLB: Mar-Nov. Season year is the calendar year.
        if today.month >= start_month:
            return today.year
        else:
            return today.year - 1


def _season_date_range(sport, season_year):
    """Return (start_date, end_date) as date objects for the given sport/season."""
    start_month, end_month = SEASON_MONTHS[sport]
    wraps = start_month > end_month

    start_date = date(season_year, start_month, 1)
    if wraps:
        end_year = season_year + 1
    else:
        end_year = season_year

    # Last day of end_month
    if end_month == 12:
        end_date = date(end_year, 12, 31)
    else:
        end_date = date(end_year, end_month + 1, 1) - timedelta(days=1)

    return start_date, end_date


def _date_range_list(start_date, end_date):
    """Return list of date objects from start_date to end_date inclusive."""
    dates = []
    cur = start_date
    while cur <= end_date:
        dates.append(cur)
        cur += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Event discovery
# ---------------------------------------------------------------------------

def _discover_events_for_date(sport_key, target_date, api_key, quota_floor):
    """
    Call the event discovery endpoint for a single calendar date.
    Returns list of event dicts or empty list.
    date parameter per API: noon UTC on the target date.
    commenceTimeFrom/commenceTimeTo: full day window.
    """
    date_iso = f"{target_date}T12:00:00Z"
    from_iso = f"{target_date}T00:00:00Z"
    to_iso = f"{target_date}T23:59:59Z"

    _check_quota_before_call(quota_floor)

    url = f"{BASE_URL}/v4/historical/sports/{sport_key}/events"
    params = {
        "apiKey": api_key,
        "date": date_iso,
        "commenceTimeFrom": from_iso,
        "commenceTimeTo": to_iso,
    }
    data, _ = _request(url, params, quota_floor)
    if data is None:
        return []
    return data.get("data", []) or []


def _discover_events_for_date_with_fallback(sport_key, target_date, api_key, quota_floor, max_walk=7):
    """
    Discover events for target_date. Walk forward up to max_walk days if none found.
    Returns (events_list, actual_date_used).
    """
    for offset in range(max_walk + 1):
        check_date = target_date + timedelta(days=offset)
        events = _discover_events_for_date(sport_key, check_date, api_key, quota_floor)
        if events:
            if offset > 0:
                print(f"    No events on {target_date}, found {len(events)} events on {check_date}")
            return events, check_date
    print(f"    No events found within {max_walk} days of {target_date}")
    return [], target_date


# ---------------------------------------------------------------------------
# Bulk featured odds
# ---------------------------------------------------------------------------

def _fetch_bulk_featured(sport_key, snapshot_iso, markets_list, api_key, quota_floor):
    """
    Fetch bulk featured odds for all events for a sport at a snapshot timestamp.
    Returns (data_list, snapshot_timestamp_str) or ([], None).
    """
    _check_quota_before_call(quota_floor)
    url = f"{BASE_URL}/v4/historical/sports/{sport_key}/odds"
    params = {
        "apiKey": api_key,
        "bookmakers": BOOKMAKERS,
        "markets": ",".join(markets_list),
        "oddsFormat": "american",
        "date": snapshot_iso,
    }
    data, _ = _request(url, params, quota_floor)
    if data is None:
        return [], None
    return data.get("data", []) or [], data.get("timestamp")


# ---------------------------------------------------------------------------
# Per-event additional market odds
# ---------------------------------------------------------------------------

def _fetch_event_odds(sport_key, event_id, snapshot_iso, markets_list, api_key, quota_floor):
    """
    Fetch per-event additional market odds.
    Returns (event_dict_or_None, snapshot_timestamp_str).
    """
    _check_quota_before_call(quota_floor)
    url = f"{BASE_URL}/v4/historical/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "bookmakers": BOOKMAKERS,
        "markets": ",".join(markets_list),
        "oddsFormat": "american",
        "date": snapshot_iso,
    }
    data, _ = _request(url, params, quota_floor)
    if data is None:
        return None, None
    event_obj = data.get("data")
    return event_obj, data.get("timestamp")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_event_to_row(event, sport_key, season_year):
    """Return a dict suitable for odds.events from an event object."""
    commence_raw = event.get("commence_time")
    try:
        commence_dt = datetime.fromisoformat(commence_raw.replace("Z", "+00:00")) if commence_raw else None
    except Exception:
        commence_dt = None

    return {
        "event_id": event.get("id"),
        "sport_key": sport_key,
        "sport_title": event.get("sport_title"),
        "commence_time": commence_dt,
        "home_team": event.get("home_team"),
        "away_team": event.get("away_team"),
        "season_year": season_year,
    }


def _parse_bookmakers(event_obj, event_id, sport_key, snapshot_timestamp):
    """
    Parse bookmaker/market/outcome data from an event object.
    Returns (game_lines_rows, player_props_rows).
    Outcomes with a description field go to player_props; others go to game_lines.
    """
    game_lines = []
    player_props = []

    snapshot_dt = None
    if snapshot_timestamp:
        try:
            snapshot_dt = datetime.fromisoformat(snapshot_timestamp.replace("Z", "+00:00"))
        except Exception:
            pass

    for bookmaker in event_obj.get("bookmakers", []) or []:
        bk_key = bookmaker.get("key")
        bk_title = bookmaker.get("title")
        for market in bookmaker.get("markets", []) or []:
            mkt_key = market.get("key")
            for outcome in market.get("outcomes", []) or []:
                out_name = outcome.get("name")
                out_price = outcome.get("price")
                out_point = outcome.get("point")
                description = outcome.get("description")

                if description:
                    player_props.append({
                        "event_id": event_id,
                        "sport_key": sport_key,
                        "market_key": mkt_key,
                        "bookmaker_key": bk_key,
                        "bookmaker_title": bk_title,
                        "player_name": description,
                        "outcome_name": out_name,
                        "outcome_price": out_price,
                        "outcome_point": out_point,
                        "snapshot_timestamp": snapshot_dt,
                    })
                else:
                    game_lines.append({
                        "event_id": event_id,
                        "sport_key": sport_key,
                        "market_key": mkt_key,
                        "bookmaker_key": bk_key,
                        "bookmaker_title": bk_title,
                        "outcome_name": out_name,
                        "outcome_price": out_price,
                        "outcome_point": out_point,
                        "snapshot_timestamp": snapshot_dt,
                    })

    return game_lines, player_props


def _snapshot_iso(commence_time_dt):
    """Return ISO string for commence_time minus 30 minutes."""
    if isinstance(commence_time_dt, str):
        try:
            commence_time_dt = datetime.fromisoformat(commence_time_dt.replace("Z", "+00:00"))
        except Exception:
            return None
    if commence_time_dt is None:
        return None
    snap = commence_time_dt - timedelta(minutes=30)
    return snap.strftime("%Y-%m-%dT%H:%M:%SZ")


def _commence_dt(event):
    """Parse commence_time from an event dict to a timezone-aware datetime."""
    raw = event.get("commence_time")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Probe mode
# ---------------------------------------------------------------------------

PROBE_BEST_CASE = {
    "nfl": [
        date(2024, 11, 7),   # ~Week 8
        date(2024, 12, 12),  # ~Week 14
    ],
    "nba": [
        date(2024, 12, 15),  # mid-December
        date(2025, 2, 15),   # mid-February
    ],
    "mlb": [
        date(2024, 6, 15),   # mid-June
        date(2024, 8, 15),   # mid-August
    ],
}
PROBE_WORST_CASE = {
    "nfl": [
        date(2024, 9, 8),    # first week
        date(2025, 2, 2),    # final regular-season week
    ],
    "nba": [
        date(2024, 10, 22),  # first week
        date(2025, 6, 1),    # final week
    ],
    "mlb": [
        date(2024, 3, 20),   # opening week
        date(2024, 9, 28),   # final week
    ],
}


def _probe_select_events(sport, sport_key, api_key, quota_floor):
    """
    For probe mode: select 5 sample events.
    2 best-case, 2 worst-case, 1 wildcard (latest commence_time from any sampled date).
    Returns list of event dicts.
    """
    candidate_dates = PROBE_BEST_CASE[sport] + PROBE_WORST_CASE[sport]
    all_events_by_date = {}

    for target_date in candidate_dates:
        events, actual_date = _discover_events_for_date_with_fallback(
            sport_key, target_date, api_key, quota_floor
        )
        if events:
            all_events_by_date[actual_date] = events

    selected = []
    wildcard_candidate = None

    for i, target_date in enumerate(PROBE_BEST_CASE[sport]):
        actual_date = None
        for d in all_events_by_date:
            if abs((d - target_date).days) <= 7:
                actual_date = d
                break
        if actual_date and all_events_by_date.get(actual_date):
            events = all_events_by_date[actual_date]
            selected.append(events[0])
            for ev in events:
                cdt = _commence_dt(ev)
                if cdt:
                    if wildcard_candidate is None or cdt > _commence_dt(wildcard_candidate):
                        wildcard_candidate = ev

    for i, target_date in enumerate(PROBE_WORST_CASE[sport]):
        actual_date = None
        for d in all_events_by_date:
            if abs((d - target_date).days) <= 7:
                actual_date = d
                break
        if actual_date and all_events_by_date.get(actual_date):
            events = all_events_by_date[actual_date]
            selected.append(events[0])
            for ev in events:
                cdt = _commence_dt(ev)
                if cdt:
                    if wildcard_candidate is None or cdt > _commence_dt(wildcard_candidate):
                        wildcard_candidate = ev

    if wildcard_candidate:
        already_ids = {e.get("id") for e in selected}
        if wildcard_candidate.get("id") not in already_ids:
            selected.append(wildcard_candidate)

    return selected[:5]


def run_probe(sport, api_key, quota_floor, engine):
    """
    Run coverage discovery for a single sport. Writes to odds.market_probe.
    """
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Probe: {sport.upper()} ({sport_key}) ===")

    events = _probe_select_events(sport, sport_key, api_key, quota_floor)
    if not events:
        print(f"  No sample events found for {sport}. Skipping probe.")
        return

    print(f"  Selected {len(events)} sample events.")

    featured_markets = FEATURED_MARKETS[sport]
    prop_markets = PROP_MARKETS[sport]
    alt_prop_markets = ALT_PROP_MARKETS[sport]

    featured_coverage = {m: {"bk_set": set(), "outcome_count": 0, "hit_count": 0} for m in featured_markets}
    prop_coverage = {m: {"bk_set": set(), "outcome_count": 0, "hit_count": 0} for m in prop_markets + alt_prop_markets}

    sample_event_ids = [e.get("id") for e in events]
    sample_dates = []

    for event in events:
        eid = event.get("id")
        cdt = _commence_dt(event)
        if cdt:
            sample_dates.append(str(cdt.date()))
        snap_iso = _snapshot_iso(cdt) if cdt else None
        if not snap_iso:
            continue

        bulk_data, _ = _fetch_bulk_featured(sport_key, snap_iso, featured_markets, api_key, quota_floor)
        event_data = next((e for e in bulk_data if e.get("id") == eid), None)
        if event_data:
            for bk in event_data.get("bookmakers", []) or []:
                for mkt in bk.get("markets", []) or []:
                    mk = mkt.get("key")
                    if mk in featured_coverage:
                        outcomes = mkt.get("outcomes", []) or []
                        if outcomes:
                            featured_coverage[mk]["bk_set"].add(bk.get("key"))
                            featured_coverage[mk]["outcome_count"] += len(outcomes)
                            featured_coverage[mk]["hit_count"] += 1

        if cdt and cdt >= PROPS_CUTOFF:
            prop_obj, _ = _fetch_event_odds(sport_key, eid, snap_iso, prop_markets, api_key, quota_floor)
            if prop_obj:
                for bk in prop_obj.get("bookmakers", []) or []:
                    for mkt in bk.get("markets", []) or []:
                        mk = mkt.get("key")
                        if mk in prop_coverage:
                            outcomes = mkt.get("outcomes", []) or []
                            if outcomes:
                                prop_coverage[mk]["bk_set"].add(bk.get("key"))
                                prop_coverage[mk]["outcome_count"] += len(outcomes)
                                prop_coverage[mk]["hit_count"] += 1
            time.sleep(1.5)

            alt_obj, _ = _fetch_event_odds(sport_key, eid, snap_iso, alt_prop_markets, api_key, quota_floor)
            if alt_obj:
                for bk in alt_obj.get("bookmakers", []) or []:
                    for mkt in bk.get("markets", []) or []:
                        mk = mkt.get("key")
                        if mk in prop_coverage:
                            outcomes = mkt.get("outcomes", []) or []
                            if outcomes:
                                prop_coverage[mk]["bk_set"].add(bk.get("key"))
                                prop_coverage[mk]["outcome_count"] += len(outcomes)
                                prop_coverage[mk]["hit_count"] += 1
            time.sleep(1.5)
        else:
            print(f"    Event {eid} before props cutoff ({cdt}). Skipping prop calls.")

    testable_event_count = len(events)
    coverage_threshold = 3
    probe_timestamp = datetime.now(tz=timezone.utc)
    sample_event_ids_str = ",".join(str(i) for i in sample_event_ids if i)
    sample_dates_str = ",".join(sorted(set(sample_dates)))

    probe_rows = []

    print(f"\n=== {sport.upper()} Market Coverage ({testable_event_count} events sampled) ===")

    for market_key in featured_markets:
        cov = featured_coverage[market_key]
        is_covered = cov["hit_count"] >= coverage_threshold
        covered_bks = sorted(cov["bk_set"])
        status = "COVERED    " if is_covered else "NOT COVERED"
        print(
            f"  {status} {market_key:<45} "
            f"{len(covered_bks)} books  {cov['outcome_count']} outcomes  "
            f"{covered_bks}"
        )
        probe_rows.append({
            "sport_key": sport_key,
            "market_key": market_key,
            "market_type": "featured",
            "bookmaker_count": len(covered_bks),
            "outcome_count": cov["outcome_count"],
            "is_covered": 1 if is_covered else 0,
            "covered_bookmakers": ",".join(covered_bks)[:200],
            "sample_event_ids": sample_event_ids_str[:500],
            "sample_dates": sample_dates_str[:200],
            "probe_timestamp": probe_timestamp,
        })

    all_prop_keys = prop_markets + alt_prop_markets
    for market_key in all_prop_keys:
        cov = prop_coverage[market_key]
        is_covered = cov["hit_count"] >= coverage_threshold
        covered_bks = sorted(cov["bk_set"])
        mtype = "alt_prop" if market_key in alt_prop_markets else "prop"
        status = "COVERED    " if is_covered else "NOT COVERED"
        print(
            f"  {status} {market_key:<45} "
            f"{len(covered_bks)} books  {cov['outcome_count']} outcomes  "
            f"{covered_bks}"
        )
        probe_rows.append({
            "sport_key": sport_key,
            "market_key": market_key,
            "market_type": mtype,
            "bookmaker_count": len(covered_bks),
            "outcome_count": cov["outcome_count"],
            "is_covered": 1 if is_covered else 0,
            "covered_bookmakers": ",".join(covered_bks)[:200],
            "sample_event_ids": sample_event_ids_str[:500],
            "sample_dates": sample_dates_str[:200],
            "probe_timestamp": probe_timestamp,
        })

    covered_count = sum(1 for r in probe_rows if r["is_covered"])
    not_covered_count = len(probe_rows) - covered_count
    print(
        f"\n  Summary: {len(probe_rows)} markets tested, "
        f"{covered_count} covered, {not_covered_count} not covered."
    )

    if _remaining_credits is not None:
        print(f"\n  Credits remaining: {_remaining_credits:,}")

    df = pd.DataFrame(probe_rows)
    df = clean_dataframe(df)
    upsert(engine, df, schema="odds", table="market_probe", keys=["sport_key", "market_key"])
    print(f"  Probe results written to odds.market_probe ({len(probe_rows)} rows).")


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------

def _load_probe_results(engine, sport_key):
    try:
        df = pd.read_sql(
            "SELECT market_key, is_covered FROM odds.market_probe WHERE sport_key = :sk",
            engine,
            params={"sk": sport_key},
        )
    except Exception:
        return None
    if df.empty:
        return None
    return {row["market_key"]: bool(row["is_covered"]) for _, row in df.iterrows()}


def _get_covered_markets(probe_results, all_markets, market_type_label):
    if probe_results is None:
        print(
            f"    WARNING: No probe results found for {market_type_label} markets. "
            "Using full market list."
        )
        return all_markets

    covered = [m for m in all_markets if probe_results.get(m, True)]
    skipped = [m for m in all_markets if not probe_results.get(m, True)]
    if skipped:
        print(f"    Skipping {len(skipped)} uncovered {market_type_label} markets: {skipped}")
    return covered


def _get_existing_event_ids(engine, sport_key, season_year):
    df = pd.read_sql(
        "SELECT event_id FROM odds.events WHERE sport_key = :sk AND season_year = :sy",
        engine,
        params={"sk": sport_key, "sy": season_year},
    )
    return set(df["event_id"].astype(str))


def run_backfill(sport, api_key, quota_floor, games_limit, season_year, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Backfill: {sport.upper()} ({sport_key}) Season {season_year} ===")

    probe_results = _load_probe_results(engine, sport_key)

    featured_markets = _get_covered_markets(
        probe_results, FEATURED_MARKETS[sport], "featured"
    )
    prop_markets = _get_covered_markets(
        probe_results, PROP_MARKETS[sport], "prop"
    )
    alt_prop_markets = _get_covered_markets(
        probe_results, ALT_PROP_MARKETS[sport], "alt_prop"
    )

    start_date, end_date = _season_date_range(sport, season_year)
    today = date.today()
    end_date = min(end_date, today - timedelta(days=1))

    if start_date > end_date:
        print(f"  No past dates in season range. Nothing to do.")
        return

    print(f"  Season range: {start_date} to {end_date}")

    all_dates = _date_range_list(start_date, end_date)
    print(f"  Discovering events across {len(all_dates)} dates...")

    all_events_by_id = {}
    for d in all_dates:
        events = _discover_events_for_date(sport_key, d, api_key, quota_floor)
        for ev in events:
            eid = ev.get("id")
            if eid:
                all_events_by_id[eid] = ev

    desired_ids = set(all_events_by_id.keys())
    existing_ids = _get_existing_event_ids(engine, sport_key, season_year)
    missing_ids = desired_ids - existing_ids

    if not missing_ids:
        print("  All events already loaded. Nothing to do.")
        return

    missing_events = [all_events_by_id[eid] for eid in missing_ids]
    missing_events.sort(key=lambda e: e.get("commence_time", ""))
    work_events = missing_events[:games_limit]

    print(
        f"  {len(missing_ids)} missing events. Processing {len(work_events)} "
        f"(oldest first, limit={games_limit})."
    )

    for event in work_events:
        eid = event.get("id")
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        cdt = _commence_dt(event)
        snap_iso = _snapshot_iso(cdt) if cdt else None
        event_label = f"{away} @ {home} ({cdt.date() if cdt else 'unknown'})"

        print(f"\n  Processing: {event_label}")

        if not snap_iso:
            print(f"    Could not compute snapshot time. Skipping.")
            continue

        game_lines_all = []
        player_props_all = []

        if featured_markets:
            bulk_data, bulk_ts = _fetch_bulk_featured(
                sport_key, snap_iso, featured_markets, api_key, quota_floor
            )
            event_obj = next((e for e in bulk_data if e.get("id") == eid), None)
            if event_obj:
                gl, pp = _parse_bookmakers(event_obj, eid, sport_key, bulk_ts)
                game_lines_all.extend(gl)
                player_props_all.extend(pp)
            else:
                print(f"    Event not found in bulk featured response.")

        if cdt and cdt >= PROPS_CUTOFF:
            if prop_markets:
                prop_obj, prop_ts = _fetch_event_odds(
                    sport_key, eid, snap_iso, prop_markets, api_key, quota_floor
                )
                if prop_obj:
                    gl, pp = _parse_bookmakers(prop_obj, eid, sport_key, prop_ts)
                    game_lines_all.extend(gl)
                    player_props_all.extend(pp)
                time.sleep(1.5)

            if alt_prop_markets:
                alt_obj, alt_ts = _fetch_event_odds(
                    sport_key, eid, snap_iso, alt_prop_markets, api_key, quota_floor
                )
                if alt_obj:
                    gl, pp = _parse_bookmakers(alt_obj, eid, sport_key, alt_ts)
                    game_lines_all.extend(gl)
                    player_props_all.extend(pp)
                time.sleep(1.5)
        else:
            print(f"    Event before props cutoff ({cdt}). Skipping prop calls.")

        event_row = _parse_event_to_row(event, sport_key, season_year)
        df_events = pd.DataFrame([event_row])
        df_events = clean_dataframe(df_events)
        upsert(engine, df_events, schema="odds", table="events", keys=["event_id"])

        gl_written = 0
        if game_lines_all:
            df_gl = pd.DataFrame(game_lines_all)
            df_gl = clean_dataframe(df_gl)
            upsert(
                engine, df_gl, schema="odds", table="game_lines",
                keys=["event_id", "market_key", "bookmaker_key", "outcome_name"],
            )
            gl_written = len(df_gl)

        pp_written = 0
        if player_props_all:
            df_pp = pd.DataFrame(player_props_all)
            df_pp = clean_dataframe(df_pp)
            upsert(
                engine, df_pp, schema="odds", table="player_props",
                keys=["event_id", "market_key", "bookmaker_key", "player_name", "outcome_name"],
            )
            pp_written = len(df_pp)

        credits_str = str(_remaining_credits) if _remaining_credits is not None else "unknown"
        print(
            f"    Loaded: events=1  game_lines={gl_written}  "
            f"player_props={pp_written}  credits_remaining={credits_str}"
        )

        time.sleep(1.5)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="The Odds API v4 ETL")
    parser.add_argument("--mode", choices=["probe", "backfill"], default="backfill")
    parser.add_argument("--sport", default="all", choices=["nfl", "nba", "mlb", "all"])
    parser.add_argument("--season", type=int, default=None)
    parser.add_argument("--games", type=int, default=10)
    parser.add_argument("--quota-floor", type=int, default=50000, dest="quota_floor")
    args = parser.parse_args()

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        raise EnvironmentError(
            "ODDS_API_KEY environment variable is not set. "
            "Add it to your GitHub Actions secrets and workflow env block."
        )

    sports = ["nfl", "nba", "mlb"] if args.sport == "all" else [args.sport]

    print(f"Mode:        {args.mode}")
    print(f"Sport(s):    {', '.join(sports)}")
    print(f"Quota floor: {args.quota_floor:,}")

    engine = get_engine()
    ensure_schema(engine)

    for sport in sports:
        season_year = args.season if args.season else _default_season(sport)

        if args.mode == "probe":
            print(f"Season:      {season_year} (probe uses hardcoded sample dates near this season)")
            run_probe(sport, api_key, args.quota_floor, engine)
        else:
            print(f"Season:      {season_year}")
            print(f"Games limit: {args.games}")
            run_backfill(sport, api_key, args.quota_floor, args.games, season_year, engine)

    if _remaining_credits is not None:
        print(f"\nFinal credits remaining: {_remaining_credits:,}")


if __name__ == "__main__":
    main()
