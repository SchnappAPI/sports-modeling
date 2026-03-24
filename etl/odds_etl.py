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

Featured market routing
  The bulk /odds endpoint only supports h2h, spreads, and totals.
  All other featured markets (team_totals, h1/q1 lines) must be fetched
  via the per-event /events/{id}/odds endpoint. The script splits the
  featured market list into BULK_FEATURED and EVENT_FEATURED and routes
  each group to the correct endpoint automatically.
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

# Allow `from etl.db import ...` when invoked as `python etl/odds_etl.py`
# from the repo root. Python adds etl/ to sys.path but not the root.
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

SEASON_MONTHS = {
    "nfl": (9, 2),   # September through February (wraps year)
    "nba": (10, 6),  # October through June (wraps year)
    "mlb": (3, 11),  # March through November (same year)
}

PROPS_CUTOFF = datetime(2023, 5, 3, 5, 30, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Market constants
#
# BULK_FEATURED: supported by the /historical/sports/{sport}/odds endpoint.
# EVENT_FEATURED: only supported by the per-event /events/{id}/odds endpoint.
# ---------------------------------------------------------------------------

# Markets valid on the bulk endpoint for all sports
BULK_FEATURED_MARKETS = ["h2h", "spreads", "totals"]

NFL_EVENT_FEATURED = [
    "team_totals",
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

NBA_EVENT_FEATURED = [
    "team_totals",
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

MLB_EVENT_FEATURED = [
    "team_totals",
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

# All featured markets per sport (bulk + event), used for probe coverage display
ALL_FEATURED_MARKETS = {
    "nfl": BULK_FEATURED_MARKETS + NFL_EVENT_FEATURED,
    "nba": BULK_FEATURED_MARKETS + NBA_EVENT_FEATURED,
    "mlb": BULK_FEATURED_MARKETS + MLB_EVENT_FEATURED,
}
EVENT_FEATURED_MARKETS = {
    "nfl": NFL_EVENT_FEATURED,
    "nba": NBA_EVENT_FEATURED,
    "mlb": MLB_EVENT_FEATURED,
}
PROP_MARKETS = {"nfl": NFL_PROPS, "nba": NBA_PROPS, "mlb": MLB_PROPS}
ALT_PROP_MARKETS = {"nfl": NFL_ALT_PROPS, "nba": NBA_ALT_PROPS, "mlb": MLB_ALT_PROPS}

BOOKMAKERS = "fanduel,draftkings,betmgm,williamhill_us"

# ---------------------------------------------------------------------------
# DDL
# Note: column is named probed_at, not probe_timestamp, to avoid collision
# with SQL Server's reserved TIMESTAMP type which breaks pandas to_sql.
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
        probe_id           INT IDENTITY PRIMARY KEY,
        sport_key          VARCHAR(50)   NOT NULL,
        market_key         VARCHAR(100)  NOT NULL,
        market_type        VARCHAR(20)   NULL,
        bookmaker_count    INT           NULL,
        outcome_count      INT           NULL,
        is_covered         BIT           NULL,
        covered_bookmakers VARCHAR(200)  NULL,
        sample_event_ids   VARCHAR(500)  NULL,
        sample_dates       VARCHAR(200)  NULL,
        probed_at          DATETIME2     NULL,
        created_at         DATETIME2     NOT NULL DEFAULT GETUTCDATE()
    )
    """,

    # Migration: rename probe_timestamp -> probed_at if the table was created
    # with the old column name before this fix was applied.
    """
    IF EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'odds'
          AND TABLE_NAME   = 'market_probe'
          AND COLUMN_NAME  = 'probe_timestamp'
    )
    EXEC sp_rename 'odds.market_probe.probe_timestamp', 'probed_at', 'COLUMN'
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

_remaining_credits = None


def _request(url, params, quota_floor, retries=3):
    global _remaining_credits
    wait_times = [10, 30, 60]
    last_exc = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
        except Exception as exc:
            last_exc = exc
            if attempt < retries - 1:
                print(f"    [retry {attempt+1}] Request exception: {exc}. Waiting {wait_times[attempt]}s...")
                time.sleep(wait_times[attempt])
            continue

        remaining_header = resp.headers.get("x-requests-remaining")
        used_header = resp.headers.get("x-requests-used")
        last_header = resp.headers.get("x-requests-last")
        if remaining_header is not None:
            _remaining_credits = int(remaining_header)
            print(f"    [quota] remaining={remaining_header}  used={used_header}  last={last_header}")

        if resp.status_code == 200:
            if _remaining_credits is not None and _remaining_credits < quota_floor:
                print(f"WARNING: remaining credits ({_remaining_credits}) below quota floor ({quota_floor}). Stopping.")
                sys.exit(1)
            return resp.json(), resp.headers

        if resp.status_code in (401, 403, 404):
            print(f"    [skip] HTTP {resp.status_code}: {resp.text[:200]}")
            return None, None

        if resp.status_code == 429 or resp.status_code >= 500:
            wait = wait_times[min(attempt, len(wait_times) - 1)]
            print(f"    [retry {attempt+1}] HTTP {resp.status_code}: {resp.text[:200]}. Waiting {wait}s...")
            time.sleep(wait)
            continue

        print(f"    [skip] Unexpected HTTP {resp.status_code}: {resp.text[:200]}")
        return None, None

    print(f"    [skip] All retries exhausted. Last exception: {last_exc}")
    return None, None


def _check_quota_before_call(quota_floor):
    if _remaining_credits is not None and _remaining_credits < quota_floor:
        print(f"WARNING: remaining credits ({_remaining_credits}) below quota floor ({quota_floor}). Stopping.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------

def _default_season(sport):
    today = date.today()
    start_month, end_month = SEASON_MONTHS[sport]
    wraps = start_month > end_month
    if wraps:
        return today.year if today.month >= start_month else today.year - 1
    else:
        return today.year if today.month >= start_month else today.year - 1


def _season_date_range(sport, season_year):
    start_month, end_month = SEASON_MONTHS[sport]
    wraps = start_month > end_month
    start_date = date(season_year, start_month, 1)
    end_year = season_year + 1 if wraps else season_year
    if end_month == 12:
        end_date = date(end_year, 12, 31)
    else:
        end_date = date(end_year, end_month + 1, 1) - timedelta(days=1)
    return start_date, end_date


def _date_range_list(start_date, end_date):
    dates, cur = [], start_date
    while cur <= end_date:
        dates.append(cur)
        cur += timedelta(days=1)
    return dates


# ---------------------------------------------------------------------------
# Event discovery
# ---------------------------------------------------------------------------

def _discover_events_for_date(sport_key, target_date, api_key, quota_floor):
    _check_quota_before_call(quota_floor)
    url = f"{BASE_URL}/v4/historical/sports/{sport_key}/events"
    params = {
        "apiKey": api_key,
        "date": f"{target_date}T12:00:00Z",
        "commenceTimeFrom": f"{target_date}T00:00:00Z",
        "commenceTimeTo": f"{target_date}T23:59:59Z",
    }
    data, _ = _request(url, params, quota_floor)
    if data is None:
        return []
    return data.get("data", []) or []


def _discover_events_for_date_with_fallback(sport_key, target_date, api_key, quota_floor, max_walk=7):
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
# Odds fetching
# ---------------------------------------------------------------------------

def _fetch_bulk_featured(sport_key, snapshot_iso, markets_list, api_key, quota_floor):
    """Bulk endpoint: returns all events for the sport. Only h2h/spreads/totals supported."""
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


def _fetch_event_odds(sport_key, event_id, snapshot_iso, markets_list, api_key, quota_floor):
    """Per-event endpoint: supports all markets including team_totals and h1/q1 lines."""
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
    return data.get("data"), data.get("timestamp")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _parse_event_to_row(event, sport_key, season_year):
    commence_raw = event.get("commence_time")
    try:
        commence_dt = datetime.fromisoformat(commence_raw.replace("Z", "+00:00")) if commence_raw else None
    except Exception:
        commence_dt = None
    return {
        "event_id":     event.get("id"),
        "sport_key":    sport_key,
        "sport_title":  event.get("sport_title"),
        "commence_time": commence_dt,
        "home_team":    event.get("home_team"),
        "away_team":    event.get("away_team"),
        "season_year":  season_year,
    }


def _parse_bookmakers(event_obj, event_id, sport_key, snapshot_timestamp):
    """
    Split outcomes into game_lines (no description) and player_props (has description).
    """
    game_lines, player_props = [], []
    snapshot_dt = None
    if snapshot_timestamp:
        try:
            snapshot_dt = datetime.fromisoformat(snapshot_timestamp.replace("Z", "+00:00"))
        except Exception:
            pass

    for bk in event_obj.get("bookmakers", []) or []:
        bk_key, bk_title = bk.get("key"), bk.get("title")
        for mkt in bk.get("markets", []) or []:
            mkt_key = mkt.get("key")
            for outcome in mkt.get("outcomes", []) or []:
                row = {
                    "event_id":           event_id,
                    "sport_key":          sport_key,
                    "market_key":         mkt_key,
                    "bookmaker_key":      bk_key,
                    "bookmaker_title":    bk_title,
                    "outcome_name":       outcome.get("name"),
                    "outcome_price":      outcome.get("price"),
                    "outcome_point":      outcome.get("point"),
                    "snapshot_timestamp": snapshot_dt,
                }
                description = outcome.get("description")
                if description:
                    player_props.append({**row, "player_name": description})
                    del player_props[-1]["outcome_name"]  # will re-add below
                    # rebuild with correct field order for player_props table
                    player_props[-1] = {
                        "event_id":           event_id,
                        "sport_key":          sport_key,
                        "market_key":         mkt_key,
                        "bookmaker_key":      bk_key,
                        "bookmaker_title":    bk_title,
                        "player_name":        description,
                        "outcome_name":       outcome.get("name"),
                        "outcome_price":      outcome.get("price"),
                        "outcome_point":      outcome.get("point"),
                        "snapshot_timestamp": snapshot_dt,
                    }
                else:
                    game_lines.append(row)

    return game_lines, player_props


def _snapshot_iso(commence_time_dt):
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
    "nfl": [date(2024, 11, 7),  date(2024, 12, 12)],
    "nba": [date(2024, 12, 15), date(2025, 2, 15)],
    "mlb": [date(2024, 6, 15),  date(2024, 8, 15)],
}
PROBE_WORST_CASE = {
    "nfl": [date(2024, 9, 8),  date(2025, 2, 2)],
    "nba": [date(2024, 10, 22), date(2025, 6, 1)],
    "mlb": [date(2024, 3, 20), date(2024, 9, 28)],
}


def _probe_select_events(sport, sport_key, api_key, quota_floor):
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

    for target_date in PROBE_BEST_CASE[sport] + PROBE_WORST_CASE[sport]:
        actual_date = next(
            (d for d in all_events_by_date if abs((d - target_date).days) <= 7), None
        )
        if actual_date and all_events_by_date.get(actual_date):
            evs = all_events_by_date[actual_date]
            selected.append(evs[0])
            for ev in evs:
                cdt = _commence_dt(ev)
                if cdt and (wildcard_candidate is None or cdt > _commence_dt(wildcard_candidate)):
                    wildcard_candidate = ev

    if wildcard_candidate and wildcard_candidate.get("id") not in {e.get("id") for e in selected}:
        selected.append(wildcard_candidate)

    return selected[:5]


def run_probe(sport, api_key, quota_floor, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Probe: {sport.upper()} ({sport_key}) ===")

    events = _probe_select_events(sport, sport_key, api_key, quota_floor)
    if not events:
        print(f"  No sample events found for {sport}. Skipping probe.")
        return
    print(f"  Selected {len(events)} sample events.")

    all_featured  = ALL_FEATURED_MARKETS[sport]
    event_featured = EVENT_FEATURED_MARKETS[sport]
    prop_markets  = PROP_MARKETS[sport]
    alt_prop_markets = ALT_PROP_MARKETS[sport]

    all_tracked = all_featured + prop_markets + alt_prop_markets
    coverage = {m: {"bk_set": set(), "outcome_count": 0, "hit_count": 0} for m in all_tracked}

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

        # Bulk featured: h2h, spreads, totals only
        bulk_data, _ = _fetch_bulk_featured(
            sport_key, snap_iso, BULK_FEATURED_MARKETS, api_key, quota_floor
        )
        event_data = next((e for e in bulk_data if e.get("id") == eid), None)
        if event_data:
            for bk in event_data.get("bookmakers", []) or []:
                for mkt in bk.get("markets", []) or []:
                    mk = mkt.get("key")
                    if mk in coverage:
                        outcomes = mkt.get("outcomes", []) or []
                        if outcomes:
                            coverage[mk]["bk_set"].add(bk.get("key"))
                            coverage[mk]["outcome_count"] += len(outcomes)
                            coverage[mk]["hit_count"] += 1

        # Per-event: event_featured + props + alt_props (all via same endpoint)
        # Combine event_featured with props into two calls to keep market counts manageable.
        # Call 1: event_featured markets (team_totals, h1/q1 lines)
        if event_featured:
            ef_obj, _ = _fetch_event_odds(
                sport_key, eid, snap_iso, event_featured, api_key, quota_floor
            )
            if ef_obj:
                for bk in ef_obj.get("bookmakers", []) or []:
                    for mkt in bk.get("markets", []) or []:
                        mk = mkt.get("key")
                        if mk in coverage:
                            outcomes = mkt.get("outcomes", []) or []
                            if outcomes:
                                coverage[mk]["bk_set"].add(bk.get("key"))
                                coverage[mk]["outcome_count"] += len(outcomes)
                                coverage[mk]["hit_count"] += 1
            time.sleep(1.5)

        if cdt and cdt >= PROPS_CUTOFF:
            # Call 2: standard props
            prop_obj, _ = _fetch_event_odds(
                sport_key, eid, snap_iso, prop_markets, api_key, quota_floor
            )
            if prop_obj:
                for bk in prop_obj.get("bookmakers", []) or []:
                    for mkt in bk.get("markets", []) or []:
                        mk = mkt.get("key")
                        if mk in coverage:
                            outcomes = mkt.get("outcomes", []) or []
                            if outcomes:
                                coverage[mk]["bk_set"].add(bk.get("key"))
                                coverage[mk]["outcome_count"] += len(outcomes)
                                coverage[mk]["hit_count"] += 1
            time.sleep(1.5)

            # Call 3: alt props
            alt_obj, _ = _fetch_event_odds(
                sport_key, eid, snap_iso, alt_prop_markets, api_key, quota_floor
            )
            if alt_obj:
                for bk in alt_obj.get("bookmakers", []) or []:
                    for mkt in bk.get("markets", []) or []:
                        mk = mkt.get("key")
                        if mk in coverage:
                            outcomes = mkt.get("outcomes", []) or []
                            if outcomes:
                                coverage[mk]["bk_set"].add(bk.get("key"))
                                coverage[mk]["outcome_count"] += len(outcomes)
                                coverage[mk]["hit_count"] += 1
            time.sleep(1.5)
        else:
            print(f"    Event {eid} before props cutoff ({cdt}). Skipping prop calls.")

    coverage_threshold = 3
    probed_at = datetime.now(tz=timezone.utc)
    sample_event_ids_str = ",".join(str(i) for i in sample_event_ids if i)
    sample_dates_str = ",".join(sorted(set(sample_dates)))

    probe_rows = []
    print(f"\n=== {sport.upper()} Market Coverage ({len(events)} events sampled) ===")

    for market_key in all_featured:
        cov = coverage[market_key]
        is_covered = cov["hit_count"] >= coverage_threshold
        covered_bks = sorted(cov["bk_set"])
        mtype = "bulk_featured" if market_key in BULK_FEATURED_MARKETS else "event_featured"
        status = "COVERED    " if is_covered else "NOT COVERED"
        print(
            f"  {status} {market_key:<45} "
            f"{len(covered_bks)} books  {cov['outcome_count']} outcomes  {covered_bks}"
        )
        probe_rows.append({
            "sport_key":          sport_key,
            "market_key":         market_key,
            "market_type":        mtype,
            "bookmaker_count":    len(covered_bks),
            "outcome_count":      cov["outcome_count"],
            "is_covered":         1 if is_covered else 0,
            "covered_bookmakers": ",".join(covered_bks)[:200],
            "sample_event_ids":   sample_event_ids_str[:500],
            "sample_dates":       sample_dates_str[:200],
            "probed_at":          probed_at,
        })

    for market_key in prop_markets + alt_prop_markets:
        cov = coverage[market_key]
        is_covered = cov["hit_count"] >= coverage_threshold
        covered_bks = sorted(cov["bk_set"])
        mtype = "alt_prop" if market_key in alt_prop_markets else "prop"
        status = "COVERED    " if is_covered else "NOT COVERED"
        print(
            f"  {status} {market_key:<45} "
            f"{len(covered_bks)} books  {cov['outcome_count']} outcomes  {covered_bks}"
        )
        probe_rows.append({
            "sport_key":          sport_key,
            "market_key":         market_key,
            "market_type":        mtype,
            "bookmaker_count":    len(covered_bks),
            "outcome_count":      cov["outcome_count"],
            "is_covered":         1 if is_covered else 0,
            "covered_bookmakers": ",".join(covered_bks)[:200],
            "sample_event_ids":   sample_event_ids_str[:500],
            "sample_dates":       sample_dates_str[:200],
            "probed_at":          probed_at,
        })

    covered_count = sum(1 for r in probe_rows if r["is_covered"])
    print(
        f"\n  Summary: {len(probe_rows)} markets tested, "
        f"{covered_count} covered, {len(probe_rows) - covered_count} not covered."
    )
    if _remaining_credits is not None:
        print(f"  Credits remaining: {_remaining_credits:,}")

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


def _get_covered_markets(probe_results, all_markets, label):
    if probe_results is None:
        print(f"    WARNING: No probe results for {label}. Using full market list.")
        return all_markets
    covered = [m for m in all_markets if probe_results.get(m, True)]
    skipped = [m for m in all_markets if not probe_results.get(m, True)]
    if skipped:
        print(f"    Skipping {len(skipped)} uncovered {label} markets: {skipped}")
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

    bulk_featured    = BULK_FEATURED_MARKETS  # always use all three; bulk endpoint is reliable
    event_featured   = _get_covered_markets(probe_results, EVENT_FEATURED_MARKETS[sport], "event_featured")
    prop_markets     = _get_covered_markets(probe_results, PROP_MARKETS[sport], "prop")
    alt_prop_markets = _get_covered_markets(probe_results, ALT_PROP_MARKETS[sport], "alt_prop")

    start_date, end_date = _season_date_range(sport, season_year)
    end_date = min(end_date, date.today() - timedelta(days=1))

    if start_date > end_date:
        print("  No past dates in season range. Nothing to do.")
        return

    print(f"  Season range: {start_date} to {end_date}")
    all_dates = _date_range_list(start_date, end_date)
    print(f"  Discovering events across {len(all_dates)} dates...")

    all_events_by_id = {}
    for d in all_dates:
        for ev in _discover_events_for_date(sport_key, d, api_key, quota_floor):
            eid = ev.get("id")
            if eid:
                all_events_by_id[eid] = ev

    existing_ids = _get_existing_event_ids(engine, sport_key, season_year)
    missing_events = [
        all_events_by_id[eid]
        for eid in set(all_events_by_id) - existing_ids
    ]
    if not missing_events:
        print("  All events already loaded. Nothing to do.")
        return

    missing_events.sort(key=lambda e: e.get("commence_time", ""))
    work_events = missing_events[:games_limit]
    print(f"  {len(missing_events)} missing events. Processing {len(work_events)} (oldest first).")

    for event in work_events:
        eid  = event.get("id")
        cdt  = _commence_dt(event)
        snap_iso = _snapshot_iso(cdt) if cdt else None
        label = f"{event.get('away_team','')} @ {event.get('home_team','')} ({cdt.date() if cdt else 'unknown'})"
        print(f"\n  Processing: {label}")

        if not snap_iso:
            print("    Could not compute snapshot time. Skipping.")
            continue

        game_lines_all, player_props_all = [], []

        # Bulk featured (h2h, spreads, totals)
        bulk_data, bulk_ts = _fetch_bulk_featured(
            sport_key, snap_iso, bulk_featured, api_key, quota_floor
        )
        event_obj = next((e for e in bulk_data if e.get("id") == eid), None)
        if event_obj:
            gl, pp = _parse_bookmakers(event_obj, eid, sport_key, bulk_ts)
            game_lines_all.extend(gl)
            player_props_all.extend(pp)
        else:
            print("    Event not found in bulk featured response.")

        # Per-event featured (team_totals, h1/q1 lines)
        if event_featured:
            ef_obj, ef_ts = _fetch_event_odds(
                sport_key, eid, snap_iso, event_featured, api_key, quota_floor
            )
            if ef_obj:
                gl, pp = _parse_bookmakers(ef_obj, eid, sport_key, ef_ts)
                game_lines_all.extend(gl)
                player_props_all.extend(pp)
            time.sleep(1.5)

        # Props
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
            print(f"    Before props cutoff ({cdt}). Skipping prop calls.")

        # Write
        upsert(engine, clean_dataframe(pd.DataFrame([_parse_event_to_row(event, sport_key, season_year)])),
               schema="odds", table="events", keys=["event_id"])

        gl_written = 0
        if game_lines_all:
            upsert(engine, clean_dataframe(pd.DataFrame(game_lines_all)),
                   schema="odds", table="game_lines",
                   keys=["event_id", "market_key", "bookmaker_key", "outcome_name"])
            gl_written = len(game_lines_all)

        pp_written = 0
        if player_props_all:
            upsert(engine, clean_dataframe(pd.DataFrame(player_props_all)),
                   schema="odds", table="player_props",
                   keys=["event_id", "market_key", "bookmaker_key", "player_name", "outcome_name"])
            pp_written = len(player_props_all)

        print(
            f"    Loaded: events=1  game_lines={gl_written}  "
            f"player_props={pp_written}  credits_remaining={_remaining_credits}"
        )
        time.sleep(1.5)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="The Odds API v4 ETL")
    parser.add_argument("--mode",        choices=["probe", "backfill"], default="backfill")
    parser.add_argument("--sport",       default="all", choices=["nfl", "nba", "mlb", "all"])
    parser.add_argument("--season",      type=int, default=None)
    parser.add_argument("--games",       type=int, default=10)
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
            print(f"Season:      {season_year} (probe uses hardcoded sample dates)")
            run_probe(sport, api_key, args.quota_floor, engine)
        else:
            print(f"Season:      {season_year}")
            print(f"Games limit: {args.games}")
            run_backfill(sport, api_key, args.quota_floor, args.games, season_year, engine)

    if _remaining_credits is not None:
        print(f"\nFinal credits remaining: {_remaining_credits:,}")


if __name__ == "__main__":
    main()
