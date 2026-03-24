"""
odds_etl.py

Ingests historical odds data from The Odds API v4 into Azure SQL.
Schema: odds (events, game_lines, player_props, market_probe)

Modes:
  probe    -- Coverage discovery pass. Writes only to odds.market_probe.
  backfill -- Incremental ingestion of historical event odds.

Featured market routing
  Bulk /odds endpoint: h2h, spreads, totals only.
  Per-event /events/{id}/odds endpoint: all other markets.

Datetime handling
  All datetime values stored in row dicts are naive UTC strings (no tzinfo).
  This prevents pandas from inferring DatetimeTZDtype, which SQL Server's
  ODBC driver incorrectly maps to the TIMESTAMP rowversion type on temp tables.

Parameter binding
  Never use pd.read_sql with named parameters (:name style) against a pyodbc
  engine. pyodbc only understands ? placeholders; named params cause
  "SQL contains 0 parameter markers" errors. All parameterised reads use
  engine.connect() + text() + SQLAlchemy binding instead.

Response shapes
  Call 1 (bulk /odds):          data["data"] is a LIST of event objects.
  Calls 2-4 (per-event /odds):  data["data"] is a single event DICT.
  Both shapes are handled: bulk iterates the list; per-event passes the dict
  directly to _parse_bookmakers, which reads event_obj["bookmakers"].
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

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
    "nfl": (9, 2),
    "nba": (10, 6),
    "mlb": (3, 11),
}

PROPS_CUTOFF = datetime(2023, 5, 3, 5, 30, 0, tzinfo=timezone.utc)

# FanDuel and DraftKings only. BetMGM and William Hill removed: rarely/never
# used and trimming books cuts per-event credit cost roughly in half.
BOOKMAKERS = "fanduel,draftkings"

# ---------------------------------------------------------------------------
# Market constants
# ---------------------------------------------------------------------------

BULK_FEATURED_MARKETS = ["h2h", "spreads", "totals"]

NFL_EVENT_FEATURED = [
    "team_totals",
    "h2h_h1", "spreads_h1", "totals_h1",
    "h2h_q1", "spreads_q1", "totals_q1",
    "team_totals_h1",
]
# Removed: player_pass_yds_q1 (only fanduel, does not meet 2-book threshold)
NFL_PROPS = [
    "player_pass_yds", "player_pass_tds", "player_pass_attempts",
    "player_pass_completions", "player_pass_interceptions",
    "player_pass_longest_completion",
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
# Removed: player_points_q1, player_rebounds_q1, player_assists_q1
#   (only williamhill_us, which is no longer in BOOKMAKERS)
# Removed: player_first_team_basket, player_method_of_first_basket
#   (not covered by fanduel or draftkings)
NBA_PROPS = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes", "player_blocks", "player_steals",
    "player_points_rebounds_assists", "player_points_rebounds",
    "player_points_assists", "player_rebounds_assists",
    "player_first_basket",
    "player_double_double", "player_triple_double",
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
# Removed: pitcher_outs (only draftkings, does not meet 2-book threshold)
MLB_PROPS = [
    "batter_home_runs", "batter_first_home_run",
    "batter_hits", "batter_total_bases", "batter_rbis",
    "batter_runs_scored", "batter_hits_runs_rbis",
    "batter_singles", "batter_doubles", "batter_triples",
    "batter_walks", "batter_strikeouts", "batter_stolen_bases",
    "pitcher_strikeouts", "pitcher_hits_allowed", "pitcher_walks",
    "pitcher_earned_runs",
]
# Removed: batter_runs_scored_alternate (zero coverage)
MLB_ALT_PROPS = [
    "batter_total_bases_alternate", "batter_home_runs_alternate",
    "batter_hits_alternate", "batter_rbis_alternate",
    "pitcher_strikeouts_alternate",
]

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
PROP_MARKETS     = {"nfl": NFL_PROPS,     "nba": NBA_PROPS,     "mlb": MLB_PROPS}
ALT_PROP_MARKETS = {"nfl": NFL_ALT_PROPS, "nba": NBA_ALT_PROPS, "mlb": MLB_ALT_PROPS}

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'odds') EXEC('CREATE SCHEMA odds')",
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='events')
    CREATE TABLE odds.events (
        event_id      VARCHAR(50)  NOT NULL PRIMARY KEY,
        sport_key     VARCHAR(50)  NOT NULL,
        sport_title   VARCHAR(50)  NULL,
        commence_time DATETIME2    NOT NULL,
        home_team     VARCHAR(100) NULL,
        away_team     VARCHAR(100) NULL,
        season_year   INT          NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='game_lines')
    CREATE TABLE odds.game_lines (
        event_id           VARCHAR(50)  NOT NULL,
        sport_key          VARCHAR(50)  NOT NULL,
        market_key         VARCHAR(100) NOT NULL,
        bookmaker_key      VARCHAR(50)  NOT NULL,
        bookmaker_title    VARCHAR(100) NULL,
        outcome_name       VARCHAR(100) NOT NULL,
        outcome_price      INT          NULL,
        outcome_point      DECIMAL(6,1) NULL,
        snap_ts            DATETIME2    NULL,
        created_at         DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='player_props')
    CREATE TABLE odds.player_props (
        event_id        VARCHAR(50)  NOT NULL,
        sport_key       VARCHAR(50)  NOT NULL,
        market_key      VARCHAR(100) NOT NULL,
        bookmaker_key   VARCHAR(50)  NOT NULL,
        bookmaker_title VARCHAR(100) NULL,
        player_name     VARCHAR(100) NOT NULL,
        outcome_name    VARCHAR(20)  NOT NULL,
        outcome_price   INT          NULL,
        outcome_point   DECIMAL(6,1) NULL,
        snap_ts         DATETIME2    NULL,
        created_at      DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='market_probe')
    CREATE TABLE odds.market_probe (
        probe_id           INT IDENTITY PRIMARY KEY,
        sport_key          VARCHAR(50)  NOT NULL,
        market_key         VARCHAR(100) NOT NULL,
        market_type        VARCHAR(20)  NULL,
        bookmaker_count    INT          NULL,
        outcome_count      INT          NULL,
        is_covered         BIT          NULL,
        covered_bookmakers VARCHAR(200) NULL,
        sample_event_ids   VARCHAR(500) NULL,
        sample_dates       VARCHAR(200) NULL,
        probed_at          DATETIME2    NULL,
        created_at         DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='market_probe'
               AND COLUMN_NAME='probe_timestamp')
    EXEC sp_rename 'odds.market_probe.probe_timestamp', 'probed_at', 'COLUMN'
    """,
    """
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='game_lines'
               AND COLUMN_NAME='snapshot_timestamp')
    EXEC sp_rename 'odds.game_lines.snapshot_timestamp', 'snap_ts', 'COLUMN'
    """,
    """
    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
               WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='player_props'
               AND COLUMN_NAME='snapshot_timestamp')
    EXEC sp_rename 'odds.player_props.snapshot_timestamp', 'snap_ts', 'COLUMN'
    """,
]


def ensure_schema(engine):
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))


# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------

def _to_utc_str(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return None
    if isinstance(dt, datetime):
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    return None


def clean_dataframe(df):
    # Replace NaN/NaT with None (SQL NULL) first, before any type coercion.
    # This must happen before the int-conversion lambda because float NaN
    # cannot be passed to int() and raises ValueError.
    df = df.where(pd.notna(df), other=None)
    for col in df.select_dtypes(include=["int64", "float64"]).columns:
        df[col] = df[col].apply(
            lambda x: None if x is None
            else int(x) if isinstance(x, float) and not pd.isna(x) and x == int(x)
            else int(x) if isinstance(x, int) and not isinstance(x, bool)
            else float(x) if isinstance(x, float)
            else x
        )
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) else x.strftime("%Y-%m-%d %H:%M:%S")
            )
        elif df[col].dtype == object:
            sample = df[col].dropna()
            if not sample.empty and isinstance(sample.iloc[0], datetime):
                df[col] = df[col].apply(
                    lambda x: None if x is None else _to_utc_str(x)
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
                print(f"    [retry {attempt+1}] exception: {exc}. Waiting {wait_times[attempt]}s...")
                time.sleep(wait_times[attempt])
            continue

        rh = resp.headers.get("x-requests-remaining")
        uh = resp.headers.get("x-requests-used")
        lh = resp.headers.get("x-requests-last")
        if rh is not None:
            _remaining_credits = int(rh)
            print(f"    [quota] remaining={rh}  used={uh}  last={lh}")

        if resp.status_code == 200:
            if _remaining_credits is not None and _remaining_credits < quota_floor:
                print(f"WARNING: {_remaining_credits} credits remaining, below floor {quota_floor}. Stopping.")
                sys.exit(1)
            return resp.json(), resp.headers

        if resp.status_code in (401, 403, 404):
            print(f"    [skip] HTTP {resp.status_code}: {resp.text[:200]}")
            return None, None

        if resp.status_code == 429 or resp.status_code >= 500:
            wait = wait_times[min(attempt, len(wait_times) - 1)]
            print(f"    [retry {attempt+1}] HTTP {resp.status_code}. Waiting {wait}s...")
            time.sleep(wait)
            continue

        print(f"    [skip] HTTP {resp.status_code}: {resp.text[:200]}")
        return None, None

    print(f"    [skip] All retries exhausted. Last: {last_exc}")
    return None, None


def _check_quota(quota_floor):
    if _remaining_credits is not None and _remaining_credits < quota_floor:
        print(f"WARNING: {_remaining_credits} credits remaining, below floor {quota_floor}. Stopping.")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Database read helpers
#
# Never use pd.read_sql with named parameters (:name) against a pyodbc engine.
# pyodbc only understands ? placeholders. Use engine.connect() + text() instead
# so SQLAlchemy handles the parameter binding before it reaches pyodbc.
# ---------------------------------------------------------------------------

def _query_rows(engine, sql, params):
    """Execute a parameterised SELECT and return list of Row objects."""
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        return result.fetchall()


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------

def _default_season(sport):
    today = date.today()
    start_month, end_month = SEASON_MONTHS[sport]
    wraps = start_month > end_month
    if wraps:
        return today.year if today.month >= start_month else today.year - 1
    return today.year if today.month >= start_month else today.year - 1


def _season_date_range(sport, season_year):
    start_month, end_month = SEASON_MONTHS[sport]
    wraps = start_month > end_month
    start_date = date(season_year, start_month, 1)
    end_year = season_year + 1 if wraps else season_year
    end_date = (
        date(end_year, 12, 31) if end_month == 12
        else date(end_year, end_month + 1, 1) - timedelta(days=1)
    )
    return start_date, end_date


def _date_list(start_date, end_date):
    out, cur = [], start_date
    while cur <= end_date:
        out.append(cur)
        cur += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# Event discovery
# ---------------------------------------------------------------------------

def _discover_events(sport_key, target_date, api_key, quota_floor):
    _check_quota(quota_floor)
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/events",
        {
            "apiKey": api_key,
            "date": f"{target_date}T12:00:00Z",
            "commenceTimeFrom": f"{target_date}T00:00:00Z",
            "commenceTimeTo":   f"{target_date}T23:59:59Z",
        },
        quota_floor,
    )
    return (data.get("data") or []) if data else []


def _discover_events_with_fallback(sport_key, target_date, api_key, quota_floor, max_walk=7):
    for offset in range(max_walk + 1):
        check = target_date + timedelta(days=offset)
        events = _discover_events(sport_key, check, api_key, quota_floor)
        if events:
            if offset:
                print(f"    No events on {target_date}, found {len(events)} on {check}")
            return events, check
    print(f"    No events within {max_walk} days of {target_date}")
    return [], target_date


# ---------------------------------------------------------------------------
# Odds fetching
#
# _fetch_bulk:  hits /v4/historical/sports/{sport}/odds
#               data["data"] is a LIST of event objects.
#               The caller filters by event_id to find the specific event.
#
# _fetch_event: hits /v4/historical/sports/{sport}/events/{id}/odds
#               data["data"] is a single event DICT (not a list).
#               Passed directly to _parse_bookmakers.
# ---------------------------------------------------------------------------

def _fetch_bulk(sport_key, snap_iso, markets, api_key, quota_floor):
    _check_quota(quota_floor)
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american", "date": snap_iso},
        quota_floor,
    )
    return ((data.get("data") or []), data.get("timestamp")) if data else ([], None)


def _fetch_event(sport_key, event_id, snap_iso, markets, api_key, quota_floor):
    _check_quota(quota_floor)
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/events/{event_id}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american", "date": snap_iso},
        quota_floor,
    )
    return (data.get("data"), data.get("timestamp")) if data else (None, None)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_event_row(event, sport_key, season_year):
    return {
        "event_id":      event.get("id"),
        "sport_key":     sport_key,
        "sport_title":   event.get("sport_title"),
        "commence_time": _to_utc_str(event.get("commence_time")),
        "home_team":     event.get("home_team"),
        "away_team":     event.get("away_team"),
        "season_year":   season_year,
    }


def _parse_bookmakers(event_obj, event_id, sport_key, snap_ts_raw):
    """
    Parse bookmaker/market/outcome data from an event object.

    event_obj is always a single event dict with a "bookmakers" key.
    For bulk calls, the caller extracts the matching event from the list first.
    For per-event calls, data["data"] is already that dict.

    Outcomes with a "description" field are player props (player_name = description).
    Outcomes without "description" are game lines.
    """
    snap_ts = _to_utc_str(snap_ts_raw)
    game_lines, player_props = [], []
    for bk in event_obj.get("bookmakers") or []:
        bk_key, bk_title = bk.get("key"), bk.get("title")
        for mkt in bk.get("markets") or []:
            mkt_key = mkt.get("key")
            for outcome in mkt.get("outcomes") or []:
                description = outcome.get("description")
                base = {
                    "event_id":        event_id,
                    "sport_key":       sport_key,
                    "market_key":      mkt_key,
                    "bookmaker_key":   bk_key,
                    "bookmaker_title": bk_title,
                    "outcome_name":    outcome.get("name"),
                    "outcome_price":   outcome.get("price"),
                    "outcome_point":   outcome.get("point"),
                    "snap_ts":         snap_ts,
                }
                if description:
                    player_props.append({**base, "player_name": description})
                else:
                    game_lines.append(base)
    return game_lines, player_props


def _snap_iso(commence_raw):
    if not commence_raw:
        return None
    try:
        dt = datetime.fromisoformat(str(commence_raw).replace("Z", "+00:00"))
        return (dt - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _cdt(event):
    raw = event.get("commence_time")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
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
    "nfl": [date(2024, 9, 8),   date(2025, 2, 2)],
    "nba": [date(2024, 10, 22), date(2025, 6, 1)],
    "mlb": [date(2024, 3, 20),  date(2024, 9, 28)],
}


def _probe_select_events(sport, sport_key, api_key, quota_floor):
    candidate_dates = PROBE_BEST_CASE[sport] + PROBE_WORST_CASE[sport]
    events_by_date = {}
    for td in candidate_dates:
        evs, actual = _discover_events_with_fallback(sport_key, td, api_key, quota_floor)
        if evs:
            events_by_date[actual] = evs

    selected, wildcard = [], None
    for td in candidate_dates:
        actual = next((d for d in events_by_date if abs((d - td).days) <= 7), None)
        if actual:
            evs = events_by_date[actual]
            selected.append(evs[0])
            for ev in evs:
                cdt = _cdt(ev)
                if cdt and (wildcard is None or cdt > _cdt(wildcard)):
                    wildcard = ev

    if wildcard and wildcard.get("id") not in {e.get("id") for e in selected}:
        selected.append(wildcard)

    return selected[:5]


def run_probe(sport, api_key, quota_floor, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Probe: {sport.upper()} ({sport_key}) ===")

    events = _probe_select_events(sport, sport_key, api_key, quota_floor)
    if not events:
        print("  No sample events found. Skipping.")
        return
    print(f"  Selected {len(events)} sample events.")

    all_markets  = ALL_FEATURED_MARKETS[sport] + PROP_MARKETS[sport] + ALT_PROP_MARKETS[sport]
    coverage     = {m: {"bk_set": set(), "outcomes": 0, "hits": 0} for m in all_markets}
    sample_ids   = [e.get("id") for e in events]
    sample_dates = []

    for event in events:
        eid = event.get("id")
        cdt = _cdt(event)
        if cdt:
            sample_dates.append(str(cdt.date()))
        snap = _snap_iso(event.get("commence_time"))
        if not snap:
            continue

        def _tally(event_obj):
            if not event_obj:
                return
            for bk in event_obj.get("bookmakers") or []:
                for mkt in bk.get("markets") or []:
                    mk = mkt.get("key")
                    if mk not in coverage:
                        continue
                    outs = mkt.get("outcomes") or []
                    if outs:
                        coverage[mk]["bk_set"].add(bk.get("key"))
                        coverage[mk]["outcomes"] += len(outs)
                        coverage[mk]["hits"] += 1

        bulk_data, _ = _fetch_bulk(sport_key, snap, BULK_FEATURED_MARKETS, api_key, quota_floor)
        _tally(next((e for e in bulk_data if e.get("id") == eid), None))

        ef_obj, _ = _fetch_event(sport_key, eid, snap, EVENT_FEATURED_MARKETS[sport], api_key, quota_floor)
        _tally(ef_obj)
        time.sleep(1.5)

        if cdt and cdt >= PROPS_CUTOFF:
            prop_obj, _ = _fetch_event(sport_key, eid, snap, PROP_MARKETS[sport], api_key, quota_floor)
            _tally(prop_obj)
            time.sleep(1.5)

            alt_obj, _ = _fetch_event(sport_key, eid, snap, ALT_PROP_MARKETS[sport], api_key, quota_floor)
            _tally(alt_obj)
            time.sleep(1.5)
        else:
            print(f"    {eid}: before props cutoff. Skipping prop calls.")

    probed_at_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ids_str   = ",".join(str(i) for i in sample_ids if i)[:500]
    dates_str = ",".join(sorted(set(sample_dates)))[:200]

    rows = []
    print(f"\n=== {sport.upper()} Market Coverage ({len(events)} events sampled) ===")

    for mkt in all_markets:
        cov = coverage[mkt]
        covered = cov["hits"] >= 3
        bks = sorted(cov["bk_set"])
        mtype = (
            "bulk_featured"   if mkt in BULK_FEATURED_MARKETS
            else "event_featured" if mkt in EVENT_FEATURED_MARKETS[sport]
            else "alt_prop"       if mkt in ALT_PROP_MARKETS[sport]
            else "prop"
        )
        print(f"  {'COVERED    ' if covered else 'NOT COVERED'} {mkt:<45} "
              f"{len(bks)} books  {cov['outcomes']} outcomes  {bks}")
        rows.append({
            "sport_key":          sport_key,
            "market_key":         mkt,
            "market_type":        mtype,
            "bookmaker_count":    len(bks),
            "outcome_count":      cov["outcomes"],
            "is_covered":         1 if covered else 0,
            "covered_bookmakers": ",".join(bks)[:200],
            "sample_event_ids":   ids_str,
            "sample_dates":       dates_str,
            "probed_at":          probed_at_str,
        })

    covered_count = sum(1 for r in rows if r["is_covered"])
    print(f"\n  Summary: {len(rows)} markets, {covered_count} covered, {len(rows)-covered_count} not covered.")
    if _remaining_credits is not None:
        print(f"  Credits remaining: {_remaining_credits:,}")

    df = pd.DataFrame(rows)
    df = clean_dataframe(df)
    upsert(engine, df, schema="odds", table="market_probe", keys=["sport_key", "market_key"])
    print(f"  Written to odds.market_probe ({len(rows)} rows).")


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------

def _load_probe_results(engine, sport_key):
    """Return {market_key: bool} from odds.market_probe, or None if no rows."""
    try:
        rows = _query_rows(
            engine,
            "SELECT market_key, is_covered FROM odds.market_probe WHERE sport_key = :sk",
            {"sk": sport_key},
        )
        if not rows:
            return None
        return {r[0]: bool(r[1]) for r in rows}
    except Exception:
        return None


def _filter_markets(probe, all_markets, label):
    if probe is None:
        print(f"    WARNING: No probe results for {label}. Using full list.")
        return all_markets
    covered = [m for m in all_markets if probe.get(m, True)]
    skipped = [m for m in all_markets if not probe.get(m, True)]
    if skipped:
        print(f"    Skipping {len(skipped)} uncovered {label}: {skipped}")
    return covered


def _existing_event_ids(engine, sport_key, season_year):
    """Return set of event_id strings already loaded for this sport/season."""
    rows = _query_rows(
        engine,
        "SELECT event_id FROM odds.events WHERE sport_key = :sk AND season_year = :sy",
        {"sk": sport_key, "sy": season_year},
    )
    return {str(r[0]) for r in rows}


def _latest_loaded_date(engine, sport_key, season_year):
    """
    Return the date of the latest commence_time already loaded for this
    sport/season, or None if nothing is loaded yet.

    Used to trim the discovery range on incremental runs so we only scan
    dates that could still have missing events.
    """
    rows = _query_rows(
        engine,
        """
        SELECT CAST(MAX(commence_time) AS DATE)
        FROM odds.events
        WHERE sport_key = :sk AND season_year = :sy
        """,
        {"sk": sport_key, "sy": season_year},
    )
    if rows and rows[0][0] is not None:
        val = rows[0][0]
        # SQL DATE comes back as a Python date object via pyodbc; normalise.
        if isinstance(val, str):
            return date.fromisoformat(val)
        if hasattr(val, "date"):
            return val.date()
        return val
    return None


def run_backfill(sport, api_key, quota_floor, games_limit, season_year, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Backfill: {sport.upper()} Season {season_year} ===")

    probe        = _load_probe_results(engine, sport_key)
    event_feat   = _filter_markets(probe, EVENT_FEATURED_MARKETS[sport], "event_featured")
    prop_markets = _filter_markets(probe, PROP_MARKETS[sport], "prop")
    alt_markets  = _filter_markets(probe, ALT_PROP_MARKETS[sport], "alt_prop")

    start_date, end_date = _season_date_range(sport, season_year)
    end_date = min(end_date, date.today() - timedelta(days=1))
    if start_date > end_date:
        print("  No past dates in range. Nothing to do.")
        return

    # Trim discovery range: start from the day already loaded so we catch
    # any games on that date that were missed, then continue forward.
    # This avoids firing hundreds of discovery calls for dates already fully
    # loaded, which is what causes rate-limit skips during backfill.
    latest = _latest_loaded_date(engine, sport_key, season_year)
    discover_from = max(start_date, latest) if latest else start_date
    print(f"  Season range: {start_date} to {end_date}  |  Discovering from: {discover_from}")

    all_dates = _date_list(discover_from, end_date)
    print(f"  Discovering events across {len(all_dates)} dates (0.5s sleep between calls)...")

    events_by_id = {}
    for i, d in enumerate(all_dates):
        for ev in _discover_events(sport_key, d, api_key, quota_floor):
            eid = ev.get("id")
            if eid:
                events_by_id[eid] = ev
        if i < len(all_dates) - 1:
            time.sleep(0.5)

    existing = _existing_event_ids(engine, sport_key, season_year)
    missing  = [events_by_id[eid] for eid in set(events_by_id) - existing]
    if not missing:
        print("  All events loaded. Nothing to do.")
        return

    missing.sort(key=lambda e: e.get("commence_time", ""))
    work = missing[:games_limit]
    print(f"  {len(missing)} missing. Processing {len(work)} (oldest first).")

    for event in work:
        eid   = event.get("id")
        cdt   = _cdt(event)
        snap  = _snap_iso(event.get("commence_time"))
        label = f"{event.get('away_team','')} @ {event.get('home_team','')} ({cdt.date() if cdt else '?'})"
        print(f"\n  {label}")

        if not snap:
            print("    No snapshot time. Skipping.")
            continue

        gl_all, pp_all = [], []

        # Call 1: bulk featured (h2h, spreads, totals)
        # Returns data["data"] as a list; find this event by ID.
        bulk_data, bulk_ts = _fetch_bulk(sport_key, snap, BULK_FEATURED_MARKETS, api_key, quota_floor)
        ev_obj = next((e for e in bulk_data if e.get("id") == eid), None)
        if ev_obj:
            gl, pp = _parse_bookmakers(ev_obj, eid, sport_key, bulk_ts)
            gl_all.extend(gl); pp_all.extend(pp)
        else:
            print("    Not found in bulk response.")

        # Call 2: event featured (team totals, halves, quarters)
        # Returns data["data"] as a single event dict.
        if event_feat:
            ef_obj, ef_ts = _fetch_event(sport_key, eid, snap, event_feat, api_key, quota_floor)
            if ef_obj:
                gl, pp = _parse_bookmakers(ef_obj, eid, sport_key, ef_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)

        if cdt and cdt >= PROPS_CUTOFF:
            # Call 3: standard player props
            # Returns data["data"] as a single event dict.
            if prop_markets:
                p_obj, p_ts = _fetch_event(sport_key, eid, snap, prop_markets, api_key, quota_floor)
                if p_obj:
                    gl, pp = _parse_bookmakers(p_obj, eid, sport_key, p_ts)
                    gl_all.extend(gl); pp_all.extend(pp)
                time.sleep(1.5)

            # Call 4: alternate props
            # Returns data["data"] as a single event dict.
            if alt_markets:
                a_obj, a_ts = _fetch_event(sport_key, eid, snap, alt_markets, api_key, quota_floor)
                if a_obj:
                    gl, pp = _parse_bookmakers(a_obj, eid, sport_key, a_ts)
                    gl_all.extend(gl); pp_all.extend(pp)
                time.sleep(1.5)
        else:
            print("    Before props cutoff. Skipping prop calls.")

        upsert(engine, clean_dataframe(pd.DataFrame([_parse_event_row(event, sport_key, season_year)])),
               schema="odds", table="events", keys=["event_id"])

        gl_n = pp_n = 0
        if gl_all:
            upsert(engine, clean_dataframe(pd.DataFrame(gl_all)),
                   schema="odds", table="game_lines",
                   keys=["event_id", "market_key", "bookmaker_key", "outcome_name"])
            gl_n = len(gl_all)
        if pp_all:
            upsert(engine, clean_dataframe(pd.DataFrame(pp_all)),
                   schema="odds", table="player_props",
                   keys=["event_id", "market_key", "bookmaker_key", "player_name", "outcome_name"])
            pp_n = len(pp_all)

        print(f"    events=1  game_lines={gl_n}  player_props={pp_n}  credits={_remaining_credits}")
        time.sleep(1.5)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",        choices=["probe", "backfill"], default="backfill")
    parser.add_argument("--sport",       default="all", choices=["nfl", "nba", "mlb", "all"])
    parser.add_argument("--season",      type=int, default=None)
    parser.add_argument("--games",       type=int, default=10)
    parser.add_argument("--quota-floor", type=int, default=50000, dest="quota_floor")
    args = parser.parse_args()

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key:
        raise EnvironmentError("ODDS_API_KEY environment variable is not set.")

    sports = ["nfl", "nba", "mlb"] if args.sport == "all" else [args.sport]
    print(f"Mode: {args.mode}  Sports: {', '.join(sports)}  Quota floor: {args.quota_floor:,}")

    engine = get_engine()
    ensure_schema(engine)

    for sport in sports:
        season_year = args.season or _default_season(sport)
        if args.mode == "probe":
            run_probe(sport, api_key, args.quota_floor, engine)
        else:
            run_backfill(sport, api_key, args.quota_floor, args.games, season_year, engine)

    if _remaining_credits is not None:
        print(f"\nFinal credits remaining: {_remaining_credits:,}")


if __name__ == "__main__":
    main()
