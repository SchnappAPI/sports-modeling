"""
odds_etl.py

Ingests historical odds data from The Odds API v4 into Azure SQL.
Schema: odds

Modes
  discover  -- Scans the full season date range and writes every found event
               into odds.discovered_events. Tracks every scanned date in
               odds.discovered_dates regardless of whether events were found,
               so empty off-days are never re-scanned. Idempotent.
               Costs 1 credit per unscanned date.

  backfill  -- Reads odds.discovered_events as the authoritative game list.
               Diffs against odds.events to find gaps. Processes the oldest
               N missing events (--games N). Never calls the discovery
               endpoint. Discovery and odds fetching are fully decoupled.

  mappings  -- Builds/refreshes odds.team_map, odds.player_map,
               odds.event_game_map. Run after backfill.

  upcoming  -- Fetches current pre-game lines for the next --days-ahead game
               days. Truncates and reloads odds.upcoming_* tables each run.

  probe     -- Coverage discovery pass. Writes to odds.market_probe only.

Featured market routing
  Bulk /odds endpoint:         h2h, spreads, totals only.
  Per-event /events/{id}/odds: all other markets.

Datetime handling
  All datetime values stored in row dicts are naive UTC strings (no tzinfo).
  This prevents pandas from inferring DatetimeTZDtype, which SQL Server's
  ODBC driver incorrectly maps to the TIMESTAMP rowversion type on temp tables.

Parameter binding
  Never use pd.read_sql with named parameters (:name style) against a pyodbc
  engine. pyodbc only understands ? placeholders. All parameterised reads use
  engine.connect() + text() + SQLAlchemy binding instead.

Response shapes
  Bulk /odds:          data["data"] is a LIST of event objects.
  Per-event /odds:     data["data"] is a single event DICT.
  Live bulk /odds:     top-level list (no "data" wrapper).
"""

import argparse
import os
import re
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
BOOKMAKERS   = "fanduel,draftkings"

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
NBA_PROPS = [
    "player_points", "player_rebounds", "player_assists",
    "player_threes", "player_blocks", "player_steals",
    "player_points_rebounds_assists", "player_points_rebounds",
    "player_points_assists", "player_rebounds_assists",
    "player_first_basket", "player_double_double", "player_triple_double",
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
    "pitcher_earned_runs",
]
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
# NBA team name mapping
# ---------------------------------------------------------------------------
NBA_TEAM_NAME_TO_TRICODE = {
    "Atlanta Hawks":          "ATL",
    "Boston Celtics":         "BOS",
    "Brooklyn Nets":          "BKN",
    "Charlotte Hornets":      "CHA",
    "Chicago Bulls":          "CHI",
    "Cleveland Cavaliers":    "CLE",
    "Dallas Mavericks":       "DAL",
    "Denver Nuggets":         "DEN",
    "Detroit Pistons":        "DET",
    "Golden State Warriors":  "GSW",
    "Houston Rockets":        "HOU",
    "Indiana Pacers":         "IND",
    "Los Angeles Clippers":   "LAC",
    "Los Angeles Lakers":     "LAL",
    "Memphis Grizzlies":      "MEM",
    "Miami Heat":             "MIA",
    "Milwaukee Bucks":        "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans":   "NOP",
    "New York Knicks":        "NYK",
    "Oklahoma City Thunder":  "OKC",
    "Orlando Magic":          "ORL",
    "Philadelphia 76ers":     "PHI",
    "Phoenix Suns":           "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings":       "SAC",
    "San Antonio Spurs":      "SAS",
    "Toronto Raptors":        "TOR",
    "Utah Jazz":              "UTA",
    "Washington Wizards":     "WAS",
}

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'odds') EXEC('CREATE SCHEMA odds')",
    # discovered_dates: every date that has been successfully scanned by discover
    # mode, regardless of whether any events were found. This is the skip-list
    # that prevents re-scanning known-empty off-days.
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='discovered_dates')
    CREATE TABLE odds.discovered_dates (
        scan_date   DATE        NOT NULL,
        sport_key   VARCHAR(50) NOT NULL,
        season_year INT         NOT NULL,
        event_count INT         NOT NULL DEFAULT 0,
        scanned_at  DATETIME2   NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_odds_discovered_dates PRIMARY KEY (scan_date, sport_key, season_year)
    )
    """,
    # discovered_events: the authoritative event catalog populated by discover mode.
    # Backfill reads this table instead of calling the discovery endpoint.
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='discovered_events')
    CREATE TABLE odds.discovered_events (
        event_id      VARCHAR(50)  NOT NULL PRIMARY KEY,
        sport_key     VARCHAR(50)  NOT NULL,
        sport_title   VARCHAR(50)  NULL,
        commence_time DATETIME2    NOT NULL,
        home_team     VARCHAR(100) NULL,
        away_team     VARCHAR(100) NULL,
        season_year   INT          NULL,
        discovered_at DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
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
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='team_map')
    CREATE TABLE odds.team_map (
        odds_team_name VARCHAR(100) NOT NULL PRIMARY KEY,
        sport_key      VARCHAR(50)  NOT NULL,
        team_tricode   CHAR(3)      NULL,
        team_id        BIGINT       NULL,
        created_at     DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='player_map')
    CREATE TABLE odds.player_map (
        odds_player_name  VARCHAR(100) NOT NULL,
        sport_key         VARCHAR(50)  NOT NULL,
        player_id         BIGINT       NULL,
        matched_name      VARCHAR(100) NULL,
        match_method      VARCHAR(20)  NULL,
        created_at        DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_odds_player_map PRIMARY KEY (odds_player_name, sport_key)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='event_game_map')
    CREATE TABLE odds.event_game_map (
        event_id      VARCHAR(50)  NOT NULL PRIMARY KEY,
        sport_key     VARCHAR(50)  NOT NULL,
        game_id       VARCHAR(15)  NULL,
        game_date     DATE         NULL,
        home_tricode  CHAR(3)      NULL,
        away_tricode  CHAR(3)      NULL,
        match_method  VARCHAR(30)  NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_events')
    CREATE TABLE odds.upcoming_events (
        event_id      VARCHAR(50)  NOT NULL PRIMARY KEY,
        sport_key     VARCHAR(50)  NOT NULL,
        sport_title   VARCHAR(50)  NULL,
        commence_time DATETIME2    NOT NULL,
        home_team     VARCHAR(100) NULL,
        away_team     VARCHAR(100) NULL,
        home_tricode  CHAR(3)      NULL,
        away_tricode  CHAR(3)      NULL,
        game_id       VARCHAR(15)  NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_game_lines')
    CREATE TABLE odds.upcoming_game_lines (
        event_id        VARCHAR(50)  NOT NULL,
        sport_key       VARCHAR(50)  NOT NULL,
        market_key      VARCHAR(100) NOT NULL,
        bookmaker_key   VARCHAR(50)  NOT NULL,
        bookmaker_title VARCHAR(100) NULL,
        outcome_name    VARCHAR(100) NOT NULL,
        outcome_price   INT          NULL,
        outcome_point   DECIMAL(6,1) NULL,
        snap_ts         DATETIME2    NULL,
        created_at      DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props')
    CREATE TABLE odds.upcoming_player_props (
        event_id        VARCHAR(50)  NOT NULL,
        sport_key       VARCHAR(50)  NOT NULL,
        market_key      VARCHAR(100) NOT NULL,
        bookmaker_key   VARCHAR(50)  NOT NULL,
        bookmaker_title VARCHAR(100) NULL,
        player_name     VARCHAR(100) NOT NULL,
        player_id       BIGINT       NULL,
        outcome_name    VARCHAR(20)  NOT NULL,
        outcome_price   INT          NULL,
        outcome_point   DECIMAL(6,1) NULL,
        snap_ts         DATETIME2    NULL,
        created_at      DATETIME2    NOT NULL DEFAULT GETUTCDATE()
    )
    """,
    # Legacy column renames
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

VIEW_DROP = """
IF OBJECT_ID('odds.vw_nba_player_prop_results', 'V') IS NOT NULL
    DROP VIEW odds.vw_nba_player_prop_results;
"""

VIEW_CREATE = """
CREATE VIEW odds.vw_nba_player_prop_results AS
WITH game_totals AS (
    SELECT
        game_id, player_id,
        SUM(pts)  AS pts,  SUM(reb)  AS reb,  SUM(ast)  AS ast,
        SUM(fg3m) AS fg3m, SUM(blk)  AS blk,  SUM(stl)  AS stl,
        SUM(fgm)  AS fgm,  SUM(fga)  AS fga,  SUM(ftm)  AS ftm,
        SUM(fta)  AS fta,  SUM(oreb) AS oreb, SUM(dreb) AS dreb,
        SUM(tov)  AS tov,  SUM(pf)   AS pf,
        MAX(game_date)    AS game_date,
        MAX(team_id)      AS team_id,
        MAX(team_tricode) AS team_tricode,
        MAX(player_name)  AS player_name
    FROM nba.player_box_score_stats
    GROUP BY game_id, player_id
)
SELECT
    pp.event_id,
    pp.market_key,
    pp.bookmaker_key,
    pp.player_name        AS odds_player_name,
    pm.player_id,
    pm.matched_name,
    egm.game_id,
    gt.game_date,
    gt.team_tricode,
    pp.outcome_name,
    pp.outcome_point      AS line,
    CASE pp.market_key
        WHEN 'player_points'                            THEN CAST(gt.pts  AS DECIMAL(8,1))
        WHEN 'player_points_alternate'                  THEN CAST(gt.pts  AS DECIMAL(8,1))
        WHEN 'player_rebounds'                          THEN CAST(gt.reb  AS DECIMAL(8,1))
        WHEN 'player_rebounds_alternate'                THEN CAST(gt.reb  AS DECIMAL(8,1))
        WHEN 'player_assists'                           THEN CAST(gt.ast  AS DECIMAL(8,1))
        WHEN 'player_assists_alternate'                 THEN CAST(gt.ast  AS DECIMAL(8,1))
        WHEN 'player_threes'                            THEN CAST(gt.fg3m AS DECIMAL(8,1))
        WHEN 'player_threes_alternate'                  THEN CAST(gt.fg3m AS DECIMAL(8,1))
        WHEN 'player_blocks'                            THEN CAST(gt.blk  AS DECIMAL(8,1))
        WHEN 'player_blocks_alternate'                  THEN CAST(gt.blk  AS DECIMAL(8,1))
        WHEN 'player_steals'                            THEN CAST(gt.stl  AS DECIMAL(8,1))
        WHEN 'player_steals_alternate'                  THEN CAST(gt.stl  AS DECIMAL(8,1))
        WHEN 'player_points_rebounds_assists'           THEN CAST(gt.pts + gt.reb + gt.ast AS DECIMAL(8,1))
        WHEN 'player_points_rebounds_assists_alternate' THEN CAST(gt.pts + gt.reb + gt.ast AS DECIMAL(8,1))
        WHEN 'player_points_rebounds'                   THEN CAST(gt.pts + gt.reb AS DECIMAL(8,1))
        WHEN 'player_points_rebounds_alternate'         THEN CAST(gt.pts + gt.reb AS DECIMAL(8,1))
        WHEN 'player_points_assists'                    THEN CAST(gt.pts + gt.ast AS DECIMAL(8,1))
        WHEN 'player_points_assists_alternate'          THEN CAST(gt.pts + gt.ast AS DECIMAL(8,1))
        WHEN 'player_rebounds_assists'                  THEN CAST(gt.reb + gt.ast AS DECIMAL(8,1))
        WHEN 'player_rebounds_assists_alternate'        THEN CAST(gt.reb + gt.ast AS DECIMAL(8,1))
        ELSE NULL
    END AS stat_value,
    CASE
        WHEN pp.outcome_name = 'Over' AND pp.outcome_point IS NOT NULL
        THEN CASE
            WHEN CASE pp.market_key
                WHEN 'player_points'                            THEN CAST(gt.pts  AS DECIMAL(8,1))
                WHEN 'player_points_alternate'                  THEN CAST(gt.pts  AS DECIMAL(8,1))
                WHEN 'player_rebounds'                          THEN CAST(gt.reb  AS DECIMAL(8,1))
                WHEN 'player_rebounds_alternate'                THEN CAST(gt.reb  AS DECIMAL(8,1))
                WHEN 'player_assists'                           THEN CAST(gt.ast  AS DECIMAL(8,1))
                WHEN 'player_assists_alternate'                 THEN CAST(gt.ast  AS DECIMAL(8,1))
                WHEN 'player_threes'                            THEN CAST(gt.fg3m AS DECIMAL(8,1))
                WHEN 'player_threes_alternate'                  THEN CAST(gt.fg3m AS DECIMAL(8,1))
                WHEN 'player_blocks'                            THEN CAST(gt.blk  AS DECIMAL(8,1))
                WHEN 'player_blocks_alternate'                  THEN CAST(gt.blk  AS DECIMAL(8,1))
                WHEN 'player_steals'                            THEN CAST(gt.stl  AS DECIMAL(8,1))
                WHEN 'player_steals_alternate'                  THEN CAST(gt.stl  AS DECIMAL(8,1))
                WHEN 'player_points_rebounds_assists'           THEN CAST(gt.pts + gt.reb + gt.ast AS DECIMAL(8,1))
                WHEN 'player_points_rebounds_assists_alternate' THEN CAST(gt.pts + gt.reb + gt.ast AS DECIMAL(8,1))
                WHEN 'player_points_rebounds'                   THEN CAST(gt.pts + gt.reb AS DECIMAL(8,1))
                WHEN 'player_points_rebounds_alternate'         THEN CAST(gt.pts + gt.reb AS DECIMAL(8,1))
                WHEN 'player_points_assists'                    THEN CAST(gt.pts + gt.ast AS DECIMAL(8,1))
                WHEN 'player_points_assists_alternate'          THEN CAST(gt.pts + gt.ast AS DECIMAL(8,1))
                WHEN 'player_rebounds_assists'                  THEN CAST(gt.reb + gt.ast AS DECIMAL(8,1))
                WHEN 'player_rebounds_assists_alternate'        THEN CAST(gt.reb + gt.ast AS DECIMAL(8,1))
                ELSE NULL
            END > pp.outcome_point THEN 1 ELSE 0
        END
        ELSE NULL
    END AS over_hit,
    pp.snap_ts
FROM odds.player_props   pp
JOIN odds.event_game_map egm ON egm.event_id        = pp.event_id
JOIN odds.player_map     pm  ON pm.odds_player_name = pp.player_name
                             AND pm.sport_key        = pp.sport_key
                             AND pm.player_id IS NOT NULL
JOIN game_totals         gt  ON gt.game_id   = egm.game_id
                             AND gt.player_id = pm.player_id
WHERE pp.sport_key    = 'basketball_nba'
  AND pp.outcome_name IN ('Over', 'Under')
  AND pp.outcome_point IS NOT NULL
  AND egm.game_id IS NOT NULL;
"""


def ensure_schema(engine):
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))
        conn.execute(text(VIEW_DROP))
        conn.execute(text(VIEW_CREATE))


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


def _query_rows(engine, sql, params):
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
# Event discovery API call
# ---------------------------------------------------------------------------

def _api_discover_date(sport_key, target_date, api_key, quota_floor):
    """Call the historical events endpoint for one calendar date. Returns list."""
    _check_quota(quota_floor)
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/events",
        {
            "apiKey":           api_key,
            "date":             f"{target_date}T12:00:00Z",
            "commenceTimeFrom": f"{target_date}T00:00:00Z",
            "commenceTimeTo":   f"{target_date}T23:59:59Z",
        },
        quota_floor,
    )
    return (data.get("data") or []) if data else []


# ---------------------------------------------------------------------------
# DISCOVER mode
# ---------------------------------------------------------------------------

def run_discover(sport, api_key, quota_floor, season_year, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Discover: {sport.upper()} Season {season_year} ===")

    start_date, end_date = _season_date_range(sport, season_year)
    end_date = min(end_date, date.today() - timedelta(days=1))
    if start_date > end_date:
        print("  No past dates in range. Nothing to do.")
        return

    all_dates = _date_list(start_date, end_date)

    # Load the explicit scan log. Every date that has been successfully
    # queried (whether or not events were found) is in this table.
    # On first run after the table is created, backfill it from
    # discovered_events so dates already scanned in the previous run
    # are not re-scanned.
    with engine.begin() as conn:
        # Backfill discovered_dates from discovered_events for dates
        # that predate the discovered_dates table.
        conn.execute(text("""
            INSERT INTO odds.discovered_dates (scan_date, sport_key, season_year, event_count)
            SELECT
                CAST(commence_time AS DATE),
                sport_key,
                season_year,
                COUNT(*)
            FROM odds.discovered_events
            WHERE sport_key   = :sk
              AND season_year = :sy
              AND NOT EXISTS (
                  SELECT 1 FROM odds.discovered_dates d
                  WHERE d.scan_date   = CAST(odds.discovered_events.commence_time AS DATE)
                    AND d.sport_key   = odds.discovered_events.sport_key
                    AND d.season_year = odds.discovered_events.season_year
              )
            GROUP BY CAST(commence_time AS DATE), sport_key, season_year
        """), {"sk": sport_key, "sy": season_year})

    with engine.connect() as conn:
        scanned_rows = conn.execute(
            text("""
                SELECT scan_date FROM odds.discovered_dates
                WHERE sport_key = :sk AND season_year = :sy
            """),
            {"sk": sport_key, "sy": season_year},
        ).fetchall()

    scanned_dates = set()
    for r in scanned_rows:
        val = r[0]
        if val is not None:
            scanned_dates.add(str(val.date()) if hasattr(val, "date") else str(val)[:10])

    missing_dates = [d for d in all_dates if str(d) not in scanned_dates]
    if not missing_dates:
        print(f"  All {len(all_dates)} dates already scanned. Nothing to do.")
        return

    print(f"  {len(all_dates)} total dates, {len(scanned_dates)} already scanned, "
          f"{len(missing_dates)} to scan (1s sleep between calls).")

    total_found = 0
    for i, d in enumerate(missing_dates):
        events = _api_discover_date(sport_key, d, api_key, quota_floor)
        n_found = 0
        if events:
            rows = [
                {
                    "event_id":      ev.get("id"),
                    "sport_key":     sport_key,
                    "sport_title":   ev.get("sport_title"),
                    "commence_time": _to_utc_str(ev.get("commence_time")),
                    "home_team":     ev.get("home_team"),
                    "away_team":     ev.get("away_team"),
                    "season_year":   season_year,
                }
                for ev in events if ev.get("id")
            ]
            if rows:
                upsert(engine, clean_dataframe(pd.DataFrame(rows)),
                       schema="odds", table="discovered_events", keys=["event_id"])
                n_found = len(rows)
                total_found += n_found
                print(f"  {d}: {n_found} events stored.")
        else:
            print(f"  {d}: no events.")

        # Always mark this date as scanned, even when empty.
        # This is what prevents re-scanning on the next discover run.
        with engine.begin() as conn:
            conn.execute(text("""
                MERGE odds.discovered_dates AS t
                USING (VALUES (:sd, :sk, :sy, :ec)) AS s (scan_date, sport_key, season_year, event_count)
                ON t.scan_date = s.scan_date AND t.sport_key = s.sport_key AND t.season_year = s.season_year
                WHEN MATCHED     THEN UPDATE SET t.event_count = s.event_count
                WHEN NOT MATCHED THEN INSERT (scan_date, sport_key, season_year, event_count)
                                      VALUES (s.scan_date, s.sport_key, s.season_year, s.event_count);
            """), {"sd": str(d), "sk": sport_key, "sy": season_year, "ec": n_found})

        if i < len(missing_dates) - 1:
            time.sleep(1.0)

    print(f"\n  Discover complete. {total_found} events stored across {len(missing_dates)} dates scanned.")
    if _remaining_credits is not None:
        print(f"  Credits remaining: {_remaining_credits:,}")


# ---------------------------------------------------------------------------
# Odds fetching (historical)
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

def _parse_bookmakers(event_obj, event_id, sport_key, snap_ts_raw):
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
# BACKFILL mode
# ---------------------------------------------------------------------------

def _load_probe_results(engine, sport_key):
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


def run_backfill(sport, api_key, quota_floor, games_limit, season_year, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Backfill: {sport.upper()} Season {season_year} ===")

    probe        = _load_probe_results(engine, sport_key)
    event_feat   = _filter_markets(probe, EVENT_FEATURED_MARKETS[sport], "event_featured")
    prop_markets = _filter_markets(probe, PROP_MARKETS[sport], "prop")
    alt_markets  = _filter_markets(probe, ALT_PROP_MARKETS[sport], "alt_prop")

    with engine.connect() as conn:
        discovered = conn.execute(
            text("""
                SELECT event_id, sport_title, commence_time, home_team, away_team
                FROM odds.discovered_events
                WHERE sport_key = :sk AND season_year = :sy
                  AND commence_time < GETUTCDATE()
            """),
            {"sk": sport_key, "sy": season_year},
        ).fetchall()

    if not discovered:
        print("  odds.discovered_events is empty for this sport/season.")
        print("  Run --mode discover first to populate the event catalog.")
        return

    with engine.connect() as conn:
        loaded_ids = {str(r[0]) for r in conn.execute(
            text("SELECT event_id FROM odds.events WHERE sport_key = :sk AND season_year = :sy"),
            {"sk": sport_key, "sy": season_year},
        ).fetchall()}

    missing = [
        {"event_id": r[0], "sport_title": r[1],
         "commence_time": r[2], "home_team": r[3], "away_team": r[4]}
        for r in discovered if str(r[0]) not in loaded_ids
    ]

    if not missing:
        print(f"  All {len(discovered)} discovered events loaded. Nothing to do.")
        return

    missing.sort(key=lambda e: str(e["commence_time"]))
    work = missing[:games_limit]
    print(f"  Discovered: {len(discovered)}  Loaded: {len(loaded_ids)}  "
          f"Missing: {len(missing)}  Processing: {len(work)} (oldest first).")

    for ev in work:
        eid   = ev["event_id"]
        ctime = ev["commence_time"]
        event = {
            "id":            eid,
            "sport_title":   ev["sport_title"],
            "commence_time": str(ctime) if not isinstance(ctime, str) else ctime,
            "home_team":     ev["home_team"],
            "away_team":     ev["away_team"],
        }
        cdt  = _cdt(event)
        snap = _snap_iso(event["commence_time"])
        label = f"{ev['away_team'] or ''} @ {ev['home_team'] or ''} ({cdt.date() if cdt else '?'})"
        print(f"\n  {label}")

        if not snap:
            print("    No snapshot time. Skipping.")
            continue

        gl_all, pp_all = [], []

        bulk_data, bulk_ts = _fetch_bulk(sport_key, snap, BULK_FEATURED_MARKETS, api_key, quota_floor)
        ev_obj = next((e for e in bulk_data if e.get("id") == eid), None)
        if ev_obj:
            gl, pp = _parse_bookmakers(ev_obj, eid, sport_key, bulk_ts)
            gl_all.extend(gl); pp_all.extend(pp)
        else:
            print("    Not found in bulk response.")

        if event_feat:
            ef_obj, ef_ts = _fetch_event(sport_key, eid, snap, event_feat, api_key, quota_floor)
            if ef_obj:
                gl, pp = _parse_bookmakers(ef_obj, eid, sport_key, ef_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)

        if cdt and cdt >= PROPS_CUTOFF:
            if prop_markets:
                p_obj, p_ts = _fetch_event(sport_key, eid, snap, prop_markets, api_key, quota_floor)
                if p_obj:
                    gl, pp = _parse_bookmakers(p_obj, eid, sport_key, p_ts)
                    gl_all.extend(gl); pp_all.extend(pp)
                time.sleep(1.5)
            if alt_markets:
                a_obj, a_ts = _fetch_event(sport_key, eid, snap, alt_markets, api_key, quota_floor)
                if a_obj:
                    gl, pp = _parse_bookmakers(a_obj, eid, sport_key, a_ts)
                    gl_all.extend(gl); pp_all.extend(pp)
                time.sleep(1.5)
        else:
            print("    Before props cutoff. Skipping prop calls.")

        upsert(engine,
               clean_dataframe(pd.DataFrame([{
                   "event_id":      eid,
                   "sport_key":     sport_key,
                   "sport_title":   ev["sport_title"],
                   "commence_time": _to_utc_str(event["commence_time"]),
                   "home_team":     ev["home_team"],
                   "away_team":     ev["away_team"],
                   "season_year":   season_year,
               }])),
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
# PROBE mode
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
        for offset in range(8):
            check = td + timedelta(days=offset)
            evs = _api_discover_date(sport_key, check, api_key, quota_floor)
            if evs:
                events_by_date[check] = evs
                break

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

    all_markets = ALL_FEATURED_MARKETS[sport] + PROP_MARKETS[sport] + ALT_PROP_MARKETS[sport]
    coverage    = {m: {"bk_set": set(), "outcomes": 0, "hits": 0} for m in all_markets}
    sample_ids, sample_dates = [], []

    for event in events:
        eid = event.get("id")
        cdt = _cdt(event)
        if cdt:
            sample_dates.append(str(cdt.date()))
        snap = _snap_iso(event.get("commence_time"))
        if not snap:
            continue
        sample_ids.append(eid)

        def _tally(event_obj):
            if not event_obj:
                return
            for bk in event_obj.get("bookmakers") or []:
                for mkt in bk.get("markets") or []:
                    mk = mkt.get("key")
                    if mk in coverage:
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
            p_obj, _ = _fetch_event(sport_key, eid, snap, PROP_MARKETS[sport], api_key, quota_floor)
            _tally(p_obj)
            time.sleep(1.5)
            a_obj, _ = _fetch_event(sport_key, eid, snap, ALT_PROP_MARKETS[sport], api_key, quota_floor)
            _tally(a_obj)
            time.sleep(1.5)
        else:
            print(f"    {eid}: before props cutoff.")

    probed_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    ids_str   = ",".join(str(i) for i in sample_ids if i)[:500]
    dates_str = ",".join(sorted(set(sample_dates)))[:200]
    rows = []
    print(f"\n=== {sport.upper()} Coverage ===")
    for mkt in all_markets:
        cov = coverage[mkt]
        covered = cov["hits"] >= 3
        bks = sorted(cov["bk_set"])
        mtype = (
            "bulk_featured" if mkt in BULK_FEATURED_MARKETS
            else "event_featured" if mkt in EVENT_FEATURED_MARKETS[sport]
            else "alt_prop" if mkt in ALT_PROP_MARKETS[sport]
            else "prop"
        )
        print(f"  {'COVERED    ' if covered else 'NOT COVERED'} {mkt:<45} "
              f"{len(bks)} books  {cov['outcomes']} outcomes")
        rows.append({
            "sport_key": sport_key, "market_key": mkt, "market_type": mtype,
            "bookmaker_count": len(bks), "outcome_count": cov["outcomes"],
            "is_covered": 1 if covered else 0,
            "covered_bookmakers": ",".join(bks)[:200],
            "sample_event_ids": ids_str, "sample_dates": dates_str,
            "probed_at": probed_at,
        })
    upsert(engine, clean_dataframe(pd.DataFrame(rows)),
           schema="odds", table="market_probe", keys=["sport_key", "market_key"])
    print(f"  Written {len(rows)} rows to odds.market_probe.")


# ---------------------------------------------------------------------------
# MAPPINGS mode
# ---------------------------------------------------------------------------

def _normalize_name(name):
    if not name:
        return ""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", name.lower())).strip()


def run_mappings(sport, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Mappings: {sport.upper()} ===")

    if sport == "nba":
        with engine.connect() as conn:
            tricode_to_id = {r[0]: r[1] for r in conn.execute(
                text("SELECT team_tricode, team_id FROM nba.teams")
            ).fetchall()}
        team_rows = [
            {"odds_team_name": n, "sport_key": sport_key,
             "team_tricode": tc, "team_id": tricode_to_id.get(tc)}
            for n, tc in NBA_TEAM_NAME_TO_TRICODE.items()
        ]
        upsert(engine, clean_dataframe(pd.DataFrame(team_rows)),
               schema="odds", table="team_map", keys=["odds_team_name"])
        print(f"  team_map: {len(team_rows)} rows.")

        with engine.connect() as conn:
            db_players = conn.execute(
                text("SELECT player_id, player_name FROM nba.players")
            ).fetchall()
        norm_to_pid  = {_normalize_name(n): pid  for pid, n in db_players}
        norm_to_name = {_normalize_name(n): n    for _, n  in db_players}

        with engine.connect() as conn:
            hist_names = [r[0] for r in conn.execute(
                text("SELECT DISTINCT player_name FROM odds.player_props WHERE sport_key = :sk"),
                {"sk": sport_key},
            ).fetchall() if r[0]]
            upco_names = [r[0] for r in conn.execute(
                text("SELECT DISTINCT player_name FROM odds.upcoming_player_props WHERE sport_key = :sk"),
                {"sk": sport_key},
            ).fetchall() if r[0]]
        all_names = list(set(hist_names + upco_names))

        pm_rows = []
        matched = unmatched = 0
        for oname in all_names:
            norm  = _normalize_name(oname)
            pid   = norm_to_pid.get(norm)
            mname = norm_to_name.get(norm)
            if pid: matched += 1
            else:
                unmatched += 1
                print(f"  [no_match] {oname!r}")
            pm_rows.append({"odds_player_name": oname, "sport_key": sport_key,
                            "player_id": pid, "matched_name": mname,
                            "match_method": "exact" if pid else "no_match"})
        if pm_rows:
            upsert(engine, clean_dataframe(pd.DataFrame(pm_rows)),
                   schema="odds", table="player_map", keys=["odds_player_name", "sport_key"])
            print(f"  player_map: {len(pm_rows)} rows ({matched} matched, {unmatched} unmatched).")

        with engine.connect() as conn:
            unmapped = conn.execute(
                text("""
                    SELECT e.event_id, e.commence_time, e.home_team, e.away_team
                    FROM odds.events e
                    LEFT JOIN odds.event_game_map m ON m.event_id = e.event_id
                    WHERE e.sport_key = :sk AND m.event_id IS NULL
                """),
                {"sk": sport_key},
            ).fetchall()

        if not unmapped:
            print("  event_game_map: all events already mapped.")
        else:
            with engine.connect() as conn:
                nba_games = conn.execute(
                    text("SELECT game_id, game_date, home_team_tricode, away_team_tricode FROM nba.games")
                ).fetchall()
            game_lookup = {(str(gdate), htc): gid for gid, gdate, htc, atc in nba_games}

            with engine.connect() as conn:
                name_to_tc = {r[0]: r[1] for r in conn.execute(
                    text("SELECT odds_team_name, team_tricode FROM odds.team_map WHERE sport_key = :sk"),
                    {"sk": sport_key},
                ).fetchall()}

            egm_rows = []
            matched = unmatched = 0
            for eid, ctime, home_name, away_name in unmapped:
                try:
                    ctime_dt   = datetime.fromisoformat(str(ctime).replace("Z", "+00:00")) if isinstance(ctime, str) else ctime
                    ctime_date = str(ctime_dt.date()) if hasattr(ctime_dt, "date") else str(ctime_dt)[:10]
                except Exception:
                    ctime_date = None
                home_tc  = name_to_tc.get(home_name)
                away_tc  = name_to_tc.get(away_name)
                game_id  = game_lookup.get((ctime_date, home_tc)) if (ctime_date and home_tc) else None
                if game_id: matched += 1
                else:       unmatched += 1
                egm_rows.append({
                    "event_id": eid, "sport_key": sport_key, "game_id": game_id,
                    "game_date": ctime_date, "home_tricode": home_tc,
                    "away_tricode": away_tc,
                    "match_method": "date_home_tricode" if game_id else "unmatched",
                })
            upsert(engine, clean_dataframe(pd.DataFrame(egm_rows)),
                   schema="odds", table="event_game_map", keys=["event_id"])
            print(f"  event_game_map: {len(egm_rows)} rows ({matched} matched, {unmatched} unmatched).")


# ---------------------------------------------------------------------------
# UPCOMING mode
# ---------------------------------------------------------------------------

def _fetch_upcoming_bulk(sport_key, markets, api_key, quota_floor):
    _check_quota(quota_floor)
    data, _ = _request(
        f"{BASE_URL}/v4/sports/{sport_key}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american"},
        quota_floor,
    )
    if data is None: return []
    if isinstance(data, list): return data
    return data.get("data") or []


def _fetch_upcoming_event(sport_key, event_id, markets, api_key, quota_floor):
    _check_quota(quota_floor)
    data, _ = _request(
        f"{BASE_URL}/v4/sports/{sport_key}/events/{event_id}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american"},
        quota_floor,
    )
    if data is None: return None, None
    if isinstance(data, dict) and "bookmakers" in data: return data, None
    return data.get("data"), None


def run_upcoming(sport, api_key, quota_floor, days_ahead, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Upcoming: {sport.upper()} (next {days_ahead} game day(s)) ===")

    probe        = _load_probe_results(engine, sport_key)
    event_feat   = _filter_markets(probe, EVENT_FEATURED_MARKETS[sport], "event_featured")
    prop_markets = _filter_markets(probe, PROP_MARKETS[sport], "prop")
    alt_markets  = _filter_markets(probe, ALT_PROP_MARKETS[sport], "alt_prop")

    all_upcoming = _fetch_upcoming_bulk(sport_key, ["h2h"], api_key, quota_floor)
    if not all_upcoming:
        print("  No upcoming events found.")
        return

    now_utc = datetime.now(tz=timezone.utc)
    cutoff  = now_utc + timedelta(days=days_ahead)
    in_window = [ev for ev in all_upcoming if _cdt(ev) and _cdt(ev) <= cutoff]
    if not in_window:
        print("  No events within window.")
        return

    if days_ahead <= 1:
        earliest = min(_cdt(ev).date() for ev in in_window if _cdt(ev))
        in_window = [ev for ev in in_window if _cdt(ev) and _cdt(ev).date() == earliest]
        print(f"  Next game day: {earliest} ({len(in_window)} events).")
    else:
        print(f"  {len(in_window)} events in window.")

    with engine.begin() as conn:
        for tbl in ("upcoming_player_props", "upcoming_game_lines", "upcoming_events"):
            conn.execute(text(f"DELETE FROM odds.{tbl} WHERE sport_key = :sk"), {"sk": sport_key})

    snap_ts = _to_utc_str(now_utc)

    if sport == "nba":
        with engine.connect() as conn:
            name_to_tc = {r[0]: r[1] for r in conn.execute(
                text("SELECT odds_team_name, team_tricode FROM odds.team_map WHERE sport_key = :sk"),
                {"sk": sport_key},
            ).fetchall()}
            future_lookup = {(str(r[1]), r[2]): r[0] for r in conn.execute(
                text("SELECT game_id, game_date, home_team_tricode FROM nba.schedule WHERE game_date >= :today"),
                {"today": date.today()},
            ).fetchall()}
            pid_map = {r[0]: r[1] for r in conn.execute(
                text("SELECT odds_player_name, player_id FROM odds.player_map WHERE sport_key = :sk AND player_id IS NOT NULL"),
                {"sk": sport_key},
            ).fetchall()}
    else:
        name_to_tc = future_lookup = pid_map = {}

    gl_total = pp_total = 0
    for event in in_window:
        eid       = event.get("id")
        cdt       = _cdt(event)
        home_name = event.get("home_team")
        away_name = event.get("away_team")
        home_tc   = name_to_tc.get(home_name)
        away_tc   = name_to_tc.get(away_name)
        cdt_date  = str(cdt.date()) if cdt else None
        game_id   = future_lookup.get((cdt_date, home_tc)) if (cdt_date and home_tc) else None
        print(f"\n  {away_name or ''} @ {home_name or ''} ({cdt_date or '?'})")

        gl_all, pp_all = [], []
        bulk_data = _fetch_upcoming_bulk(sport_key, BULK_FEATURED_MARKETS, api_key, quota_floor)
        ev_obj = next((e for e in bulk_data if e.get("id") == eid), None)
        if ev_obj:
            gl, pp = _parse_bookmakers(ev_obj, eid, sport_key, snap_ts)
            gl_all.extend(gl); pp_all.extend(pp)
        time.sleep(1.5)
        if event_feat:
            ef_obj, _ = _fetch_upcoming_event(sport_key, eid, event_feat, api_key, quota_floor)
            if ef_obj:
                gl, pp = _parse_bookmakers(ef_obj, eid, sport_key, snap_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)
        if prop_markets:
            p_obj, _ = _fetch_upcoming_event(sport_key, eid, prop_markets, api_key, quota_floor)
            if p_obj:
                gl, pp = _parse_bookmakers(p_obj, eid, sport_key, snap_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)
        if alt_markets:
            a_obj, _ = _fetch_upcoming_event(sport_key, eid, alt_markets, api_key, quota_floor)
            if a_obj:
                gl, pp = _parse_bookmakers(a_obj, eid, sport_key, snap_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)

        upsert(engine, clean_dataframe(pd.DataFrame([{
            "event_id": eid, "sport_key": sport_key,
            "sport_title": event.get("sport_title"),
            "commence_time": _to_utc_str(event.get("commence_time")),
            "home_team": home_name, "away_team": away_name,
            "home_tricode": home_tc, "away_tricode": away_tc, "game_id": game_id,
        }])), schema="odds", table="upcoming_events", keys=["event_id"])

        if gl_all:
            upsert(engine, clean_dataframe(pd.DataFrame(gl_all)),
                   schema="odds", table="upcoming_game_lines",
                   keys=["event_id", "market_key", "bookmaker_key", "outcome_name"])
            gl_total += len(gl_all)
        if pp_all:
            pp_df = pd.DataFrame(pp_all)
            pp_df["player_id"] = pp_df["player_name"].map(pid_map)
            upsert(engine, clean_dataframe(pp_df),
                   schema="odds", table="upcoming_player_props",
                   keys=["event_id", "market_key", "bookmaker_key", "player_name", "outcome_name"])
            pp_total += len(pp_all)

        print(f"    game_lines={len(gl_all)}  player_props={len(pp_all)}  "
              f"game_id={game_id or 'not resolved'}  credits={_remaining_credits}")

    print(f"\n  Totals: {len(in_window)} events  game_lines={gl_total}  player_props={pp_total}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",
                        choices=["discover", "probe", "backfill", "mappings", "upcoming"],
                        default="backfill")
    parser.add_argument("--sport",      default="all", choices=["nfl", "nba", "mlb", "all"])
    parser.add_argument("--season",     type=int, default=None)
    parser.add_argument("--games",      type=int, default=10,
                        help="Backfill mode: max events to process per run.")
    parser.add_argument("--days-ahead", type=int, default=1, dest="days_ahead",
                        help="Upcoming mode: calendar days ahead. Default 1 = next game day.")
    parser.add_argument("--quota-floor",type=int, default=50000, dest="quota_floor")
    args = parser.parse_args()

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key and args.mode not in ("mappings",):
        raise EnvironmentError("ODDS_API_KEY environment variable is not set.")

    sports = ["nfl", "nba", "mlb"] if args.sport == "all" else [args.sport]
    print(f"Mode: {args.mode}  Sports: {', '.join(sports)}  Quota floor: {args.quota_floor:,}")

    engine = get_engine()
    ensure_schema(engine)

    for sport in sports:
        season_year = args.season or _default_season(sport)
        if args.mode == "discover":
            run_discover(sport, api_key, args.quota_floor, season_year, engine)
        elif args.mode == "probe":
            run_probe(sport, api_key, args.quota_floor, engine)
        elif args.mode == "backfill":
            run_backfill(sport, api_key, args.quota_floor, args.games, season_year, engine)
        elif args.mode == "mappings":
            run_mappings(sport, engine)
        elif args.mode == "upcoming":
            run_upcoming(sport, api_key, args.quota_floor, args.days_ahead, engine)

    if _remaining_credits is not None:
        print(f"\nFinal credits remaining: {_remaining_credits:,}")


if __name__ == "__main__":
    main()
