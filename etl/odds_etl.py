"""
odds_etl.py

Ingests historical odds data from The Odds API v4 into Azure SQL.
Schema: odds

Modes
  discover  -- Walks backward through the historical snapshot chain one
               calendar date at a time. For each date it requests a snapshot
               at T23:59:59Z, stores whatever events are returned, then
               subtracts exactly 1 calendar day and pins to T23:59:59Z for
               the next request. This eliminates timestamp drift, costs
               1 credit per calendar date, and guarantees each request always
               lands at the same time of day regardless of what the API
               returned. Progress is checkpointed in odds.discover_cursors.

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
  Per-event /events/{id}/odds: all other markets (game period + props + alts).

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
  Historical /events:  top-level dict with keys timestamp, previous_timestamp,
                       next_timestamp, data (list of event objects).

Snapshot timing
  Historical backfill uses commence_time - 1 minute as the snapshot request
  time. The API returns the snapshot at or before the requested time, so this
  yields the closest available pre-game line for each event.

Bookmaker
  FanDuel only (bookmakers=fanduel). DraftKings not stored.

Budget tracking
  --budget caps credits spent during this process invocation. Spend is
  computed as (current x-requests-used) - (x-requests-used at first response).
  This is independent of account balance, never needs updating between runs,
  and stops a runaway loop from consuming more than intended.

Discover deduplication
  The historical events endpoint uses a sliding window. Adjacent snapshot
  requests can return the same event_id in multiple responses within a single
  batch. The upsert MERGE requires each source row to match at most one target
  row, so the rows list is deduplicated by event_id before staging.
"""

import argparse
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

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

BASE_URL   = "https://api.the-odds-api.com"
EASTERN_TZ = ZoneInfo("America/New_York")

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
BOOKMAKERS   = "fanduel"

# ---------------------------------------------------------------------------
# Market constants
#
# These lists are derived from the AllMarkets audit run against live FanDuel
# data (March-April 2026). GamePeriod markets that are genuinely team-level
# (team_totals and all variants) are kept here for completeness but routed to
# game_lines rather than player_props by the parser.
# ---------------------------------------------------------------------------

# The three core markets sent via the cheaper bulk /odds endpoint.
# One bulk call returns all games on the slate.
BULK_FEATURED_MARKETS = ["h2h", "spreads", "totals"]

# ---------------------------------------------------------------------------
# NFL markets
# ---------------------------------------------------------------------------
NFL_EVENT_GAME_PERIOD = [
    # moneyline
    "h2h_h1", "h2h_h2",
    "h2h_q1", "h2h_q2", "h2h_q3", "h2h_q4",
    # spreads
    "spreads_h1", "spreads_h2",
    "spreads_q1", "spreads_q2", "spreads_q3", "spreads_q4",
    # totals
    "totals_h1", "totals_h2",
    "totals_q1", "totals_q2", "totals_q3", "totals_q4",
    # team totals
    "team_totals", "team_totals_h1", "team_totals_h2",
    # alternate game lines
    "alternate_spreads", "alternate_spreads_h1", "alternate_spreads_h2",
    "alternate_spreads_q1", "alternate_spreads_q2", "alternate_spreads_q3", "alternate_spreads_q4",
    "alternate_totals", "alternate_totals_h1", "alternate_totals_h2",
    "alternate_totals_q1", "alternate_totals_q2", "alternate_totals_q3", "alternate_totals_q4",
    "alternate_team_totals",
]

NFL_PROPS = [
    "player_pass_yds", "player_pass_tds", "player_pass_attempts",
    "player_pass_completions", "player_pass_interceptions",
    "player_pass_longest_completion",
    "player_rush_yds", "player_rush_longest", "player_rush_attempts",
    "player_reception_yds", "player_receptions", "player_reception_longest",
    "player_pass_rush_yds", "player_rush_reception_yds",
    "player_1st_td", "player_anytime_td", "player_last_td",
    "player_sacks",
]

NFL_ALT_PROPS = [
    "player_pass_yds_alternate", "player_pass_tds_alternate",
    "player_pass_attempts_alternate", "player_pass_completions_alternate",
    "player_pass_interceptions_alternate",
    "player_rush_yds_alternate", "player_rush_attempts_alternate",
    "player_reception_yds_alternate", "player_receptions_alternate",
    "player_rush_reception_yds_alternate",
]

# ---------------------------------------------------------------------------
# NBA markets
# ---------------------------------------------------------------------------
NBA_EVENT_GAME_PERIOD = [
    # moneyline
    "h2h_h1", "h2h_h2",
    "h2h_q1", "h2h_q2", "h2h_q3", "h2h_q4",
    # spreads
    "spreads_h1",
    "spreads_q1", "spreads_q2", "spreads_q3", "spreads_q4",
    # totals
    "totals_h1",
    "totals_q1", "totals_q2", "totals_q3", "totals_q4",
    # team totals
    "team_totals", "team_totals_h1",
    "team_totals_q1", "team_totals_q2", "team_totals_q3", "team_totals_q4",
    # alternate game lines
    "alternate_spreads", "alternate_totals",
    "alternate_team_totals",
]

NBA_PROPS = [
    "player_points", "player_points_q1",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
    "player_points_rebounds_assists",
    "player_points_rebounds",
    "player_points_assists",
    "player_rebounds_assists",
    "player_first_basket", "player_first_team_basket",
    "player_double_double", "player_triple_double",
]

NBA_ALT_PROPS = [
    "player_points_alternate",
    "player_rebounds_alternate",
    "player_assists_alternate",
    "player_blocks_alternate",
    "player_steals_alternate",
    "player_threes_alternate",
    "player_points_assists_alternate",
    "player_points_rebounds_alternate",
    "player_rebounds_assists_alternate",
    "player_points_rebounds_assists_alternate",
]

# ---------------------------------------------------------------------------
# MLB markets
# ---------------------------------------------------------------------------
MLB_EVENT_GAME_PERIOD = [
    # moneyline
    "h2h_1st_5_innings", "h2h_1st_7_innings",
    # spreads
    "spreads_1st_5_innings", "spreads_1st_7_innings",
    # totals
    "totals_1st_5_innings", "totals_1st_7_innings",
    # team totals
    "team_totals",
    # alternate game lines
    "alternate_spreads", "alternate_spreads_1st_5_innings",
    "alternate_totals", "alternate_totals_1st_5_innings",
    "alternate_team_totals",
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
    "batter_runs_scored_alternate",
    "batter_singles_alternate", "batter_doubles_alternate", "batter_triples_alternate",
    "pitcher_strikeouts_alternate",
]

# ---------------------------------------------------------------------------
# Aggregated market dicts
# ---------------------------------------------------------------------------

ALL_FEATURED_MARKETS = {
    "nfl": BULK_FEATURED_MARKETS + NFL_EVENT_GAME_PERIOD,
    "nba": BULK_FEATURED_MARKETS + NBA_EVENT_GAME_PERIOD,
    "mlb": BULK_FEATURED_MARKETS + MLB_EVENT_GAME_PERIOD,
}
EVENT_FEATURED_MARKETS = {
    "nfl": NFL_EVENT_GAME_PERIOD,
    "nba": NBA_EVENT_GAME_PERIOD,
    "mlb": MLB_EVENT_GAME_PERIOD,
}
PROP_MARKETS     = {"nfl": NFL_PROPS,     "nba": NBA_PROPS,     "mlb": MLB_PROPS}
ALT_PROP_MARKETS = {"nfl": NFL_ALT_PROPS, "nba": NBA_ALT_PROPS, "mlb": MLB_ALT_PROPS}

# Markets that are always team-level, never player-level.
# The odds API populates the outcome description with a team name for these,
# which would otherwise cause them to be misrouted into player_props.
TEAM_LEVEL_MARKETS = {
    "team_totals",
    "team_totals_h1", "team_totals_h2",
    "team_totals_q1", "team_totals_q2", "team_totals_q3", "team_totals_q4",
    "alternate_team_totals",
}

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

    """
    IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                   WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='discover_cursors')
    CREATE TABLE odds.discover_cursors (
        sport_key            VARCHAR(50)  NOT NULL,
        season_year          INT          NOT NULL,
        oldest_snapshot_ts   VARCHAR(30)  NOT NULL,
        snapshots_walked     INT          NOT NULL DEFAULT 0,
        events_found         INT          NOT NULL DEFAULT 0,
        last_walked_at       DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_odds_discover_cursors PRIMARY KEY (sport_key, season_year)
    )
    """,
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
    # Legacy column renames kept for idempotency on existing databases
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
    """Return a naive UTC string 'YYYY-MM-DD HH:MM:SS', or None."""
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


def _parse_iso(ts_str):
    """Parse an ISO 8601 string (with or without Z) to a timezone-aware UTC datetime."""
    if not ts_str:
        return None
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except Exception:
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
# Budget tracking
#
# _used_at_start  -- x-requests-used value captured from the first API
#                    response of this process invocation. None until set.
# _used_current   -- most recent x-requests-used value seen.
# _budget         -- maximum credits this invocation is allowed to spend.
#                    0 = unlimited.
#
# Spend = _used_current - _used_at_start.
# Checked after every successful API response via _check_budget().
# ---------------------------------------------------------------------------

_used_at_start  = None
_used_current   = None
_budget         = 0       # set from --budget arg in main()


def _check_budget():
    """Exit if this invocation has spent more than _budget credits."""
    if _budget <= 0:
        return
    if _used_at_start is None or _used_current is None:
        return
    spent = _used_current - _used_at_start
    if spent >= _budget:
        print(f"BUDGET REACHED: spent {spent:,} credits this run (budget={_budget:,}). Stopping.")
        sys.exit(0)


def _record_quota_headers(headers):
    """Update global tracking from API response headers."""
    global _used_at_start, _used_current
    used_str = headers.get("x-requests-used") if headers else None
    if used_str is None:
        return
    try:
        used = int(used_str)
    except (ValueError, TypeError):
        return
    if _used_at_start is None:
        _used_at_start = used
    _used_current = used


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _request(url, params, retries=3):
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

        remaining = resp.headers.get("x-requests-remaining")
        used      = resp.headers.get("x-requests-used")
        last      = resp.headers.get("x-requests-last")

        if resp.status_code == 200:
            _record_quota_headers(resp.headers)
            spent = (_used_current - _used_at_start) if (_used_at_start and _used_current) else 0
            print(f"    [quota] remaining={remaining}  used={used}  last={last}  spent_this_run={spent}")
            _check_budget()
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


def _query_rows(engine, sql, params):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        return result.fetchall()


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------

def _default_season(sport):
    today = date.today()
    start_month, _ = SEASON_MONTHS[sport]
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


# ---------------------------------------------------------------------------
# DISCOVER mode
# ---------------------------------------------------------------------------

def _api_discover_snapshot(sport_key, snapshot_ts_iso, api_key):
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/events",
        {"apiKey": api_key, "date": snapshot_ts_iso},
    )
    return data


def _load_cursor(engine, sport_key, season_year):
    rows = _query_rows(
        engine,
        "SELECT oldest_snapshot_ts FROM odds.discover_cursors "
        "WHERE sport_key = :sk AND season_year = :sy",
        {"sk": sport_key, "sy": season_year},
    )
    return rows[0][0] if rows else None


def _save_cursor(engine, sport_key, season_year, oldest_ts_iso, snapshots_delta, events_delta):
    with engine.begin() as conn:
        conn.execute(text("""
            MERGE odds.discover_cursors AS t
            USING (VALUES (:sk, :sy, :ots, :sw, :ef)) AS s
                (sport_key, season_year, oldest_snapshot_ts, snapshots_walked, events_found)
            ON t.sport_key = s.sport_key AND t.season_year = s.season_year
            WHEN MATCHED THEN UPDATE SET
                t.oldest_snapshot_ts = s.oldest_snapshot_ts,
                t.snapshots_walked   = t.snapshots_walked + s.snapshots_walked,
                t.events_found       = t.events_found + s.events_found,
                t.last_walked_at     = GETUTCDATE()
            WHEN NOT MATCHED THEN INSERT
                (sport_key, season_year, oldest_snapshot_ts, snapshots_walked, events_found)
            VALUES (s.sport_key, s.season_year, s.oldest_snapshot_ts,
                    s.snapshots_walked, s.events_found);
        """), {"sk": sport_key, "sy": season_year, "ots": oldest_ts_iso,
               "sw": snapshots_delta, "ef": events_delta})


def run_discover(sport, api_key, season_year, snapshots_limit, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Discover: {sport.upper()} Season {season_year} ===")

    season_start, season_end = _season_date_range(sport, season_year)
    effective_end = min(season_end, date.today() - timedelta(days=1))
    if season_start > effective_end:
        print("  Season has not started yet or no past dates exist. Nothing to do.")
        return

    season_start_dt = datetime(
        season_start.year, season_start.month, season_start.day,
        tzinfo=timezone.utc
    )

    cursor_ts = _load_cursor(engine, sport_key, season_year)
    if cursor_ts:
        next_request_iso = cursor_ts
        print(f"  Resuming from cursor: {next_request_iso}")
    else:
        next_request_iso = f"{effective_end.isoformat()}T23:59:59Z"
        print(f"  No cursor found. Starting from: {next_request_iso}")

    print(f"  Walking backward. Limit: {snapshots_limit} dates this run.")

    dates_walked  = 0
    events_stored = 0

    while dates_walked < snapshots_limit:
        print(f"  Date {dates_walked + 1}: requesting {next_request_iso} ...")

        resp = _api_discover_snapshot(sport_key, next_request_iso, api_key)
        if resp is None:
            print("  API returned None. Stopping.")
            break

        actual_ts   = resp.get("timestamp")
        previous_ts = resp.get("previous_timestamp")
        events      = resp.get("data") or []

        print(f"    actual={actual_ts}  events={len(events)}  previous={previous_ts or 'none'}")

        season_events = [
            ev for ev in events
            if ev.get("id") and _parse_iso(ev.get("commence_time")) is not None
            and _parse_iso(ev.get("commence_time")) >= season_start_dt
        ]

        if season_events:
            # Deduplicate by event_id before staging. The API's sliding window
            # can return the same event_id in multiple responses within a batch.
            # SQL Server's MERGE requires each source row to match at most one
            # target row; duplicates in the staging table cause error 8672.
            seen = {}
            for ev in season_events:
                seen[ev["id"]] = ev
            season_events = list(seen.values())

            rows = [{
                "event_id":      ev["id"],
                "sport_key":     sport_key,
                "sport_title":   ev.get("sport_title"),
                "commence_time": _to_utc_str(ev.get("commence_time")),
                "home_team":     ev.get("home_team"),
                "away_team":     ev.get("away_team"),
                "season_year":   season_year,
            } for ev in season_events]
            upsert(engine, clean_dataframe(pd.DataFrame(rows)),
                   schema="odds", table="discovered_events", keys=["event_id"])
            events_stored += len(rows)
            print(f"    Stored {len(rows)} events.")

        dates_walked += 1

        if not previous_ts:
            print("  No previous_timestamp. Reached beginning of history.")
            _save_cursor(engine, sport_key, season_year, actual_ts or next_request_iso,
                         dates_walked, events_stored)
            break

        actual_dt = _parse_iso(actual_ts)
        if actual_dt and actual_dt < season_start_dt:
            print(f"  Crossed season start ({season_start}). Walk complete.")
            _save_cursor(engine, sport_key, season_year, actual_ts,
                         dates_walked, events_stored)
            break

        current_date = _parse_iso(next_request_iso)
        if current_date:
            prev_date        = (current_date - timedelta(days=1)).date()
            next_request_iso = f"{prev_date.isoformat()}T23:59:59Z"
        else:
            next_request_iso = previous_ts

        _save_cursor(engine, sport_key, season_year, next_request_iso,
                     snapshots_delta=1, events_delta=len(season_events))

        time.sleep(1.0)

    print(f"\n  Run complete. Dates walked: {dates_walked}  Events stored this run: {events_stored}")
    if _used_at_start is not None and _used_current is not None:
        print(f"  Credits spent this run: {_used_current - _used_at_start:,}")


# ---------------------------------------------------------------------------
# Odds fetching (historical)
# ---------------------------------------------------------------------------

def _fetch_bulk(sport_key, snap_iso, markets, api_key):
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american", "date": snap_iso},
    )
    return ((data.get("data") or []), data.get("timestamp")) if data else ([], None)


def _fetch_event(sport_key, event_id, snap_iso, markets, api_key):
    data, _ = _request(
        f"{BASE_URL}/v4/historical/sports/{sport_key}/events/{event_id}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american", "date": snap_iso},
    )
    return (data.get("data"), data.get("timestamp")) if data else (None, None)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _parse_bookmakers(event_obj, event_id, sport_key, snap_ts_raw):
    """
    Parse bookmaker odds from an event object into game_lines and player_props rows.

    Routing logic:
      - Any market in TEAM_LEVEL_MARKETS always goes to game_lines, even if the
        odds API populates the outcome description field with a team name.
      - All other markets: outcomes with a description go to player_props;
        outcomes without a description go to game_lines.
    """
    snap_ts = _to_utc_str(snap_ts_raw)
    game_lines, player_props = [], []
    for bk in event_obj.get("bookmakers") or []:
        bk_key, bk_title = bk.get("key"), bk.get("title")
        for mkt in bk.get("markets") or []:
            mkt_key = mkt.get("key")
            is_team_market = mkt_key in TEAM_LEVEL_MARKETS
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
                if description and not is_team_market:
                    player_props.append({**base, "player_name": description})
                else:
                    game_lines.append(base)
    return game_lines, player_props


def _snap_iso(commence_raw):
    """
    Return a snapshot ISO string 1 minute before game start.

    The historical odds API returns the snapshot at or before the requested
    time. Requesting commence_time - 1 minute yields the closest available
    pre-game line for each event.
    """
    if not commence_raw:
        return None
    try:
        dt = datetime.fromisoformat(str(commence_raw).replace("Z", "+00:00"))
        return (dt - timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def _cdt(event):
    raw = event.get("commence_time")
    if not raw:
        return None
    try:
        if isinstance(raw, datetime):
            if raw.tzinfo is None:
                return raw.replace(tzinfo=timezone.utc)
            return raw.astimezone(timezone.utc)
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _eastern_date(dt_utc):
    """Convert a UTC-aware datetime to its Eastern calendar date."""
    if dt_utc is None:
        return None
    return dt_utc.astimezone(EASTERN_TZ).date()


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


def run_backfill(sport, api_key, games_limit, season_year, engine):
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

        bulk_data, bulk_ts = _fetch_bulk(sport_key, snap, BULK_FEATURED_MARKETS, api_key)
        ev_obj = next((e for e in bulk_data if e.get("id") == eid), None)
        if ev_obj:
            gl, pp = _parse_bookmakers(ev_obj, eid, sport_key, bulk_ts)
            gl_all.extend(gl); pp_all.extend(pp)
        else:
            print("    Not found in bulk response.")

        if event_feat:
            ef_obj, ef_ts = _fetch_event(sport_key, eid, snap, event_feat, api_key)
            if ef_obj:
                gl, pp = _parse_bookmakers(ef_obj, eid, sport_key, ef_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)

        if cdt and cdt >= PROPS_CUTOFF:
            if prop_markets:
                p_obj, p_ts = _fetch_event(sport_key, eid, snap, prop_markets, api_key)
                if p_obj:
                    gl, pp = _parse_bookmakers(p_obj, eid, sport_key, p_ts)
                    gl_all.extend(gl); pp_all.extend(pp)
                time.sleep(1.5)
            if alt_markets:
                a_obj, a_ts = _fetch_event(sport_key, eid, snap, alt_markets, api_key)
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

        spent = (_used_current - _used_at_start) if (_used_at_start and _used_current) else 0
        print(f"    events=1  game_lines={gl_n}  player_props={pp_n}  spent_this_run={spent}")
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


def _probe_select_events(sport, sport_key, api_key):
    candidate_dates = PROBE_BEST_CASE[sport] + PROBE_WORST_CASE[sport]
    selected, seen_ids = [], set()

    for target_date in candidate_dates:
        snap_iso = f"{target_date.isoformat()}T12:00:00Z"
        resp = _api_discover_snapshot(sport_key, snap_iso, api_key)
        if not resp:
            continue
        events = resp.get("data") or []
        hops = 0
        while not events and resp.get("next_timestamp") and hops < 7:
            resp = _api_discover_snapshot(sport_key, resp["next_timestamp"], api_key)
            if not resp:
                break
            events = resp.get("data") or []
            hops += 1
            time.sleep(1.0)
        for ev in events:
            eid = ev.get("id")
            if eid and eid not in seen_ids:
                selected.append(ev)
                seen_ids.add(eid)
        if len(selected) >= 5:
            break

    return selected[:5]


def run_probe(sport, api_key, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Probe: {sport.upper()} ({sport_key}) ===")
    events = _probe_select_events(sport, sport_key, api_key)
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

        bulk_data, _ = _fetch_bulk(sport_key, snap, BULK_FEATURED_MARKETS, api_key)
        _tally(next((e for e in bulk_data if e.get("id") == eid), None))
        ef_obj, _ = _fetch_event(sport_key, eid, snap, EVENT_FEATURED_MARKETS[sport], api_key)
        _tally(ef_obj)
        time.sleep(1.5)
        if cdt and cdt >= PROPS_CUTOFF:
            p_obj, _ = _fetch_event(sport_key, eid, snap, PROP_MARKETS[sport], api_key)
            _tally(p_obj)
            time.sleep(1.5)
            a_obj, _ = _fetch_event(sport_key, eid, snap, ALT_PROP_MARKETS[sport], api_key)
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
            else "event_game_period" if mkt in EVENT_FEATURED_MARKETS[sport]
            else "alt_prop" if mkt in ALT_PROP_MARKETS[sport]
            else "prop"
        )
        print(f"  {'COVERED    ' if covered else 'NOT COVERED'} {mkt:<50} "
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

_NAME_SUFFIXES = re.compile(
    r'\b(jr\.?|sr\.?|ii|iii|iv)\s*$',
    re.IGNORECASE
)

_NBA_PLAYER_ALIASES = {
    "Moe Wagner":          "Moritz Wagner",
    "Herb Jones":          "Herbert Jones",
    "Nicolas Claxton":     "Nic Claxton",
    "Vincent Williams Jr": "Vince Williams Jr.",
    "Ron Holland":         "Ronald Holland II",
    "Carlton Carrington":  "Bub Carrington",
}


def _normalize_name(name):
    if not name:
        return ""
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower()
    name = _NAME_SUFFIXES.sub("", name)
    name = re.sub(r"[^a-z0-9 ]", "", name)
    return re.sub(r"\s+", " ", name).strip()


def run_mappings(sport, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Mappings: {sport.upper()} ===")
    if sport == "nba":
        _run_mappings_nba(sport_key, engine)
    elif sport == "mlb":
        print("  MLB mappings not yet implemented. Skipping.")
    elif sport == "nfl":
        print("  NFL mappings not yet implemented. Skipping.")


def _run_mappings_nba(sport_key, engine):
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
        lookup_name = _NBA_PLAYER_ALIASES.get(oname, oname)
        norm  = _normalize_name(lookup_name)
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
        all_events = conn.execute(
            text("""
                SELECT e.event_id, e.commence_time, e.home_team, e.away_team
                FROM odds.events e
                WHERE e.sport_key = :sk
            """),
            {"sk": sport_key},
        ).fetchall()

    if not all_events:
        print("  event_game_map: no events to map.")
        return

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
    for eid, ctime, home_name, away_name in all_events:
        try:
            ctime_dt = (
                datetime.fromisoformat(str(ctime).replace("Z", "+00:00"))
                if isinstance(ctime, str) else ctime
            )
            if hasattr(ctime_dt, "tzinfo") and ctime_dt.tzinfo is None:
                ctime_dt = ctime_dt.replace(tzinfo=timezone.utc)
            utc_date      = ctime_dt.date() if hasattr(ctime_dt, "date") else None
            utc_prev_date = (utc_date - timedelta(days=1)) if utc_date else None
        except Exception:
            utc_date = utc_prev_date = None

        home_tc = name_to_tc.get(home_name)
        away_tc = name_to_tc.get(away_name)

        game_id = None
        used_date = None
        if home_tc:
            for candidate in [utc_date, utc_prev_date]:
                if candidate is None:
                    continue
                game_id = game_lookup.get((str(candidate), home_tc))
                if game_id:
                    used_date = candidate
                    break

        if game_id:
            matched += 1
        else:
            unmatched += 1

        egm_rows.append({
            "event_id":     eid,
            "sport_key":    sport_key,
            "game_id":      game_id,
            "game_date":    str(used_date) if used_date else (str(utc_date) if utc_date else None),
            "home_tricode": home_tc,
            "away_tricode": away_tc,
            "match_method": "date_home_tricode" if game_id else "unmatched",
        })

    upsert(engine, clean_dataframe(pd.DataFrame(egm_rows)),
           schema="odds", table="event_game_map", keys=["event_id"])
    print(f"  event_game_map: {len(egm_rows)} rows ({matched} matched, {unmatched} unmatched).")


# ---------------------------------------------------------------------------
# UPCOMING mode
# ---------------------------------------------------------------------------

def _fetch_upcoming_bulk(sport_key, markets, api_key):
    data, _ = _request(
        f"{BASE_URL}/v4/sports/{sport_key}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american"},
    )
    if data is None: return []
    if isinstance(data, list): return data
    return data.get("data") or []


def _fetch_upcoming_event(sport_key, event_id, markets, api_key):
    data, _ = _request(
        f"{BASE_URL}/v4/sports/{sport_key}/events/{event_id}/odds",
        {"apiKey": api_key, "bookmakers": BOOKMAKERS,
         "markets": ",".join(markets), "oddsFormat": "american"},
    )
    if data is None: return None, None
    if isinstance(data, dict) and "bookmakers" in data: return data, None
    return data.get("data"), None


def run_upcoming(sport, api_key, days_ahead, engine):
    sport_key = SPORT_KEYS[sport]
    print(f"\n=== Upcoming: {sport.upper()} (next {days_ahead} game day(s)) ===")

    probe        = _load_probe_results(engine, sport_key)
    event_feat   = _filter_markets(probe, EVENT_FEATURED_MARKETS[sport], "event_featured")
    prop_markets = _filter_markets(probe, PROP_MARKETS[sport], "prop")
    alt_markets  = _filter_markets(probe, ALT_PROP_MARKETS[sport], "alt_prop")

    all_upcoming = _fetch_upcoming_bulk(sport_key, ["h2h"], api_key)
    if not all_upcoming:
        print("  No upcoming events found.")
        return

    now_eastern      = datetime.now(tz=EASTERN_TZ)
    today_eastern    = now_eastern.date()
    cutoff_eastern   = today_eastern + timedelta(days=days_ahead - 1)

    in_window = [
        ev for ev in all_upcoming
        if _cdt(ev) and _eastern_date(_cdt(ev)) is not None
        and today_eastern <= _eastern_date(_cdt(ev)) <= cutoff_eastern
    ]

    if not in_window:
        print("  No events within window.")
        return

    if days_ahead <= 1:
        in_window = [ev for ev in in_window if _eastern_date(_cdt(ev)) == today_eastern]
        print(f"  Next game day: {today_eastern} ({len(in_window)} events).")
    else:
        print(f"  {len(in_window)} events in window.")

    with engine.begin() as conn:
        for tbl in ("upcoming_player_props", "upcoming_game_lines", "upcoming_events"):
            conn.execute(text(f"DELETE FROM odds.{tbl} WHERE sport_key = :sk"), {"sk": sport_key})

    snap_ts = _to_utc_str(datetime.now(tz=timezone.utc))

    if sport == "nba":
        with engine.connect() as conn:
            name_to_tc = {r[0]: r[1] for r in conn.execute(
                text("SELECT odds_team_name, team_tricode FROM odds.team_map WHERE sport_key = :sk"),
                {"sk": sport_key},
            ).fetchall()}
            future_lookup = {(str(r[1]), r[2]): r[0] for r in conn.execute(
                text("SELECT game_id, game_date, home_team_tricode FROM nba.schedule WHERE game_date >= :today"),
                {"today": today_eastern},
            ).fetchall()}
            pid_map = {r[0]: r[1] for r in conn.execute(
                text("SELECT odds_player_name, player_id FROM odds.player_map WHERE sport_key = :sk AND player_id IS NOT NULL"),
                {"sk": sport_key},
            ).fetchall()}
    else:
        name_to_tc = future_lookup = pid_map = {}

    gl_total = pp_total = 0
    for event in in_window:
        eid          = event.get("id")
        cdt          = _cdt(event)
        home_name    = event.get("home_team")
        away_name    = event.get("away_team")
        home_tc      = name_to_tc.get(home_name)
        away_tc      = name_to_tc.get(away_name)
        eastern_date = _eastern_date(cdt)
        game_id      = future_lookup.get((str(eastern_date), home_tc)) if (eastern_date and home_tc) else None
        print(f"\n  {away_name or ''} @ {home_name or ''} ({eastern_date or '?'})")

        gl_all, pp_all = [], []
        bulk_data = _fetch_upcoming_bulk(sport_key, BULK_FEATURED_MARKETS, api_key)
        ev_obj = next((e for e in bulk_data if e.get("id") == eid), None)
        if ev_obj:
            gl, pp = _parse_bookmakers(ev_obj, eid, sport_key, snap_ts)
            gl_all.extend(gl); pp_all.extend(pp)
        time.sleep(1.5)
        if event_feat:
            ef_obj, _ = _fetch_upcoming_event(sport_key, eid, event_feat, api_key)
            if ef_obj:
                gl, pp = _parse_bookmakers(ef_obj, eid, sport_key, snap_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)
        if prop_markets:
            p_obj, _ = _fetch_upcoming_event(sport_key, eid, prop_markets, api_key)
            if p_obj:
                gl, pp = _parse_bookmakers(p_obj, eid, sport_key, snap_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)
        if alt_markets:
            a_obj, _ = _fetch_upcoming_event(sport_key, eid, alt_markets, api_key)
            if a_obj:
                gl, pp = _parse_bookmakers(a_obj, eid, sport_key, snap_ts)
                gl_all.extend(gl); pp_all.extend(pp)
            time.sleep(1.5)

        upsert(engine, clean_dataframe(pd.DataFrame([{
            "event_id":     eid,
            "sport_key":    sport_key,
            "sport_title":  event.get("sport_title"),
            "commence_time": _to_utc_str(event.get("commence_time")),
            "home_team":    home_name,
            "away_team":    away_name,
            "home_tricode": home_tc,
            "away_tricode": away_tc,
            "game_id":      game_id,
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

        spent = (_used_current - _used_at_start) if (_used_at_start and _used_current) else 0
        print(f"    game_lines={len(gl_all)}  player_props={len(pp_all)}  "
              f"game_id={game_id or 'not resolved'}  spent_this_run={spent}")

    print(f"\n  Totals: {len(in_window)} events  game_lines={gl_total}  player_props={pp_total}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global _budget

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",
                        choices=["discover", "probe", "backfill", "mappings", "upcoming"],
                        default="backfill")
    parser.add_argument("--sport",      default="all", choices=["nfl", "nba", "mlb", "all"])
    parser.add_argument("--season",     type=int, default=None)
    parser.add_argument("--games",      type=int, default=10,
                        help="Backfill mode: max events to process per run.")
    parser.add_argument("--snapshots",  type=int, default=50,
                        help="Discover mode: max calendar dates to walk per run. Default 50.")
    parser.add_argument("--days-ahead", type=int, default=1, dest="days_ahead",
                        help="Upcoming mode: calendar days ahead to fetch.")
    parser.add_argument("--budget",     type=int, default=0, dest="budget",
                        help="Max credits to spend this invocation. 0 = unlimited.")
    args = parser.parse_args()

    _budget = args.budget
    if _budget > 0:
        print(f"Budget cap: {_budget:,} credits for this run.")

    api_key = os.environ.get("ODDS_API_KEY")
    if not api_key and args.mode not in ("mappings",):
        raise EnvironmentError("ODDS_API_KEY environment variable is not set.")

    sports = ["nfl", "nba", "mlb"] if args.sport == "all" else [args.sport]
    print(f"Mode: {args.mode}  Sports: {', '.join(sports)}")

    engine = get_engine()
    ensure_schema(engine)

    for sport in sports:
        season_year = args.season or _default_season(sport)
        if args.mode == "discover":
            run_discover(sport, api_key, season_year, args.snapshots, engine)
        elif args.mode == "probe":
            run_probe(sport, api_key, engine)
        elif args.mode == "backfill":
            run_backfill(sport, api_key, args.games, season_year, engine)
        elif args.mode == "mappings":
            run_mappings(sport, engine)
        elif args.mode == "upcoming":
            run_upcoming(sport, api_key, args.days_ahead, engine)

    if _used_at_start is not None and _used_current is not None:
        print(f"\nTotal credits spent this run: {_used_current - _used_at_start:,}")


if __name__ == "__main__":
    main()
