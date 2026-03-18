"""
nba_etl.py

NBA ETL for the sports modeling database.
Runs exclusively in GitHub Actions. Never runs locally.

Design
------
Teams, players, and game discovery all use direct HTTP requests with
browser-mimicking headers and NO proxy. These endpoints work reliably
from GitHub Actions without a proxy when the correct headers are sent.

Box scores use BoxScoreTraditionalV3 via the nba_api wrapper, which
still requires the proxy because the wrapper does not send browser headers.

Passing and rebounding tracking stats use direct HTTP with no proxy,
same as teams/players/games.

Batch unit is DAYS. --days N processes the N oldest dates with missing
box score data, then fetches pt stats for those same dates.

Tables written
  nba.teams                  leaguestandings direct HTTP, every run.
  nba.players                commonteamroster direct HTTP x30, first run or --load-rosters.
  nba.games                  leaguegamelog direct HTTP for game discovery;
                             scoreboardv3 direct HTTP for per-game metadata.
  nba.player_box_score_stats Quarter-level player stats (Q1/Q2/Q3/Q4/OT).
  nba.team_box_score_stats   Quarter-level team stats (Q1/Q2/Q3/Q4/OT).
  nba.player_passing_stats   Daily passing tracking stats per player.
  nba.player_rebound_chances Daily rebounding chances per player.

Args
  --days N          Dates to process per run (default: 3).
  --season S        Season string, e.g. 2025-26 (default: 2025-26).
  --load-rosters    Force roster reload even if players table is not empty.
  --skip-pt-stats   Skip passing and rebounding stats (box scores only).

Secrets required
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import math
import os
import time
import logging
from datetime import date

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXY_URL   = os.environ.get("NBA_PROXY_URL")
API_DELAY   = 1.5
RETRY_WAIT  = 30
RETRY_COUNT = 3

RETRY_WAIT_TIMEOUT = 30
RETRY_WAIT_500     = 60

PT_STATS_BETWEEN_DELAY = 15

# Browser headers for all direct HTTP calls to stats.nba.com.
# No proxy is used for these. The proxy is only needed for nba_api
# wrapper calls (BoxScoreTraditionalV3) which cannot send these headers.
NBA_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Origin":             "https://www.nba.com",
    "Referer":            "https://www.nba.com/",
}

# Explicit no-proxy dict. Passed to every direct requests.get call so the
# NBA_PROXY_URL environment variable is never inherited for these calls.
NO_PROXY = {"http": None, "https": None}

# Proxy dict for nba_api wrapper calls only.
def get_proxies():
    if not PROXY_URL:
        return None
    return {"http": PROXY_URL, "https": PROXY_URL}

# Quarter range boundaries in tenths of a second.
PERIOD_RANGES = [
    ("Q1", 0,     7200),
    ("Q2", 7200,  14400),
    ("Q3", 14400, 21600),
    ("Q4", 21600, 28800),
]
OT_START_RANGE = 28800
OT_PERIOD_LEN  = 3000

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_STATEMENTS = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.teams') AND type = 'U')
    CREATE TABLE nba.teams (
        nba_team_id   BIGINT       NOT NULL,
        nba_team      CHAR(3)      NOT NULL,
        nba_team_name VARCHAR(60)  NOT NULL,
        team_city     VARCHAR(40)  NULL,
        conference    VARCHAR(10)  NULL,
        division      VARCHAR(20)  NULL,
        w             SMALLINT     NULL,
        l             SMALLINT     NULL,
        conf_rank     SMALLINT     NULL,
        div_rank      SMALLINT     NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_teams PRIMARY KEY (nba_team_id),
        CONSTRAINT uq_nba_team  UNIQUE      (nba_team)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.players') AND type = 'U')
    CREATE TABLE nba.players (
        nba_player_id BIGINT        NOT NULL,
        player_name   VARCHAR(100)  NOT NULL,
        position      VARCHAR(10)   NULL,
        jersey_num    VARCHAR(5)    NULL,
        height        VARCHAR(10)   NULL,
        weight        VARCHAR(10)   NULL,
        birth_date    DATE          NULL,
        age           DECIMAL(5,1)  NULL,
        experience    VARCHAR(5)    NULL,
        school        VARCHAR(100)  NULL,
        nba_team_id   BIGINT        NULL,
        nba_team      CHAR(3)       NULL,
        created_at    DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_players      PRIMARY KEY (nba_player_id),
        CONSTRAINT fk_nba_players_team FOREIGN KEY (nba_team_id)
            REFERENCES nba.teams (nba_team_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.games') AND type = 'U')
    CREATE TABLE nba.games (
        game_id       VARCHAR(15)  NOT NULL,
        game_date     DATE         NOT NULL,
        game_code     VARCHAR(30)  NULL,
        game_display  VARCHAR(20)  NULL,
        home_team_id  BIGINT       NULL,
        home_team     CHAR(3)      NULL,
        away_team_id  BIGINT       NULL,
        away_team     CHAR(3)      NULL,
        season_year   CHAR(7)      NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_games      PRIMARY KEY (game_id),
        CONSTRAINT fk_nba_games_home FOREIGN KEY (home_team_id)
            REFERENCES nba.teams (nba_team_id),
        CONSTRAINT fk_nba_games_away FOREIGN KEY (away_team_id)
            REFERENCES nba.teams (nba_team_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_box_score_stats') AND type = 'U')
    CREATE TABLE nba.player_box_score_stats (
        game_id           VARCHAR(15)   NOT NULL,
        player_id         BIGINT        NOT NULL,
        quarter           VARCHAR(5)    NOT NULL,
        first_name        VARCHAR(60)   NULL,
        last_name         VARCHAR(60)   NULL,
        team_id           BIGINT        NULL,
        team_abbreviation CHAR(3)       NULL,
        position          VARCHAR(10)   NULL,
        minutes           VARCHAR(20)   NULL,
        fgm               SMALLINT      NULL,
        fga               SMALLINT      NULL,
        fg_pct            DECIMAL(6,4)  NULL,
        fg3m              SMALLINT      NULL,
        fg3a              SMALLINT      NULL,
        fg3_pct           DECIMAL(6,4)  NULL,
        ftm               SMALLINT      NULL,
        fta               SMALLINT      NULL,
        ft_pct            DECIMAL(6,4)  NULL,
        oreb              SMALLINT      NULL,
        dreb              SMALLINT      NULL,
        reb               SMALLINT      NULL,
        ast               SMALLINT      NULL,
        stl               SMALLINT      NULL,
        blk               SMALLINT      NULL,
        tov               SMALLINT      NULL,
        pf                SMALLINT      NULL,
        pts               SMALLINT      NULL,
        plus_minus        DECIMAL(6,1)  NULL,
        created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pbss        PRIMARY KEY (game_id, player_id, quarter),
        CONSTRAINT fk_nba_pbss_game   FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_pbss_player FOREIGN KEY (player_id)
            REFERENCES nba.players (nba_player_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.team_box_score_stats') AND type = 'U')
    CREATE TABLE nba.team_box_score_stats (
        game_id           VARCHAR(15)   NOT NULL,
        team_id           BIGINT        NOT NULL,
        quarter           VARCHAR(5)    NOT NULL,
        team_abbreviation CHAR(3)       NULL,
        fgm               SMALLINT      NULL,
        fga               SMALLINT      NULL,
        fg_pct            DECIMAL(6,4)  NULL,
        fg3m              SMALLINT      NULL,
        fg3a              SMALLINT      NULL,
        fg3_pct           DECIMAL(6,4)  NULL,
        ftm               SMALLINT      NULL,
        fta               SMALLINT      NULL,
        ft_pct            DECIMAL(6,4)  NULL,
        oreb              SMALLINT      NULL,
        dreb              SMALLINT      NULL,
        reb               SMALLINT      NULL,
        ast               SMALLINT      NULL,
        stl               SMALLINT      NULL,
        blk               SMALLINT      NULL,
        tov               SMALLINT      NULL,
        pf                SMALLINT      NULL,
        pts               SMALLINT      NULL,
        plus_minus        DECIMAL(6,1)  NULL,
        created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_tbss      PRIMARY KEY (game_id, team_id, quarter),
        CONSTRAINT fk_nba_tbss_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_tbss_team FOREIGN KEY (team_id)
            REFERENCES nba.teams (nba_team_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_passing_stats') AND type = 'U')
    CREATE TABLE nba.player_passing_stats (
        player_id            BIGINT        NOT NULL,
        game_date            DATE          NOT NULL,
        player_name          VARCHAR(100)  NULL,
        team_id              BIGINT        NULL,
        team_abbreviation    CHAR(3)       NULL,
        potential_ast        DECIMAL(8,1)  NULL,
        ast                  DECIMAL(8,1)  NULL,
        ft_ast               DECIMAL(8,1)  NULL,
        secondary_ast        DECIMAL(8,1)  NULL,
        passes_made          DECIMAL(10,1) NULL,
        passes_received      DECIMAL(10,1) NULL,
        ast_points_created   DECIMAL(8,1)  NULL,
        ast_adj              DECIMAL(8,1)  NULL,
        ast_to_pass_pct      DECIMAL(6,4)  NULL,
        ast_to_pass_pct_adj  DECIMAL(6,4)  NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pps        PRIMARY KEY (player_id, game_date),
        CONSTRAINT fk_nba_pps_player FOREIGN KEY (player_id)
            REFERENCES nba.players (nba_player_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_rebound_chances') AND type = 'U')
    CREATE TABLE nba.player_rebound_chances (
        player_id          BIGINT        NOT NULL,
        game_date          DATE          NOT NULL,
        player_name        VARCHAR(100)  NULL,
        team_id            BIGINT        NULL,
        team_abbreviation  CHAR(3)       NULL,
        oreb               DECIMAL(8,1)  NULL,
        oreb_chances       DECIMAL(8,1)  NULL,
        dreb               DECIMAL(8,1)  NULL,
        dreb_chances       DECIMAL(8,1)  NULL,
        reb_chances        DECIMAL(8,1)  NULL,
        created_at         DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_prc        PRIMARY KEY (player_id, game_date),
        CONSTRAINT fk_nba_prc_player FOREIGN KEY (player_id)
            REFERENCES nba.players (nba_player_id)
    )
    """,
]


def ensure_tables(engine):
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))
    log.info("Schema verified.")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(f"DB connection attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                log.info("Waiting 60s for Azure SQL to resume...")
                time.sleep(60)
    raise RuntimeError("Could not connect to Azure SQL after 3 attempts.")


# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------
def safe_float(val):
    try:
        if val is None:
            return None
        if isinstance(val, float):
            return None if (math.isnan(val) or math.isinf(val)) else val
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None

def safe_int(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)
    except (ValueError, TypeError):
        return None

def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None

def safe_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None

def safe_pct(num, den):
    n, d = safe_int(num), safe_int(den)
    if n is None or d is None or d == 0:
        return None
    return round(n / d, 4)


# ---------------------------------------------------------------------------
# Direct HTTP helper (no proxy, browser headers)
# ---------------------------------------------------------------------------
def _direct_get(url, label, timeout=60):
    """Single direct HTTP GET with browser headers and no proxy. Returns parsed JSON or None."""
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, headers=NBA_HEADERS, proxies=NO_PROXY, timeout=timeout)
            if resp.status_code == 500:
                raise ValueError("HTTP 500")
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            time.sleep(API_DELAY)
            return resp.json()
        except Exception as exc:
            wait = RETRY_WAIT_500 if "500" in str(exc) else RETRY_WAIT_TIMEOUT
            log.warning(f"  {label} attempt {attempt}/{RETRY_COUNT} failed: {exc}")
            if attempt < RETRY_COUNT:
                time.sleep(wait)
    log.error(f"  {label} failed after {RETRY_COUNT} attempts, skipping")
    return None


def _parse_result_set(data, index=0):
    """Parse a standard NBA stats resultSets response into a DataFrame."""
    if data is None:
        return None
    try:
        rs       = data["resultSets"][index]
        headers  = rs["headers"]
        row_set  = rs["rowSet"]
        if not row_set:
            return None
        return pd.DataFrame(row_set, columns=headers)
    except Exception as exc:
        log.warning(f"  resultSets parse failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# MERGE upsert
# ---------------------------------------------------------------------------
def _clean_val(v):
    import numpy as np
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    return v

def upsert(df, engine, schema, table, pk_cols):
    if df is None or df.empty:
        return
    records = [
        {col: _clean_val(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]
    non_pk    = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = (
        ", ".join(f"tgt.{c} = src.{c}" for c in non_pk)
        if non_pk else f"tgt.{pk_cols[0]} = tgt.{pk_cols[0]}"
    )
    merge_sql = f"""
        MERGE {schema}.{table} AS tgt
        USING (VALUES ({val_list})) AS src ({col_list})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
    """
    with engine.begin() as conn:
        conn.execute(text(merge_sql), records)


# ---------------------------------------------------------------------------
# Teams  (direct HTTP, no proxy)
# Source: leaguestandings returns all 30 teams with conference/division/W/L
# in a single call.
# ---------------------------------------------------------------------------
def load_teams(engine, season):
    log.info(f"Loading nba.teams via leaguestandings for season {season}")
    url  = (
        "https://stats.nba.com/stats/leaguestandings"
        f"?LeagueID=00&Season={season}&SeasonType=Regular+Season"
    )
    data = _direct_get(url, "leaguestandings")
    df   = _parse_result_set(data)
    if df is None or df.empty:
        log.warning("  leaguestandings returned no rows")
        return

    rows = []
    for _, row in df.iterrows():
        tid = safe_int(row.get("TeamID"))
        if tid is None:
            continue
        rows.append({
            "nba_team_id":   tid,
            "nba_team":      safe_str(row.get("TeamAbbreviation")),
            "nba_team_name": safe_str(row.get("TeamName")),
            "team_city":     safe_str(row.get("TeamCity")),
            "conference":    safe_str(row.get("Conference")),
            "division":      safe_str(row.get("Division")),
            "w":             safe_int(row.get("WINS")),
            "l":             safe_int(row.get("LOSSES")),
            "conf_rank":     safe_int(row.get("PlayoffRank")),
            "div_rank":      safe_int(row.get("DivisionRank")),
        })

    if rows:
        upsert(pd.DataFrame(rows), engine, "nba", "teams", ["nba_team_id"])
        log.info(f"  {len(rows)} teams upserted")
    else:
        log.warning("  No team rows produced")


# ---------------------------------------------------------------------------
# Players  (direct HTTP, no proxy)
# Source: commonteamroster called once per team (30 calls).
# Team IDs come from nba.teams which was just loaded.
# ---------------------------------------------------------------------------
def players_table_empty(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(1) FROM nba.players")).scalar() == 0

def load_players(engine, season):
    log.info(f"Loading nba.players via commonteamroster for season {season}")

    # Pull team IDs and abbreviations from the teams we just loaded
    with engine.connect() as conn:
        team_rows = list(conn.execute(
            text("SELECT nba_team_id, nba_team FROM nba.teams")
        ))

    if not team_rows:
        log.warning("  nba.teams is empty, cannot load players")
        return

    rows = []
    for team_id, team_abbr in team_rows:
        url  = (
            "https://stats.nba.com/stats/commonteamroster"
            f"?TeamID={team_id}&Season={season}"
        )
        data = _direct_get(url, f"commonteamroster {team_abbr}")
        if data is None:
            continue
        # commonteamroster returns two result sets: index 0 = roster, index 1 = coaches
        df = _parse_result_set(data, index=0)
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            pid = safe_int(row.get("PLAYER_ID"))
            if pid is None:
                continue
            rows.append({
                "nba_player_id": pid,
                "player_name":   safe_str(row.get("PLAYER")) or "Unknown",
                "position":      safe_str(row.get("POSITION")),
                "jersey_num":    safe_str(row.get("NUM")),
                "height":        safe_str(row.get("HEIGHT")),
                "weight":        safe_str(row.get("WEIGHT")),
                "birth_date":    safe_date(row.get("BIRTH_DATE")),
                "age":           safe_float(row.get("AGE")),
                "experience":    safe_str(row.get("EXP")),
                "school":        safe_str(row.get("SCHOOL")),
                "nba_team_id":   safe_int(row.get("TeamID")),
                "nba_team":      team_abbr,
            })

    if rows:
        upsert(pd.DataFrame(rows), engine, "nba", "players", ["nba_player_id"])
        log.info(f"  {len(rows)} players upserted")
    else:
        log.warning("  No player rows produced")

def _seed_players(rows, engine):
    seed_sql = """
        MERGE nba.players AS tgt
        USING (VALUES (:nba_player_id, :player_name))
              AS src (nba_player_id, player_name)
        ON tgt.nba_player_id = src.nba_player_id
        WHEN NOT MATCHED THEN INSERT
            (nba_player_id, player_name, created_at)
        VALUES (src.nba_player_id, src.player_name, GETUTCDATE());
    """
    seed_rows = []
    for r in rows:
        pid  = r.get("player_id") or r.get("nba_player_id")
        if pid is None:
            continue
        fn   = r.get("first_name") or ""
        ln   = r.get("last_name") or ""
        name = (fn + " " + ln).strip() or "Unknown"
        seed_rows.append({"nba_player_id": pid, "player_name": name})
    if not seed_rows:
        return
    with engine.begin() as conn:
        conn.execute(text(seed_sql), seed_rows)


# ---------------------------------------------------------------------------
# Game discovery  (direct HTTP, no proxy)
# Source: leaguegamelog returns all games for the season in one call.
# Excludes preseason (game IDs starting with 001) and today.
# ---------------------------------------------------------------------------
def get_all_season_game_ids(season):
    log.info(f"Fetching all game IDs for season {season}")
    url  = (
        "https://stats.nba.com/stats/leaguegamelog"
        f"?LeagueID=00&Season={season}&SeasonType=Regular+Season"
        "&PlayerOrTeam=T&Direction=ASC&Sorter=DATE"
    )
    data = _direct_get(url, "leaguegamelog")
    df   = _parse_result_set(data)
    if df is None or df.empty:
        log.warning("  leaguegamelog returned no rows")
        return []

    today = date.today()
    result = []
    seen   = set()
    for _, row in df.iterrows():
        gid   = str(row.get("GAME_ID", ""))
        gdate = safe_date(row.get("GAME_DATE"))
        if not gid or gdate is None:
            continue
        if gid.startswith("001"):
            continue
        if gdate >= today:
            continue
        if gid not in seen:
            seen.add(gid)
            result.append((gid, gdate))

    result.sort(key=lambda x: x[1])
    log.info(f"  Found {len(result)} completed games in season {season}")
    return result


# ---------------------------------------------------------------------------
# Filter to games with missing box score data
# ---------------------------------------------------------------------------
def get_unloaded_game_ids(all_pairs, engine):
    with engine.connect() as conn:
        loaded_game_ids = {
            row[0] for row in
            conn.execute(text("SELECT DISTINCT game_id FROM nba.player_box_score_stats"))
        }
    unloaded = [p for p in all_pairs if p[0] not in loaded_game_ids]
    log.info(f"  {len(loaded_game_ids)} already loaded, {len(unloaded)} games remaining")
    return unloaded


# ---------------------------------------------------------------------------
# Dates with missing pt stats
# ---------------------------------------------------------------------------
def get_unloaded_pt_dates(candidate_dates, engine):
    with engine.connect() as conn:
        loaded_dates = {
            row[0] for row in
            conn.execute(text("SELECT DISTINCT game_date FROM nba.player_passing_stats"))
        }
    missing = sorted([d for d in candidate_dates if d not in loaded_dates])
    log.info(f"  Pt stats: {len(loaded_dates)} dates loaded, {len(missing)} dates remaining")
    return missing


# ---------------------------------------------------------------------------
# ScoreboardV3 metadata  (direct HTTP, no proxy)
# ---------------------------------------------------------------------------
def fetch_scoreboard_metadata(target_dates, season):
    metadata = {}
    for game_date in sorted(set(target_dates)):
        date_str = game_date.strftime("%Y-%m-%d")
        url  = (
            "https://stats.nba.com/stats/scoreboardv3"
            f"?GameDate={date_str}&LeagueID=00"
        )
        data = _direct_get(url, f"scoreboardv3 {date_str}")
        if data is None:
            continue
        try:
            games = data.get("scoreboard", {}).get("games", [])
            for g in games:
                gid       = str(g.get("gameId", ""))
                home      = g.get("homeTeam", {})
                away      = g.get("awayTeam", {})
                home_abbr = safe_str(home.get("teamTricode"))
                away_abbr = safe_str(away.get("teamTricode"))
                metadata[gid] = {
                    "game_date":    game_date,
                    "game_code":    safe_str(g.get("gameCode")),
                    "game_display": f"{away_abbr}@{home_abbr}" if away_abbr else None,
                    "home_team_id": safe_int(home.get("teamId")),
                    "home_team":    home_abbr,
                    "away_team_id": safe_int(away.get("teamId")),
                    "away_team":    away_abbr,
                    "season_year":  season[:7],
                }
        except Exception as exc:
            log.warning(f"  scoreboardv3 parse failed for {date_str}: {exc}")
    log.info(f"  Scoreboard metadata fetched for {len(metadata)} game(s)")
    return metadata


# ---------------------------------------------------------------------------
# Box score row builders
# ---------------------------------------------------------------------------
def _trad_player_rows(game_id, quarter_label, df):
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        comment = safe_str(row.get("comment")) or ""
        if comment:
            continue
        pid = safe_int(row.get("personId"))
        if pid is None:
            continue
        pos_raw  = safe_str(row.get("position"))
        position = pos_raw if pos_raw and pos_raw.lower() != "nan" else "BENCH"
        rows.append({
            "game_id":           game_id,
            "player_id":         pid,
            "quarter":           quarter_label,
            "first_name":        safe_str(row.get("firstName")),
            "last_name":         safe_str(row.get("familyName")),
            "team_id":           safe_int(row.get("teamId")),
            "team_abbreviation": safe_str(row.get("teamTricode")),
            "position":          position,
            "minutes":           safe_str(row.get("minutes")),
            "fgm":               safe_int(row.get("fieldGoalsMade")),
            "fga":               safe_int(row.get("fieldGoalsAttempted")),
            "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
            "fg3m":              safe_int(row.get("threePointersMade")),
            "fg3a":              safe_int(row.get("threePointersAttempted")),
            "fg3_pct":           safe_float(row.get("threePointersPercentage")),
            "ftm":               safe_int(row.get("freeThrowsMade")),
            "fta":               safe_int(row.get("freeThrowsAttempted")),
            "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
            "oreb":              safe_int(row.get("reboundsOffensive")),
            "dreb":              safe_int(row.get("reboundsDefensive")),
            "reb":               safe_int(row.get("reboundsTotal")),
            "ast":               safe_int(row.get("assists")),
            "stl":               safe_int(row.get("steals")),
            "blk":               safe_int(row.get("blocks")),
            "tov":               safe_int(row.get("turnovers")),
            "pf":                safe_int(row.get("foulsPersonal")),
            "pts":               safe_int(row.get("points")),
            "plus_minus":        safe_float(row.get("plusMinusPoints")),
        })
    return rows

def _trad_team_rows(game_id, quarter_label, df):
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        tid = safe_int(row.get("teamId"))
        if tid is None:
            continue
        rows.append({
            "game_id":           game_id,
            "team_id":           tid,
            "quarter":           quarter_label,
            "team_abbreviation": safe_str(row.get("teamTricode")),
            "fgm":               safe_int(row.get("fieldGoalsMade")),
            "fga":               safe_int(row.get("fieldGoalsAttempted")),
            "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
            "fg3m":              safe_int(row.get("threePointersMade")),
            "fg3a":              safe_int(row.get("threePointersAttempted")),
            "fg3_pct":           safe_float(row.get("threePointersPercentage")),
            "ftm":               safe_int(row.get("freeThrowsMade")),
            "fta":               safe_int(row.get("freeThrowsAttempted")),
            "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
            "oreb":              safe_int(row.get("reboundsOffensive")),
            "dreb":              safe_int(row.get("reboundsDefensive")),
            "reb":               safe_int(row.get("reboundsTotal")),
            "ast":               safe_int(row.get("assists")),
            "stl":               safe_int(row.get("steals")),
            "blk":               safe_int(row.get("blocks")),
            "tov":               safe_int(row.get("turnovers")),
            "pf":                safe_int(row.get("foulsPersonal")),
            "pts":               safe_int(row.get("points")),
            "plus_minus":        safe_float(row.get("plusMinusPoints")),
        })
    return rows

def _sum_ot_player_rows(game_id, ot_periods_data):
    if not ot_periods_data:
        return []
    all_rows = []
    for player_df, _ in ot_periods_data:
        all_rows.extend(_trad_player_rows(game_id, "OT", player_df))
    if not all_rows:
        return []
    df = pd.DataFrame(all_rows)
    count_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                  "oreb","dreb","reb","ast","stl","blk","tov","pf","pts","plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    meta_cols  = ["game_id","player_id","quarter","first_name","last_name",
                  "team_id","team_abbreviation","position","minutes"]
    agg_meta   = df.groupby("player_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("player_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"player_id": "_pid"}, inplace=True)
    agg_meta["_pid"] = agg_meta["player_id"]
    merged = agg_meta.merge(agg_counts, on="_pid").drop(columns=["_pid"])
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"
    return merged.to_dict(orient="records")

def _sum_ot_team_rows(game_id, ot_periods_data):
    if not ot_periods_data:
        return []
    all_rows = []
    for _, team_df in ot_periods_data:
        all_rows.extend(_trad_team_rows(game_id, "OT", team_df))
    if not all_rows:
        return []
    df = pd.DataFrame(all_rows)
    count_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                  "oreb","dreb","reb","ast","stl","blk","tov","pf","pts","plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    meta_cols  = ["game_id","team_id","quarter","team_abbreviation"]
    agg_meta   = df.groupby("team_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("team_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"team_id": "_tid"}, inplace=True)
    agg_meta["_tid"] = agg_meta["team_id"]
    merged = agg_meta.merge(agg_counts, on="_tid").drop(columns=["_tid"])
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"
    return merged.to_dict(orient="records")


# ---------------------------------------------------------------------------
# Process one game  (box scores still use nba_api wrapper + proxy)
# ---------------------------------------------------------------------------
def process_game(game_id, game_date, game_meta, engine):
    log.info(f"  Processing {game_id} ({game_date})")

    meta = game_meta.get(game_id)
    games_row = {
        "game_id":      game_id,
        "game_date":    meta["game_date"]    if meta else game_date,
        "game_code":    meta["game_code"]    if meta else None,
        "game_display": meta["game_display"] if meta else None,
        "home_team_id": meta["home_team_id"] if meta else None,
        "home_team":    meta["home_team"]    if meta else None,
        "away_team_id": meta["away_team_id"] if meta else None,
        "away_team":    meta["away_team"]    if meta else None,
        "season_year":  meta["season_year"]  if meta else None,
    }
    try:
        upsert(pd.DataFrame([games_row]), engine, "nba", "games", ["game_id"])
    except Exception as exc:
        log.error(f"  games upsert failed for {game_id}: {exc}")
        return

    all_player_rows = []
    for quarter_label, start_range, end_range in PERIOD_RANGES:
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                ep = boxscoretraditionalv3.BoxScoreTraditionalV3(
                    game_id=game_id,
                    start_period=0,
                    end_period=0,
                    range_type=2,
                    start_range=start_range,
                    end_range=end_range,
                    proxy=PROXY_URL,
                )
                time.sleep(API_DELAY)
                break
            except Exception as exc:
                log.warning(f"  BoxScoreTraditionalV3 {game_id} {quarter_label} attempt {attempt}/{RETRY_COUNT} failed: {exc}")
                if attempt < RETRY_COUNT:
                    time.sleep(RETRY_WAIT)
                else:
                    ep = None

        if ep is None:
            continue
        try:
            p_df = ep.player_stats.get_data_frame()
            t_df = ep.team_stats.get_data_frame()
        except Exception as exc:
            log.warning(f"  Parse failed {game_id} {quarter_label}: {exc}")
            continue

        p_rows = _trad_player_rows(game_id, quarter_label, p_df)
        t_rows = _trad_team_rows(game_id, quarter_label, t_df)

        if p_rows:
            _seed_players(p_rows, engine)
            upsert(pd.DataFrame(p_rows), engine,
                   "nba", "player_box_score_stats", ["game_id", "player_id", "quarter"])
            all_player_rows.extend(p_rows)
        if t_rows:
            upsert(pd.DataFrame(t_rows), engine,
                   "nba", "team_box_score_stats", ["game_id", "team_id", "quarter"])
        log.info(f"    {quarter_label}: {len(p_rows)} player, {len(t_rows)} team")

    ot_periods_data = []
    ot_start = OT_START_RANGE
    while True:
        ot_end = ot_start + OT_PERIOD_LEN
        try:
            ep_ot = boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=0,
                end_period=0,
                range_type=2,
                start_range=ot_start,
                end_range=ot_end,
                proxy=PROXY_URL,
            )
            ot_p_df = ep_ot.player_stats.get_data_frame()
            ot_t_df = ep_ot.team_stats.get_data_frame()
        except Exception:
            break
        if ot_p_df is None or ot_p_df.empty:
            break
        has_data = ot_p_df["minutes"].notna().any() if "minutes" in ot_p_df.columns else False
        if not has_data:
            break
        time.sleep(API_DELAY)
        ot_periods_data.append((ot_p_df, ot_t_df))
        ot_start += OT_PERIOD_LEN

    if ot_periods_data:
        ot_p_rows = _sum_ot_player_rows(game_id, ot_periods_data)
        ot_t_rows = _sum_ot_team_rows(game_id, ot_periods_data)
        if ot_p_rows:
            _seed_players(ot_p_rows, engine)
            upsert(pd.DataFrame(ot_p_rows), engine,
                   "nba", "player_box_score_stats", ["game_id", "player_id", "quarter"])
            all_player_rows.extend(ot_p_rows)
        if ot_t_rows:
            upsert(pd.DataFrame(ot_t_rows), engine,
                   "nba", "team_box_score_stats", ["game_id", "team_id", "quarter"])
        log.info(f"    OT ({len(ot_periods_data)} period(s)): {len(ot_p_rows)} player, {len(ot_t_rows)} team")
    else:
        log.info(f"    No overtime for {game_id}")

    log.info(f"  Done {game_id}: {len(all_player_rows)} total player-quarter rows")


# ---------------------------------------------------------------------------
# Pt stats  (direct HTTP, no proxy)
# ---------------------------------------------------------------------------
def _fetch_pt_stats_direct(game_date, pt_measure_type, season, timeout=60):
    date_str = game_date.strftime("%m/%d/%Y")
    encoded  = requests.utils.quote(date_str)
    url = (
        "https://stats.nba.com/stats/leaguedashptstats"
        f"?Season={season}&SeasonType=Regular+Season"
        "&PlayerOrTeam=Player"
        f"&PtMeasureType={pt_measure_type}"
        "&PerMode=Totals&LastNGames=0&Month=0&OpponentTeamID=0"
        f"&DateFrom={encoded}&DateTo={encoded}"
    )
    log.info(f"  Fetching {pt_measure_type} for {date_str} (no proxy)")
    data = _direct_get(url, f"{pt_measure_type} {date_str}", timeout=timeout)
    df   = _parse_result_set(data)
    if df is None or df.empty:
        log.warning(f"  No {pt_measure_type} rows for {date_str}")
        return None
    log.info(f"  {len(df)} rows returned")
    return df

def load_passing_stats(game_date, season, engine):
    df = _fetch_pt_stats_direct(game_date, "Passing", season)
    if df is None or df.empty:
        return 0
    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PLAYER_ID"))
        if pid is None:
            continue
        rows.append({
            "player_id":           pid,
            "game_date":           game_date,
            "player_name":         safe_str(row.get("PLAYER_NAME")),
            "team_id":             safe_int(row.get("TEAM_ID")),
            "team_abbreviation":   safe_str(row.get("TEAM_ABBREVIATION")),
            "potential_ast":       safe_float(row.get("POTENTIAL_AST")),
            "ast":                 safe_float(row.get("AST")),
            "ft_ast":              safe_float(row.get("FT_AST")),
            "secondary_ast":       safe_float(row.get("SECONDARY_AST")),
            "passes_made":         safe_float(row.get("PASSES_MADE")),
            "passes_received":     safe_float(row.get("PASSES_RECEIVED")),
            "ast_points_created":  safe_float(row.get("AST_POINTS_CREATED")),
            "ast_adj":             safe_float(row.get("AST_ADJ")),
            "ast_to_pass_pct":     safe_float(row.get("AST_TO_PASS_PCT")),
            "ast_to_pass_pct_adj": safe_float(row.get("AST_TO_PASS_PCT_ADJ")),
        })
    if rows:
        _seed_players([{"player_id": r["player_id"], "first_name": "",
                        "last_name": r.get("player_name") or "Unknown"} for r in rows], engine)
        upsert(pd.DataFrame(rows), engine, "nba", "player_passing_stats", ["player_id", "game_date"])
        log.info(f"  Passing stats: {len(rows)} rows upserted for {game_date}")
    return len(rows)

def load_rebound_chances(game_date, season, engine):
    df = _fetch_pt_stats_direct(game_date, "Rebounding", season)
    if df is None or df.empty:
        return 0
    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PLAYER_ID"))
        if pid is None:
            continue
        rows.append({
            "player_id":         pid,
            "game_date":         game_date,
            "player_name":       safe_str(row.get("PLAYER_NAME")),
            "team_id":           safe_int(row.get("TEAM_ID")),
            "team_abbreviation": safe_str(row.get("TEAM_ABBREVIATION")),
            "oreb":              safe_float(row.get("OREB")),
            "oreb_chances":      safe_float(row.get("OREB_CHANCES")),
            "dreb":              safe_float(row.get("DREB")),
            "dreb_chances":      safe_float(row.get("DREB_CHANCES")),
            "reb_chances":       safe_float(row.get("REB_CHANCES")),
        })
    if rows:
        _seed_players([{"player_id": r["player_id"], "first_name": "",
                        "last_name": r.get("player_name") or "Unknown"} for r in rows], engine)
        upsert(pd.DataFrame(rows), engine, "nba", "player_rebound_chances", ["player_id", "game_date"])
        log.info(f"  Rebound chances: {len(rows)} rows upserted for {game_date}")
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA ETL")
    parser.add_argument("--days",         type=int, default=3)
    parser.add_argument("--season",       type=str, default="2025-26")
    parser.add_argument("--load-rosters", action="store_true")
    parser.add_argument("--skip-pt-stats",action="store_true")
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set (only needed for box score calls).")

    engine = get_engine()
    ensure_tables(engine)

    load_teams(engine, args.season)

    if args.load_rosters or players_table_empty(engine):
        load_players(engine, args.season)
    else:
        log.info("nba.players already populated, skipping roster load.")

    all_pairs      = get_all_season_game_ids(args.season)
    unloaded_pairs = get_unloaded_game_ids(all_pairs, engine)

    if not unloaded_pairs:
        log.info("Box scores: all games up to date.")
    else:
        oldest_dates = []
        for _, gdate in unloaded_pairs:
            if gdate not in oldest_dates:
                oldest_dates.append(gdate)
            if len(oldest_dates) == args.days:
                break

        batch_pairs = [(gid, gdate) for gid, gdate in unloaded_pairs if gdate in oldest_dates]
        log.info(
            f"Batch: {len(oldest_dates)} date(s), {len(batch_pairs)} game(s). "
            f"{len(unloaded_pairs) - len(batch_pairs)} games remain after this run."
        )

        target_dates = list(set(gdate for _, gdate in batch_pairs))
        game_meta    = fetch_scoreboard_metadata(target_dates, args.season)

        for game_id, game_date in batch_pairs:
            process_game(game_id, game_date, game_meta, engine)

        log.info("Box score phase complete.")

    if not args.skip_pt_stats:
        candidate_dates  = sorted(set(gdate for _, gdate in all_pairs))
        missing_pt_dates = get_unloaded_pt_dates(candidate_dates, engine)
        pt_batch_dates   = missing_pt_dates[:args.days]

        if not pt_batch_dates:
            log.info("Pt stats: all dates up to date.")
        else:
            log.info(f"Pt stats: fetching {len(pt_batch_dates)} date(s), "
                     f"{len(missing_pt_dates) - len(pt_batch_dates)} remain after this run.")
            for i, pt_date in enumerate(pt_batch_dates):
                passing_count = load_passing_stats(pt_date, args.season, engine)
                if passing_count > 0:
                    log.info(f"  Waiting {PT_STATS_BETWEEN_DELAY}s before rebounding call...")
                    time.sleep(PT_STATS_BETWEEN_DELAY)
                load_rebound_chances(pt_date, args.season, engine)
                if i < len(pt_batch_dates) - 1:
                    log.info(f"  Waiting {PT_STATS_BETWEEN_DELAY}s before next date...")
                    time.sleep(PT_STATS_BETWEEN_DELAY)
            log.info("Pt stats phase complete.")
    else:
        log.info("Skipping pt stats.")

    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
