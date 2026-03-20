"""
nba_etl.py

NBA ETL for the sports modeling database.
Runs exclusively in GitHub Actions. Never runs locally.

Design
------
Teams:           Hardcoded static dict, zero HTTP calls.
Players:         Direct HTTP to commonallplayers via proxy.
Schedule:        Direct HTTP to scheduleleaguev2 via proxy.
Box scores:      Direct HTTP to playergamelogs via proxy.
                 5 calls per run (one per period: 1Q/2Q/3Q/4Q/OT).
                 DateFrom = earliest missing date, DateTo = empty.
                 ALL rows returned are upserted regardless of date.
                 --days does NOT apply to box scores.
Pt stats:        Direct HTTP to leaguedashptstats via proxy.
                 One passing call + one rebounding call per missing date.
                 DateFrom = DateTo = that date (single-day filter).
                 --days controls how many dates are processed per run.
Daily lineups:   Direct HTTP to NBA daily lineups JSON via proxy.

All stats.nba.com calls are routed through the Webshare rotating residential
proxy (NBA_PROXY_URL secret). GitHub Actions datacenter IPs are throttled or
silently dropped by stats.nba.com regardless of headers.

Tables written
  nba.teams                  Hardcoded seed, every run.
  nba.players                commonallplayers, first run or --load-rosters.
  nba.schedule               scheduleleaguev2, full reload every run.
  nba.games                  Derived from schedule (completed games only).
  nba.player_box_score_stats Quarter-level player stats (1Q/2Q/3Q/4Q/OT).
  nba.player_passing_stats   Daily potential_ast per player.
  nba.player_rebound_chances Daily reb_chances per player.
  nba.daily_lineups          Per-game lineup status, incremental.

Args
  --days N          Number of missing dates to process per run for pt stats
                    (passing and rebounding). Does not affect box scores,
                    which always fetch from the earliest missing date forward.
                    Default: 3.
  --season S        Season string, e.g. 2025-26 (default: 2025-26).
  --load-rosters    Force player reload even if players table is not empty.
  --skip-pt-stats   Skip passing and rebounding stats.
  --skip-lineups    Skip daily lineups.

Secrets required
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import math
import os
import time
import logging
from datetime import date, datetime
from collections import defaultdict

import pandas as pd
import requests
from sqlalchemy import create_engine, text

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
PROXY_URL              = os.environ.get("NBA_PROXY_URL")
API_DELAY              = 1.5
RETRY_WAIT             = 30
RETRY_COUNT            = 3
RETRY_WAIT_TIMEOUT     = 30
RETRY_WAIT_500         = 60
PT_STATS_BETWEEN_DELAY = 15

NBA_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Origin":             "https://www.nba.com",
    "Referer":            "https://www.nba.com/",
}

def get_proxies():
    if not PROXY_URL:
        return None
    return {"http": PROXY_URL, "https": PROXY_URL}

PERIOD_CONFIG = [
    ("1",  None,       "1Q"),
    ("2",  None,       "2Q"),
    ("3",  None,       "3Q"),
    ("4",  None,       "4Q"),
    ("",   "Overtime", "OT"),
]

# ---------------------------------------------------------------------------
# Static team data
# ---------------------------------------------------------------------------
STATIC_TEAMS = [
    (1610612737, "ATL", "Hawks",         "East", "Southeast"),
    (1610612738, "BOS", "Celtics",        "East", "Atlantic"),
    (1610612739, "CLE", "Cavaliers",      "East", "Central"),
    (1610612740, "NOP", "Pelicans",       "West", "Southwest"),
    (1610612741, "CHI", "Bulls",          "East", "Central"),
    (1610612742, "DAL", "Mavericks",      "West", "Southwest"),
    (1610612743, "DEN", "Nuggets",        "West", "Northwest"),
    (1610612744, "GSW", "Warriors",       "West", "Pacific"),
    (1610612745, "HOU", "Rockets",        "West", "Southwest"),
    (1610612746, "LAC", "Clippers",       "West", "Pacific"),
    (1610612747, "LAL", "Lakers",         "West", "Pacific"),
    (1610612748, "MIA", "Heat",           "East", "Southeast"),
    (1610612749, "MIL", "Bucks",          "East", "Central"),
    (1610612750, "MIN", "Timberwolves",   "West", "Northwest"),
    (1610612751, "BKN", "Nets",           "East", "Atlantic"),
    (1610612752, "NYK", "Knicks",         "East", "Atlantic"),
    (1610612753, "ORL", "Magic",          "East", "Southeast"),
    (1610612754, "IND", "Pacers",         "East", "Central"),
    (1610612755, "PHI", "76ers",          "East", "Atlantic"),
    (1610612756, "PHX", "Suns",           "West", "Pacific"),
    (1610612757, "POR", "Trail Blazers",  "West", "Northwest"),
    (1610612758, "SAC", "Kings",          "West", "Pacific"),
    (1610612759, "SAS", "Spurs",          "West", "Southwest"),
    (1610612760, "OKC", "Thunder",        "West", "Northwest"),
    (1610612761, "TOR", "Raptors",        "East", "Atlantic"),
    (1610612762, "UTA", "Jazz",           "West", "Northwest"),
    (1610612763, "MEM", "Grizzlies",      "West", "Southwest"),
    (1610612764, "WAS", "Wizards",        "East", "Southeast"),
    (1610612765, "DET", "Pistons",        "East", "Central"),
    (1610612766, "CHA", "Hornets",        "East", "Southeast"),
]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_STATEMENTS = [
    """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'nba')
        EXEC('CREATE SCHEMA nba')
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.teams') AND type = 'U')
    CREATE TABLE nba.teams (
        team_id      BIGINT      NOT NULL,
        team_name    VARCHAR(60) NOT NULL,
        team_tricode CHAR(3)     NOT NULL,
        conference   VARCHAR(10) NULL,
        division     VARCHAR(20) NULL,
        created_at   DATETIME2   NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_teams   PRIMARY KEY (team_id),
        CONSTRAINT uq_nba_tricode UNIQUE      (team_tricode)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.players') AND type = 'U')
    CREATE TABLE nba.players (
        player_id     BIGINT       NOT NULL,
        player_name   VARCHAR(100) NOT NULL,
        team_id       BIGINT       NULL,
        team_name     VARCHAR(60)  NULL,
        team_tricode  CHAR(3)      NULL,
        roster_status TINYINT      NULL,
        from_year     SMALLINT     NULL,
        to_year       SMALLINT     NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_players      PRIMARY KEY (player_id),
        CONSTRAINT fk_nba_players_team FOREIGN KEY (team_id)
            REFERENCES nba.teams (team_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.schedule') AND type = 'U')
    CREATE TABLE nba.schedule (
        game_id           VARCHAR(15) NOT NULL,
        game_date         DATE        NOT NULL,
        season_type       VARCHAR(20) NULL,
        game_code         VARCHAR(30) NULL,
        game_status       TINYINT     NULL,
        game_status_text  VARCHAR(30) NULL,
        home_team_id      BIGINT      NULL,
        home_team_tricode CHAR(3)     NULL,
        home_score        SMALLINT    NULL,
        away_team_id      BIGINT      NULL,
        away_team_tricode CHAR(3)     NULL,
        away_score        SMALLINT    NULL,
        created_at        DATETIME2   NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_schedule PRIMARY KEY (game_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.games') AND type = 'U')
    CREATE TABLE nba.games (
        game_id           VARCHAR(15) NOT NULL,
        game_date         DATE        NOT NULL,
        season_type       VARCHAR(20) NULL,
        game_code         VARCHAR(30) NULL,
        game_status       TINYINT     NULL,
        game_status_text  VARCHAR(30) NULL,
        home_team_id      BIGINT      NULL,
        home_team_tricode CHAR(3)     NULL,
        home_score        SMALLINT    NULL,
        away_team_id      BIGINT      NULL,
        away_team_tricode CHAR(3)     NULL,
        away_score        SMALLINT    NULL,
        created_at        DATETIME2   NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_games      PRIMARY KEY (game_id),
        CONSTRAINT fk_nba_games_home FOREIGN KEY (home_team_id)
            REFERENCES nba.teams (team_id),
        CONSTRAINT fk_nba_games_away FOREIGN KEY (away_team_id)
            REFERENCES nba.teams (team_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_box_score_stats') AND type = 'U')
    CREATE TABLE nba.player_box_score_stats (
        game_id        VARCHAR(15)  NOT NULL,
        player_id      BIGINT       NOT NULL,
        period         VARCHAR(5)   NOT NULL,
        season_year    VARCHAR(10)  NULL,
        player_name    VARCHAR(100) NULL,
        team_id        BIGINT       NULL,
        team_tricode   CHAR(3)      NULL,
        game_date      DATE         NULL,
        matchup        VARCHAR(20)  NULL,
        minutes        DECIMAL(6,2) NULL,
        minutes_sec    VARCHAR(10)  NULL,
        fgm            SMALLINT     NULL,
        fga            SMALLINT     NULL,
        fg_pct         DECIMAL(6,4) NULL,
        fg3m           SMALLINT     NULL,
        fg3a           SMALLINT     NULL,
        fg3_pct        DECIMAL(6,4) NULL,
        ftm            SMALLINT     NULL,
        fta            SMALLINT     NULL,
        ft_pct         DECIMAL(6,4) NULL,
        oreb           SMALLINT     NULL,
        dreb           SMALLINT     NULL,
        reb            SMALLINT     NULL,
        ast            SMALLINT     NULL,
        tov            SMALLINT     NULL,
        stl            SMALLINT     NULL,
        blk            SMALLINT     NULL,
        blka           SMALLINT     NULL,
        pf             SMALLINT     NULL,
        pfd            SMALLINT     NULL,
        pts            SMALLINT     NULL,
        plus_minus     SMALLINT     NULL,
        dd2            SMALLINT     NULL,
        td3            SMALLINT     NULL,
        available_flag SMALLINT     NULL,
        created_at     DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pbss        PRIMARY KEY (game_id, player_id, period),
        CONSTRAINT fk_nba_pbss_game   FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_pbss_player FOREIGN KEY (player_id)
            REFERENCES nba.players (player_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_passing_stats') AND type = 'U')
    CREATE TABLE nba.player_passing_stats (
        player_id     BIGINT       NOT NULL,
        game_date     DATE         NOT NULL,
        player_name   VARCHAR(100) NULL,
        team_id       BIGINT       NULL,
        team_tricode  CHAR(3)      NULL,
        potential_ast DECIMAL(8,1) NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pps        PRIMARY KEY (player_id, game_date),
        CONSTRAINT fk_nba_pps_player FOREIGN KEY (player_id)
            REFERENCES nba.players (player_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_rebound_chances') AND type = 'U')
    CREATE TABLE nba.player_rebound_chances (
        player_id    BIGINT       NOT NULL,
        game_date    DATE         NOT NULL,
        player_name  VARCHAR(100) NULL,
        team_id      BIGINT       NULL,
        team_tricode CHAR(3)      NULL,
        reb_chances  DECIMAL(8,1) NULL,
        created_at   DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_prc        PRIMARY KEY (player_id, game_date),
        CONSTRAINT fk_nba_prc_player FOREIGN KEY (player_id)
            REFERENCES nba.players (player_id)
    )
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.daily_lineups') AND type = 'U')
    CREATE TABLE nba.daily_lineups (
        game_id        VARCHAR(15)  NOT NULL,
        game_date      DATE         NOT NULL,
        home_away      VARCHAR(5)   NOT NULL,
        team_tricode   CHAR(3)      NOT NULL,
        player_name    VARCHAR(100) NOT NULL,
        position       VARCHAR(10)  NULL,
        lineup_status  VARCHAR(30)  NULL,
        roster_status  VARCHAR(20)  NULL,
        starter_status VARCHAR(10)  NULL,
        created_at     DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_lineups PRIMARY KEY (game_id, team_tricode, player_name)
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
# Direct HTTP helper
# ---------------------------------------------------------------------------
def _direct_get(url, label, params=None, proxies=None, timeout=60):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers=NBA_HEADERS,
                params=params,
                proxies=proxies,
                timeout=timeout,
            )
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
    log.error(f"  {label} failed after {RETRY_COUNT} attempts")
    return None

def _parse_result_set(data, index=0):
    if data is None:
        return None
    try:
        rs      = data["resultSets"][index]
        headers = rs["headers"]
        row_set = rs["rowSet"]
        if not row_set:
            return None
        return pd.DataFrame(row_set, columns=headers)
    except Exception as exc:
        log.warning(f"  resultSets parse failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Teams
# ---------------------------------------------------------------------------
def load_teams(engine):
    log.info("Loading nba.teams from static data")
    rows = [
        {"team_id": tid, "team_name": name, "team_tricode": tricode,
         "conference": conf, "division": div}
        for tid, tricode, name, conf, div in STATIC_TEAMS
    ]
    upsert(pd.DataFrame(rows), engine, "nba", "teams", ["team_id"])
    log.info(f"  {len(rows)} teams upserted")


# ---------------------------------------------------------------------------
# Players
# ---------------------------------------------------------------------------
def players_table_empty(engine):
    with engine.connect() as conn:
        return conn.execute(text("SELECT COUNT(1) FROM nba.players")).scalar() == 0

def load_players(engine, season):
    log.info(f"Loading nba.players via commonallplayers for season {season}")
    url  = (
        "https://stats.nba.com/stats/commonallplayers"
        f"?IsOnlyCurrentSeason=1&LeagueID=00&Season={season}"
    )
    data = _direct_get(url, "commonallplayers", proxies=get_proxies(), timeout=120)
    df   = _parse_result_set(data, index=0)
    if df is None or df.empty:
        log.warning("  commonallplayers returned no data")
        return
    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PERSON_ID"))
        if pid is None:
            continue
        tid = safe_int(row.get("TEAM_ID"))
        if tid == 0:
            tid = None
        rows.append({
            "player_id":     pid,
            "player_name":   safe_str(row.get("DISPLAY_FIRST_LAST")) or "Unknown",
            "team_id":       tid,
            "team_name":     safe_str(row.get("TEAM_NAME")),
            "team_tricode":  safe_str(row.get("TEAM_ABBREVIATION")),
            "roster_status": safe_int(row.get("ROSTERSTATUS")),
            "from_year":     safe_int(row.get("FROM_YEAR")),
            "to_year":       safe_int(row.get("TO_YEAR")),
        })
    if rows:
        upsert(pd.DataFrame(rows), engine, "nba", "players", ["player_id"])
        log.info(f"  {len(rows)} players upserted")
    else:
        log.warning("  No player rows produced")


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------
def load_schedule(engine, season):
    log.info(f"Loading nba.schedule for season {season}")
    url  = f"https://stats.nba.com/stats/scheduleleaguev2?Season={season}&LeagueID=00"
    data = _direct_get(url, "scheduleleaguev2", proxies=get_proxies(), timeout=120)
    if data is None:
        log.error("  scheduleleaguev2 failed, cannot continue")
        return []

    today         = date.today()
    schedule_rows = []
    games_rows    = []

    for gd_block in data.get("leagueSchedule", {}).get("gameDates", []):
        raw_date = gd_block.get("gameDate", "")
        try:
            game_date = datetime.strptime(raw_date[:10], "%m/%d/%Y").date()
        except (ValueError, TypeError):
            continue

        for g in gd_block.get("games", []):
            if g.get("gameLabel") == "Preseason":
                continue
            label = g.get("gameLabel") or ""
            week  = g.get("weekName") or ""
            if label == "Emirates NBA Cup":
                season_type = "IST"
            elif week == "All-Star":
                continue
            elif label == "SoFi Play-In Tournament":
                season_type = "PlayIn"
            elif label == "NBA Finals":
                season_type = "Playoffs"
            else:
                season_type = "Regular Season"

            home    = g.get("homeTeam", {})
            away    = g.get("awayTeam", {})
            home_id = safe_int(home.get("teamId"))
            away_id = safe_int(away.get("teamId"))
            if home_id == 0:
                home_id = None
            if away_id == 0:
                away_id = None

            row = {
                "game_id":           safe_str(g.get("gameId")),
                "game_date":         game_date,
                "season_type":       season_type,
                "game_code":         safe_str(g.get("gameCode")),
                "game_status":       safe_int(g.get("gameStatus")),
                "game_status_text":  safe_str(g.get("gameStatusText")),
                "home_team_id":      home_id,
                "home_team_tricode": safe_str(home.get("teamTricode")),
                "home_score":        safe_int(home.get("score")) if home.get("score") not in (None, "") else None,
                "away_team_id":      away_id,
                "away_team_tricode": safe_str(away.get("teamTricode")),
                "away_score":        safe_int(away.get("score")) if away.get("score") not in (None, "") else None,
            }
            if row["game_id"] is None:
                continue
            schedule_rows.append(row)
            if game_date < today and safe_int(g.get("gameStatus")) == 3:
                games_rows.append(row)

    if schedule_rows:
        upsert(pd.DataFrame(schedule_rows), engine, "nba", "schedule", ["game_id"])
        log.info(f"  {len(schedule_rows)} schedule rows upserted")
    if games_rows:
        upsert(pd.DataFrame(games_rows), engine, "nba", "games", ["game_id"])
        log.info(f"  {len(games_rows)} completed games upserted into nba.games")

    return sorted(
        [(r["game_id"], r["game_date"]) for r in games_rows],
        key=lambda x: x[1],
    )


# ---------------------------------------------------------------------------
# Box scores: always fetch from earliest missing date, no batch limit.
# --days does NOT apply here.
# ---------------------------------------------------------------------------
def _fetch_playergamelogs_from(period_value, game_segment, period_label, date_from, season, timeout=90):
    date_str = date_from.strftime("%m/%d/%Y")
    params = {
        "Season":       season,
        "SeasonType":   "Regular Season",
        "PlayerOrTeam": "P",
        "MeasureType":  "Base",
        "DateFrom":     date_str,
        "DateTo":       "",
    }
    if game_segment:
        params["GameSegment"] = game_segment
        params["Period"]      = ""
    else:
        params["Period"] = period_value

    label = f"playergamelogs {period_label} from {date_str}"
    data  = _direct_get(
        "https://stats.nba.com/stats/playergamelogs",
        label, params=params, proxies=get_proxies(), timeout=timeout,
    )
    df = _parse_result_set(data, index=0)
    if df is None or df.empty:
        log.info(f"  {label}: no rows returned")
        return None
    log.info(f"  {label}: {len(df)} total rows")
    return df


def fetch_and_upsert_box_scores(date_from, season, engine):
    rows_by_date = defaultdict(list)
    for period_value, game_segment, period_label in PERIOD_CONFIG:
        df = _fetch_playergamelogs_from(period_value, game_segment, period_label, date_from, season)
        if df is None:
            continue
        for _, row in df.iterrows():
            pid      = safe_int(row.get("PLAYER_ID"))
            gid      = safe_str(row.get("GAME_ID"))
            row_date = safe_date(row.get("GAME_DATE"))
            if pid is None or gid is None or row_date is None:
                continue
            rows_by_date[row_date].append({
                "game_id":        gid,
                "player_id":      pid,
                "period":         period_label,
                "season_year":    safe_str(row.get("SEASON_YEAR")),
                "player_name":    safe_str(row.get("PLAYER_NAME")),
                "team_id":        safe_int(row.get("TEAM_ID")),
                "team_tricode":   safe_str(row.get("TEAM_ABBREVIATION")),
                "game_date":      row_date,
                "matchup":        safe_str(row.get("MATCHUP")),
                "minutes":        safe_float(row.get("MIN")),
                "minutes_sec":    safe_str(row.get("MIN_SEC")),
                "fgm":            safe_int(row.get("FGM")),
                "fga":            safe_int(row.get("FGA")),
                "fg_pct":         safe_float(row.get("FG_PCT")),
                "fg3m":           safe_int(row.get("FG3M")),
                "fg3a":           safe_int(row.get("FG3A")),
                "fg3_pct":        safe_float(row.get("FG3_PCT")),
                "ftm":            safe_int(row.get("FTM")),
                "fta":            safe_int(row.get("FTA")),
                "ft_pct":         safe_float(row.get("FT_PCT")),
                "oreb":           safe_int(row.get("OREB")),
                "dreb":           safe_int(row.get("DREB")),
                "reb":            safe_int(row.get("REB")),
                "ast":            safe_int(row.get("AST")),
                "tov":            safe_int(row.get("TOV")),
                "stl":            safe_int(row.get("STL")),
                "blk":            safe_int(row.get("BLK")),
                "blka":           safe_int(row.get("BLKA")),
                "pf":             safe_int(row.get("PF")),
                "pfd":            safe_int(row.get("PFD")),
                "pts":            safe_int(row.get("PTS")),
                "plus_minus":     safe_int(row.get("PLUS_MINUS")),
                "dd2":            safe_int(row.get("DD2")),
                "td3":            safe_int(row.get("TD3")),
                "available_flag": safe_int(row.get("AVAILABLE_FLAG")),
            })

    total_rows = 0
    for game_date in sorted(rows_by_date):
        date_rows = rows_by_date[game_date]
        upsert(pd.DataFrame(date_rows), engine, "nba", "player_box_score_stats",
               ["game_id", "player_id", "period"])
        log.info(f"  {len(date_rows)} rows upserted for {game_date}")
        total_rows += len(date_rows)
    log.info(f"  Box scores total: {total_rows} rows across {len(rows_by_date)} date(s)")


def get_earliest_missing_box_date(completed_pairs, engine):
    with engine.connect() as conn:
        loaded = {
            str(row[0]) for row in
            conn.execute(text("SELECT DISTINCT game_date FROM nba.player_box_score_stats"))
        }
    all_dates = sorted(set(gdate for _, gdate in completed_pairs))
    missing   = [d for d in all_dates if str(d) not in loaded]
    log.info(f"  Box scores: {len(loaded)} dates loaded, {len(missing)} remaining")
    return missing[0] if missing else None


# ---------------------------------------------------------------------------
# Pt stats: --days controls how many missing dates are processed per run.
# ---------------------------------------------------------------------------
def get_unloaded_pt_dates(completed_pairs, engine):
    with engine.connect() as conn:
        loaded = {
            str(row[0]) for row in
            conn.execute(text("SELECT DISTINCT game_date FROM nba.player_passing_stats"))
        }
    all_dates = sorted(set(gdate for _, gdate in completed_pairs))
    missing   = [d for d in all_dates if str(d) not in loaded]
    log.info(f"  Pt stats: {len(loaded)} dates loaded, {len(missing)} remaining")
    return missing


def _fetch_pt_stats_direct(game_date, pt_measure_type, season, timeout=60):
    date_str = game_date.strftime("%m/%d/%Y")
    params = {
        "Season":         season,
        "SeasonType":     "Regular Season",
        "PlayerOrTeam":   "Player",
        "PtMeasureType":  pt_measure_type,
        "PerMode":        "Totals",
        "LastNGames":     "0",
        "Month":          "0",
        "OpponentTeamID": "0",
        "DateFrom":       date_str,
        "DateTo":         date_str,
    }
    log.info(f"  Fetching {pt_measure_type} for {date_str} (via proxy)")
    data = _direct_get(
        "https://stats.nba.com/stats/leaguedashptstats",
        f"{pt_measure_type} {date_str}",
        params=params, proxies=get_proxies(), timeout=timeout,
    )
    df = _parse_result_set(data)
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
            "player_id":     pid,
            "game_date":     game_date,
            "player_name":   safe_str(row.get("PLAYER_NAME")),
            "team_id":       safe_int(row.get("TEAM_ID")),
            "team_tricode":  safe_str(row.get("TEAM_ABBREVIATION")),
            "potential_ast": safe_float(row.get("POTENTIAL_AST")),
        })
    if rows:
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
            "player_id":    pid,
            "game_date":    game_date,
            "player_name":  safe_str(row.get("PLAYER_NAME")),
            "team_id":      safe_int(row.get("TEAM_ID")),
            "team_tricode": safe_str(row.get("TEAM_ABBREVIATION")),
            "reb_chances":  safe_float(row.get("REB_CHANCES")),
        })
    if rows:
        upsert(pd.DataFrame(rows), engine, "nba", "player_rebound_chances", ["player_id", "game_date"])
        log.info(f"  Rebound chances: {len(rows)} rows upserted for {game_date}")
    return len(rows)


# ---------------------------------------------------------------------------
# Daily lineups
# ---------------------------------------------------------------------------
def get_lineup_games_to_fetch(schedule_rows_today, engine):
    with engine.connect() as conn:
        loaded_games = {
            str(row[0]) for row in
            conn.execute(text("SELECT DISTINCT game_id FROM nba.daily_lineups"))
        }
    to_fetch = []
    for r in schedule_rows_today:
        gid    = r["game_id"]
        status = r.get("game_status")
        if status != 3:
            to_fetch.append(r)
        elif gid not in loaded_games:
            to_fetch.append(r)
    to_fetch.sort(key=lambda r: r["game_date"])
    log.info(f"  Daily lineups: {len(to_fetch)} games to fetch")
    return to_fetch


def fetch_lineups_for_game_date(game_date):
    date_key = game_date.strftime("%Y%m%d")
    url      = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date_key}.json"
    data     = _direct_get(url, f"daily_lineups {date_key}", proxies=get_proxies(), timeout=30)
    if data is None:
        return []
    rows = []
    for g in data.get("games", []):
        game_id = safe_str(g.get("gameId"))
        if game_id is None:
            continue
        for side, home_away in (("homeTeam", "Home"), ("awayTeam", "Away")):
            team    = g.get(side, {})
            tricode = safe_str(team.get("teamAbbreviation"))
            for p in team.get("players", []):
                pos    = safe_str(p.get("position"))
                roster = safe_str(p.get("rosterStatus"))
                starter = "Starter" if pos else ("Bench" if roster == "Active" else "Inactive")
                rows.append({
                    "game_id":        game_id,
                    "game_date":      game_date,
                    "home_away":      home_away,
                    "team_tricode":   tricode,
                    "player_name":    safe_str(p.get("playerName")),
                    "position":       pos,
                    "lineup_status":  safe_str(p.get("lineupStatus")),
                    "roster_status":  roster,
                    "starter_status": starter,
                })
    return rows


def load_daily_lineups(schedule_today, engine):
    from itertools import groupby
    to_fetch = get_lineup_games_to_fetch(schedule_today, engine)
    if not to_fetch:
        log.info("  Daily lineups: nothing to fetch.")
        return
    for game_date, group in groupby(to_fetch, key=lambda r: r["game_date"]):
        game_ids_for_date = [r["game_id"] for r in group]
        all_rows = fetch_lineups_for_game_date(game_date)
        if not all_rows:
            log.warning(f"  No lineup data for {game_date}")
            continue
        filtered = [r for r in all_rows if r["game_id"] in game_ids_for_date]
        if filtered:
            with engine.begin() as conn:
                for gid in game_ids_for_date:
                    conn.execute(text("DELETE FROM nba.daily_lineups WHERE game_id = :gid"), {"gid": gid})
            upsert(pd.DataFrame(filtered), engine, "nba", "daily_lineups",
                   ["game_id", "team_tricode", "player_name"])
            log.info(f"  Lineups {game_date}: {len(filtered)} rows upserted")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA ETL")
    parser.add_argument("--days",          type=int, default=3,
                        help="Dates per run for pt stats only. Does not affect box scores.")
    parser.add_argument("--season",        type=str, default="2025-26")
    parser.add_argument("--load-rosters",  action="store_true")
    parser.add_argument("--skip-pt-stats", action="store_true")
    parser.add_argument("--skip-lineups",  action="store_true")
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set. All stats.nba.com calls will fail from datacenter IPs.")

    engine = get_engine()
    ensure_tables(engine)
    load_teams(engine)

    if args.load_rosters or players_table_empty(engine):
        load_players(engine, args.season)
    else:
        log.info("nba.players already populated, skipping roster load.")

    completed_pairs = load_schedule(engine, args.season)

    # Box scores: no batch limit, always fetch from earliest missing date.
    if not completed_pairs:
        log.info("Box scores: no completed games found.")
    else:
        date_from = get_earliest_missing_box_date(completed_pairs, engine)
        if date_from is None:
            log.info("Box scores: all dates up to date.")
        else:
            log.info(f"Box scores: fetching from {date_from}, upserting all returned dates.")
            fetch_and_upsert_box_scores(date_from, args.season, engine)
            log.info("Box score phase complete.")

    # Pt stats: --days controls how many dates per run.
    if not args.skip_pt_stats:
        missing_pt = get_unloaded_pt_dates(completed_pairs, engine)
        pt_batch   = missing_pt[:args.days] if args.days else missing_pt
        if not pt_batch:
            log.info("Pt stats: all dates up to date.")
        else:
            remain = len(missing_pt) - len(pt_batch)
            log.info(f"Pt stats: fetching {len(pt_batch)} date(s), {remain} remain after this run.")
            for i, pt_date in enumerate(pt_batch):
                passing_count = load_passing_stats(pt_date, args.season, engine)
                if passing_count > 0:
                    log.info(f"  Waiting {PT_STATS_BETWEEN_DELAY}s before rebounding call...")
                    time.sleep(PT_STATS_BETWEEN_DELAY)
                load_rebound_chances(pt_date, args.season, engine)
                if i < len(pt_batch) - 1:
                    log.info(f"  Waiting {PT_STATS_BETWEEN_DELAY}s before next date...")
                    time.sleep(PT_STATS_BETWEEN_DELAY)
            log.info("Pt stats phase complete.")
    else:
        log.info("Skipping pt stats.")

    # Daily lineups.
    if not args.skip_lineups:
        today = date.today()
        with engine.connect() as conn:
            sched_rows = [
                dict(row._mapping)
                for row in conn.execute(
                    text("SELECT game_id, game_date, game_status FROM nba.schedule WHERE game_date <= :today"),
                    {"today": today},
                )
            ]
        load_daily_lineups(sched_rows, engine)
    else:
        log.info("Skipping daily lineups.")

    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
