"""
nba_supplemental_etl.py

Loads four NBA tables sourced from the Power Query M queries in nba_M_queries.docx.
Translated faithfully to Python / GitHub Actions pattern.

Tables written
  nba.schedule            Full season schedule from scheduleleaguev2.
                          Truncate-and-reload every run (mirrors M query behavior).
  nba.daily_lineups       Starter / bench / inactive status per player per game.
                          Incremental by game_date.
  nba.player_game_logs    Per-period player stats from playergamelogs endpoint.
                          Incremental by game_date. Covers Q1/Q2/Q3/Q4/OT.
  nba.player_rebound_chances_v2
                          Full rebound chances detail from leaguedashptstats.
                          Incremental by game_date. Superset of the columns in
                          nba.player_rebound_chances (which is written by nba_etl.py).

Design notes
  schedule      - Direct HTTP, no proxy. scheduleleaguev2 works from datacenter IPs.
  lineups       - Direct HTTP, no proxy. Static JSON file endpoint.
  player_game_logs - Direct HTTP, no proxy. playergamelogs works from datacenter IPs.
  rebound_chances_v2 - Direct HTTP, no proxy. leaguedashptstats is called directly
                       with browser headers and explicit proxies=None to bypass any
                       proxy env vars. Validated to work without residential proxy
                       when headers are set correctly.

Args
  --days N       Game dates to process per incremental run (default 10).
  --season S     Season string, e.g. 2025-26 (default 2025-26).
  --skip-schedule    Skip schedule reload.
  --skip-lineups     Skip lineup ingestion.
  --skip-gamelogs    Skip player game log ingestion.
  --skip-rebounding  Skip rebound chances ingestion.

Secrets required
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import math
import os
import time
import logging
from datetime import date, datetime

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
# Constants
# ---------------------------------------------------------------------------
SEASON_DEFAULT = "2025-26"
BATCH_DEFAULT  = 10          # matches M query BatchSize

API_DELAY      = 1.5         # seconds between calls
RETRY_COUNT    = 3
RETRY_WAIT     = 30          # seconds on transient failure
PT_BETWEEN_DELAY = 15        # seconds between leaguedashptstats calls

# Explicit no-proxy dict.  Prevents inheriting NBA_PROXY_URL env var which would
# cause leaguedashptstats and playergamelogs to route through the residential proxy
# unnecessarily (these endpoints work fine from datacenter IPs with browser headers).
NO_PROXY = {"http": None, "https": None}

NBA_HEADERS = {
    "User-Agent":          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":              "application/json, text/plain, */*",
    "Accept-Language":     "en-US,en;q=0.9",
    "x-nba-stats-origin":  "stats",
    "x-nba-stats-token":   "true",
    "Origin":              "https://www.nba.com",
    "Referer":             "https://www.nba.com/",
}

PERIODS = [
    ("1", "Q1"),
    ("2", "Q2"),
    ("3", "Q3"),
    ("4", "Q4"),
    ("OT", "OT"),   # OT: GameSegment=Overtime, Period="" in the API call
]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL = [
    # ------------------------------------------------------------------
    # nba.schedule
    # Full season schedule.  Truncated and reloaded every run.
    # gameId is the natural key.  No FK to nba.games intentionally --
    # schedule may include future games not yet in nba.games.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.schedule') AND type = 'U')
    CREATE TABLE nba.schedule (
        game_id              VARCHAR(15)   NOT NULL,
        game_date            DATE          NOT NULL,
        game_code            VARCHAR(30)   NULL,
        game_status          TINYINT       NULL,
        game_status_text     VARCHAR(30)   NULL,
        game_date_time_est   DATETIME2     NULL,
        game_date_time_utc   DATETIME2     NULL,
        day_name             VARCHAR(15)   NULL,
        month_num            SMALLINT      NULL,
        week_number          SMALLINT      NULL,
        week_name            VARCHAR(30)   NULL,
        game_label           VARCHAR(60)   NULL,
        game_sub_label       VARCHAR(60)   NULL,
        arena_name           VARCHAR(60)   NULL,
        arena_city           VARCHAR(40)   NULL,
        arena_state          VARCHAR(30)   NULL,
        if_necessary         BIT           NULL,
        series_game_number   VARCHAR(5)    NULL,
        series_text          VARCHAR(40)   NULL,
        home_team_id         BIGINT        NULL,
        home_team_city       VARCHAR(30)   NULL,
        home_team_name       VARCHAR(30)   NULL,
        home_team_tricode    CHAR(3)       NULL,
        home_wins            SMALLINT      NULL,
        home_losses          SMALLINT      NULL,
        home_score           SMALLINT      NULL,
        home_seed            SMALLINT      NULL,
        away_team_id         BIGINT        NULL,
        away_team_city       VARCHAR(30)   NULL,
        away_team_name       VARCHAR(30)   NULL,
        away_team_tricode    CHAR(3)       NULL,
        away_wins            SMALLINT      NULL,
        away_losses          SMALLINT      NULL,
        away_score           SMALLINT      NULL,
        away_seed            SMALLINT      NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_schedule PRIMARY KEY (game_id)
    )
    """,
    # ------------------------------------------------------------------
    # nba.daily_lineups
    # Starter / bench / inactive status per player per game date.
    # Incremental by game_date.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.daily_lineups') AND type = 'U')
    CREATE TABLE nba.daily_lineups (
        game_id              VARCHAR(15)   NOT NULL,
        game_date            DATE          NOT NULL,
        home_away            VARCHAR(5)    NOT NULL,
        team_abbreviation    CHAR(3)       NOT NULL,
        player_name          VARCHAR(100)  NOT NULL,
        position             VARCHAR(10)   NULL,
        lineup_status        VARCHAR(20)   NULL,
        roster_status        VARCHAR(20)   NULL,
        starter_status       VARCHAR(10)   NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_daily_lineups PRIMARY KEY (game_id, player_name, home_away)
    )
    """,
    # ------------------------------------------------------------------
    # nba.player_game_logs
    # Per-period player stats from playergamelogs.
    # Incremental by game_date.  Period values: Q1 Q2 Q3 Q4 OT.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_game_logs') AND type = 'U')
    CREATE TABLE nba.player_game_logs (
        season_year          CHAR(7)       NULL,
        player_id            BIGINT        NOT NULL,
        player_name          VARCHAR(100)  NULL,
        team_id              BIGINT        NULL,
        team_tricode         CHAR(3)       NULL,
        game_id              VARCHAR(15)   NOT NULL,
        game_date            DATE          NOT NULL,
        matchup              VARCHAR(20)   NULL,
        period               VARCHAR(5)    NOT NULL,
        minutes              DECIMAL(6,2)  NULL,
        minutes_sec          VARCHAR(20)   NULL,
        fgm                  SMALLINT      NULL,
        fga                  SMALLINT      NULL,
        fg_pct               DECIMAL(6,4)  NULL,
        fg3m                 SMALLINT      NULL,
        fg3a                 SMALLINT      NULL,
        fg3_pct              DECIMAL(6,4)  NULL,
        ftm                  SMALLINT      NULL,
        fta                  SMALLINT      NULL,
        ft_pct               DECIMAL(6,4)  NULL,
        oreb                 SMALLINT      NULL,
        dreb                 SMALLINT      NULL,
        reb                  SMALLINT      NULL,
        ast                  SMALLINT      NULL,
        tov                  SMALLINT      NULL,
        stl                  SMALLINT      NULL,
        blk                  SMALLINT      NULL,
        blka                 SMALLINT      NULL,
        pf                   SMALLINT      NULL,
        pfd                  SMALLINT      NULL,
        pts                  SMALLINT      NULL,
        plus_minus           SMALLINT      NULL,
        dd2                  SMALLINT      NULL,
        td3                  SMALLINT      NULL,
        available_flag       SMALLINT      NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pgl PRIMARY KEY (game_id, player_id, period)
    )
    """,
    # ------------------------------------------------------------------
    # nba.player_rebound_chances_v2
    # Full rebound chances detail.  Superset of nba.player_rebound_chances.
    # Incremental by game_date.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_rebound_chances_v2') AND type = 'U')
    CREATE TABLE nba.player_rebound_chances_v2 (
        player_id            BIGINT        NOT NULL,
        game_date            DATE          NOT NULL,
        player_name          VARCHAR(100)  NULL,
        team_id              BIGINT        NULL,
        team_tricode         CHAR(3)       NULL,
        oreb                 SMALLINT      NULL,
        oreb_contest         SMALLINT      NULL,
        oreb_uncontest       SMALLINT      NULL,
        oreb_contest_pct     DECIMAL(6,4)  NULL,
        oreb_chances         SMALLINT      NULL,
        oreb_chance_pct      DECIMAL(6,4)  NULL,
        oreb_chance_defer    SMALLINT      NULL,
        oreb_chance_pct_adj  DECIMAL(6,4)  NULL,
        avg_oreb_dist        DECIMAL(6,2)  NULL,
        dreb                 SMALLINT      NULL,
        dreb_contest         SMALLINT      NULL,
        dreb_uncontest       SMALLINT      NULL,
        dreb_contest_pct     DECIMAL(6,4)  NULL,
        dreb_chances         SMALLINT      NULL,
        dreb_chance_pct      DECIMAL(6,4)  NULL,
        dreb_chance_defer    SMALLINT      NULL,
        dreb_chance_pct_adj  DECIMAL(6,4)  NULL,
        avg_dreb_dist        DECIMAL(6,2)  NULL,
        reb                  SMALLINT      NULL,
        reb_contest          SMALLINT      NULL,
        reb_uncontest        SMALLINT      NULL,
        reb_contest_pct      DECIMAL(6,4)  NULL,
        reb_chances          SMALLINT      NULL,
        reb_chance_pct       DECIMAL(6,4)  NULL,
        reb_chance_defer     SMALLINT      NULL,
        reb_chance_pct_adj   DECIMAL(6,4)  NULL,
        avg_reb_dist         DECIMAL(6,2)  NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_prc2 PRIMARY KEY (player_id, game_date)
    )
    """,
]

DDL_INDEXES = [
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nba_schedule_game_date') "
    "CREATE NONCLUSTERED INDEX ix_nba_schedule_game_date ON nba.schedule (game_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nba_daily_lineups_game_date') "
    "CREATE NONCLUSTERED INDEX ix_nba_daily_lineups_game_date ON nba.daily_lineups (game_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nba_pgl_game_date') "
    "CREATE NONCLUSTERED INDEX ix_nba_pgl_game_date ON nba.player_game_logs (game_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nba_prc2_game_date') "
    "CREATE NONCLUSTERED INDEX ix_nba_prc2_game_date ON nba.player_rebound_chances_v2 (game_date)",
]


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


def ensure_tables(engine):
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
        for stmt in DDL_INDEXES:
            conn.execute(text(stmt))
    log.info("Schema verified.")


# ---------------------------------------------------------------------------
# Safe type helpers (mirrors nba_etl.py conventions)
# ---------------------------------------------------------------------------
def safe_int(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val):
    try:
        if val is None:
            return None
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None


def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def safe_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        return bool(val)
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes")
    return None


def safe_datetime(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val).to_pydatetime()
    except Exception:
        return None


def safe_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# MERGE upsert (same pattern as nba_etl.py)
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
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    return v


def upsert(df, engine, schema, table, pk_cols):
    if df is None or df.empty:
        return
    records  = [{col: _clean_val(val) for col, val in row.items()}
                for row in df.to_dict(orient="records")]
    non_pk   = [c for c in df.columns if c not in pk_cols]
    col_list = ", ".join(df.columns)
    val_list = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = (
        ", ".join(f"tgt.{c} = src.{c}" for c in non_pk)
        if non_pk
        else f"tgt.{pk_cols[0]} = tgt.{pk_cols[0]}"
    )
    sql = f"""
        MERGE {schema}.{table} AS tgt
        USING (VALUES ({val_list})) AS src ({col_list})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
    """
    with engine.begin() as conn:
        conn.execute(text(sql), records)


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def _get(url, label, params=None, timeout=60):
    """Direct HTTP GET with browser headers and explicit no-proxy."""
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers=NBA_HEADERS,
                params=params,
                proxies=NO_PROXY,
                timeout=timeout,
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                raise ValueError(f"HTTP {resp.status_code}")
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code} (non-retryable)")
            time.sleep(API_DELAY)
            return resp.json()
        except Exception as exc:
            log.warning(f"  {label} attempt {attempt}/{RETRY_COUNT}: {exc}")
            if attempt < RETRY_COUNT:
                wait = 60 if "500" in str(exc) else RETRY_WAIT
                time.sleep(wait)
    log.error(f"  {label} failed after {RETRY_COUNT} attempts")
    return None


def _parse_result_set(data, index=0):
    if data is None:
        return None
    try:
        rs      = data["resultSets"][index]
        headers = rs["headers"]
        rows    = rs["rowSet"]
        if not rows:
            return None
        return pd.DataFrame(rows, columns=headers)
    except Exception as exc:
        log.warning(f"  resultSets parse failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Existing key helpers
# ---------------------------------------------------------------------------
def get_existing_dates(engine, schema, table, col="game_date"):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT DISTINCT {col} FROM {schema}.{table}")
            )
            return {row[0] for row in result}
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Desired game dates from schedule
# ---------------------------------------------------------------------------
def get_season_game_dates(season):
    """
    Fetches scheduleleaguev2 and returns a sorted list of distinct game dates
    (as date objects) that are strictly before today.
    Mirrors the M gameDates / gameList queries.
    """
    url  = "https://stats.nba.com/stats/scheduleleaguev2"
    data = _get(url, "scheduleleaguev2", params={"Season": season, "LeagueID": "00"})
    if data is None:
        return []
    today = date.today()
    dates = []
    for gd_entry in data.get("leagueSchedule", {}).get("gameDates", []):
        raw = gd_entry.get("gameDate")
        if not raw:
            continue
        d = safe_date(raw[:10])
        if d and d < today:
            dates.append(d)
    return sorted(set(dates))


# ===========================================================================
# SCHEDULE  (nba.schedule)
# ===========================================================================
def load_schedule(engine, season):
    """
    Truncates nba.schedule and reloads the full season.
    Mirrors the M SCHEDULE query which rebuilt from scratch on each refresh.
    """
    log.info(f"Loading nba.schedule for season {season}")
    url  = "https://stats.nba.com/stats/scheduleleaguev2"
    data = _get(url, "scheduleleaguev2", params={"Season": season, "LeagueID": "00"})
    if data is None:
        log.error("  scheduleleaguev2 failed -- schedule not loaded")
        return

    rows = []
    for gd_entry in data.get("leagueSchedule", {}).get("gameDates", []):
        raw_date = gd_entry.get("gameDate")
        game_date = safe_date(raw_date[:10]) if raw_date else None
        for g in gd_entry.get("games", []):
            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            rows.append({
                "game_id":              safe_str(g.get("gameId")),
                "game_date":            game_date,
                "game_code":            safe_str(g.get("gameCode")),
                "game_status":          safe_int(g.get("gameStatus")),
                "game_status_text":     safe_str(g.get("gameStatusText")),
                "game_date_time_est":   safe_datetime(g.get("gameDateTimeEst")),
                "game_date_time_utc":   safe_datetime(g.get("gameDateTimeUTC")),
                "day_name":             safe_str(g.get("day")),
                "month_num":            safe_int(g.get("monthNum")),
                "week_number":          safe_int(g.get("weekNumber")),
                "week_name":            safe_str(g.get("weekName")),
                "game_label":           safe_str(g.get("gameLabel")),
                "game_sub_label":       safe_str(g.get("gameSubLabel")),
                "arena_name":           safe_str(g.get("arenaName")),
                "arena_city":           safe_str(g.get("arenaCity")),
                "arena_state":          safe_str(g.get("arenaState")),
                "if_necessary":         safe_bool(g.get("ifNecessary")),
                "series_game_number":   safe_str(g.get("seriesGameNumber")),
                "series_text":          safe_str(g.get("seriesText")),
                "home_team_id":         safe_int(home.get("teamId")),
                "home_team_city":       safe_str(home.get("teamCity")),
                "home_team_name":       safe_str(home.get("teamName")),
                "home_team_tricode":    safe_str(home.get("teamTricode")),
                "home_wins":            safe_int(home.get("wins")),
                "home_losses":          safe_int(home.get("losses")),
                "home_score":           safe_int(home.get("score")),
                "home_seed":            safe_int(home.get("seed")),
                "away_team_id":         safe_int(away.get("teamId")),
                "away_team_city":       safe_str(away.get("teamCity")),
                "away_team_name":       safe_str(away.get("teamName")),
                "away_team_tricode":    safe_str(away.get("teamTricode")),
                "away_wins":            safe_int(away.get("wins")),
                "away_losses":          safe_int(away.get("losses")),
                "away_score":           safe_int(away.get("score")),
                "away_seed":            safe_int(away.get("seed")),
            })

    if not rows:
        log.warning("  No schedule rows produced")
        return

    # Truncate then reload -- same pattern as reference tables in mlb_etl.py.
    # DELETE used instead of TRUNCATE because TRUNCATE is blocked when FK
    # constraints reference the table (even if child tables are empty).
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM nba.schedule"))
    log.info(f"  Cleared existing schedule rows")

    df = pd.DataFrame(rows)
    upsert(df, engine, "nba", "schedule", ["game_id"])
    log.info(f"  {len(df)} schedule rows loaded")


# ===========================================================================
# LINEUPS  (nba.daily_lineups)
# ===========================================================================
def _fetch_lineups_for_date(game_date):
    """
    Fetches the daily lineup JSON for a single date.
    URL format mirrors the M fnGetLineups query:
      https://stats.nba.com/js/data/leaders/00_daily_lineups_{YYYYMMDD}.json
    Returns a list of row dicts or an empty list on failure.
    """
    date_str = game_date.strftime("%Y%m%d")
    url      = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date_str}.json"
    data     = _get(url, f"lineups {date_str}", timeout=30)
    if data is None:
        return []

    rows = []
    for g in data.get("games", []):
        game_id = safe_str(g.get("gameId"))
        if game_id is None:
            continue
        for side, team_key in [("Home", "homeTeam"), ("Away", "awayTeam")]:
            team = g.get(team_key, {})
            team_abbr = safe_str(team.get("teamAbbreviation"))
            for p in team.get("players", []):
                position      = safe_str(p.get("position"))
                roster_status = safe_str(p.get("rosterStatus"))
                # Mirror M starterStatus logic exactly:
                #   if position is not null and not empty -> Starter
                #   elif rosterStatus == "Active"         -> Bench
                #   else                                  -> Inactive
                if position:
                    starter_status = "Starter"
                elif roster_status == "Active":
                    starter_status = "Bench"
                else:
                    starter_status = "Inactive"
                rows.append({
                    "game_id":           game_id,
                    "game_date":         game_date,
                    "home_away":         side,
                    "team_abbreviation": team_abbr,
                    "player_name":       safe_str(p.get("playerName")),
                    "position":          position,
                    "lineup_status":     safe_str(p.get("lineupStatus")),
                    "roster_status":     roster_status,
                    "starter_status":    starter_status,
                })
    return rows


def load_lineups(engine, season, days):
    log.info("Loading nba.daily_lineups")
    all_dates     = get_season_game_dates(season)
    existing      = get_existing_dates(engine, "nba", "daily_lineups")
    missing       = sorted([d for d in all_dates if d not in existing])
    batch         = missing[:days]

    if not batch:
        log.info("  daily_lineups: all dates loaded.")
        return

    log.info(f"  {len(missing)} dates missing, processing {len(batch)}")
    total_rows = 0
    for d in batch:
        rows = _fetch_lineups_for_date(d)
        if not rows:
            log.info(f"  {d}: no data (game may not have lineup file yet)")
            continue
        df = pd.DataFrame(rows)
        upsert(df, engine, "nba", "daily_lineups", ["game_id", "player_name", "home_away"])
        total_rows += len(df)
        log.info(f"  {d}: {len(df)} lineup rows")

    log.info(f"  daily_lineups complete: {total_rows} rows across {len(batch)} dates")


# ===========================================================================
# PLAYER GAME LOGS  (nba.player_game_logs)
# ===========================================================================
def _fetch_game_logs_for_date(game_date, season, period_val, period_label):
    """
    Calls playergamelogs for a single date and period.
    Mirrors the M fnGetBoxScore query exactly:
      - For OT: GameSegment=Overtime, Period="" (blank)
      - For Q1-Q4: Period=1/2/3/4, no GameSegment
      - DateFrom=DateTo=game_date (one date per call)
      - DateTo is always empty in M, but we use DateFrom=DateTo for clean isolation
    """
    fmt_date = game_date.strftime("%m/%d/%Y")
    params = {
        "Season":        season,
        "SeasonType":    "Regular Season",
        "PlayerOrTeam":  "P",
        "MeasureType":   "Base",
        "DateFrom":      fmt_date,
        "DateTo":        fmt_date,
    }
    if period_label == "OT":
        params["GameSegment"] = "Overtime"
        params["Period"]      = ""
    else:
        params["Period"] = period_val

    data = _get(
        "https://stats.nba.com/stats/playergamelogs",
        f"playergamelogs {game_date} {period_label}",
        params=params,
        timeout=60,
    )
    df = _parse_result_set(data)
    if df is None or df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "season_year":   safe_str(row.get("SEASON_YEAR")),
            "player_id":     safe_int(row.get("PLAYER_ID")),
            "player_name":   safe_str(row.get("PLAYER_NAME")),
            "team_id":       safe_int(row.get("TEAM_ID")),
            "team_tricode":  safe_str(row.get("TEAM_ABBREVIATION")),
            "game_id":       safe_str(row.get("GAME_ID")),
            "game_date":     safe_date(row.get("GAME_DATE")),
            "matchup":       safe_str(row.get("MATCHUP")),
            "period":        period_label,
            "minutes":       safe_float(row.get("MIN")),
            "minutes_sec":   safe_str(row.get("MIN_SEC")),
            "fgm":           safe_int(row.get("FGM")),
            "fga":           safe_int(row.get("FGA")),
            "fg_pct":        safe_float(row.get("FG_PCT")),
            "fg3m":          safe_int(row.get("FG3M")),
            "fg3a":          safe_int(row.get("FG3A")),
            "fg3_pct":       safe_float(row.get("FG3_PCT")),
            "ftm":           safe_int(row.get("FTM")),
            "fta":           safe_int(row.get("FTA")),
            "ft_pct":        safe_float(row.get("FT_PCT")),
            "oreb":          safe_int(row.get("OREB")),
            "dreb":          safe_int(row.get("DREB")),
            "reb":           safe_int(row.get("REB")),
            "ast":           safe_int(row.get("AST")),
            "tov":           safe_int(row.get("TOV")),
            "stl":           safe_int(row.get("STL")),
            "blk":           safe_int(row.get("BLK")),
            "blka":          safe_int(row.get("BLKA")),
            "pf":            safe_int(row.get("PF")),
            "pfd":           safe_int(row.get("PFD")),
            "pts":           safe_int(row.get("PTS")),
            "plus_minus":    safe_int(row.get("PLUS_MINUS")),
            "dd2":           safe_int(row.get("DD2")),
            "td3":           safe_int(row.get("TD3")),
            "available_flag": safe_int(row.get("AVAILABLE_FLAG")),
        })
    return rows


def load_game_logs(engine, season, days):
    """
    Incremental load of nba.player_game_logs.
    Processes --days oldest missing dates.
    For each date, makes 5 API calls (Q1/Q2/Q3/Q4/OT).
    M BatchSize was 10 but HalfBatch = 5 was applied for box scores because
    5 calls per date (5 periods) is expensive.  We use days directly as the
    date batch size here; tune --days down if rate limiting occurs.
    """
    log.info("Loading nba.player_game_logs")
    all_dates = get_season_game_dates(season)
    existing  = get_existing_dates(engine, "nba", "player_game_logs")
    missing   = sorted([d for d in all_dates if d not in existing])
    batch     = missing[:days]

    if not batch:
        log.info("  player_game_logs: all dates loaded.")
        return

    log.info(f"  {len(missing)} dates missing, processing {len(batch)}")
    total_rows = 0
    for d in batch:
        date_rows = []
        for period_val, period_label in PERIODS:
            rows = _fetch_game_logs_for_date(d, season, period_val, period_label)
            date_rows.extend(rows)
            if not rows:
                log.info(f"  {d} {period_label}: no data")
            else:
                log.info(f"  {d} {period_label}: {len(rows)} rows")
            time.sleep(API_DELAY)

        if date_rows:
            df = pd.DataFrame(date_rows)
            upsert(df, engine, "nba", "player_game_logs",
                   ["game_id", "player_id", "period"])
            total_rows += len(df)

    log.info(f"  player_game_logs complete: {total_rows} rows across {len(batch)} dates")


# ===========================================================================
# REBOUND CHANCES V2  (nba.player_rebound_chances_v2)
# ===========================================================================
def _fetch_rebound_chances_for_date(game_date, season):
    """
    Calls leaguedashptstats with PtMeasureType=Rebounding for a single date.
    Mirrors the M RebChances FetchDate function exactly.
    """
    fmt_date = game_date.strftime("%m/%d/%Y")
    params = {
        "Season":          season,
        "SeasonType":      "Regular Season",
        "PlayerOrTeam":    "Player",
        "PtMeasureType":   "Rebounding",
        "PerMode":         "Totals",
        "LastNGames":      "0",
        "Month":           "0",
        "OpponentTeamID":  "0",
        "DateFrom":        fmt_date,
        "DateTo":          fmt_date,
    }
    data = _get(
        "https://stats.nba.com/stats/leaguedashptstats",
        f"leaguedashptstats Rebounding {game_date}",
        params=params,
        timeout=60,
    )
    df = _parse_result_set(data)
    if df is None or df.empty:
        return []

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
            "team_tricode":        safe_str(row.get("TEAM_ABBREVIATION")),
            "oreb":                safe_int(row.get("OREB")),
            "oreb_contest":        safe_int(row.get("OREB_CONTEST")),
            "oreb_uncontest":      safe_int(row.get("OREB_UNCONTEST")),
            "oreb_contest_pct":    safe_float(row.get("OREB_CONTEST_PCT")),
            "oreb_chances":        safe_int(row.get("OREB_CHANCES")),
            "oreb_chance_pct":     safe_float(row.get("OREB_CHANCE_PCT")),
            "oreb_chance_defer":   safe_int(row.get("OREB_CHANCE_DEFER")),
            "oreb_chance_pct_adj": safe_float(row.get("OREB_CHANCE_PCT_ADJ")),
            "avg_oreb_dist":       safe_float(row.get("AVG_OREB_DIST")),
            "dreb":                safe_int(row.get("DREB")),
            "dreb_contest":        safe_int(row.get("DREB_CONTEST")),
            "dreb_uncontest":      safe_int(row.get("DREB_UNCONTEST")),
            "dreb_contest_pct":    safe_float(row.get("DREB_CONTEST_PCT")),
            "dreb_chances":        safe_int(row.get("DREB_CHANCES")),
            "dreb_chance_pct":     safe_float(row.get("DREB_CHANCE_PCT")),
            "dreb_chance_defer":   safe_int(row.get("DREB_CHANCE_DEFER")),
            "dreb_chance_pct_adj": safe_float(row.get("DREB_CHANCE_PCT_ADJ")),
            "avg_dreb_dist":       safe_float(row.get("AVG_DREB_DIST")),
            "reb":                 safe_int(row.get("REB")),
            "reb_contest":         safe_int(row.get("REB_CONTEST")),
            "reb_uncontest":       safe_int(row.get("REB_UNCONTEST")),
            "reb_contest_pct":     safe_float(row.get("REB_CONTEST_PCT")),
            "reb_chances":         safe_int(row.get("REB_CHANCES")),
            "reb_chance_pct":      safe_float(row.get("REB_CHANCE_PCT")),
            "reb_chance_defer":    safe_int(row.get("REB_CHANCE_DEFER")),
            "reb_chance_pct_adj":  safe_float(row.get("REB_CHANCE_PCT_ADJ")),
            "avg_reb_dist":        safe_float(row.get("AVG_REB_DIST")),
        })
    return rows


def load_rebound_chances_v2(engine, season, days):
    log.info("Loading nba.player_rebound_chances_v2")
    all_dates = get_season_game_dates(season)
    existing  = get_existing_dates(engine, "nba", "player_rebound_chances_v2")
    missing   = sorted([d for d in all_dates if d not in existing])
    batch     = missing[:days]

    if not batch:
        log.info("  player_rebound_chances_v2: all dates loaded.")
        return

    log.info(f"  {len(missing)} dates missing, processing {len(batch)}")
    total_rows = 0
    for i, d in enumerate(batch):
        rows = _fetch_rebound_chances_for_date(d, season)
        if not rows:
            log.info(f"  {d}: no rebound chances data")
        else:
            df = pd.DataFrame(rows)
            upsert(df, engine, "nba", "player_rebound_chances_v2",
                   ["player_id", "game_date"])
            total_rows += len(df)
            log.info(f"  {d}: {len(df)} rows")

        if i < len(batch) - 1:
            log.info(f"  Waiting {PT_BETWEEN_DELAY}s before next date...")
            time.sleep(PT_BETWEEN_DELAY)

    log.info(f"  player_rebound_chances_v2 complete: {total_rows} rows across {len(batch)} dates")


# ===========================================================================
# Main
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="NBA Supplemental ETL")
    parser.add_argument("--days",             type=int, default=BATCH_DEFAULT)
    parser.add_argument("--season",           type=str, default=SEASON_DEFAULT)
    parser.add_argument("--skip-schedule",    action="store_true")
    parser.add_argument("--skip-lineups",     action="store_true")
    parser.add_argument("--skip-gamelogs",    action="store_true")
    parser.add_argument("--skip-rebounding",  action="store_true")
    args = parser.parse_args()

    engine = get_engine()
    ensure_tables(engine)

    if not args.skip_schedule:
        load_schedule(engine, args.season)

    if not args.skip_lineups:
        load_lineups(engine, args.season, args.days)

    if not args.skip_gamelogs:
        load_game_logs(engine, args.season, args.days)

    if not args.skip_rebounding:
        load_rebound_chances_v2(engine, args.season, args.days)

    log.info("NBA supplemental ETL complete.")


if __name__ == "__main__":
    main()
