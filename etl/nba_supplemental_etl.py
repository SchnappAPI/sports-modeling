"""
nba_supplemental_etl.py

Direct translation of the Power Query M queries in nba_M_queries.docx.
Every endpoint, field name, parameter, and column is taken from the M source.
No redesign. No substitutions.

Tables written
  nba.all_players             commonallplayers, active roster only.
                              Source: PLAYER M query.
                              Truncate-and-reload every run.
  nba.schedule                scheduleleaguev2, full season.
                              Source: SCHEDULE M query.
                              Truncate-and-reload every run.
  nba.daily_lineups           00_daily_lineups_{YYYYMMDD}.json
                              Source: DailyLineups / fnGetLineups M queries.
                              Incremental by game_date.
  nba.player_game_logs        playergamelogs per period (1Q/2Q/3Q/4Q/OT).
                              Source: BoxScores / fnGetBoxScore M queries.
                              Incremental by game_date. DateTo left empty,
                              MinDate strategy mirrors M batch logic exactly.
  nba.player_rebound_chances  leaguedashptstats PtMeasureType=Rebounding.
                              Source: RebChances M query.
                              Incremental by game_date.
  nba.player_passing_stats    leaguedashptstats PtMeasureType=Passing.
                              Source: PotentialAst M query.
                              Incremental by game_date.

M query translation notes
  PLAYER       endpoint: stats.nba.com/stats/commonallplayers
               fields: PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ID, TEAM_NAME,
                       TEAM_ABBREVIATION, ROSTERSTATUS, FROM_YEAR, TO_YEAR
               filter: ROSTERSTATUS = 1 (active only)

  SCHEDULE     endpoint: stats.nba.com/stats/scheduleleaguev2
               drills into leagueSchedule.gameDates[].games[]
               home/away nested under homeTeam{} / awayTeam{}

  fnGetLineups endpoint: stats.nba.com/js/data/leaders/00_daily_lineups_{YYYYMMDD}.json
               gameDate arg is YYYYMMDD string
               starterStatus logic: position not null/empty -> Starter,
               rosterStatus == Active -> Bench, else Inactive

  fnGetBoxScore endpoint: stats.nba.com/stats/playergamelogs
               period labels in M: 1Q, 2Q, 3Q, 4Q, OT
               OT params: GameSegment=Overtime, Period=""
               DateTo always empty (M leaves it blank)
               batch strategy: MinDate of batch, fetch everything >= MinDate
               per period, filter down to batch dates in Python

  RebChances   endpoint: stats.nba.com/stats/leaguedashptstats
               PtMeasureType=Rebounding, DateFrom=DateTo=single date

  PotentialAst endpoint: stats.nba.com/stats/leaguedashptstats
               PtMeasureType=Passing, DateFrom=DateTo=single date

Args
  --days N          Game dates to process per incremental run (default 10,
                    matches M BatchSize). BoxScores use HalfBatch = days // 2
                    matching M HalfBatch logic.
  --season S        Season string e.g. 2025-26 (default 2025-26).
  --skip-players    Skip all_players reload.
  --skip-schedule   Skip schedule reload.
  --skip-lineups    Skip lineup ingestion.
  --skip-gamelogs   Skip player game log ingestion.
  --skip-pt-stats   Skip rebounding and passing stats ingestion.

Secrets required
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
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
SEASON_DEFAULT   = "2025-26"
BATCH_DEFAULT    = 10        # M BatchSize = 10
API_DELAY        = 1.5
RETRY_COUNT      = 3
RETRY_WAIT       = 30
PT_BETWEEN_DELAY = 15        # between leaguedashptstats calls

# Explicit no-proxy: prevents NBA_PROXY_URL env var from routing these calls
# through the residential proxy. All endpoints in this script work from
# datacenter IPs with the browser headers below.
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

# Period labels exactly as M uses them: 1Q 2Q 3Q 4Q OT
# period_value is the Period= param; period_label is what gets stored.
PERIODS = [
    ("1", "1Q"),
    ("2", "2Q"),
    ("3", "3Q"),
    ("4", "4Q"),
    ("",  "OT"),   # OT: Period="" + GameSegment="Overtime"
]

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL = [
    # ------------------------------------------------------------------
    # nba.all_players
    # Source: commonallplayers, active players only (ROSTERSTATUS=1).
    # Matches M PLAYER query field list exactly.
    # Truncate-and-reload every run (M rebuilds from scratch each refresh).
    # Separate from nba.players which uses commonteamroster in nba_etl.py.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.all_players') AND type = 'U')
    CREATE TABLE nba.all_players (
        player_id            BIGINT        NOT NULL,
        player_name          VARCHAR(100)  NOT NULL,
        team_id              BIGINT        NULL,
        team_name            VARCHAR(60)   NULL,
        team_abbreviation    CHAR(3)       NULL,
        roster_status        TINYINT       NULL,
        from_year            SMALLINT      NULL,
        to_year              SMALLINT      NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_all_players PRIMARY KEY (player_id)
    )
    """,
    # ------------------------------------------------------------------
    # nba.schedule
    # Source: scheduleleaguev2 -> leagueSchedule.gameDates[].games[].
    # Matches M SCHEDULE query field list exactly.
    # Truncate-and-reload every run.
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
        home_team_id         INT           NULL,
        home_team_city       VARCHAR(30)   NULL,
        home_team_name       VARCHAR(30)   NULL,
        home_team_tricode    CHAR(3)       NULL,
        home_wins            SMALLINT      NULL,
        home_losses          SMALLINT      NULL,
        home_score           SMALLINT      NULL,
        home_seed            SMALLINT      NULL,
        away_team_id         INT           NULL,
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
    # Source: 00_daily_lineups_{YYYYMMDD}.json -> games[].homeTeam/awayTeam.players[].
    # Matches M fnGetLineups / DailyLineups field list exactly.
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
    # Source: playergamelogs per period.
    # Period values: 1Q 2Q 3Q 4Q OT (matches M fnGetBoxScore periodLabel exactly).
    # Incremental by game_date.
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
    # nba.player_rebound_chances
    # Source: leaguedashptstats PtMeasureType=Rebounding.
    # Matches M RebChances field list exactly.
    # Incremental by game_date.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_rebound_chances') AND type = 'U')
    CREATE TABLE nba.player_rebound_chances (
        game_date            DATE          NOT NULL,
        player_id            BIGINT        NOT NULL,
        player_name          VARCHAR(100)  NULL,
        team_id              INT           NULL,
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
        CONSTRAINT pk_nba_prc PRIMARY KEY (player_id, game_date)
    )
    """,
    # ------------------------------------------------------------------
    # nba.player_passing_stats
    # Source: leaguedashptstats PtMeasureType=Passing.
    # Matches M PotentialAst field list exactly.
    # Incremental by game_date.
    # ------------------------------------------------------------------
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_passing_stats') AND type = 'U')
    CREATE TABLE nba.player_passing_stats (
        game_date            DATE          NOT NULL,
        player_id            BIGINT        NOT NULL,
        player_name          VARCHAR(100)  NULL,
        team_id              INT           NULL,
        team_tricode         CHAR(3)       NULL,
        passes_made          SMALLINT      NULL,
        passes_received      SMALLINT      NULL,
        ft_ast               SMALLINT      NULL,
        secondary_ast        SMALLINT      NULL,
        potential_ast        SMALLINT      NULL,
        ast_pts_created      SMALLINT      NULL,
        ast_adj              SMALLINT      NULL,
        ast_to_pass_pct      DECIMAL(6,4)  NULL,
        ast_to_pass_pct_adj  DECIMAL(6,4)  NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pps PRIMARY KEY (player_id, game_date)
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
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nba_prc_game_date') "
    "CREATE NONCLUSTERED INDEX ix_nba_prc_game_date ON nba.player_rebound_chances (game_date)",
    "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_nba_pps_game_date') "
    "CREATE NONCLUSTERED INDEX ix_nba_pps_game_date ON nba.player_passing_stats (game_date)",
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
# Type helpers
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
# Upsert
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
    records   = [{col: _clean_val(val) for col, val in row.items()}
                 for row in df.to_dict(orient="records")]
    non_pk    = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
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
# HTTP
# ---------------------------------------------------------------------------
def _get(url, label, params=None, timeout=60):
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
# Game date list  (M: gameList query)
# Returns sorted list of date objects strictly before today.
# ---------------------------------------------------------------------------
def get_season_game_dates(season):
    url  = "https://stats.nba.com/stats/scheduleleaguev2"
    data = _get(url, "scheduleleaguev2 (game dates)",
                params={"Season": season, "LeagueID": "00"})
    if data is None:
        return []
    today = date.today()
    dates = []
    for gd_entry in data.get("leagueSchedule", {}).get("gameDates", []):
        raw = gd_entry.get("gameDate")
        if not raw:
            continue
        # M parses "10/02/2025" -> date. pd.to_datetime handles both formats.
        d = safe_date(raw[:10])
        if d and d < today:
            dates.append(d)
    return sorted(set(dates))


# ===========================================================================
# ALL PLAYERS  (M: PLAYER query)
# Endpoint: commonallplayers
# Fields: PERSON_ID, DISPLAY_FIRST_LAST, TEAM_ID, TEAM_NAME,
#         TEAM_ABBREVIATION, ROSTERSTATUS, FROM_YEAR, TO_YEAR
# Filter: ROSTERSTATUS = 1
# Behavior: truncate-and-reload (M rebuilt from scratch each refresh)
# ===========================================================================
def load_all_players(engine, season):
    log.info(f"Loading nba.all_players from commonallplayers season={season}")
    url  = "https://stats.nba.com/stats/commonallplayers"
    data = _get(url, "commonallplayers",
                params={"IsOnlyCurrentSeason": "1", "LeagueID": "00",
                        "Season": season})
    if data is None:
        log.error("  commonallplayers failed -- all_players not loaded")
        return

    df = _parse_result_set(data)
    if df is None or df.empty:
        log.warning("  commonallplayers returned no rows")
        return

    rows = []
    for _, row in df.iterrows():
        # M filter: rosterStatus = 1
        if safe_int(row.get("ROSTERSTATUS")) != 1:
            continue
        rows.append({
            "player_id":          safe_int(row["PERSON_ID"]),
            "player_name":        safe_str(row["DISPLAY_FIRST_LAST"]),
            "team_id":            safe_int(row["TEAM_ID"]),
            "team_name":          safe_str(row["TEAM_NAME"]),
            "team_abbreviation":  safe_str(row["TEAM_ABBREVIATION"]),
            "roster_status":      safe_int(row["ROSTERSTATUS"]),
            "from_year":          safe_int(row["FROM_YEAR"]),
            "to_year":            safe_int(row["TO_YEAR"]),
        })

    if not rows:
        log.warning("  No active players returned")
        return

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM nba.all_players"))
    df_out = pd.DataFrame(rows)
    upsert(df_out, engine, "nba", "all_players", ["player_id"])
    log.info(f"  {len(df_out)} active players loaded")


# ===========================================================================
# SCHEDULE  (M: SCHEDULE query)
# Endpoint: scheduleleaguev2
# Drills: leagueSchedule.gameDates[].games[]
# Behavior: truncate-and-reload
# ===========================================================================
def load_schedule(engine, season):
    log.info(f"Loading nba.schedule for season {season}")
    url  = "https://stats.nba.com/stats/scheduleleaguev2"
    data = _get(url, "scheduleleaguev2 (schedule)",
                params={"Season": season, "LeagueID": "00"})
    if data is None:
        log.error("  scheduleleaguev2 failed -- schedule not loaded")
        return

    rows = []
    for gd_entry in data.get("leagueSchedule", {}).get("gameDates", []):
        raw_date  = gd_entry.get("gameDate")
        game_date = safe_date(raw_date[:10]) if raw_date else None
        for g in gd_entry.get("games", []):
            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            rows.append({
                "game_id":            safe_str(g.get("gameId")),
                "game_date":          game_date,
                "game_code":          safe_str(g.get("gameCode")),
                "game_status":        safe_int(g.get("gameStatus")),
                "game_status_text":   safe_str(g.get("gameStatusText")),
                "game_date_time_est": safe_datetime(g.get("gameDateTimeEst")),
                "game_date_time_utc": safe_datetime(g.get("gameDateTimeUTC")),
                "day_name":           safe_str(g.get("day")),
                "month_num":          safe_int(g.get("monthNum")),
                "week_number":        safe_int(g.get("weekNumber")),
                "week_name":          safe_str(g.get("weekName")),
                "game_label":         safe_str(g.get("gameLabel")),
                "game_sub_label":     safe_str(g.get("gameSubLabel")),
                "arena_name":         safe_str(g.get("arenaName")),
                "arena_city":         safe_str(g.get("arenaCity")),
                "arena_state":        safe_str(g.get("arenaState")),
                "if_necessary":       safe_bool(g.get("ifNecessary")),
                "series_game_number": safe_str(g.get("seriesGameNumber")),
                "series_text":        safe_str(g.get("seriesText")),
                "home_team_id":       safe_int(home.get("teamId")),
                "home_team_city":     safe_str(home.get("teamCity")),
                "home_team_name":     safe_str(home.get("teamName")),
                "home_team_tricode":  safe_str(home.get("teamTricode")),
                "home_wins":          safe_int(home.get("wins")),
                "home_losses":        safe_int(home.get("losses")),
                "home_score":         safe_int(home.get("score")),
                "home_seed":          safe_int(home.get("seed")),
                "away_team_id":       safe_int(away.get("teamId")),
                "away_team_city":     safe_str(away.get("teamCity")),
                "away_team_name":     safe_str(away.get("teamName")),
                "away_team_tricode":  safe_str(away.get("teamTricode")),
                "away_wins":          safe_int(away.get("wins")),
                "away_losses":        safe_int(away.get("losses")),
                "away_score":         safe_int(away.get("score")),
                "away_seed":          safe_int(away.get("seed")),
            })

    if not rows:
        log.warning("  No schedule rows produced")
        return

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM nba.schedule"))
    upsert(pd.DataFrame(rows), engine, "nba", "schedule", ["game_id"])
    log.info(f"  {len(rows)} schedule rows loaded")


# ===========================================================================
# DAILY LINEUPS  (M: DailyLineups / fnGetLineups)
# Endpoint: stats.nba.com/js/data/leaders/00_daily_lineups_{YYYYMMDD}.json
# gameDate arg: YYYYMMDD string (matches M fnGetLineups signature)
# Incremental by game_date.
# ===========================================================================
def _fetch_lineups_for_date(game_date):
    """
    game_date: date object. Converted to YYYYMMDD string to build the URL,
    matching M's fnGetLineups(gameDate as text) signature exactly.
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
            team      = g.get(team_key, {})
            team_abbr = safe_str(team.get("teamAbbreviation"))
            for p in team.get("players", []):
                position      = safe_str(p.get("position"))
                roster_status = safe_str(p.get("rosterStatus"))
                # M starterStatus logic (translated exactly):
                #   if position <> null and position <> "" -> "Starter"
                #   else if rosterStatus = "Active"        -> "Bench"
                #   else                                   -> "Inactive"
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
    all_dates = get_season_game_dates(season)
    existing  = get_existing_dates(engine, "nba", "daily_lineups")
    missing   = sorted([d for d in all_dates if d not in existing])
    batch     = missing[:days]

    if not batch:
        log.info("  daily_lineups: all dates loaded.")
        return

    log.info(f"  {len(missing)} dates missing, processing {len(batch)}")
    total_rows = 0
    for d in batch:
        rows = _fetch_lineups_for_date(d)
        if not rows:
            log.info(f"  {d}: no lineup file (may not exist for this date)")
            continue
        df = pd.DataFrame(rows)
        upsert(df, engine, "nba", "daily_lineups",
               ["game_id", "player_name", "home_away"])
        total_rows += len(df)
        log.info(f"  {d}: {len(df)} rows")

    log.info(f"  daily_lineups complete: {total_rows} rows across {len(batch)} dates")


# ===========================================================================
# PLAYER GAME LOGS  (M: BoxScores / fnGetBoxScore)
# Endpoint: playergamelogs
# Period labels: 1Q 2Q 3Q 4Q OT  (exactly as M stores them)
# OT params: GameSegment=Overtime, Period=""
# DateTo: empty string  (M leaves DateTo blank -- returns all games >= DateFrom)
# Batch strategy: take MinDate of the batch, fetch all >= MinDate per period,
#   then filter down to only batch dates. Mirrors M HalfBatch / MinDate logic.
# Incremental by game_date.
# ===========================================================================
def _fetch_game_logs_for_period(min_date, season, period_val, period_label):
    """
    Fetches playergamelogs for all games on or after min_date for one period.
    DateTo is intentionally left empty, mirroring M fnGetBoxScore exactly.
    Returns list of row dicts.
    """
    fmt_date = min_date.strftime("%m/%d/%Y")
    params = {
        "Season":       season,
        "SeasonType":   "Regular Season",
        "PlayerOrTeam": "P",
        "MeasureType":  "Base",
        "DateFrom":     fmt_date,
        "DateTo":       "",          # M leaves DateTo empty
    }
    if period_label == "OT":
        params["GameSegment"] = "Overtime"
        params["Period"]      = ""
    else:
        params["Period"] = period_val

    data = _get(
        "https://stats.nba.com/stats/playergamelogs",
        f"playergamelogs {min_date} {period_label}",
        params=params,
        timeout=60,
    )
    df = _parse_result_set(data)
    if df is None or df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        rows.append({
            "season_year":    safe_str(row.get("SEASON_YEAR")),
            "player_id":      safe_int(row.get("PLAYER_ID")),
            "player_name":    safe_str(row.get("PLAYER_NAME")),
            "team_id":        safe_int(row.get("TEAM_ID")),
            "team_tricode":   safe_str(row.get("TEAM_ABBREVIATION")),
            "game_id":        safe_str(row.get("GAME_ID")),
            "game_date":      safe_date(row.get("GAME_DATE")),
            "matchup":        safe_str(row.get("MATCHUP")),
            "period":         period_label,   # 1Q / 2Q / 3Q / 4Q / OT
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
    return rows


def load_game_logs(engine, season, days):
    """
    Mirrors M BoxScores batch logic exactly:
      HalfBatch = BatchSize // 2  (M uses HalfBatch for box scores)
      MinDate   = minimum date in the batch
      Fetch all 5 periods from MinDate forward, filter to batch dates only.
    """
    log.info("Loading nba.player_game_logs")
    all_dates = get_season_game_dates(season)
    existing  = get_existing_dates(engine, "nba", "player_game_logs")
    missing   = sorted([d for d in all_dates if d not in existing])

    # M: HalfBatch = BatchSize // 2, minimum 1
    half_batch = max(1, days // 2)
    batch      = missing[:half_batch]

    if not batch:
        log.info("  player_game_logs: all dates loaded.")
        return

    min_date   = min(batch)
    batch_set  = set(batch)
    log.info(f"  {len(missing)} dates missing, HalfBatch={half_batch}, "
             f"MinDate={min_date}, fetching {len(batch)} dates")

    all_rows = []
    for period_val, period_label in PERIODS:
        rows = _fetch_game_logs_for_period(min_date, season, period_val, period_label)
        # Filter to only the batch dates (M fetches >= MinDate then combines
        # with existing history; we filter here since we only want the batch)
        rows = [r for r in rows if r["game_date"] in batch_set]
        log.info(f"  {period_label}: {len(rows)} rows")
        all_rows.extend(rows)
        time.sleep(API_DELAY)

    if all_rows:
        df = pd.DataFrame(all_rows)
        upsert(df, engine, "nba", "player_game_logs",
               ["game_id", "player_id", "period"])
        log.info(f"  player_game_logs complete: {len(df)} rows")
    else:
        log.info("  player_game_logs: no rows returned for batch")


# ===========================================================================
# REBOUND CHANCES  (M: RebChances)
# Endpoint: leaguedashptstats, PtMeasureType=Rebounding
# DateFrom=DateTo=single date (M passes each date individually)
# Incremental by game_date.
# ===========================================================================
def _fetch_rebound_chances_for_date(game_date, season):
    fmt_date = game_date.strftime("%m/%d/%Y")
    params = {
        "Season":         season,
        "SeasonType":     "Regular Season",
        "PlayerOrTeam":   "Player",
        "PtMeasureType":  "Rebounding",
        "PerMode":        "Totals",
        "LastNGames":     "0",
        "Month":          "0",
        "OpponentTeamID": "0",
        "DateFrom":       fmt_date,
        "DateTo":         fmt_date,
    }
    data = _get("https://stats.nba.com/stats/leaguedashptstats",
                f"leaguedashptstats Rebounding {game_date}",
                params=params, timeout=60)
    df = _parse_result_set(data)
    if df is None or df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PLAYER_ID"))
        if pid is None:
            continue
        rows.append({
            "game_date":           game_date,
            "player_id":           pid,
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


# ===========================================================================
# PASSING STATS  (M: PotentialAst)
# Endpoint: leaguedashptstats, PtMeasureType=Passing
# DateFrom=DateTo=single date
# Incremental by game_date.
# ===========================================================================
def _fetch_passing_stats_for_date(game_date, season):
    fmt_date = game_date.strftime("%m/%d/%Y")
    params = {
        "Season":         season,
        "SeasonType":     "Regular Season",
        "PlayerOrTeam":   "Player",
        "PtMeasureType":  "Passing",
        "PerMode":        "Totals",
        "LastNGames":     "0",
        "Month":          "0",
        "OpponentTeamID": "0",
        "DateFrom":       fmt_date,
        "DateTo":         fmt_date,
    }
    data = _get("https://stats.nba.com/stats/leaguedashptstats",
                f"leaguedashptstats Passing {game_date}",
                params=params, timeout=60)
    df = _parse_result_set(data)
    if df is None or df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PLAYER_ID"))
        if pid is None:
            continue
        rows.append({
            "game_date":          game_date,
            "player_id":          pid,
            "player_name":        safe_str(row.get("PLAYER_NAME")),
            "team_id":            safe_int(row.get("TEAM_ID")),
            "team_tricode":       safe_str(row.get("TEAM_ABBREVIATION")),
            "passes_made":        safe_int(row.get("PASSES_MADE")),
            "passes_received":    safe_int(row.get("PASSES_RECEIVED")),
            "ft_ast":             safe_int(row.get("FT_AST")),
            "secondary_ast":      safe_int(row.get("SECONDARY_AST")),
            "potential_ast":      safe_int(row.get("POTENTIAL_AST")),
            "ast_pts_created":    safe_int(row.get("AST_PTS_CREATED")),
            "ast_adj":            safe_int(row.get("AST_ADJ")),
            "ast_to_pass_pct":    safe_float(row.get("AST_TO_PASS_PCT")),
            "ast_to_pass_pct_adj": safe_float(row.get("AST_TO_PASS_PCT_ADJ")),
        })
    return rows


def load_pt_stats(engine, season, days):
    """
    Loads both rebound chances and passing stats.
    Each date makes two leaguedashptstats calls with PT_BETWEEN_DELAY between them,
    matching M's sequential fetch pattern.
    """
    log.info("Loading nba.player_rebound_chances and nba.player_passing_stats")

    all_dates = get_season_game_dates(season)

    reb_existing  = get_existing_dates(engine, "nba", "player_rebound_chances")
    pass_existing = get_existing_dates(engine, "nba", "player_passing_stats")
    # Use rebound chances as the state driver (M used the same gameList for both)
    missing = sorted([d for d in all_dates
                      if d not in reb_existing or d not in pass_existing])
    batch   = missing[:days]

    if not batch:
        log.info("  pt stats: all dates loaded.")
        return

    log.info(f"  {len(missing)} dates missing, processing {len(batch)}")
    reb_total = pass_total = 0

    for i, d in enumerate(batch):
        # Rebounding
        reb_rows = _fetch_rebound_chances_for_date(d, season)
        if reb_rows:
            upsert(pd.DataFrame(reb_rows), engine,
                   "nba", "player_rebound_chances", ["player_id", "game_date"])
            reb_total += len(reb_rows)
            log.info(f"  {d} rebounding: {len(reb_rows)} rows")
        else:
            log.info(f"  {d} rebounding: no data")

        # M waits between the two leaguedashptstats calls
        log.info(f"  Waiting {PT_BETWEEN_DELAY}s before passing call...")
        time.sleep(PT_BETWEEN_DELAY)

        # Passing
        pass_rows = _fetch_passing_stats_for_date(d, season)
        if pass_rows:
            upsert(pd.DataFrame(pass_rows), engine,
                   "nba", "player_passing_stats", ["player_id", "game_date"])
            pass_total += len(pass_rows)
            log.info(f"  {d} passing: {len(pass_rows)} rows")
        else:
            log.info(f"  {d} passing: no data")

        if i < len(batch) - 1:
            log.info(f"  Waiting {PT_BETWEEN_DELAY}s before next date...")
            time.sleep(PT_BETWEEN_DELAY)

    log.info(f"  pt stats complete: {reb_total} rebound rows, "
             f"{pass_total} passing rows across {len(batch)} dates")


# ===========================================================================
# Main
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="NBA Supplemental ETL")
    parser.add_argument("--days",           type=int, default=BATCH_DEFAULT)
    parser.add_argument("--season",         type=str, default=SEASON_DEFAULT)
    parser.add_argument("--skip-players",   action="store_true")
    parser.add_argument("--skip-schedule",  action="store_true")
    parser.add_argument("--skip-lineups",   action="store_true")
    parser.add_argument("--skip-gamelogs",  action="store_true")
    parser.add_argument("--skip-pt-stats",  action="store_true")
    args = parser.parse_args()

    engine = get_engine()
    ensure_tables(engine)

    if not args.skip_players:
        load_all_players(engine, args.season)

    if not args.skip_schedule:
        load_schedule(engine, args.season)

    if not args.skip_lineups:
        load_lineups(engine, args.season, args.days)

    if not args.skip_gamelogs:
        load_game_logs(engine, args.season, args.days)

    if not args.skip_pt_stats:
        load_pt_stats(engine, args.season, args.days)

    log.info("NBA supplemental ETL complete.")


if __name__ == "__main__":
    main()
