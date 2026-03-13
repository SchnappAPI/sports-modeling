"""
nba_etl.py
NBA Stats API ETL script for the sports-modeling database.
Runs in GitHub Actions on the nightly schedule.

Tables populated:
  nba.player_box_score_stats  -- per player, per game, per quarter (wide format)
  nba.player_reb_chances      -- per player, per game rebounding tracking
  nba.player_pot_assists      -- per player, per game potential assists tracking

Upsert key for player_box_score_stats: (game_id, nba_player_id, period)
Upsert key for player_reb_chances:     (game_date, nba_player_id, nba_team_id)
Upsert key for player_pot_assists:     (game_date, nba_player_id, nba_team_id)

Schema notes:
  player_reb_chances PK column is reb_chance_id (not id).
  player_pot_assists PK column is pot_assist_id (not id).
  Both tables use game_date as the date dimension (not snapshot_date).
  game_id is nullable on both tables; the tracking endpoints do not return it.

The NBA Stats API requires browser-mimicking headers or it returns 403.
All credentials are read from environment variables set by GitHub Secrets.
"""

import os
import time
import logging
from datetime import datetime, timedelta, date

import requests
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    driver   = "ODBC+Driver+18+for+SQL+Server"
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"
    return create_engine(conn_str, fast_executemany=True)

# ---------------------------------------------------------------------------
# NBA Stats API helpers
# ---------------------------------------------------------------------------

# These headers are required. Without them the API returns 403.
NBA_HEADERS = {
    "Accept":                    "application/json, text/plain, */*",
    "Accept-Encoding":           "gzip, deflate, br",
    "Accept-Language":           "en-US,en;q=0.9",
    "Connection":                "keep-alive",
    "Host":                      "stats.nba.com",
    "Origin":                    "https://www.nba.com",
    "Referer":                   "https://www.nba.com/",
    "Sec-Fetch-Dest":            "empty",
    "Sec-Fetch-Mode":            "cors",
    "Sec-Fetch-Site":            "same-site",
    "User-Agent":                (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "x-nba-stats-origin":        "stats",
    "x-nba-stats-token":         "true",
}

NBA_BASE = "https://stats.nba.com/stats"

# Polite delay between API calls to avoid rate-limiting
REQUEST_DELAY_SECONDS = 1.0


def nba_get(endpoint: str, params: dict, retries: int = 3) -> dict:
    """
    GET a stats.nba.com endpoint and return the parsed JSON.
    Retries up to `retries` times on transient errors.
    """
    url = f"{NBA_BASE}/{endpoint}"
    for attempt in range(1, retries + 1):
        try:
            time.sleep(REQUEST_DELAY_SECONDS)
            resp = requests.get(url, headers=NBA_HEADERS, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            log.warning("HTTP %s on attempt %d for %s", exc.response.status_code, attempt, endpoint)
            if attempt == retries:
                raise
            time.sleep(5 * attempt)
        except requests.RequestException as exc:
            log.warning("Request error on attempt %d: %s", attempt, exc)
            if attempt == retries:
                raise
            time.sleep(5 * attempt)


def result_set_to_df(data: dict, result_set_index: int = 0) -> pd.DataFrame:
    """Convert a stats.nba.com resultSets block to a DataFrame."""
    rs = data["resultSets"][result_set_index]
    return pd.DataFrame(rs["rowSet"], columns=rs["headers"])


# ---------------------------------------------------------------------------
# Period label normalisation
# The API returns '1Q', '2Q', '3Q', '4Q' and 'OT'.
# Normalise to 'Q1', 'Q2', 'Q3', 'Q4', 'OT'.
# ---------------------------------------------------------------------------
PERIOD_MAP = {"1Q": "Q1", "2Q": "Q2", "3Q": "Q3", "4Q": "Q4", "OT": "OT"}


def normalize_period(raw: str) -> str:
    return PERIOD_MAP.get(str(raw).strip(), str(raw).strip())


# ---------------------------------------------------------------------------
# Determine which season string to pass to the API.
# The NBA season that spans two calendar years is labelled as the starting
# year, e.g. the 2025-26 season is "2025-26".
# ---------------------------------------------------------------------------
def current_season_str() -> str:
    today = date.today()
    # NBA seasons typically start in October.
    # Before October, the current season started in the previous calendar year.
    if today.month >= 10:
        start_year = today.year
    else:
        start_year = today.year - 1
    end_year_short = str(start_year + 1)[-2:]
    return f"{start_year}-{end_year_short}"


# ---------------------------------------------------------------------------
# Fetch quarter-level box scores via PlayerGameLog with Period parameter.
# One call per period (1, 2, 3, 4, 0-for-OT).
# ---------------------------------------------------------------------------
PERIOD_API_VALUES = [
    (1,  "Q1",  None),
    (2,  "Q2",  None),
    (3,  "Q3",  None),
    (4,  "Q4",  None),
    ("", "OT",  "Overtime"),   # OT requires GameSegment=Overtime and Period left blank
]

BOX_SCORE_RENAME = {
    "SEASON_YEAR":      "season_year",
    "PLAYER_ID":        "nba_player_id",
    "PLAYER_NAME":      "player_name",
    "TEAM_ID":          "nba_team_id",
    "TEAM_ABBREVIATION":"nba_team",
    "GAME_ID":          "game_id",
    "GAME_DATE":        "game_date",
    "MATCHUP":          "matchup",
    "WL":               "win_loss",
    "MIN":              "min",
    "FGM":              "fgm",
    "FGA":              "fga",
    "FG_PCT":           "fg_pct",
    "FG3M":             "fg3m",
    "FG3A":             "fg3a",
    "FG3_PCT":          "fg3_pct",
    "FTM":              "ftm",
    "FTA":              "fta",
    "FT_PCT":           "ft_pct",
    "OREB":             "oreb",
    "DREB":             "dreb",
    "REB":              "reb",
    "AST":              "ast",
    "TOV":              "tov",
    "STL":              "stl",
    "BLK":              "blk",
    "BLKA":             "blk_against",
    "PF":               "pf",
    "PFD":              "pfd",
    "PTS":              "pts",
    "PLUS_MINUS":       "plus_minus",
    "DD2":              "double_double",
    "TD3":              "triple_double",
    "AVAILABLE_FLAG":   "available_flag",
    "MIN_SEC":          "min_sec",
}

BOX_SCORE_KEEP = list(BOX_SCORE_RENAME.values()) + ["period"]


def fetch_box_scores(season: str) -> pd.DataFrame:
    """
    Fetch per-quarter box scores for all players for the given season.
    Returns a combined DataFrame across all periods with a 'period' column.

    Q1-Q4 use Period=1-4 with no GameSegment parameter.
    OT uses Period='' (blank) and GameSegment=Overtime, matching the Power Query fnGetPeriod pattern.
    """
    frames = []
    for period_value, period_label, game_segment in PERIOD_API_VALUES:
        log.info("Fetching box scores period %s season %s", period_label, season)

        params = {
            "Season":       season,
            "SeasonType":   "Regular Season",
            "PlayerOrTeam": "P",
            "MeasureType":  "Base",
            "Period":       period_value,
            "DateFrom":     "",
            "DateTo":       "",
        }
        if game_segment:
            params["GameSegment"] = game_segment

        try:
            data = nba_get("playergamelogs", params=params)
            df = result_set_to_df(data)
        except Exception as exc:
            log.error("Failed to fetch period %s: %s", period_label, exc)
            continue

        if df.empty:
            log.info("No rows returned for period %s", period_label)
            continue

        # Filter out rows where AVAILABLE_FLAG is 0 (player did not play)
        if "AVAILABLE_FLAG" in df.columns:
            df = df[df["AVAILABLE_FLAG"] != 0].copy()

        # Attach normalised period label
        df["period"] = period_label

        # Rename columns
        df = df.rename(columns=BOX_SCORE_RENAME)

        # Keep only the columns we care about
        cols_present = [c for c in BOX_SCORE_KEEP if c in df.columns]
        df = df[cols_present].copy()

        # Type coercions
        df["game_date"]     = pd.to_datetime(df["game_date"]).dt.date
        df["nba_player_id"] = pd.to_numeric(df["nba_player_id"], errors="coerce").astype("Int64")
        df["nba_team_id"]   = pd.to_numeric(df["nba_team_id"],   errors="coerce").astype("Int64")
        df["min"]           = pd.to_numeric(df["min"],            errors="coerce")

        for int_col in ["fgm","fga","fg3m","fg3a","ftm","fta","oreb","dreb","reb",
                        "ast","tov","stl","blk","blk_against","pf","pfd","pts",
                        "plus_minus","double_double","triple_double","available_flag"]:
            if int_col in df.columns:
                df[int_col] = pd.to_numeric(df[int_col], errors="coerce").astype("Int64")

        for pct_col in ["fg_pct","fg3_pct","ft_pct"]:
            if pct_col in df.columns:
                df[pct_col] = pd.to_numeric(df[pct_col], errors="coerce")

        frames.append(df)
        log.info("Period %s: %d rows", period_label, len(df))

    if not frames:
        log.warning("No box score data fetched for season %s", season)
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    log.info("Total box score rows for %s: %d", season, len(combined))
    return combined


# ---------------------------------------------------------------------------
# Fetch rebounding chances via LeagueDashPtReb
# ---------------------------------------------------------------------------
REB_RENAME = {
    "PLAYER_ID":        "nba_player_id",
    "PLAYER_NAME":      "player_name",
    "TEAM_ID":          "nba_team_id",
    "TEAM_ABBREVIATION":"nba_team",
    "OREB":             "oreb",
    "OREB_CHANCES":     "oreb_chances",
    "DREB":             "dreb",
    "DREB_CHANCES":     "dreb_chances",
    "REB_CHANCES":      "reb_chances",
}


def fetch_reb_chances(season: str, date_from: str = "", date_to: str = "") -> pd.DataFrame:
    """
    Fetch per-player per-game rebounding chances.
    When date_from and date_to are both provided the API returns a single-date
    slice; call once per game date for incremental loads, or without dates for
    the full season.

    Note: The LeagueDashPtReb endpoint returns season-to-date aggregates, not
    per-game rows. To get per-game detail we call PlayerGameLogs from the
    rebounding tracking endpoint. The correct endpoint for per-game is
    playerindex/leaguedashptreb with PerMode=PerGame, but for per-game
    individual rows we use the player tracking game log endpoint below.
    """
    log.info("Fetching rebounding chances season %s (DateFrom=%r DateTo=%r)", season, date_from, date_to)
    try:
        data = nba_get(
            "leaguedashptstats",
            params={
                "Season":         season,
                "SeasonType":     "Regular Season",
                "PerMode":        "PerGame",
                "PlayerOrTeam":   "Player",
                "PtMeasureType":  "Rebounding",
                "LeagueID":       "00",
                "DateFrom":       date_from,
                "DateTo":         date_to,
                "GameScope":      "",
                "PlayerExperience": "",
                "PlayerPosition": "",
                "StarterBench":   "",
                "LastNGames":     0,
                "Month":          0,
                "OpponentTeamID": 0,
                "TeamID":         0,
                "VsConference":   "",
                "VsDivision":     "",
            },
        )
        df = result_set_to_df(data)
    except Exception as exc:
        log.error("Failed to fetch rebounding chances: %s", exc)
        return pd.DataFrame()

    if df.empty:
        return df

    df = df.rename(columns=REB_RENAME)

    # The tracking endpoint does not return a per-row game date.
    # Use date_to when provided (incremental load), otherwise today.
    # This maps to game_date in the database schema.
    capture_date = date_to if date_to else str(date.today())
    df["game_date"] = pd.to_datetime(capture_date).date()

    for col in ["nba_player_id","nba_team_id","oreb","oreb_chances","dreb","dreb_chances","reb_chances"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    keep = [c for c in ["game_date","nba_player_id","player_name","nba_team_id","nba_team",
                         "oreb","oreb_chances","dreb","dreb_chances","reb_chances"] if c in df.columns]
    df = df[keep].copy()
    log.info("Rebounding chances: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Fetch potential assists via LeagueDashPtStats (Passing)
# ---------------------------------------------------------------------------
AST_RENAME = {
    "PLAYER_ID":        "nba_player_id",
    "PLAYER_NAME":      "player_name",
    "TEAM_ID":          "nba_team_id",
    "TEAM_ABBREVIATION":"nba_team",
    "POTENTIAL_AST":    "potential_ast",
}


def fetch_pot_assists(season: str, date_from: str = "", date_to: str = "") -> pd.DataFrame:
    """
    Fetch per-player potential assists from the NBA passing tracking endpoint.
    Like rebounding chances, this returns season-to-date aggregates per call.
    A snapshot_date column is added to record the capture date.
    """
    log.info("Fetching potential assists season %s (DateFrom=%r DateTo=%r)", season, date_from, date_to)
    try:
        data = nba_get(
            "leaguedashptstats",
            params={
                "Season":         season,
                "SeasonType":     "Regular Season",
                "PerMode":        "PerGame",
                "PlayerOrTeam":   "Player",
                "PtMeasureType":  "Passing",
                "LeagueID":       "00",
                "DateFrom":       date_from,
                "DateTo":         date_to,
                "GameScope":      "",
                "PlayerExperience": "",
                "PlayerPosition": "",
                "StarterBench":   "",
                "LastNGames":     0,
                "Month":          0,
                "OpponentTeamID": 0,
                "TeamID":         0,
                "VsConference":   "",
                "VsDivision":     "",
            },
        )
        df = result_set_to_df(data)
    except Exception as exc:
        log.error("Failed to fetch potential assists: %s", exc)
        return pd.DataFrame()

    if df.empty:
        return df

    df = df.rename(columns=AST_RENAME)

    capture_date = date_to if date_to else str(date.today())
    df["game_date"] = pd.to_datetime(capture_date).date()

    for col in ["nba_player_id","nba_team_id","potential_ast"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    keep = [c for c in ["game_date","nba_player_id","player_name","nba_team_id","nba_team",
                         "potential_ast"] if c in df.columns]
    df = df[keep].copy()
    log.info("Potential assists: %d rows", len(df))
    return df


# ---------------------------------------------------------------------------
# Upsert helpers
# ---------------------------------------------------------------------------

def upsert_box_scores(df: pd.DataFrame, engine) -> None:
    """
    Upsert nba.player_box_score_stats.
    Upsert key: (game_id, nba_player_id, period).
    Uses a staging table + MERGE pattern against Azure SQL.
    """
    if df.empty:
        log.info("Box scores DataFrame is empty, skipping upsert.")
        return

    staging = "##nba_box_stage"
    log.info("Loading %d rows into staging table %s", len(df), staging)

    with engine.begin() as conn:
        # Write to staging
        df.to_sql(
            name=staging.replace("##",""),
            con=conn,
            schema=None,
            if_exists="replace",
            index=False,
            method="multi",
        )

    # MERGE into the target table
    merge_sql = """
    MERGE nba.player_box_score_stats AS tgt
    USING (
        SELECT
            game_id, nba_player_id, period,
            season_year, player_name, nba_team_id, nba_team,
            game_date, matchup, win_loss,
            min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
            ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk,
            blk_against, pf, pfd, pts, plus_minus,
            double_double, triple_double, available_flag, min_sec
        FROM nba_box_stage
    ) AS src
        ON tgt.game_id        = src.game_id
       AND tgt.nba_player_id  = src.nba_player_id
       AND tgt.period         = src.period
    WHEN MATCHED THEN UPDATE SET
        season_year    = src.season_year,
        player_name    = src.player_name,
        nba_team_id    = src.nba_team_id,
        nba_team       = src.nba_team,
        game_date      = src.game_date,
        matchup        = src.matchup,
        win_loss       = src.win_loss,
        min            = src.min,
        fgm            = src.fgm,
        fga            = src.fga,
        fg_pct         = src.fg_pct,
        fg3m           = src.fg3m,
        fg3a           = src.fg3a,
        fg3_pct        = src.fg3_pct,
        ftm            = src.ftm,
        fta            = src.fta,
        ft_pct         = src.ft_pct,
        oreb           = src.oreb,
        dreb           = src.dreb,
        reb            = src.reb,
        ast            = src.ast,
        tov            = src.tov,
        stl            = src.stl,
        blk            = src.blk,
        blk_against    = src.blk_against,
        pf             = src.pf,
        pfd            = src.pfd,
        pts            = src.pts,
        plus_minus     = src.plus_minus,
        double_double  = src.double_double,
        triple_double  = src.triple_double,
        available_flag = src.available_flag,
        min_sec        = src.min_sec
    WHEN NOT MATCHED THEN INSERT (
        game_id, nba_player_id, period,
        season_year, player_name, nba_team_id, nba_team,
        game_date, matchup, win_loss,
        min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
        ftm, fta, ft_pct, oreb, dreb, reb, ast, tov, stl, blk,
        blk_against, pf, pfd, pts, plus_minus,
        double_double, triple_double, available_flag, min_sec
    ) VALUES (
        src.game_id, src.nba_player_id, src.period,
        src.season_year, src.player_name, src.nba_team_id, src.nba_team,
        src.game_date, src.matchup, src.win_loss,
        src.min, src.fgm, src.fga, src.fg_pct, src.fg3m, src.fg3a, src.fg3_pct,
        src.ftm, src.fta, src.ft_pct, src.oreb, src.dreb, src.reb, src.ast,
        src.tov, src.stl, src.blk, src.blk_against, src.pf, src.pfd, src.pts,
        src.plus_minus, src.double_double, src.triple_double,
        src.available_flag, src.min_sec
    );
    """
    with engine.begin() as conn:
        result = conn.execute(text(merge_sql))
        log.info("Box score MERGE complete: %d rows affected", result.rowcount)


def upsert_reb_chances(df: pd.DataFrame, engine) -> None:
    """
    Upsert nba.player_reb_chances.
    Upsert key: (game_date, nba_player_id, nba_team_id).
    PK column in the database is reb_chance_id (identity, not written by ETL).
    game_id is nullable on the table; the tracking endpoint does not return one.
    """
    if df.empty:
        log.info("Rebounding chances DataFrame is empty, skipping upsert.")
        return

    staging = "nba_reb_stage"
    log.info("Loading %d rows into rebounding staging", len(df))

    with engine.begin() as conn:
        df.to_sql(staging, conn, schema=None, if_exists="replace", index=False, method="multi")

    merge_sql = """
    MERGE nba.player_reb_chances AS tgt
    USING nba_reb_stage AS src
        ON tgt.game_date      = src.game_date
       AND tgt.nba_player_id  = src.nba_player_id
       AND tgt.nba_team_id    = src.nba_team_id
    WHEN MATCHED THEN UPDATE SET
        player_name   = src.player_name,
        nba_team      = src.nba_team,
        oreb          = src.oreb,
        oreb_chances  = src.oreb_chances,
        dreb          = src.dreb,
        dreb_chances  = src.dreb_chances,
        reb_chances   = src.reb_chances
    WHEN NOT MATCHED THEN INSERT (
        game_date, nba_player_id, player_name,
        nba_team_id, nba_team,
        oreb, oreb_chances, dreb, dreb_chances, reb_chances
    ) VALUES (
        src.game_date, src.nba_player_id, src.player_name,
        src.nba_team_id, src.nba_team,
        src.oreb, src.oreb_chances, src.dreb, src.dreb_chances, src.reb_chances
    );
    """
    with engine.begin() as conn:
        result = conn.execute(text(merge_sql))
        log.info("Rebounding chances MERGE complete: %d rows affected", result.rowcount)


def upsert_pot_assists(df: pd.DataFrame, engine) -> None:
    """
    Upsert nba.player_pot_assists.
    Upsert key: (game_date, nba_player_id, nba_team_id).
    PK column in the database is pot_assist_id (identity, not written by ETL).
    game_id is nullable on the table; the tracking endpoint does not return one.
    """
    if df.empty:
        log.info("Potential assists DataFrame is empty, skipping upsert.")
        return

    staging = "nba_ast_stage"
    log.info("Loading %d rows into potential assists staging", len(df))

    with engine.begin() as conn:
        df.to_sql(staging, conn, schema=None, if_exists="replace", index=False, method="multi")

    merge_sql = """
    MERGE nba.player_pot_assists AS tgt
    USING nba_ast_stage AS src
        ON tgt.game_date      = src.game_date
       AND tgt.nba_player_id  = src.nba_player_id
       AND tgt.nba_team_id    = src.nba_team_id
    WHEN MATCHED THEN UPDATE SET
        player_name   = src.player_name,
        nba_team      = src.nba_team,
        potential_ast = src.potential_ast
    WHEN NOT MATCHED THEN INSERT (
        game_date, nba_player_id, player_name,
        nba_team_id, nba_team, potential_ast
    ) VALUES (
        src.game_date, src.nba_player_id, src.player_name,
        src.nba_team_id, src.nba_team, src.potential_ast
    );
    """
    with engine.begin() as conn:
        result = conn.execute(text(merge_sql))
        log.info("Potential assists MERGE complete: %d rows affected", result.rowcount)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    season = current_season_str()
    log.info("Starting NBA ETL for season %s", season)

    engine = get_engine()

    # -- Box scores (all quarters, full season, upsert handles duplicates)
    box_df = fetch_box_scores(season)
    upsert_box_scores(box_df, engine)

    # -- Rebounding chances (season-to-date snapshot)
    reb_df = fetch_reb_chances(season)
    upsert_reb_chances(reb_df, engine)

    # -- Potential assists (season-to-date snapshot)
    ast_df = fetch_pot_assists(season)
    upsert_pot_assists(ast_df, engine)

    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
