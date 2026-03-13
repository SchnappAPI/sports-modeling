"""
NBA ETL - Quarter Box Scores
Uses nba_api (https://github.com/swar/nba_api) to fetch per-quarter player
box scores for yesterday's games and upsert them into nba.player_box_score_stats.

Run environment: GitHub Actions (Python 3.10+)
Target table:    nba.player_box_score_stats
"""

import logging
import os
import time
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import boxscoretraditionalv3, leaguegamefinder

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SEASON = "2025-26"
SEASON_TYPE = "Regular Season"
LEAGUE_ID = "00"
QUARTERS = [1, 2, 3, 4]

# nba_api built-in rate limiting: pause between every API call to avoid 429s
# The library has its own delay but we add an explicit one for safety
CALL_DELAY_SECONDS = 1.0

# How many times to retry a failed API call before skipping
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 10

# ---------------------------------------------------------------------------
# Column map: nba_api camelCase field -> SQL column name
# Only the fields we want to keep. Others are dropped.
# ---------------------------------------------------------------------------
PLAYER_COLUMN_MAP = {
    "gameId":                   "game_id",
    "personId":                 "player_id",
    "firstName":                "first_name",
    "familyName":               "last_name",
    "teamId":                   "team_id",
    "teamTricode":              "team_abbreviation",
    "position":                 "position",
    "comment":                  "comment",
    "jerseyNum":                "jersey_num",
    "minutes":                  "min",
    "fieldGoalsMade":           "fgm",
    "fieldGoalsAttempted":      "fga",
    "fieldGoalsPercentage":     "fg_pct",
    "threePointersMade":        "fg3m",
    "threePointersAttempted":   "fg3a",
    "threePointersPercentage":  "fg3_pct",
    "freeThrowsMade":           "ftm",
    "freeThrowsAttempted":      "fta",
    "freeThrowsPercentage":     "ft_pct",
    "reboundsOffensive":        "oreb",
    "reboundsDefensive":        "dreb",
    "reboundsTotal":            "reb",
    "assists":                  "ast",
    "steals":                   "stl",
    "blocks":                   "blk",
    "turnovers":                "tov",
    "foulsPersonal":            "pf",
    "points":                   "pts",
    "plusMinusPoints":          "plus_minus",
}


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------
def get_engine():
    server = os.environ["DB_SERVER"]
    database = os.environ["DB_NAME"]
    username = os.environ["DB_USERNAME"]
    password = os.environ["DB_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )
    return create_engine(conn_str, fast_executemany=True)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------
def fetch_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) with retry logic. Returns result or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = fn(*args, **kwargs)
            time.sleep(CALL_DELAY_SECONDS)
            return result
        except Exception as exc:
            log.warning("Attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
    log.error("All %d attempts failed for %s %s", MAX_RETRIES, fn.__name__, kwargs)
    return None


def get_game_ids_for_date(game_date: date) -> list[str]:
    """
    Return unique game IDs for all games played on game_date.
    Uses LeagueGameFinder filtered by date range equal to the single date.
    Returns an empty list if no games were found or the call failed.
    """
    date_str = game_date.strftime("%m/%d/%Y")
    log.info("Fetching game IDs for %s", date_str)

    def _call():
        finder = leaguegamefinder.LeagueGameFinder(
            league_id_nullable=LEAGUE_ID,
            date_from_nullable=date_str,
            date_to_nullable=date_str,
            season_type_nullable=SEASON_TYPE,
            timeout=30,
        )
        return finder.get_data_frames()[0]

    df = fetch_with_retry(_call)
    if df is None or df.empty:
        log.warning("No games found for %s", date_str)
        return []

    # GAME_ID appears twice per game (one row per team). Deduplicate.
    ids = df["GAME_ID"].unique().tolist()
    log.info("Found %d game(s) on %s: %s", len(ids), date_str, ids)
    return ids


def fetch_quarter_box_score(game_id: str, quarter: int) -> pd.DataFrame | None:
    """
    Fetch player box score for a single quarter of a single game.
    Returns a DataFrame with renamed columns plus a 'quarter' column,
    or None if the call failed.
    """
    log.info("  Q%d  game %s", quarter, game_id)

    def _call():
        box = boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=game_id,
            start_period=quarter,
            end_period=quarter,
            range_type=0,
            start_range=0,
            end_range=0,
            timeout=30,
        )
        # get_data_frames()[0] is PlayerStats after nba_api >= 1.10.1
        return box.get_data_frames()[0]

    df = fetch_with_retry(_call)
    if df is None or df.empty:
        return None

    # Keep only the columns we want and rename them
    cols_present = [c for c in PLAYER_COLUMN_MAP if c in df.columns]
    df = df[cols_present].rename(columns=PLAYER_COLUMN_MAP)
    df["quarter"] = f"Q{quarter}"

    # Drop rows where the player did not play (null minutes / comment = 'DNP')
    df = df[df["comment"].str.upper().str.startswith("DNP") == False].copy()  # noqa: E712

    return df


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
UPSERT_SQL = text("""
MERGE nba.player_box_score_stats AS target
USING (VALUES (
    :game_id, :player_id, :quarter, :first_name, :last_name,
    :team_id, :team_abbreviation, :position, :comment, :jersey_num,
    :min, :fgm, :fga, :fg_pct, :fg3m, :fg3a, :fg3_pct,
    :ftm, :fta, :ft_pct, :oreb, :dreb, :reb,
    :ast, :stl, :blk, :tov, :pf, :pts, :plus_minus
)) AS source (
    game_id, player_id, quarter, first_name, last_name,
    team_id, team_abbreviation, position, comment, jersey_num,
    min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
    ftm, fta, ft_pct, oreb, dreb, reb,
    ast, stl, blk, tov, pf, pts, plus_minus
)
ON target.game_id = source.game_id
   AND target.player_id = source.player_id
   AND target.quarter = source.quarter
WHEN MATCHED THEN UPDATE SET
    first_name        = source.first_name,
    last_name         = source.last_name,
    team_id           = source.team_id,
    team_abbreviation = source.team_abbreviation,
    position          = source.position,
    comment           = source.comment,
    jersey_num        = source.jersey_num,
    min               = source.min,
    fgm               = source.fgm,
    fga               = source.fga,
    fg_pct            = source.fg_pct,
    fg3m              = source.fg3m,
    fg3a              = source.fg3a,
    fg3_pct           = source.fg3_pct,
    ftm               = source.ftm,
    fta               = source.fta,
    ft_pct            = source.ft_pct,
    oreb              = source.oreb,
    dreb              = source.dreb,
    reb               = source.reb,
    ast               = source.ast,
    stl               = source.stl,
    blk               = source.blk,
    tov               = source.tov,
    pf                = source.pf,
    pts               = source.pts,
    plus_minus        = source.plus_minus
WHEN NOT MATCHED THEN INSERT (
    game_id, player_id, quarter, first_name, last_name,
    team_id, team_abbreviation, position, comment, jersey_num,
    min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
    ftm, fta, ft_pct, oreb, dreb, reb,
    ast, stl, blk, tov, pf, pts, plus_minus
) VALUES (
    source.game_id, source.player_id, source.quarter,
    source.first_name, source.last_name,
    source.team_id, source.team_abbreviation,
    source.position, source.comment, source.jersey_num,
    source.min, source.fgm, source.fga, source.fg_pct,
    source.fg3m, source.fg3a, source.fg3_pct,
    source.ftm, source.fta, source.ft_pct,
    source.oreb, source.dreb, source.reb,
    source.ast, source.stl, source.blk, source.tov,
    source.pf, source.pts, source.plus_minus
);
""")


def upsert_rows(engine, df: pd.DataFrame) -> int:
    """Upsert all rows in df. Returns number of rows processed."""
    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    with engine.begin() as conn:
        for row in records:
            conn.execute(UPSERT_SQL, row)
    return len(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    target_date = date.today() - timedelta(days=1)
    log.info("Starting NBA ETL for %s (season %s)", target_date, SEASON)

    game_ids = get_game_ids_for_date(target_date)
    if not game_ids:
        log.info("No games to process. Exiting.")
        return

    engine = get_engine()

    total_rows = 0
    for game_id in game_ids:
        log.info("Processing game %s", game_id)
        frames = []
        for quarter in QUARTERS:
            df = fetch_quarter_box_score(game_id, quarter)
            if df is not None and not df.empty:
                frames.append(df)
            else:
                log.warning("  No data returned for Q%d game %s", quarter, game_id)

        if not frames:
            log.warning("No quarter data for game %s. Skipping.", game_id)
            continue

        combined = pd.concat(frames, ignore_index=True)
        rows_written = upsert_rows(engine, combined)
        total_rows += rows_written
        log.info("  Upserted %d rows for game %s", rows_written, game_id)

    log.info("ETL complete. Total rows upserted: %d", total_rows)


if __name__ == "__main__":
    main()
