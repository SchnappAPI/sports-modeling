"""
nfl_etl.py
NFL nflreadpy data ETL for GitHub Actions.

Fetches seven tables from nflreadpy and upserts them into Azure SQL (nfl schema).
Schema is inferred from the actual API response on first run -- no hand-written DDL
required. Subsequent runs use MERGE to upsert without duplicates.

Tables loaded:
  nfl.games               <- load_schedules(season)
  nfl.players             <- load_players()
  nfl.player_game_stats   <- load_player_stats(season, summary_level="week")
  nfl.snap_counts         <- load_snap_counts(season)
  nfl.ftn_charting        <- load_ftn_charting(season)
  nfl.rosters_weekly      <- load_rosters_weekly(season)
  nfl.team_game_stats     <- load_team_stats(season, summary_level="week")

API reference: https://nflreadpy.nflverse.com/api/load_functions/

Credentials are read from environment variables (GitHub Secrets):
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD

Usage:
  python etl/nfl_etl.py                  # current season
  python etl/nfl_etl.py --season 2024    # specific season
"""

import os
import sys
import argparse
import logging
from datetime import datetime

import pandas as pd
import nflreadpy
from nflreadpy.config import update_config
from sqlalchemy import create_engine, text, inspect

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
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
    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={driver}"
    )
    return create_engine(conn_str, fast_executemany=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def to_pandas(polars_df) -> pd.DataFrame:
    return polars_df.to_pandas()


def bool_to_bit(series: pd.Series) -> pd.Series:
    """Map True/False strings or Python booleans to 0/1 integers."""
    mapping = {True: 1, False: 0, "True": 1, "False": 0, "true": 1, "false": 0}
    return series.map(mapping)


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Global cleanup applied to every dataframe before any table-specific logic:
    - Replace empty strings with None
    - Convert boolean columns (True/False strings) to 0/1
    - Downcast object columns that are actually numeric
    """
    # Empty strings to None
    df = df.replace("", None)

    for col in df.columns:
        s = df[col]

        # Bool strings -> 0/1
        if s.dtype == object:
            non_null = s.dropna().unique()
            if set(non_null).issubset({"True", "False", True, False}):
                df[col] = bool_to_bit(s)
                continue

        # Numeric coercion for object columns that look numeric
        if s.dtype == object:
            converted = pd.to_numeric(s, errors="coerce")
            # Only apply if at least 90% of non-null values converted cleanly
            non_null_count = s.notna().sum()
            if non_null_count > 0 and converted.notna().sum() / non_null_count >= 0.9:
                df[col] = converted

    return df


def table_exists(engine, schema: str, table: str) -> bool:
    insp = inspect(engine)
    return insp.has_table(table, schema=schema)


def get_existing_columns(engine, schema: str, table: str) -> set:
    insp = inspect(engine)
    return {col["name"] for col in insp.get_columns(table, schema=schema)}


def add_missing_columns(engine, df: pd.DataFrame, schema: str, table: str) -> None:
    """
    Add any columns present in df that are missing from the existing table.
    Uses a safe type mapping: object->NVARCHAR(500), float->FLOAT, int->BIGINT,
    bool->TINYINT, datetime->DATETIME2, date->DATE.
    """
    existing = get_existing_columns(engine, schema, table)
    missing  = [c for c in df.columns if c not in existing]

    if not missing:
        return

    log.info("  Adding %d new column(s) to %s.%s: %s", len(missing), schema, table, missing)

    type_map = {
        "int64":          "BIGINT",
        "Int64":          "BIGINT",
        "int32":          "INT",
        "Int32":          "INT",
        "float64":        "FLOAT",
        "float32":        "FLOAT",
        "bool":           "TINYINT",
        "object":         "NVARCHAR(500)",
        "datetime64[ns]": "DATETIME2",
        "datetime64[us]": "DATETIME2",
    }

    with engine.begin() as conn:
        for col in missing:
            dtype_str = str(df[col].dtype)
            sql_type  = type_map.get(dtype_str, "NVARCHAR(500)")
            # Date columns stored as object after .dt.date conversion
            if "date" in col and df[col].dtype == object:
                sql_type = "DATE"
            conn.execute(text(
                f"ALTER TABLE [{schema}].[{table}] ADD [{col}] {sql_type} NULL"
            ))


def upsert(engine, df: pd.DataFrame, table: str, schema: str, upsert_key: list) -> int:
    """
    If the table does not exist, create it from the dataframe (schema inference).
    If it exists, add any new columns that appeared in the data, then MERGE.
    Always appends created_at on first creation; never overwrites it on re-run.
    """
    if df.empty:
        log.warning("  Empty dataframe for %s.%s -- skipping", schema, table)
        return 0

    if not table_exists(engine, schema, table):
        log.info("  Table %s.%s does not exist -- creating from data", schema, table)
        with engine.begin() as conn:
            df.to_sql(table, conn, schema=schema, if_exists="replace", index=False)
            # Add created_at after creation
            conn.execute(text(
                f"ALTER TABLE [{schema}].[{table}] "
                f"ADD [created_at] DATETIME2 NOT NULL DEFAULT GETUTCDATE()"
            ))
        log.info("  Created %s.%s with %d rows", schema, table, len(df))
        return len(df)

    # Table exists -- add any new columns the API now returns
    add_missing_columns(engine, df, schema, table)

    cols        = list(df.columns)
    update_cols = [c for c in cols if c not in upsert_key]
    temp_table  = f"##etl_{table}"

    with engine.begin() as conn:
        df.to_sql(temp_table, conn, schema=None, if_exists="replace", index=False)

        on_clause   = " AND ".join([f"t.[{k}] = s.[{k}]" for k in upsert_key])
        insert_cols = ", ".join([f"[{c}]" for c in cols])
        insert_vals = ", ".join([f"s.[{c}]" for c in cols])

        if update_cols:
            update_clause = ", ".join([f"t.[{c}] = s.[{c}]" for c in update_cols])
            merge_sql = f"""
                MERGE [{schema}].[{table}] AS t
                USING [{temp_table}] AS s
                ON ({on_clause})
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals});
            """
        else:
            merge_sql = f"""
                MERGE [{schema}].[{table}] AS t
                USING [{temp_table}] AS s
                ON ({on_clause})
                WHEN NOT MATCHED THEN
                    INSERT ({insert_cols}) VALUES ({insert_vals});
            """

        conn.execute(text(merge_sql))

    return len(df)


# ---------------------------------------------------------------------------
# Table-specific column selection and rename
# These functions only handle columns that need explicit renaming or dropping.
# All type coercion is handled globally by clean_df().
# ---------------------------------------------------------------------------

def load_games(engine, season: int) -> None:
    log.info("Loading nfl.games for season %d...", season)

    df = to_pandas(nflreadpy.load_schedules(season))
    df = df.rename(columns={
        "gameday":    "game_date",
        "gsis":       "gsis_id",
        "pfr":        "pfr_game_id",
        "pff":        "pff_game_id",
        "espn":       "espn_id",
        "ftn":        "ftn_id",
        "away_rest":  "away_rest_days",
        "home_rest":  "home_rest_days",
        "div_game":   "is_div_game",
        "temp":       "temp_f",
        "wind":       "wind_mph",
        "away_qb_id": "away_qb_gsis_id",
        "home_qb_id": "home_qb_gsis_id",
        "stadium":    "stadium_name",
    })

    # game_date needs to be a proper date not datetime
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date

    df = clean_df(df)
    rows = upsert(engine, df, "games", "nfl", ["game_id"])
    log.info("  nfl.games: %d rows upserted", rows)


def load_players(engine) -> None:
    log.info("Loading nfl.players...")

    df = to_pandas(nflreadpy.load_players())
    df = df.rename(columns={
        "height":                       "height_in",
        "weight":                       "weight_lbs",
        "ngs_status_short_description": "ngs_status_short",
    })

    if "birth_date" in df.columns:
        df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce").dt.date

    # Drop rows with no gsis_id -- cannot be primary key
    df = df[df["gsis_id"].notna() & (df["gsis_id"] != "")]

    df = clean_df(df)
    rows = upsert(engine, df, "players", "nfl", ["gsis_id"])
    log.info("  nfl.players: %d rows upserted", rows)


def load_player_game_stats(engine, season: int) -> None:
    log.info("Loading nfl.player_game_stats for season %d...", season)

    df = to_pandas(nflreadpy.load_player_stats(season, summary_level="week"))
    df = df.drop(columns=["headshot_url"], errors="ignore")
    df = df.rename(columns={"player_id": "player_gsis_id"})

    df = clean_df(df)
    rows = upsert(engine, df, "player_game_stats", "nfl",
                  ["player_gsis_id", "season", "week", "season_type"])
    log.info("  nfl.player_game_stats: %d rows upserted", rows)


def load_snap_counts(engine, season: int) -> None:
    log.info("Loading nfl.snap_counts for season %d...", season)

    df = to_pandas(nflreadpy.load_snap_counts(season))
    df = df.rename(columns={"player": "player_name"})

    df = clean_df(df)
    rows = upsert(engine, df, "snap_counts", "nfl", ["game_id", "pfr_player_id"])
    log.info("  nfl.snap_counts: %d rows upserted", rows)


def load_ftn_charting(engine, season: int) -> None:
    log.info("Loading nfl.ftn_charting for season %d...", season)

    df = to_pandas(nflreadpy.load_ftn_charting(season))
    df = df.rename(columns={"nflverse_game_id": "game_id"})

    if "date_pulled" in df.columns:
        df["date_pulled"] = pd.to_datetime(df["date_pulled"], errors="coerce")

    df = clean_df(df)
    rows = upsert(engine, df, "ftn_charting", "nfl", ["ftn_game_id", "ftn_play_id"])
    log.info("  nfl.ftn_charting: %d rows upserted", rows)


def load_rosters_weekly(engine, season: int) -> None:
    log.info("Loading nfl.rosters_weekly for season %d...", season)

    df = to_pandas(nflreadpy.load_rosters_weekly(season))
    df = df.rename(columns={
        "height": "height_in",
        "weight": "weight_lbs",
    })

    if "birth_date" in df.columns:
        df["birth_date"] = pd.to_datetime(df["birth_date"], errors="coerce").dt.date

    # Drop rows with no gsis_id -- cannot be part of the upsert key
    df = df[df["gsis_id"].notna() & (df["gsis_id"] != "")]

    df = clean_df(df)
    rows = upsert(engine, df, "rosters_weekly", "nfl",
                  ["season", "week", "team", "gsis_id"])
    log.info("  nfl.rosters_weekly: %d rows upserted", rows)


def load_team_game_stats(engine, season: int) -> None:
    log.info("Loading nfl.team_game_stats for season %d...", season)

    df = to_pandas(nflreadpy.load_team_stats(season, summary_level="week"))

    # game_id is not part of the nfl.team_game_stats schema -- drop it
    df = df.drop(columns=["game_id"], errors="ignore")

    df = clean_df(df)
    rows = upsert(engine, df, "team_game_stats", "nfl",
                  ["season", "week", "season_type", "team"])
    log.info("  nfl.team_game_stats: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def current_nfl_season() -> int:
    now = datetime.utcnow()
    return now.year if now.month >= 6 else now.year - 1


def main():
    parser = argparse.ArgumentParser(description="NFL nflreadpy ETL to Azure SQL")
    parser.add_argument(
        "--season", type=int, default=None,
        help="NFL season year (e.g. 2024). Defaults to current season."
    )
    args = parser.parse_args()

    season = args.season or current_nfl_season()
    log.info("=== NFL ETL START | season=%d ===", season)

    # Disable nflreadpy cache in GitHub Actions -- no persistent filesystem
    update_config(cache_mode="off")

    engine = get_engine()
    log.info("Database connection established.")

    errors = []

    def run(name: str, fn):
        try:
            fn()
        except Exception as exc:
            log.error("FAILED: %s | %s", name, exc, exc_info=True)
            errors.append(name)

    run("games",             lambda: load_games(engine, season))
    run("players",           lambda: load_players(engine))
    run("player_game_stats", lambda: load_player_game_stats(engine, season))
    run("snap_counts",       lambda: load_snap_counts(engine, season))
    run("ftn_charting",      lambda: load_ftn_charting(engine, season))
    run("rosters_weekly",    lambda: load_rosters_weekly(engine, season))
    run("team_game_stats",   lambda: load_team_game_stats(engine, season))

    if errors:
        log.error("=== NFL ETL COMPLETE WITH ERRORS: %s ===", ", ".join(errors))
        sys.exit(1)
    else:
        log.info("=== NFL ETL COMPLETE | season=%d | all tables succeeded ===", season)


if __name__ == "__main__":
    main()
