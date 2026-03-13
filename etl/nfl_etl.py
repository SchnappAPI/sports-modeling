"""
nfl_etl.py
NFL nflreadpy data ETL for GitHub Actions.

Fetches seven tables from nflreadpy and upserts them into Azure SQL (nfl schema):
  nfl.games               <- load_schedules(season)
  nfl.players             <- load_players()
  nfl.player_game_stats   <- load_player_stats(season, summary_level="week")
  nfl.snap_counts         <- load_snap_counts(season)
  nfl.ftn_charting        <- load_ftn_charting(season)
  nfl.rosters_weekly      <- load_rosters_weekly(season)
  nfl.team_game_stats     <- load_team_stats(season, summary_level="week")

API reference: https://nflreadpy.nflverse.com/api/load_functions/

Credentials come from environment variables (GitHub Secrets):
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
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
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
# Shared helpers
# ---------------------------------------------------------------------------
def to_pandas(polars_df) -> pd.DataFrame:
    """Convert a Polars DataFrame to pandas."""
    return polars_df.to_pandas()


def safe_int(series: pd.Series) -> pd.Series:
    """Cast float series (e.g. 6998.0) to nullable Int64. NaN becomes pd.NA."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def safe_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def bool_to_bit(series: pd.Series) -> pd.Series:
    """Map True/False strings or Python booleans to 1/0. Nulls stay null."""
    mapping = {True: 1, False: 0, "True": 1, "False": 0, "true": 1, "false": 0}
    return series.map(mapping)


def empty_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """Replace empty strings with None to avoid SQL constraint violations."""
    return df.replace("", None)


def keep_columns(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Keep only columns that exist in both the list and the dataframe."""
    return df[[c for c in cols if c in df.columns]].copy()


def upsert(engine, df: pd.DataFrame, table: str, schema: str, upsert_key: list) -> int:
    """
    MERGE-based upsert into Azure SQL.
    Writes to a session-scoped temp table, then MERGEs into the target table.
    Returns the number of rows processed.
    """
    if df.empty:
        log.warning("  upsert called with empty dataframe for %s.%s -- skipping", schema, table)
        return 0

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
# 1. nfl.games
#    Source: nflreadpy.load_schedules(seasons: int | list[int] | bool | None = True)
#    Default is True (all seasons). We pass a single int to get one season.
# ---------------------------------------------------------------------------
def load_games(engine, season: int) -> None:
    log.info("Loading nfl.games for season %d...", season)

    df = to_pandas(nflreadpy.load_schedules(season))

    df = keep_columns(df, [
        "game_id", "season", "game_type", "week", "gameday", "weekday", "gametime",
        "away_team", "away_score", "home_team", "home_score", "location", "result",
        "total", "overtime", "old_game_id", "gsis", "nfl_detail_id", "pfr", "pff",
        "espn", "ftn", "away_rest", "home_rest", "away_moneyline", "home_moneyline",
        "spread_line", "away_spread_odds", "home_spread_odds", "total_line",
        "under_odds", "over_odds", "div_game", "roof", "surface", "temp", "wind",
        "away_qb_id", "home_qb_id", "away_qb_name", "home_qb_name",
        "away_coach", "home_coach", "referee", "stadium_id", "stadium",
    ])

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

    df["game_date"]   = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    df["pff_game_id"] = safe_int(df["pff_game_id"])
    df["ftn_id"]      = safe_int(df.get("ftn_id", pd.Series(dtype=float)))
    df["overtime"]    = bool_to_bit(df["overtime"]) if "overtime" in df.columns else None
    df["is_div_game"] = bool_to_bit(df["is_div_game"]) if "is_div_game" in df.columns else None

    df = empty_to_none(df)

    rows = upsert(engine, df, "games", "nfl", ["game_id"])
    log.info("  nfl.games: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# 2. nfl.players
#    Source: nflreadpy.load_players()  -- no arguments, returns all players
# ---------------------------------------------------------------------------
def load_players(engine) -> None:
    log.info("Loading nfl.players...")

    df = to_pandas(nflreadpy.load_players())

    df = keep_columns(df, [
        "gsis_id", "display_name", "common_first_name", "first_name", "last_name",
        "short_name", "football_name", "suffix", "esb_id", "nfl_id", "pfr_id",
        "pff_id", "otc_id", "espn_id", "smart_id", "birth_date", "position_group",
        "position", "ngs_position_group", "ngs_position", "height", "weight",
        "headshot_url", "college_name", "college_conference", "jersey_number",
        "rookie_season", "last_season", "latest_team", "status", "ngs_status",
        "ngs_status_short_description", "years_of_experience", "pff_position",
        "pff_status", "draft_year", "draft_round", "draft_pick", "draft_team",
    ])

    df = df.rename(columns={
        "height":                       "height_in",
        "weight":                       "weight_lbs",
        "ngs_status_short_description": "ngs_status_short",
    })

    # Large integer IDs -- no tinyint risk
    for col in ["pff_id", "otc_id", "espn_id"]:
        if col in df.columns:
            df[col] = safe_int(df[col])

    # Tinyint columns in DDL -- clip to 0-255 to prevent arithmetic overflow
    # on historical players with unusual values
    for col in ["jersey_number", "draft_round", "draft_pick", "years_of_experience"]:
        if col in df.columns:
            s = safe_int(df[col])
            df[col] = s.where(s.isna() | (s <= 255), other=pd.NA)

    for col in ["rookie_season", "last_season", "draft_year"]:
        if col in df.columns:
            df[col] = safe_int(df[col])

    df["birth_date"] = pd.to_datetime(df.get("birth_date"), errors="coerce").dt.date
    df["height_in"]  = safe_float(df.get("height_in"))
    df["weight_lbs"] = safe_int(df.get("weight_lbs"))

    df = df[df["gsis_id"].notna() & (df["gsis_id"] != "")]
    df = empty_to_none(df)

    rows = upsert(engine, df, "players", "nfl", ["gsis_id"])
    log.info("  nfl.players: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# 3. nfl.player_game_stats
#    Source: nflreadpy.load_player_stats(seasons, summary_level="week")
#    summary_level options: "week", "reg", "post", "reg+post"
# ---------------------------------------------------------------------------
def load_player_game_stats(engine, season: int) -> None:
    log.info("Loading nfl.player_game_stats for season %d...", season)

    df = to_pandas(nflreadpy.load_player_stats(season, summary_level="week"))

    # headshot_url is already stored in nfl.players -- drop before insert
    df = df.drop(columns=["headshot_url"], errors="ignore")

    df = df.rename(columns={"player_id": "player_gsis_id"})

    int_cols = [
        "passing_tds", "passing_interceptions", "sacks_suffered", "sack_fumbles",
        "sack_fumbles_lost", "passing_first_downs", "passing_2pt_conversions",
        "carries", "rushing_tds", "rushing_fumbles", "rushing_fumbles_lost",
        "rushing_first_downs", "rushing_2pt_conversions",
        "receptions", "targets", "receiving_tds", "receiving_fumbles",
        "receiving_fumbles_lost", "receiving_first_downs", "receiving_2pt_conversions",
        "special_teams_tds", "def_interceptions", "def_pass_defended", "def_tds",
        "def_fumbles", "def_safeties", "fumble_recovery_own", "fumble_recovery_opp",
        "fumble_recovery_tds", "penalties", "punt_returns", "kickoff_returns",
        "fg_made", "fg_att", "fg_missed", "fg_blocked", "fg_long",
        "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49",
        "fg_made_50_59", "fg_made_60_plus", "fg_missed_0_19", "fg_missed_20_29",
        "fg_missed_30_39", "fg_missed_40_49", "fg_missed_50_59", "fg_missed_60_plus",
        "pat_made", "pat_att", "pat_missed", "pat_blocked",
        "gwfg_made", "gwfg_att", "gwfg_missed", "gwfg_blocked",
    ]
    smallint_cols = [
        "completions", "attempts", "passing_yards", "sack_yards_lost",
        "passing_air_yards", "passing_yards_after_catch",
        "rushing_yards", "receiving_yards", "receiving_air_yards",
        "receiving_yards_after_catch", "def_interception_yards", "misc_yards",
        "fumble_recovery_yards_own", "fumble_recovery_yards_opp",
        "penalty_yards", "punt_return_yards", "kickoff_return_yards",
        "fg_made_distance", "fg_missed_distance", "fg_blocked_distance",
        "gwfg_distance",
    ]
    decimal_cols = [
        "passing_epa", "passing_cpoe", "pacr",
        "rushing_epa", "receiving_epa", "racr",
        "target_share", "air_yards_share", "wopr",
        "def_tackles_solo", "def_tackles_with_assist", "def_tackle_assists",
        "def_tackles_for_loss", "def_tackles_for_loss_yards",
        "def_fumbles_forced", "def_sacks", "def_sack_yards", "def_qb_hits",
        "fg_pct", "pat_pct", "fantasy_points", "fantasy_points_ppr",
    ]

    for col in int_cols + smallint_cols:
        if col in df.columns:
            df[col] = safe_int(df[col])
    for col in decimal_cols:
        if col in df.columns:
            df[col] = safe_float(df[col])

    df = empty_to_none(df)

    rows = upsert(engine, df, "player_game_stats", "nfl",
                  ["player_gsis_id", "season", "week", "season_type"])
    log.info("  nfl.player_game_stats: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# 4. nfl.snap_counts
#    Source: nflreadpy.load_snap_counts(seasons)
#    Available since 2012.
# ---------------------------------------------------------------------------
def load_snap_counts(engine, season: int) -> None:
    log.info("Loading nfl.snap_counts for season %d...", season)

    df = to_pandas(nflreadpy.load_snap_counts(season))

    df = keep_columns(df, [
        "game_id", "pfr_game_id", "season", "game_type", "week",
        "player", "pfr_player_id", "position", "team", "opponent",
        "offense_snaps", "offense_pct", "defense_snaps", "defense_pct",
        "st_snaps", "st_pct",
    ])

    df = df.rename(columns={"player": "player_name"})

    for col in ["offense_snaps", "defense_snaps", "st_snaps"]:
        if col in df.columns:
            df[col] = safe_int(df[col])
    for col in ["offense_pct", "defense_pct", "st_pct"]:
        if col in df.columns:
            df[col] = safe_float(df[col])

    df = empty_to_none(df)

    rows = upsert(engine, df, "snap_counts", "nfl", ["game_id", "pfr_player_id"])
    log.info("  nfl.snap_counts: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# 5. nfl.ftn_charting
#    Source: nflreadpy.load_ftn_charting(seasons)
#    Available since 2022.
# ---------------------------------------------------------------------------
def load_ftn_charting(engine, season: int) -> None:
    log.info("Loading nfl.ftn_charting for season %d...", season)

    df = to_pandas(nflreadpy.load_ftn_charting(season))

    df = keep_columns(df, [
        "ftn_game_id", "nflverse_game_id", "season", "week", "ftn_play_id",
        "nflverse_play_id", "starting_hash", "qb_location",
        "n_offense_backfield", "n_defense_box",
        "is_no_huddle", "is_motion", "is_play_action", "is_screen_pass",
        "is_rpo", "is_trick_play", "is_qb_out_of_pocket",
        "is_interception_worthy", "is_throw_away", "read_thrown",
        "is_catchable_ball", "is_contested_ball", "is_created_reception",
        "is_drop", "is_qb_sneak", "n_blitzers", "n_pass_rushers",
        "is_qb_fault_sack", "date_pulled",
    ])

    df = df.rename(columns={"nflverse_game_id": "game_id"})

    bool_cols = [
        "is_no_huddle", "is_motion", "is_play_action", "is_screen_pass",
        "is_rpo", "is_trick_play", "is_qb_out_of_pocket",
        "is_interception_worthy", "is_throw_away", "is_catchable_ball",
        "is_contested_ball", "is_created_reception", "is_drop", "is_qb_sneak",
        "is_qb_fault_sack",
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = bool_to_bit(df[col])

    int_cols = [
        "ftn_game_id", "ftn_play_id", "nflverse_play_id",
        "starting_hash", "qb_location", "n_offense_backfield",
        "n_defense_box", "read_thrown", "n_blitzers", "n_pass_rushers",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = safe_int(df[col])

    if "date_pulled" in df.columns:
        df["date_pulled"] = pd.to_datetime(df["date_pulled"], errors="coerce")

    df = empty_to_none(df)

    rows = upsert(engine, df, "ftn_charting", "nfl", ["ftn_game_id", "ftn_play_id"])
    log.info("  nfl.ftn_charting: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# 6. nfl.rosters_weekly
#    Source: nflreadpy.load_rosters_weekly(seasons)
#    Available since 2002. This is the WEEKLY snapshot (player status by week).
#    Note: load_rosters() is the annual season-level roster -- not used here.
# ---------------------------------------------------------------------------
def load_rosters_weekly(engine, season: int) -> None:
    log.info("Loading nfl.rosters_weekly for season %d...", season)

    df = to_pandas(nflreadpy.load_rosters_weekly(season))

    df = keep_columns(df, [
        "season", "team", "position", "depth_chart_position", "jersey_number",
        "status", "full_name", "first_name", "last_name", "birth_date",
        "height", "weight", "college", "gsis_id", "espn_id", "sportradar_id",
        "yahoo_id", "rotowire_id", "pff_id", "pfr_id", "fantasy_data_id",
        "sleeper_id", "years_exp", "headshot_url", "ngs_position", "week",
        "game_type", "status_description_abbr", "football_name", "esb_id",
        "gsis_it_id", "smart_id", "entry_year", "rookie_year",
        "draft_club", "draft_number",
    ])

    df = df.rename(columns={
        "height": "height_in",
        "weight": "weight_lbs",
    })

    for col in ["pff_id", "espn_id", "yahoo_id", "rotowire_id", "fantasy_data_id"]:
        if col in df.columns:
            df[col] = safe_int(df[col])

    for col in ["entry_year", "rookie_year", "draft_number"]:
        if col in df.columns:
            df[col] = safe_int(df[col])

    # Tinyint columns -- clip to 0-255 defensively
    for col in ["jersey_number", "years_exp"]:
        if col in df.columns:
            s = safe_int(df[col])
            df[col] = s.where(s.isna() | (s <= 255), other=pd.NA)

    df["birth_date"] = pd.to_datetime(df.get("birth_date"), errors="coerce").dt.date
    df["height_in"]  = safe_float(df.get("height_in"))
    df["weight_lbs"] = safe_int(df.get("weight_lbs"))

    # Drop rows with no gsis_id -- they cannot be uniquely keyed
    df = df[df["gsis_id"].notna() & (df["gsis_id"] != "")]
    df = empty_to_none(df)

    rows = upsert(engine, df, "rosters_weekly", "nfl",
                  ["season", "week", "team", "gsis_id"])
    log.info("  nfl.rosters_weekly: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# 7. nfl.team_game_stats
#    Source: nflreadpy.load_team_stats(seasons, summary_level="week")
#    Explicitly keep only DDL columns. The API returns extra columns including
#    game_id which is not in the nfl.team_game_stats schema per the audit report.
# ---------------------------------------------------------------------------
def load_team_game_stats(engine, season: int) -> None:
    log.info("Loading nfl.team_game_stats for season %d...", season)

    df = to_pandas(nflreadpy.load_team_stats(season, summary_level="week"))

    # Explicit keep list matching the DDL exactly.
    # game_id is intentionally excluded -- not in nfl.team_game_stats.
    df = keep_columns(df, [
        "season", "week", "team", "season_type", "opponent_team",
        "completions", "attempts", "passing_yards", "passing_tds",
        "passing_interceptions", "sacks_suffered", "sack_yards_lost",
        "sack_fumbles", "sack_fumbles_lost", "passing_air_yards",
        "passing_yards_after_catch", "passing_first_downs", "passing_epa",
        "passing_cpoe", "passing_2pt_conversions", "carries", "rushing_yards",
        "rushing_tds", "rushing_fumbles", "rushing_fumbles_lost",
        "rushing_first_downs", "rushing_epa", "rushing_2pt_conversions",
        "receptions", "targets", "receiving_yards", "receiving_tds",
        "receiving_fumbles", "receiving_fumbles_lost", "receiving_air_yards",
        "receiving_yards_after_catch", "receiving_first_downs", "receiving_epa",
        "receiving_2pt_conversions", "special_teams_tds",
        "def_tackles_solo", "def_tackles_with_assist", "def_tackle_assists",
        "def_tackles_for_loss", "def_tackles_for_loss_yards", "def_fumbles_forced",
        "def_sacks", "def_sack_yards", "def_qb_hits", "def_interceptions",
        "def_interception_yards", "def_pass_defended", "def_tds", "def_fumbles",
        "def_safeties", "misc_yards", "fumble_recovery_own",
        "fumble_recovery_yards_own", "fumble_recovery_opp",
        "fumble_recovery_yards_opp", "fumble_recovery_tds",
        "penalties", "penalty_yards", "timeouts",
        "punt_returns", "punt_return_yards", "kickoff_returns", "kickoff_return_yards",
        "fg_made", "fg_att", "fg_missed", "fg_long", "fg_pct", "fg_made_list",
        "fg_missed_list", "pat_made", "pat_att", "pat_pct",
    ])

    int_cols = [
        "passing_tds", "passing_interceptions", "sacks_suffered",
        "sack_fumbles", "sack_fumbles_lost", "passing_first_downs",
        "passing_2pt_conversions", "carries", "rushing_tds",
        "rushing_fumbles", "rushing_fumbles_lost", "rushing_first_downs",
        "rushing_2pt_conversions", "receptions", "targets", "receiving_tds",
        "receiving_fumbles", "receiving_fumbles_lost", "receiving_first_downs",
        "receiving_2pt_conversions", "special_teams_tds",
        "def_interceptions", "def_pass_defended", "def_tds", "def_fumbles",
        "def_safeties", "fumble_recovery_own", "fumble_recovery_opp",
        "fumble_recovery_tds", "penalties", "timeouts",
        "punt_returns", "kickoff_returns",
        "fg_made", "fg_att", "fg_missed",
        "pat_made", "pat_att",
    ]
    smallint_cols = [
        "completions", "attempts", "passing_yards", "sack_yards_lost",
        "passing_air_yards", "passing_yards_after_catch",
        "rushing_yards", "receiving_yards", "receiving_air_yards",
        "receiving_yards_after_catch", "misc_yards",
        "fumble_recovery_yards_own", "fumble_recovery_yards_opp",
        "penalty_yards", "punt_return_yards", "kickoff_return_yards",
        "def_interception_yards", "def_qb_hits",
        "def_tackles_solo", "def_tackles_with_assist", "def_tackle_assists",
        "def_tackles_for_loss_yards",
    ]
    decimal_cols = [
        "passing_epa", "passing_cpoe", "rushing_epa", "receiving_epa",
        "def_tackles_for_loss", "def_fumbles_forced",
        "def_sacks", "def_sack_yards",
        "fg_long", "fg_pct", "pat_pct",
    ]

    for col in int_cols + smallint_cols:
        if col in df.columns:
            df[col] = safe_int(df[col])
    for col in decimal_cols:
        if col in df.columns:
            df[col] = safe_float(df[col])

    df = empty_to_none(df)

    rows = upsert(engine, df, "team_game_stats", "nfl",
                  ["season", "week", "season_type", "team"])
    log.info("  nfl.team_game_stats: %d rows upserted", rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def current_nfl_season() -> int:
    """Return the current or most recently completed NFL season year."""
    now = datetime.utcnow()
    # NFL seasons run Sep through Feb. Before June means we are in the
    # offseason following the prior year's season.
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
