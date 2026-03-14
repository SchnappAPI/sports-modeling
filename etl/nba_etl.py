"""
nba_etl.py
----------
Nightly NBA ETL for the sports modeling database.

Covers:
  - Quarter box scores (Q1-Q4 + OT periods) -> nba.player_box_score_stats
  - Team box scores per period              -> nba.team_box_score_stats
  - Game metadata                           -> nba.games
  - Teams reference                         -> nba.teams  (truncate/reload)
  - Players reference                       -> nba.players (upsert)
  - Advanced box scores (usage, ratings)    -> nba.player_tracking_stats
  - Player tracking (touches/passes/reb)    -> nba.player_tracking_stats
  - Matchup position aggregation            -> nba.matchup_position_stats

Run modes:
  python nba_etl.py                  # yesterday's games only (nightly)
  python nba_etl.py --backfill       # all games in the current season

Secrets (GitHub Actions env vars):
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME,
  AZURE_SQL_PASSWORD, NBA_PROXY_URL
"""

import argparse
import os
import time
import logging
from datetime import date, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import (
    LeagueGameFinder,
    BoxScoreTraditionalV3,
    BoxScoreAdvancedV3,
    BoxScorePlayerTrackV3,
)
from nba_api.stats.static import teams as static_teams

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROXY_URL     = os.environ.get("NBA_PROXY_URL")
PROXIES       = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
CURRENT_SEASON    = "2025-26"
CURRENT_SEASON_ID = "22025"
API_DELAY     = 0.75   # seconds between API calls


def period_label(period: int) -> str:
    if period <= 4:
        return f"Q{period}"
    ot = period - 4
    return "OT" if ot == 1 else f"OT{ot}"


# ---------------------------------------------------------------------------
# Database engine
# ---------------------------------------------------------------------------

def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
    )
    return create_engine(conn_str, fast_executemany=True)


# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------

def safe_float(val):
    try:
        return float(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Step 1: Load teams reference (truncate + reload)
# ---------------------------------------------------------------------------

def load_teams(engine):
    log.info("Loading nba.teams (truncate + reload)")
    raw = static_teams.get_teams()
    rows = [
        {
            "team_id":      t["id"],
            "full_name":    t["full_name"],
            "abbreviation": t["abbreviation"],
            "nickname":     t["nickname"],
            "city":         t["city"],
            "state":        t["state"],
            "year_founded": safe_int(t.get("year_founded")),
        }
        for t in raw
    ]
    df = pd.DataFrame(rows)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM nba.teams"))
    df.to_sql("teams", engine, schema="nba", if_exists="append", index=False)
    log.info(f"  Loaded {len(df)} teams")


# ---------------------------------------------------------------------------
# Step 2: Discover game IDs
# ---------------------------------------------------------------------------

def get_game_ids(target_dates: list) -> list:
    log.info(f"Fetching game IDs for {len(target_dates)} date(s)")
    date_strings = {d.strftime("%m/%d/%Y") for d in target_dates}
    finder = LeagueGameFinder(
        season_nullable=CURRENT_SEASON,
        league_id_nullable="00",
        proxy=PROXY_URL,
    )
    time.sleep(API_DELAY)
    df = finder.get_data_frames()[0]
    if df.empty:
        log.warning("LeagueGameFinder returned no games")
        return []
    df["_date_fmt"] = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%m/%d/%Y")
    df_filtered = df[df["_date_fmt"].isin(date_strings)].copy()
    pairs = (
        df_filtered[["GAME_ID", "GAME_DATE"]]
        .drop_duplicates("GAME_ID")
        .values.tolist()
    )
    result = [(gid, pd.to_datetime(gdate).date()) for gid, gdate in pairs]
    log.info(f"  Found {len(result)} distinct game(s)")
    return result


def get_all_season_game_ids() -> list:
    log.info(f"Backfill mode: fetching all game IDs for season {CURRENT_SEASON}")
    finder = LeagueGameFinder(
        season_nullable=CURRENT_SEASON,
        league_id_nullable="00",
        proxy=PROXY_URL,
    )
    time.sleep(API_DELAY)
    df = finder.get_data_frames()[0]
    if df.empty:
        return []
    pairs = (
        df[["GAME_ID", "GAME_DATE"]]
        .drop_duplicates("GAME_ID")
        .values.tolist()
    )
    return [(gid, pd.to_datetime(gdate).date()) for gid, gdate in pairs]


# ---------------------------------------------------------------------------
# Step 3: Process one game
# ---------------------------------------------------------------------------

def process_game(game_id: str, game_date: date, engine) -> None:
    log.info(f"  Processing game {game_id} ({game_date})")

    # ------------------------------------------------------------------
    # 3a. BoxScoreTraditionalV3: player rows + team rows across all periods
    # ------------------------------------------------------------------
    try:
        trad = BoxScoreTraditionalV3(game_id=game_id, proxy=PROXY_URL)
        time.sleep(API_DELAY)
    except Exception as exc:
        log.error(f"    BoxScoreTraditionalV3 failed for {game_id}: {exc}")
        return

    trad_data = trad.get_normalized_dict()
    player_stats_raw = trad_data.get("PlayerStats", [])
    team_stats_raw   = trad_data.get("TeamStats", [])

    periods_present = set()
    for row in team_stats_raw:
        p = safe_int(row.get("PERIOD") or row.get("period"))
        if p:
            periods_present.add(p)
    num_periods = max(periods_present) if periods_present else 4

    player_rows = []
    for row in player_stats_raw:
        period  = safe_int(row.get("PERIOD") or row.get("period"))
        if not period:
            continue
        comment = (row.get("COMMENT") or "").strip()
        if comment:
            continue
        player_rows.append({
            "game_id":           game_id,
            "player_id":         safe_int(row.get("PLAYER_ID")),
            "quarter":           period_label(period),
            "first_name":        (row.get("PLAYER_NAME_I") or ""),
            "last_name":         (row.get("PLAYER_NAME_I") or ""),
            "team_id":           safe_int(row.get("TEAM_ID")),
            "team_abbreviation": (row.get("TEAM_ABBREVIATION") or ""),
            "position":          (row.get("START_POSITION") or ""),
            "comment":           comment,
            "jersey_num":        str(row.get("JERSEY_NUM") or ""),
            "min":               str(row.get("MIN") or ""),
            "fgm":               safe_int(row.get("FGM")),
            "fga":               safe_int(row.get("FGA")),
            "fg_pct":            safe_float(row.get("FG_PCT")),
            "fg3m":              safe_int(row.get("FG3M")),
            "fg3a":              safe_int(row.get("FG3A")),
            "fg3_pct":           safe_float(row.get("FG3_PCT")),
            "ftm":               safe_int(row.get("FTM")),
            "fta":               safe_int(row.get("FTA")),
            "ft_pct":            safe_float(row.get("FT_PCT")),
            "oreb":              safe_int(row.get("OREB")),
            "dreb":              safe_int(row.get("DREB")),
            "reb":               safe_int(row.get("REB")),
            "ast":               safe_int(row.get("AST")),
            "stl":               safe_int(row.get("STL")),
            "blk":               safe_int(row.get("BLK")),
            "tov":               safe_int(row.get("TO")),
            "pf":                safe_int(row.get("PF")),
            "pts":               safe_int(row.get("PTS")),
            "plus_minus":        safe_float(row.get("PLUS_MINUS")),
        })

    team_rows = []
    for row in team_stats_raw:
        period = safe_int(row.get("PERIOD") or row.get("period"))
        if not period:
            continue
        team_rows.append({
            "game_id":           game_id,
            "team_id":           safe_int(row.get("TEAM_ID")),
            "team_abbreviation": (row.get("TEAM_ABBREVIATION") or ""),
            "quarter":           period_label(period),
            "min":               str(row.get("MIN") or ""),
            "fgm":               safe_int(row.get("FGM")),
            "fga":               safe_int(row.get("FGA")),
            "fg_pct":            safe_float(row.get("FG_PCT")),
            "fg3m":              safe_int(row.get("FG3M")),
            "fg3a":              safe_int(row.get("FG3A")),
            "fg3_pct":           safe_float(row.get("FG3_PCT")),
            "ftm":               safe_int(row.get("FTM")),
            "fta":               safe_int(row.get("FTA")),
            "ft_pct":            safe_float(row.get("FT_PCT")),
            "oreb":              safe_int(row.get("OREB")),
            "dreb":              safe_int(row.get("DREB")),
            "reb":               safe_int(row.get("REB")),
            "ast":               safe_int(row.get("AST")),
            "stl":               safe_int(row.get("STL")),
            "blk":               safe_int(row.get("BLK")),
            "tov":               safe_int(row.get("TO")),
            "pf":                safe_int(row.get("PF")),
            "pts":               safe_int(row.get("PTS")),
        })

    # Derive game totals from team period rows
    team_totals = {}
    for row in team_stats_raw:
        tid  = safe_int(row.get("TEAM_ID"))
        pts  = safe_int(row.get("PTS")) or 0
        abbr = row.get("TEAM_ABBREVIATION") or ""
        if tid not in team_totals:
            team_totals[tid] = {"pts": 0, "abbr": abbr}
        team_totals[tid]["pts"] += pts

    team_ids = list(team_totals.keys())
    home_team_id = away_team_id = None
    home_abbr = away_abbr = ""
    home_pts = away_pts = game_total = None

    if len(team_ids) == 2:
        home_team_id = team_ids[0]
        away_team_id = team_ids[1]
        home_abbr    = team_totals[home_team_id]["abbr"]
        away_abbr    = team_totals[away_team_id]["abbr"]
        home_pts     = team_totals[home_team_id]["pts"]
        away_pts     = team_totals[away_team_id]["pts"]
        game_total   = home_pts + away_pts

    # ------------------------------------------------------------------
    # 3b. BoxScoreAdvancedV3: usage, ratings, pace, PIE per player
    # ------------------------------------------------------------------
    advanced_by_player = {}
    try:
        adv = BoxScoreAdvancedV3(game_id=game_id, proxy=PROXY_URL)
        time.sleep(API_DELAY)
        adv_data = adv.get_normalized_dict()
        for row in adv_data.get("PlayerStats", []):
            pid = safe_int(row.get("personId") or row.get("PLAYER_ID"))
            if pid is None:
                continue
            advanced_by_player[pid] = {
                "usage_pct":  safe_float(row.get("usagePercentage") or row.get("USG_PCT")),
                "off_rating": safe_float(row.get("offensiveRating") or row.get("OFF_RATING")),
                "def_rating": safe_float(row.get("defensiveRating") or row.get("DEF_RATING")),
                "net_rating": safe_float(row.get("netRating") or row.get("NET_RATING")),
                "pace":       safe_float(row.get("pace") or row.get("PACE")),
                "pie":        safe_float(row.get("PIE")),
                "true_shooting_pct": safe_float(row.get("trueShootingPercentage")),
                "efg_pct":    safe_float(row.get("effectiveFieldGoalPercentage")),
            }
    except Exception as exc:
        log.warning(f"    BoxScoreAdvancedV3 failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 3c. BoxScorePlayerTrackV3: touches, rebound chances, passes, contested shots
    # ------------------------------------------------------------------
    tracking_by_player = {}
    try:
        trk = BoxScorePlayerTrackV3(game_id=game_id, proxy=PROXY_URL)
        time.sleep(API_DELAY)
        trk_data = trk.get_normalized_dict()
        for row in trk_data.get("PlayerStats", []):
            pid = safe_int(row.get("personId") or row.get("PLAYER_ID"))
            if pid is None:
                continue
            tracking_by_player[pid] = {
                "team_id":                      safe_int(row.get("teamId") or row.get("TEAM_ID")),
                "speed":                        safe_float(row.get("speed")),
                "distance":                     safe_float(row.get("distance")),
                "touches":                      safe_int(row.get("touches")),
                "passes_made":                  safe_int(row.get("passes")),
                "secondary_ast":                safe_int(row.get("secondaryAssists")),
                "ft_ast":                       safe_int(row.get("freeThrowAssists")),
                "reb_chances":                  safe_int(row.get("reboundChancesTotal")),
                "oreb_chances":                 safe_int(row.get("reboundChancesOffensive")),
                "dreb_chances":                 safe_int(row.get("reboundChancesDefensive")),
                "contested_fgm":                safe_int(row.get("contestedFieldGoalsMade")),
                "contested_fga":                safe_int(row.get("contestedFieldGoalsAttempted")),
                "contested_fg_pct":             safe_float(row.get("contestedFieldGoalPercentage")),
                "uncontested_fgm":              safe_int(row.get("uncontestedFieldGoalsMade")),
                "uncontested_fga":              safe_int(row.get("uncontestedFieldGoalsAttempted")),
                "uncontested_fg_pct":           safe_float(row.get("uncontestedFieldGoalsPercentage")),
                "defended_at_rim_fgm":          safe_int(row.get("defendedAtRimFieldGoalsMade")),
                "defended_at_rim_fga":          safe_int(row.get("defendedAtRimFieldGoalsAttempted")),
                "defended_at_rim_fg_pct":       safe_float(row.get("defendedAtRimFieldGoalPercentage")),
            }
    except Exception as exc:
        log.warning(f"    BoxScorePlayerTrackV3 failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 3d. Write to database
    # ------------------------------------------------------------------

    if player_rows:
        df_players = pd.DataFrame(player_rows)
        _upsert(df_players, engine, "nba", "player_box_score_stats",
                ["game_id", "player_id", "quarter"])
        log.info(f"    Upserted {len(df_players)} player box score rows")

    if team_rows:
        df_teams_box = pd.DataFrame(team_rows)
        _upsert(df_teams_box, engine, "nba", "team_box_score_stats",
                ["game_id", "team_id", "quarter"])
        log.info(f"    Upserted {len(df_teams_box)} team box score rows")

    if home_team_id and away_team_id:
        df_game = pd.DataFrame([{
            "game_id":        game_id,
            "game_date":      game_date,
            "season_id":      CURRENT_SEASON_ID,
            "home_team_id":   home_team_id,
            "home_team_abbr": home_abbr,
            "away_team_id":   away_team_id,
            "away_team_abbr": away_abbr,
            "home_score":     home_pts,
            "away_score":     away_pts,
            "game_total":     game_total,
            "num_periods":    num_periods,
        }])
        _upsert(df_game, engine, "nba", "games", ["game_id"])

    all_pids = set(advanced_by_player.keys()) | set(tracking_by_player.keys())
    if all_pids:
        tracking_rows = []
        for pid in all_pids:
            row = {"game_id": game_id, "player_id": pid}
            row.update(advanced_by_player.get(pid, {}))
            row.update(tracking_by_player.get(pid, {}))
            tracking_rows.append(row)
        df_tracking = pd.DataFrame(tracking_rows)
        _upsert(df_tracking, engine, "nba", "player_tracking_stats",
                ["game_id", "player_id"])
        log.info(f"    Upserted {len(df_tracking)} player tracking rows")

    if player_rows:
        _aggregate_matchup(player_rows, game_id, game_date, engine)


# ---------------------------------------------------------------------------
# Matchup position aggregation
# ---------------------------------------------------------------------------

def _aggregate_matchup(player_rows, game_id, game_date, engine):
    df = pd.DataFrame(player_rows)
    if df.empty or "team_id" not in df.columns:
        return

    team_ids = df["team_id"].dropna().unique().tolist()
    if len(team_ids) != 2:
        return

    team_abbr_map = (
        df[["team_id", "team_abbreviation"]]
        .drop_duplicates("team_id")
        .set_index("team_id")["team_abbreviation"]
        .to_dict()
    )

    numeric_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                    "oreb","dreb","reb","ast","stl","blk","tov","pf","pts"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    player_game = (
        df.groupby(["player_id", "team_id", "position"])[numeric_cols]
        .sum()
        .reset_index()
    )

    matchup_rows = []
    for _, row in player_game.iterrows():
        att_team = row["team_id"]
        def_team = team_ids[0] if att_team == team_ids[1] else team_ids[1]
        pos = str(row.get("position") or "").strip() or "UNKNOWN"
        matchup_rows.append({
            "game_id":             game_id,
            "game_date":           game_date,
            "defending_team_id":   def_team,
            "defending_team_abbr": team_abbr_map.get(def_team, ""),
            "position_group":      pos,
            "total_fgm":  int(row["fgm"]),  "total_fga":  int(row["fga"]),
            "total_fg3m": int(row["fg3m"]), "total_fg3a": int(row["fg3a"]),
            "total_ftm":  int(row["ftm"]),  "total_fta":  int(row["fta"]),
            "total_reb":  int(row["reb"]),  "total_ast":  int(row["ast"]),
            "total_stl":  int(row["stl"]),  "total_blk":  int(row["blk"]),
            "total_tov":  int(row["tov"]),  "total_pts":  int(row["pts"]),
        })

    if not matchup_rows:
        return

    agg_df = pd.DataFrame(matchup_rows)
    sum_cols = [c for c in agg_df.columns if c.startswith("total_")]
    grouped = (
        agg_df.groupby(["game_id","game_date","defending_team_id",
                         "defending_team_abbr","position_group"])[sum_cols]
        .sum()
        .reset_index()
    )
    counts = (
        agg_df.groupby(["game_id","game_date","defending_team_id",
                          "defending_team_abbr","position_group"])
        .size()
        .reset_index(name="player_count")
    )
    final_df = grouped.merge(counts, on=["game_id","game_date","defending_team_id",
                                          "defending_team_abbr","position_group"])

    _upsert(final_df, engine, "nba", "matchup_position_stats",
            ["game_id", "defending_team_id", "position_group"])
    log.info(f"    Upserted {len(final_df)} matchup position rows")


# ---------------------------------------------------------------------------
# Players reference upsert (runs after all box scores loaded)
# ---------------------------------------------------------------------------

def upsert_players(engine):
    log.info("Upserting nba.players from box score data")
    sql = """
        MERGE nba.players AS tgt
        USING (
            SELECT DISTINCT
                player_id,
                first_name,
                last_name,
                team_id,
                team_abbreviation,
                position
            FROM nba.player_box_score_stats
            WHERE player_id IS NOT NULL
        ) AS src
        ON tgt.player_id = src.player_id
        WHEN MATCHED THEN UPDATE SET
            tgt.first_name        = src.first_name,
            tgt.last_name         = src.last_name,
            tgt.team_id           = src.team_id,
            tgt.team_abbreviation = src.team_abbreviation,
            tgt.position          = src.position,
            tgt.updated_at        = GETUTCDATE()
        WHEN NOT MATCHED THEN INSERT
            (player_id, first_name, last_name, team_id, team_abbreviation, position)
        VALUES
            (src.player_id, src.first_name, src.last_name,
             src.team_id, src.team_abbreviation, src.position);
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql))
    log.info(f"  Players merge complete ({result.rowcount} rows affected)")


# ---------------------------------------------------------------------------
# Generic upsert via MERGE
# ---------------------------------------------------------------------------

def _upsert(df: pd.DataFrame, engine, schema: str, table: str, pk_cols: list):
    if df.empty:
        return

    non_pk    = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = (
        ", ".join(f"tgt.{c} = src.{c}" for c in non_pk)
        if non_pk
        else f"tgt.{pk_cols[0]} = tgt.{pk_cols[0]}"
    )

    merge_sql = f"""
        MERGE {schema}.{table} AS tgt
        USING (VALUES ({val_list})) AS src ({col_list})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql), df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Fetch all games in the current season rather than just yesterday.",
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Using residential proxy: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set. Requests may be blocked by stats.nba.com.")

    engine = get_engine()

    load_teams(engine)

    if args.backfill:
        game_pairs = get_all_season_game_ids()
    else:
        yesterday  = date.today() - timedelta(days=1)
        game_pairs = get_game_ids([yesterday])

    if not game_pairs:
        log.info("No games found for the target date range. Nothing to load.")
        return

    log.info(f"Processing {len(game_pairs)} game(s)")
    for game_id, game_date in game_pairs:
        process_game(game_id, game_date, engine)

    upsert_players(engine)
    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
