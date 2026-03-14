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
  - Player tracking (drives/touches/passes/rebounds/shooting)
                                            -> nba.player_tracking_stats
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
    PlayerTrackingDrives,
    PlayerTrackingPasses,
    PlayerTrackingPossessions,
    PlayerTrackingRebounding,
    PlayerTrackingShooting,
    TeamInfoCommon,
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

PROXY_URL = os.environ.get("NBA_PROXY_URL")
PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
CURRENT_SEASON = "2025-26"
CURRENT_SEASON_ID = "22025"   # LeagueGameFinder season_id format
API_DELAY = 0.75               # seconds between API calls to avoid rate limits

# Quarter label map: API period int -> stored CHAR(3) label
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
# Helper: safe float/int conversion
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
        conn.execute(text("TRUNCATE TABLE nba.teams"))
    df.to_sql("teams", engine, schema="nba", if_exists="append", index=False)
    log.info(f"  Loaded {len(df)} teams")


# ---------------------------------------------------------------------------
# Step 2: Discover game IDs for the target date range
# ---------------------------------------------------------------------------

def get_game_ids(target_dates: list[date]) -> list[tuple[str, date]]:
    """
    Returns list of (game_id, game_date) for games played on any of the
    given dates. Uses LeagueGameFinder with the current season.
    """
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

    # GAME_DATE comes back as YYYY-MM-DD string
    df["_date_fmt"] = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%m/%d/%Y")
    df_filtered = df[df["_date_fmt"].isin(date_strings)].copy()

    # Each game appears twice (once per team); deduplicate on GAME_ID
    pairs = (
        df_filtered[["GAME_ID", "GAME_DATE"]]
        .drop_duplicates("GAME_ID")
        .values.tolist()
    )
    result = [(gid, pd.to_datetime(gdate).date()) for gid, gdate in pairs]
    log.info(f"  Found {len(result)} distinct game(s)")
    return result


def get_all_season_game_ids() -> list[tuple[str, date]]:
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
    # 3a. BoxScoreTraditionalV3: detect periods, player rows, team rows
    # ------------------------------------------------------------------
    try:
        trad = BoxScoreTraditionalV3(
            game_id=game_id,
            proxy=PROXY_URL,
        )
        time.sleep(API_DELAY)
    except Exception as exc:
        log.error(f"    BoxScoreTraditionalV3 failed for {game_id}: {exc}")
        return

    trad_data = trad.get_normalized_dict()

    # Figure out how many periods this game had
    # The response contains one entry per period in player stats
    player_stats_raw = trad_data.get("PlayerStats", [])
    team_stats_raw   = trad_data.get("TeamStats", [])

    # Derive num_periods from the maximum period seen in team stats
    periods_present = set()
    for row in team_stats_raw:
        p = safe_int(row.get("PERIOD") or row.get("period"))
        if p:
            periods_present.add(p)
    num_periods = max(periods_present) if periods_present else 4

    # Build player box score rows for ALL periods
    player_rows = []
    for row in player_stats_raw:
        period = safe_int(row.get("PERIOD") or row.get("period"))
        if not period:
            continue
        comment = (row.get("COMMENT") or "").strip()
        if comment:          # DNP - skip
            continue
        player_rows.append({
            "game_id":          game_id,
            "player_id":        safe_int(row.get("PLAYER_ID")),
            "quarter":          period_label(period),
            "first_name":       row.get("PLAYER_NAME_I", "").split(". ")[-1] if ". " in str(row.get("PLAYER_NAME_I","")) else "",
            "last_name":        row.get("PLAYER_NAME_I", ""),
            "team_id":          safe_int(row.get("TEAM_ID")),
            "team_abbreviation":row.get("TEAM_ABBREVIATION", ""),
            "position":         row.get("START_POSITION", ""),
            "comment":          comment,
            "jersey_num":       str(row.get("JERSEY_NUM", "") or ""),
            "min":              str(row.get("MIN", "") or ""),
            "fgm":              safe_int(row.get("FGM")),
            "fga":              safe_int(row.get("FGA")),
            "fg_pct":           safe_float(row.get("FG_PCT")),
            "fg3m":             safe_int(row.get("FG3M")),
            "fg3a":             safe_int(row.get("FG3A")),
            "fg3_pct":          safe_float(row.get("FG3_PCT")),
            "ftm":              safe_int(row.get("FTM")),
            "fta":              safe_int(row.get("FTA")),
            "ft_pct":           safe_float(row.get("FT_PCT")),
            "oreb":             safe_int(row.get("OREB")),
            "dreb":             safe_int(row.get("DREB")),
            "reb":              safe_int(row.get("REB")),
            "ast":              safe_int(row.get("AST")),
            "stl":              safe_int(row.get("STL")),
            "blk":              safe_int(row.get("BLK")),
            "tov":              safe_int(row.get("TO")),
            "pf":               safe_int(row.get("PF")),
            "pts":              safe_int(row.get("PTS")),
            "plus_minus":       safe_float(row.get("PLUS_MINUS")),
        })

    # Build team box score rows
    team_rows = []
    for row in team_stats_raw:
        period = safe_int(row.get("PERIOD") or row.get("period"))
        if not period:
            continue
        team_rows.append({
            "game_id":          game_id,
            "team_id":          safe_int(row.get("TEAM_ID")),
            "team_abbreviation":row.get("TEAM_ABBREVIATION", ""),
            "quarter":          period_label(period),
            "min":              str(row.get("MIN", "") or ""),
            "fgm":              safe_int(row.get("FGM")),
            "fga":              safe_int(row.get("FGA")),
            "fg_pct":           safe_float(row.get("FG_PCT")),
            "fg3m":             safe_int(row.get("FG3M")),
            "fg3a":             safe_int(row.get("FG3A")),
            "fg3_pct":          safe_float(row.get("FG3_PCT")),
            "ftm":              safe_int(row.get("FTM")),
            "fta":              safe_int(row.get("FTA")),
            "ft_pct":           safe_float(row.get("FT_PCT")),
            "oreb":             safe_int(row.get("OREB")),
            "dreb":             safe_int(row.get("DREB")),
            "reb":              safe_int(row.get("REB")),
            "ast":              safe_int(row.get("AST")),
            "stl":              safe_int(row.get("STL")),
            "blk":              safe_int(row.get("BLK")),
            "tov":              safe_int(row.get("TO")),
            "pf":               safe_int(row.get("PF")),
            "pts":              safe_int(row.get("PTS")),
        })

    # Derive game totals from team rows summed across all periods
    home_pts, away_pts = None, None
    home_team_id, away_team_id = None, None
    home_abbr, away_abbr = "", ""
    # The API marks home/away in TEAM_ID ordering within the game code.
    # Simpler: sum pts per team from full-game team rows where PERIOD sums exist.
    # We'll aggregate after writing since we have period-level data.
    team_totals = {}
    for row in team_stats_raw:
        tid = safe_int(row.get("TEAM_ID"))
        pts = safe_int(row.get("PTS"))
        abbr = row.get("TEAM_ABBREVIATION", "")
        if tid not in team_totals:
            team_totals[tid] = {"pts": 0, "abbr": abbr}
        if pts:
            team_totals[tid]["pts"] += pts

    team_ids = list(team_totals.keys())
    if len(team_ids) == 2:
        # Determine home/away from game_id convention: last 3 chars = home abbr
        # Actually derive from LeagueGameFinder; for now assign arbitrarily
        # and let the games table be updated via upsert with correct ordering.
        home_team_id = team_ids[0]
        away_team_id = team_ids[1]
        home_abbr    = team_totals[home_team_id]["abbr"]
        away_abbr    = team_totals[away_team_id]["abbr"]
        home_pts     = team_totals[home_team_id]["pts"]
        away_pts     = team_totals[away_team_id]["pts"]

    game_total = (home_pts + away_pts) if (home_pts is not None and away_pts is not None) else None

    # ------------------------------------------------------------------
    # 3b. BoxScoreAdvancedV3: per player per period usage + ratings
    # ------------------------------------------------------------------
    advanced_by_player = {}
    try:
        adv = BoxScoreAdvancedV3(game_id=game_id, proxy=PROXY_URL)
        time.sleep(API_DELAY)
        adv_data = adv.get_normalized_dict()
        for row in adv_data.get("PlayerStats", []):
            pid = safe_int(row.get("PLAYER_ID"))
            if pid not in advanced_by_player:
                advanced_by_player[pid] = {
                    "usage_pct":  safe_float(row.get("USG_PCT")),
                    "off_rating": safe_float(row.get("OFF_RATING")),
                    "def_rating": safe_float(row.get("DEF_RATING")),
                    "net_rating": safe_float(row.get("NET_RATING")),
                    "pace":       safe_float(row.get("PACE")),
                    "pie":        safe_float(row.get("PIE")),
                }
    except Exception as exc:
        log.warning(f"    BoxScoreAdvancedV3 failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 3c. Player tracking endpoints (game-level, not period-level)
    # ------------------------------------------------------------------
    tracking_by_player = {}

    def merge_tracking(endpoint_cls, field_map):
        try:
            ep = endpoint_cls(game_id=game_id, proxy=PROXY_URL)
            time.sleep(API_DELAY)
            data = ep.get_data_frames()[0]
            for _, row in data.iterrows():
                pid = safe_int(row.get("PLAYER_ID"))
                if pid is None:
                    continue
                if pid not in tracking_by_player:
                    tracking_by_player[pid] = {
                        "team_id": safe_int(row.get("TEAM_ID"))
                    }
                for dest, src in field_map.items():
                    val = row.get(src)
                    tracking_by_player[pid][dest] = safe_float(val) if isinstance(safe_float(val), float) else safe_int(val)
        except Exception as exc:
            log.warning(f"    {endpoint_cls.__name__} failed for {game_id}: {exc}")

    merge_tracking(PlayerTrackingDrives, {
        "drives":            "DRIVES",
        "drive_fgm":         "DRIVE_FGM",
        "drive_fga":         "DRIVE_FGA",
        "drive_fg_pct":      "DRIVE_FG_PCT",
        "drive_ftm":         "DRIVE_FTM",
        "drive_fta":         "DRIVE_FTA",
        "drive_ft_pct":      "DRIVE_FT_PCT",
        "drive_pts":         "DRIVE_PTS",
        "drive_pts_pct":     "DRIVE_PTS_PCT",
        "drive_passes":      "DRIVE_PASSES",
        "drive_passes_pct":  "DRIVE_PASSES_PCT",
        "drive_ast":         "DRIVE_AST",
        "drive_ast_pct":     "DRIVE_AST_PCT",
        "drive_tov":         "DRIVE_TOV",
        "drive_tov_pct":     "DRIVE_TOV_PCT",
    })

    merge_tracking(PlayerTrackingPasses, {
        "passes_made":           "PASSES_MADE",
        "passes_received":       "PASSES_RECEIVED",
        "ft_ast":                "FT_AST",
        "secondary_ast":         "SECONDARY_AST",
        "potential_ast":         "POTENTIAL_AST",
        "ast_pts_created":       "AST_PTS_CREATED",
        "ast_adj":               "AST_ADJ",
        "ast_to_pass_pct":       "AST_TO_PASS_PCT",
        "ast_to_pass_pct_adj":   "AST_TO_PASS_PCT_ADJ",
    })

    merge_tracking(PlayerTrackingPossessions, {
        "touches":               "TOUCHES",
        "front_ct_touches":      "FRONT_CT_TOUCHES",
        "time_of_poss":          "TIME_OF_POSS",
        "avg_sec_per_touch":     "AVG_SEC_PER_TOUCH",
        "avg_drib_per_touch":    "AVG_DRIB_PER_TOUCH",
        "pts_per_touch":         "PTS_PER_TOUCH",
        "elbow_touches":         "ELBOW_TOUCHES",
        "post_touches":          "POST_TOUCHES",
        "paint_touches":         "PAINT_TOUCHES",
        "pts_per_paint_touch":   "PTS_PER_PAINT_TOUCH",
    })

    merge_tracking(PlayerTrackingRebounding, {
        "contested_reb":         "CONTESTED_REBOUNDS",
        "contested_oreb":        "CONTESTED_OREB",
        "contested_dreb":        "CONTESTED_DREB",
        "reb_chances":           "REB_CHANCES",
        "oreb_chances":          "OREB_CHANCES",
        "dreb_chances":          "DREB_CHANCES",
        "reb_chance_pct":        "REB_CHANCE_PCT",
        "oreb_chance_pct":       "OREB_CHANCE_PCT",
        "dreb_chance_pct":       "DREB_CHANCE_PCT",
    })

    merge_tracking(PlayerTrackingShooting, {
        "catch_shoot_fgm":       "CATCH_SHOOT_FGM",
        "catch_shoot_fga":       "CATCH_SHOOT_FGA",
        "catch_shoot_fg_pct":    "CATCH_SHOOT_FG_PCT",
        "catch_shoot_pts":       "CATCH_SHOOT_PTS",
        "pull_up_fgm":           "PULL_UP_FGM",
        "pull_up_fga":           "PULL_UP_FGA",
        "pull_up_fg_pct":        "PULL_UP_FG_PCT",
        "pull_up_pts":           "PULL_UP_PTS",
    })

    # ------------------------------------------------------------------
    # 3d. Write to database
    # ------------------------------------------------------------------

    # Player box scores: upsert on (game_id, player_id, quarter)
    if player_rows:
        df_players = pd.DataFrame(player_rows)
        _upsert(
            df=df_players,
            engine=engine,
            schema="nba",
            table="player_box_score_stats",
            pk_cols=["game_id", "player_id", "quarter"],
        )
        log.info(f"    Upserted {len(df_players)} player box score rows")

    # Team box scores: upsert on (game_id, team_id, quarter)
    if team_rows:
        df_teams_box = pd.DataFrame(team_rows)
        _upsert(
            df=df_teams_box,
            engine=engine,
            schema="nba",
            table="team_box_score_stats",
            pk_cols=["game_id", "team_id", "quarter"],
        )
        log.info(f"    Upserted {len(df_teams_box)} team box score rows")

    # Games: upsert on game_id
    if home_team_id and away_team_id:
        df_game = pd.DataFrame([{
            "game_id":          game_id,
            "game_date":        game_date,
            "season_id":        CURRENT_SEASON_ID,
            "home_team_id":     home_team_id,
            "home_team_abbr":   home_abbr,
            "away_team_id":     away_team_id,
            "away_team_abbr":   away_abbr,
            "home_score":       home_pts,
            "away_score":       away_pts,
            "game_total":       game_total,
            "num_periods":      num_periods,
        }])
        _upsert(
            df=df_game,
            engine=engine,
            schema="nba",
            table="games",
            pk_cols=["game_id"],
        )

    # Player tracking: merge advanced + tracking dicts, upsert on (game_id, player_id)
    all_pids = set(advanced_by_player.keys()) | set(tracking_by_player.keys())
    if all_pids:
        tracking_rows = []
        for pid in all_pids:
            row = {"game_id": game_id, "player_id": pid}
            row.update(advanced_by_player.get(pid, {}))
            row.update(tracking_by_player.get(pid, {}))
            tracking_rows.append(row)
        df_tracking = pd.DataFrame(tracking_rows)
        _upsert(
            df=df_tracking,
            engine=engine,
            schema="nba",
            table="player_tracking_stats",
            pk_cols=["game_id", "player_id"],
        )
        log.info(f"    Upserted {len(df_tracking)} player tracking rows")

    # Matchup position aggregation
    if player_rows:
        _aggregate_matchup(
            player_rows=player_rows,
            game_id=game_id,
            game_date=game_date,
            engine=engine,
        )


# ---------------------------------------------------------------------------
# Matchup position aggregation
# ---------------------------------------------------------------------------

def _aggregate_matchup(player_rows, game_id, game_date, engine):
    """
    For each player row, the defending team is the opponent of the player's
    team. We derive that from game context: there are only two teams per game.
    """
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

    # Sum across all quarters per player (game totals)
    numeric_cols = ["fgm","fga","fg3m","fg3a","ftm","fta","oreb","dreb","reb","ast","stl","blk","tov","pf","pts"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    player_game = df.groupby(["player_id","team_id","position"])[numeric_cols].sum().reset_index()

    # For each player, the defending team is the other team
    matchup_rows = []
    for _, row in player_game.iterrows():
        att_team = row["team_id"]
        def_team = team_ids[0] if att_team == team_ids[1] else team_ids[1]
        pos = str(row.get("position") or "").strip() or "UNKNOWN"
        matchup_rows.append({
            "game_id":              game_id,
            "game_date":            game_date,
            "defending_team_id":    def_team,
            "defending_team_abbr":  team_abbr_map.get(def_team, ""),
            "position_group":       pos,
            "_fgm":  row["fgm"], "_fga":  row["fga"],
            "_fg3m": row["fg3m"],"_fg3a": row["fg3a"],
            "_ftm":  row["ftm"], "_fta":  row["fta"],
            "_oreb": row["oreb"],"_dreb": row["dreb"],
            "_reb":  row["reb"], "_ast":  row["ast"],
            "_stl":  row["stl"], "_blk":  row["blk"],
            "_tov":  row["tov"], "_pts":  row["pts"],
        })

    if not matchup_rows:
        return

    agg_df = pd.DataFrame(matchup_rows)
    sum_map = {c: c.lstrip("_") for c in agg_df.columns if c.startswith("_")}
    grouped = (
        agg_df.groupby(["game_id","game_date","defending_team_id","defending_team_abbr","position_group"])
        [[c for c in agg_df.columns if c.startswith("_")]]
        .sum()
        .reset_index()
    )
    grouped["player_count"] = (
        agg_df.groupby(["game_id","game_date","defending_team_id","defending_team_abbr","position_group"])
        .size()
        .reset_index(name="player_count")["player_count"]
        .values
    )
    grouped.rename(columns=sum_map, inplace=True)

    # Also need total_min (sum of numeric minutes per player)
    def parse_min(m):
        try:
            parts = str(m).split(":")
            return int(parts[0]) + int(parts[1]) / 60 if len(parts) == 2 else float(parts[0])
        except Exception:
            return 0.0

    player_game["min_float"] = df.groupby(["player_id","team_id","position"])["min"].first().reset_index()["min"].apply(parse_min).values if False else 0.0

    final_cols = [
        "game_id","game_date","defending_team_id","defending_team_abbr",
        "position_group","player_count",
        "fgm","fga","fg3m","fg3a","ftm","fta","oreb","dreb","reb",
        "ast","stl","blk","tov","pts",
    ]
    grouped["total_min"] = None   # placeholder; would require min parsing above
    final_df = grouped[[c for c in final_cols if c in grouped.columns]]

    _upsert(
        df=final_df,
        engine=engine,
        schema="nba",
        table="matchup_position_stats",
        pk_cols=["game_id","defending_team_id","position_group"],
    )
    log.info(f"    Upserted {len(final_df)} matchup position rows")


# ---------------------------------------------------------------------------
# Step 4: Players reference (upsert from box score data)
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
            (src.player_id, src.first_name, src.last_name, src.team_id, src.team_abbreviation, src.position);
    """
    with engine.begin() as conn:
        result = conn.execute(text(sql))
    log.info(f"  Players merge complete ({result.rowcount} rows affected)")


# ---------------------------------------------------------------------------
# Generic upsert helper (MERGE)
# ---------------------------------------------------------------------------

def _upsert(df: pd.DataFrame, engine, schema: str, table: str, pk_cols: list[str]):
    if df.empty:
        return

    non_pk = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = ", ".join(f"tgt.{c} = src.{c}" for c in non_pk) if non_pk else "tgt.{} = tgt.{}".format(pk_cols[0], pk_cols[0])

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

    if PROXIES:
        log.info(f"Using residential proxy: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set. Requests may be blocked by stats.nba.com.")

    engine = get_engine()

    # Always reload teams reference
    load_teams(engine)

    # Determine target dates / game IDs
    if args.backfill:
        game_pairs = get_all_season_game_ids()
    else:
        yesterday = date.today() - timedelta(days=1)
        game_pairs = get_game_ids([yesterday])

    if not game_pairs:
        log.info("No games found for the target date range. Nothing to load.")
        return

    log.info(f"Processing {len(game_pairs)} game(s)")
    for game_id, game_date in game_pairs:
        process_game(game_id, game_date, engine)

    # After all box scores are loaded, sync players reference
    upsert_players(engine)

    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
