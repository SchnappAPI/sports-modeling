"""
nba_etl.py
----------
Nightly NBA ETL for the sports modeling database.

Covers:
  - Quarter box scores (Q1-Q4 + OT periods) -> nba.player_box_score_stats
  - Team box scores per period              -> nba.team_box_score_stats
  - Game metadata                           -> nba.games
  - Teams reference                         -> nba.teams  (upsert)
  - Players reference                       -> nba.players (upsert)
  - Advanced box scores (usage, ratings)    -> nba.player_tracking_stats
  - Player tracking (touches/passes/reb)    -> nba.player_tracking_stats
  - Matchup position aggregation            -> nba.matchup_position_stats

Run modes:
  python nba_etl.py                           # yesterday's games only (nightly)
  python nba_etl.py --backfill                # all unloaded games in current season
  python nba_etl.py --backfill --limit 200    # oldest 200 unloaded games only

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
    ScoreboardV3,
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

PROXY_URL         = os.environ.get("NBA_PROXY_URL")
CURRENT_SEASON    = "2025-26"
CURRENT_SEASON_ID = "22025"
API_DELAY         = 0.75   # seconds between API calls


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
# Step 1: Load teams reference (upsert, no delete)
# ---------------------------------------------------------------------------

def load_teams(engine):
    log.info("Loading nba.teams (upsert)")
    raw = static_teams.get_teams()
    rows = [
        {
            "nba_team_id":  t["id"],
            "nba_team":     t["abbreviation"],
            "nba_team_name":t["full_name"],
            "roto_team":    t["abbreviation"],
            "espn_team":    t["abbreviation"],
            "espn_team_id": None,
            "aywt_team":    t["abbreviation"],
            "aywt_team_id": None,
            "conference":   None,
        }
        for t in raw
    ]
    df = pd.DataFrame(rows)
    _upsert(df, engine, "nba", "teams", ["nba_team"])
    log.info(f"  Loaded {len(df)} teams")


# ---------------------------------------------------------------------------
# Step 2: Fetch scoreboard metadata for a set of dates
# Returns dict keyed by game_id with full metadata for nba.games.
# Uses ScoreboardV3 which is correct for the 2025-26 season.
#
# ScoreboardV3 GameHeader fields used:
#   gameId, gameCode, gameTimeUTC
# ScoreboardV3 LineScore fields used:
#   gameId, teamId, teamTricode, score
#   Row order within each game: away team first, home team second.
# ---------------------------------------------------------------------------

def fetch_scoreboard_metadata(target_dates: list) -> dict:
    metadata = {}
    unique_dates = sorted(set(target_dates))
    for game_date in unique_dates:
        date_str = game_date.strftime("%Y-%m-%d")
        try:
            sb = ScoreboardV3(
                game_date=date_str,
                league_id="00",
                proxy=PROXY_URL,
            )
            time.sleep(API_DELAY)
            sb_data = sb.get_normalized_dict()

            # GameHeader: one row per game
            headers = {
                row["gameId"]: row
                for row in sb_data.get("GameHeader", [])
            }

            # LineScore: two rows per game (away first, home second)
            # Group by gameId preserving insertion order
            line_scores = {}
            for row in sb_data.get("LineScore", []):
                gid = row.get("gameId")
                if not gid:
                    continue
                if gid not in line_scores:
                    line_scores[gid] = []
                line_scores[gid].append({
                    "team_id":  str(row.get("teamId", "")),
                    "tricode":  row.get("teamTricode", ""),
                    "score":    safe_int(row.get("score")),
                })

            for gid, hdr in headers.items():
                game_code  = hdr.get("gameCode") or ""
                game_time  = hdr.get("gameTimeUTC") or ""

                # Derive sequence number from game_code if present
                # Format is YYYYMMDD/AWYHOM, sequence not directly in V3
                # Use position in the day's game list as a proxy
                game_seq = None

                # Extract home/away from line score rows
                # Away = index 0, Home = index 1
                ls = line_scores.get(gid, [])
                away_abbr = away_tid = ""
                home_abbr = home_tid = ""
                away_pts  = home_pts = None
                if len(ls) >= 2:
                    away_abbr = ls[0]["tricode"]
                    away_tid  = ls[0]["team_id"]
                    away_pts  = ls[0]["score"]
                    home_abbr = ls[1]["tricode"]
                    home_tid  = ls[1]["team_id"]
                    home_pts  = ls[1]["score"]
                elif len(ls) == 1:
                    # Incomplete data; take what we have
                    home_abbr = ls[0]["tricode"]
                    home_tid  = ls[0]["team_id"]
                    home_pts  = ls[0]["score"]

                game_total = (
                    (home_pts + away_pts)
                    if home_pts is not None and away_pts is not None
                    else None
                )
                game_display = (
                    f"{away_abbr}@{home_abbr}"
                    if away_abbr and home_abbr
                    else ""
                )

                metadata[gid] = {
                    "game_date":     game_date,
                    "game_sequence": game_seq,
                    "game_code":     game_code,
                    "game_display":  game_display,
                    "home_team":     home_abbr,
                    "home_team_id":  home_tid,
                    "away_team":     away_abbr,
                    "away_team_id":  away_tid,
                    "home_pts":      home_pts,
                    "away_pts":      away_pts,
                    "game_total":    game_total,
                    "season_year":   CURRENT_SEASON[:4],
                }

        except Exception as exc:
            log.warning(f"    ScoreboardV3 failed for {date_str}: {exc}")

    log.info(f"  Fetched scoreboard metadata for {len(metadata)} game(s) "
             f"across {len(unique_dates)} date(s)")
    return metadata


# ---------------------------------------------------------------------------
# Step 3: Discover game IDs
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
    # Skip preseason games (IDs starting with 001)
    return [
        (gid, pd.to_datetime(gdate).date())
        for gid, gdate in pairs
        if not str(gid).startswith("001")
    ]


# ---------------------------------------------------------------------------
# Step 4: Process one game
# ---------------------------------------------------------------------------

def process_game(game_id: str, game_date: date, game_meta: dict, engine) -> None:
    log.info(f"  Processing game {game_id} ({game_date})")

    # ------------------------------------------------------------------
    # 4a. BoxScoreTraditionalV3: player rows + team rows across all periods
    # ------------------------------------------------------------------
    trad = None
    for attempt in range(1, 4):
        try:
            trad = BoxScoreTraditionalV3(game_id=game_id, proxy=PROXY_URL)
            time.sleep(API_DELAY)
            break
        except Exception as exc:
            log.warning(f"    BoxScoreTraditionalV3 attempt {attempt}/3 failed for {game_id}: {exc}")
            if attempt < 3:
                time.sleep(5)
    if trad is None:
        log.error(f"    Skipping game {game_id} after 3 failed attempts")
        return

    trad_data        = trad.get_normalized_dict()
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

    # ------------------------------------------------------------------
    # 4b. BoxScoreAdvancedV3: usage, ratings, pace, PIE per player
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
                "usage_pct":         safe_float(row.get("usagePercentage") or row.get("USG_PCT")),
                "off_rating":        safe_float(row.get("offensiveRating") or row.get("OFF_RATING")),
                "def_rating":        safe_float(row.get("defensiveRating") or row.get("DEF_RATING")),
                "net_rating":        safe_float(row.get("netRating") or row.get("NET_RATING")),
                "pace":              safe_float(row.get("pace") or row.get("PACE")),
                "pie":               safe_float(row.get("PIE")),
                "true_shooting_pct": safe_float(row.get("trueShootingPercentage")),
                "efg_pct":           safe_float(row.get("effectiveFieldGoalPercentage")),
            }
    except Exception as exc:
        log.warning(f"    BoxScoreAdvancedV3 failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 4c. BoxScorePlayerTrackV3: touches, rebound chances, passes, contested shots
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
                "team_id":                safe_int(row.get("teamId") or row.get("TEAM_ID")),
                "speed":                  safe_float(row.get("speed")),
                "distance":               safe_float(row.get("distance")),
                "touches":                safe_int(row.get("touches")),
                "passes_made":            safe_int(row.get("passes")),
                "secondary_ast":          safe_int(row.get("secondaryAssists")),
                "ft_ast":                 safe_int(row.get("freeThrowAssists")),
                "reb_chances":            safe_int(row.get("reboundChancesTotal")),
                "oreb_chances":           safe_int(row.get("reboundChancesOffensive")),
                "dreb_chances":           safe_int(row.get("reboundChancesDefensive")),
                "contested_fgm":          safe_int(row.get("contestedFieldGoalsMade")),
                "contested_fga":          safe_int(row.get("contestedFieldGoalsAttempted")),
                "contested_fg_pct":       safe_float(row.get("contestedFieldGoalPercentage")),
                "uncontested_fgm":        safe_int(row.get("uncontestedFieldGoalsMade")),
                "uncontested_fga":        safe_int(row.get("uncontestedFieldGoalsAttempted")),
                "uncontested_fg_pct":     safe_float(row.get("uncontestedFieldGoalsPercentage")),
                "defended_at_rim_fgm":    safe_int(row.get("defendedAtRimFieldGoalsMade")),
                "defended_at_rim_fga":    safe_int(row.get("defendedAtRimFieldGoalsAttempted")),
                "defended_at_rim_fg_pct": safe_float(row.get("defendedAtRimFieldGoalPercentage")),
            }
    except Exception as exc:
        log.warning(f"    BoxScorePlayerTrackV3 failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 4d. Write to database
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

    # Games: use ScoreboardV3 metadata. Wrapped in try/except so an FK
    # violation on an unrecognized team abbreviation logs a warning and
    # does not kill the run. The game is still considered processed since
    # player box scores already landed above.
    meta = game_meta.get(game_id)
    if meta:
        try:
            df_game = pd.DataFrame([{
                "game_id":       game_id,
                "game_date":     meta["game_date"],
                "game_sequence": meta["game_sequence"],
                "game_code":     meta["game_code"],
                "game_display":  meta["game_display"],
                "home_team":     meta["home_team"],
                "home_team_id":  meta["home_team_id"],
                "away_team":     meta["away_team"],
                "away_team_id":  meta["away_team_id"],
                "season_year":   meta["season_year"],
            }])
            _upsert(df_game, engine, "nba", "games", ["game_id"])
        except Exception as exc:
            log.warning(f"    Games upsert failed for {game_id}: {exc}")
    else:
        log.warning(f"    No scoreboard metadata found for {game_id}, skipping games row")

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
                player_id                    AS nba_player_id,
                first_name + ' ' + last_name AS player_name,
                team_abbreviation            AS nba_team,
                position
            FROM nba.player_box_score_stats
            WHERE player_id IS NOT NULL
        ) AS src
        ON tgt.nba_player_id = src.nba_player_id
        WHEN MATCHED THEN UPDATE SET
            tgt.player_name = src.player_name,
            tgt.nba_team    = src.nba_team,
            tgt.position    = src.position
        WHEN NOT MATCHED THEN INSERT
            (nba_player_id, player_name, nba_team, position)
        VALUES
            (src.nba_player_id, src.player_name, src.nba_team, src.position);
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
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the oldest N unloaded games. Use for batched backfill.",
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
        game_pairs.sort(key=lambda x: x[1])
        with engine.connect() as conn:
            loaded = {
                row[0] for row in conn.execute(
                    text("SELECT DISTINCT game_id FROM nba.games")
                )
            }
        game_pairs = [p for p in game_pairs if p[0] not in loaded]
        if args.limit:
            game_pairs = game_pairs[:args.limit]
            log.info(f"  Batched backfill: {len(game_pairs)} unloaded game(s) targeted")
        else:
            log.info(f"  Full backfill: {len(game_pairs)} unloaded game(s) targeted")
    else:
        yesterday  = date.today() - timedelta(days=1)
        game_pairs = get_game_ids([yesterday])

    if not game_pairs:
        log.info("No games found for the target date range. Nothing to load.")
        return

    # Fetch scoreboard metadata once per unique date for all target games
    target_dates = list({gdate for _, gdate in game_pairs})
    game_meta = fetch_scoreboard_metadata(target_dates)

    log.info(f"Processing {len(game_pairs)} game(s)")
    for game_id, game_date in game_pairs:
        process_game(game_id, game_date, game_meta, engine)

    upsert_players(engine)
    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
