"""
nba_etl.py
----------
Nightly NBA ETL for the sports modeling database.

Loads per run:
  nba.play_by_play          Raw PBP events. PK: game_id + action_number.
                            Use this table to derive per-quarter or any
                            custom time-window box scores in Power BI or SQL.

  nba.player_box_score_stats  Full-game box score per player. quarter='GAME'.
                            Provides accurate min and plus_minus which cannot
                            be reliably derived from PBP alone.

  nba.team_box_score_stats  Full-game box score per team. quarter='GAME'.

  nba.player_tracking_stats Game-level tracking stats (touches, rebound
                            chances, passes, contested shots, usage, ratings).
                            No quarter-level tracking exists in the NBA API.

  nba.games                 One row per game with scoreboard metadata.
  nba.teams                 30-team reference. Upserted each run.
  nba.players               Player reference. Upserted from box score data.
  nba.matchup_position_stats  Stats allowed by each team to each position
                            group per game.

Run modes:
  python nba_etl.py                           # yesterday only (nightly)
  python nba_etl.py --backfill                # all unloaded games
  python nba_etl.py --backfill --limit 200    # oldest 200 unloaded games

Prerequisites:
  Run nba_play_by_play.sql in Azure SQL before first use.

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
    PlayByPlayV3,
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
API_DELAY         = 0.75   # seconds between calls to avoid rate limits
RETRY_WAIT        = 5      # seconds between retry attempts


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
        return float(val) if val not in (None, "", "None") and not (isinstance(val, float) and pd.isna(val)) else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(val) if val not in (None, "", "None") and not (isinstance(val, float) and pd.isna(val)) else None
    except (ValueError, TypeError):
        return None


def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).strip() or None


def safe_bit(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(bool(val))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Retry wrapper for NBA API calls
# ---------------------------------------------------------------------------

def api_call_with_retry(fn, label, attempts=3):
    """
    Calls fn() up to `attempts` times. Returns the result or None on failure.
    fn should be a zero-argument lambda that constructs and returns the endpoint.
    """
    for attempt in range(1, attempts + 1):
        try:
            result = fn()
            time.sleep(API_DELAY)
            return result
        except Exception as exc:
            log.warning(f"    {label} attempt {attempt}/{attempts} failed: {exc}")
            if attempt < attempts:
                time.sleep(RETRY_WAIT)
    log.error(f"    {label} failed after {attempts} attempts, skipping")
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
# Step 2: Fetch scoreboard metadata via ScoreboardV3
# Returns dict keyed by game_id (string) with metadata for nba.games.
#
# ScoreboardV3 uses typed DataSet objects, NOT get_normalized_dict().
# .game_header.get_data_frame() returns one row per game.
# .line_score.get_data_frame() returns two rows per game: away first, home second.
# ---------------------------------------------------------------------------

def fetch_scoreboard_metadata(target_dates: list) -> dict:
    metadata = {}
    unique_dates = sorted(set(target_dates))
    for game_date in unique_dates:
        date_str = game_date.strftime("%Y-%m-%d")
        sb = api_call_with_retry(
            lambda d=date_str: ScoreboardV3(game_date=d, league_id="00", proxy=PROXY_URL),
            f"ScoreboardV3 {date_str}",
        )
        if sb is None:
            continue
        try:
            headers_df = sb.game_header.get_data_frame()
            lines_df   = sb.line_score.get_data_frame()

            # Ensure gameId is string in both frames for reliable matching
            headers_df["gameId"] = headers_df["gameId"].astype(str)
            lines_df["gameId"]   = lines_df["gameId"].astype(str)

            for _, hdr in headers_df.iterrows():
                gid       = str(hdr["gameId"])
                game_code = safe_str(hdr.get("gameCode")) or ""

                # LineScore rows for this game: away=index 0, home=index 1
                game_ls = lines_df[lines_df["gameId"] == gid]
                away_abbr = away_tid = home_abbr = home_tid = ""
                away_pts  = home_pts = None

                if len(game_ls) >= 2:
                    away_row  = game_ls.iloc[0]
                    home_row  = game_ls.iloc[1]
                    away_abbr = safe_str(away_row.get("teamTricode")) or ""
                    away_tid  = str(safe_int(away_row.get("teamId")) or "")
                    away_pts  = safe_int(away_row.get("score"))
                    home_abbr = safe_str(home_row.get("teamTricode")) or ""
                    home_tid  = str(safe_int(home_row.get("teamId")) or "")
                    home_pts  = safe_int(home_row.get("score"))
                elif len(game_ls) == 1:
                    home_abbr = safe_str(game_ls.iloc[0].get("teamTricode")) or ""
                    home_tid  = str(safe_int(game_ls.iloc[0].get("teamId")) or "")
                    home_pts  = safe_int(game_ls.iloc[0].get("score"))

                game_total = (
                    home_pts + away_pts
                    if home_pts is not None and away_pts is not None
                    else None
                )
                game_display = (
                    f"{away_abbr}@{home_abbr}"
                    if away_abbr and home_abbr else ""
                )

                metadata[gid] = {
                    "game_date":    game_date,
                    "game_code":    game_code,
                    "game_display": game_display,
                    "home_team":    home_abbr,
                    "home_team_id": home_tid,
                    "away_team":    away_abbr,
                    "away_team_id": away_tid,
                    "home_pts":     home_pts,
                    "away_pts":     away_pts,
                    "game_total":   game_total,
                    "season_year":  CURRENT_SEASON[:4],
                }
        except Exception as exc:
            log.warning(f"    ScoreboardV3 parse failed for {date_str}: {exc}")

    log.info(
        f"  Fetched scoreboard metadata for {len(metadata)} game(s) "
        f"across {len(unique_dates)} date(s)"
    )
    return metadata


# ---------------------------------------------------------------------------
# Step 3: Discover game IDs
# ---------------------------------------------------------------------------

def get_game_ids(target_dates: list) -> list:
    log.info(f"Fetching game IDs for {len(target_dates)} date(s)")
    date_strings = {d.strftime("%m/%d/%Y") for d in target_dates}
    finder = api_call_with_retry(
        lambda: LeagueGameFinder(
            season_nullable=CURRENT_SEASON,
            league_id_nullable="00",
            proxy=PROXY_URL,
        ),
        "LeagueGameFinder",
    )
    if finder is None:
        return []
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
    log.info(f"Backfill: fetching all game IDs for season {CURRENT_SEASON}")
    finder = api_call_with_retry(
        lambda: LeagueGameFinder(
            season_nullable=CURRENT_SEASON,
            league_id_nullable="00",
            proxy=PROXY_URL,
        ),
        "LeagueGameFinder (backfill)",
    )
    if finder is None:
        return []
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
#
# API calls per game (4 total):
#   1. PlayByPlayV3         -> nba.play_by_play (raw events, all periods)
#   2. BoxScoreTraditionalV3 -> nba.player_box_score_stats, nba.team_box_score_stats
#                              (full-game totals for accurate min + plus_minus)
#   3. BoxScoreAdvancedV3   -> nba.player_tracking_stats (usage, ratings)
#   4. BoxScorePlayerTrackV3 -> nba.player_tracking_stats (touches, reb chances, etc.)
# ---------------------------------------------------------------------------

def process_game(game_id: str, game_date: date, game_meta: dict, engine) -> None:
    log.info(f"  Processing game {game_id} ({game_date})")

    # ------------------------------------------------------------------
    # 4a. Play-by-Play (primary source for quarter-level derivations)
    # ------------------------------------------------------------------
    pbp_ep = api_call_with_retry(
        lambda: PlayByPlayV3(game_id=game_id, proxy=PROXY_URL),
        f"PlayByPlayV3 {game_id}",
    )
    if pbp_ep is None:
        log.error(f"    Skipping game {game_id}: PBP fetch failed")
        return

    try:
        pbp_df = pbp_ep.play_by_play.get_data_frame()
    except Exception as exc:
        log.error(f"    Skipping game {game_id}: PBP parse failed: {exc}")
        return

    if pbp_df.empty:
        log.warning(f"    PBP returned empty for {game_id}")
    else:
        pbp_rows = []
        for _, row in pbp_df.iterrows():
            pbp_rows.append({
                "game_id":        game_id,
                "action_number":  safe_int(row.get("actionNumber")),
                "period":         safe_int(row.get("period")),
                "clock":          safe_str(row.get("clock")),
                "team_id":        safe_int(row.get("teamId")),
                "team_tricode":   safe_str(row.get("teamTricode")),
                "person_id":      safe_int(row.get("personId")),
                "player_name":    safe_str(row.get("playerName")),
                "player_name_i":  safe_str(row.get("playerNameI")),
                "x_legacy":       safe_float(row.get("xLegacy")),
                "y_legacy":       safe_float(row.get("yLegacy")),
                "shot_distance":  safe_float(row.get("shotDistance")),
                "shot_result":    safe_str(row.get("shotResult")),
                "is_field_goal":  safe_bit(row.get("isFieldGoal")),
                "score_home":     safe_int(row.get("scoreHome")),
                "score_away":     safe_int(row.get("scoreAway")),
                "points_total":   safe_int(row.get("pointsTotal")),
                "location":       safe_str(row.get("location")),
                "description":    safe_str(row.get("description")),
                "action_type":    safe_str(row.get("actionType")),
                "sub_type":       safe_str(row.get("subType")),
                "video_available":safe_bit(row.get("videoAvailable")),
                "action_id":      safe_int(row.get("actionId")),
            })

        # Drop rows where action_number is None (cannot form PK)
        pbp_rows = [r for r in pbp_rows if r["action_number"] is not None]

        if pbp_rows:
            df_pbp = pd.DataFrame(pbp_rows)
            _upsert(df_pbp, engine, "nba", "play_by_play",
                    ["game_id", "action_number"])
            log.info(f"    Upserted {len(df_pbp)} PBP rows")

    # ------------------------------------------------------------------
    # 4b. BoxScoreTraditionalV3: full-game player and team box scores
    #     Provides accurate min and plus_minus (not derivable from PBP).
    #     quarter='GAME' marks these as full-game totals.
    #     All V3 endpoints use .dataset_attr.get_data_frame() with camelCase fields.
    # ------------------------------------------------------------------
    trad_ep = api_call_with_retry(
        lambda: BoxScoreTraditionalV3(game_id=game_id, proxy=PROXY_URL),
        f"BoxScoreTraditionalV3 {game_id}",
    )

    player_rows = []
    team_rows   = []
    team_totals = {}

    if trad_ep is not None:
        try:
            player_df = trad_ep.player_stats.get_data_frame()
            team_df   = trad_ep.team_stats.get_data_frame()

            for _, row in player_df.iterrows():
                comment = safe_str(row.get("comment")) or ""
                if comment:   # DNP players have a comment; skip them
                    continue
                pid = safe_int(row.get("personId"))
                if pid is None:
                    continue
                player_rows.append({
                    "game_id":           game_id,
                    "player_id":         pid,
                    "quarter":           "GAME",
                    "first_name":        safe_str(row.get("firstName")) or "",
                    "last_name":         safe_str(row.get("familyName")) or "",
                    "team_id":           safe_int(row.get("teamId")),
                    "team_abbreviation": safe_str(row.get("teamTricode")) or "",
                    "position":          safe_str(row.get("position")) or "",
                    "comment":           comment,
                    "jersey_num":        safe_str(row.get("jerseyNum")) or "",
                    "min":               safe_str(row.get("minutes")) or "",
                    "fgm":               safe_int(row.get("fieldGoalsMade")),
                    "fga":               safe_int(row.get("fieldGoalsAttempted")),
                    "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
                    "fg3m":              safe_int(row.get("threePointersMade")),
                    "fg3a":              safe_int(row.get("threePointersAttempted")),
                    "fg3_pct":           safe_float(row.get("threePointersPercentage")),
                    "ftm":               safe_int(row.get("freeThrowsMade")),
                    "fta":               safe_int(row.get("freeThrowsAttempted")),
                    "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
                    "oreb":              safe_int(row.get("reboundsOffensive")),
                    "dreb":              safe_int(row.get("reboundsDefensive")),
                    "reb":               safe_int(row.get("reboundsTotal")),
                    "ast":               safe_int(row.get("assists")),
                    "stl":               safe_int(row.get("steals")),
                    "blk":               safe_int(row.get("blocks")),
                    "tov":               safe_int(row.get("turnovers")),
                    "pf":                safe_int(row.get("foulsPersonal")),
                    "pts":               safe_int(row.get("points")),
                    "plus_minus":        safe_float(row.get("plusMinusPoints")),
                })

            for _, row in team_df.iterrows():
                tid  = safe_int(row.get("teamId"))
                abbr = safe_str(row.get("teamTricode")) or ""
                pts  = safe_int(row.get("points")) or 0
                if tid is not None:
                    team_totals[tid] = {"abbr": abbr, "pts": pts}
                team_rows.append({
                    "game_id":           game_id,
                    "team_id":           tid,
                    "team_abbreviation": abbr,
                    "quarter":           "GAME",
                    "min":               safe_str(row.get("minutes")) or "",
                    "fgm":               safe_int(row.get("fieldGoalsMade")),
                    "fga":               safe_int(row.get("fieldGoalsAttempted")),
                    "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
                    "fg3m":              safe_int(row.get("threePointersMade")),
                    "fg3a":              safe_int(row.get("threePointersAttempted")),
                    "fg3_pct":           safe_float(row.get("threePointersPercentage")),
                    "ftm":               safe_int(row.get("freeThrowsMade")),
                    "fta":               safe_int(row.get("freeThrowsAttempted")),
                    "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
                    "oreb":              safe_int(row.get("reboundsOffensive")),
                    "dreb":              safe_int(row.get("reboundsDefensive")),
                    "reb":               safe_int(row.get("reboundsTotal")),
                    "ast":               safe_int(row.get("assists")),
                    "stl":               safe_int(row.get("steals")),
                    "blk":               safe_int(row.get("blocks")),
                    "tov":               safe_int(row.get("turnovers")),
                    "pf":                safe_int(row.get("foulsPersonal")),
                    "pts":               safe_int(row.get("points")),
                })

        except Exception as exc:
            log.warning(f"    BoxScoreTraditionalV3 parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 4c. BoxScoreAdvancedV3: game-level usage, ratings, pace, PIE
    # ------------------------------------------------------------------
    advanced_by_player = {}
    adv_ep = api_call_with_retry(
        lambda: BoxScoreAdvancedV3(game_id=game_id, proxy=PROXY_URL),
        f"BoxScoreAdvancedV3 {game_id}",
    )
    if adv_ep is not None:
        try:
            adv_df = adv_ep.player_stats.get_data_frame()
            for _, row in adv_df.iterrows():
                pid = safe_int(row.get("personId"))
                if pid is None:
                    continue
                advanced_by_player[pid] = {
                    "usage_pct":         safe_float(row.get("usagePercentage")),
                    "off_rating":        safe_float(row.get("offensiveRating")),
                    "def_rating":        safe_float(row.get("defensiveRating")),
                    "net_rating":        safe_float(row.get("netRating")),
                    "pace":              safe_float(row.get("pace")),
                    "pie":               safe_float(row.get("PIE")),
                    "true_shooting_pct": safe_float(row.get("trueShootingPercentage")),
                    "efg_pct":           safe_float(row.get("effectiveFieldGoalPercentage")),
                }
        except Exception as exc:
            log.warning(f"    BoxScoreAdvancedV3 parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 4d. BoxScorePlayerTrackV3: game-level tracking stats
    # ------------------------------------------------------------------
    tracking_by_player = {}
    trk_ep = api_call_with_retry(
        lambda: BoxScorePlayerTrackV3(game_id=game_id, proxy=PROXY_URL),
        f"BoxScorePlayerTrackV3 {game_id}",
    )
    if trk_ep is not None:
        try:
            trk_df = trk_ep.player_stats.get_data_frame()
            for _, row in trk_df.iterrows():
                pid = safe_int(row.get("personId"))
                if pid is None:
                    continue
                tracking_by_player[pid] = {
                    "team_id":                safe_int(row.get("teamId")),
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
            log.warning(f"    BoxScorePlayerTrackV3 parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 4e. Write to database
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

    # Games row: use scoreboard metadata for home/away/scores,
    # wrapped in try/except so FK violations on edge-case team codes don't crash the run
    meta = game_meta.get(game_id)
    if meta:
        try:
            df_game = pd.DataFrame([{
                "game_id":       game_id,
                "game_date":     meta["game_date"],
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
            log.warning(f"    Games upsert skipped for {game_id}: {exc}")
    else:
        log.warning(f"    No scoreboard metadata for {game_id}, games row skipped")

    # Player tracking: merge advanced + tracking dicts
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

    # Matchup position aggregation from full-game player rows
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

    agg_df   = pd.DataFrame(matchup_rows)
    sum_cols = [c for c in agg_df.columns if c.startswith("total_")]
    grouped  = (
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
    final_df = grouped.merge(
        counts,
        on=["game_id","game_date","defending_team_id","defending_team_abbr","position_group"]
    )

    _upsert(final_df, engine, "nba", "matchup_position_stats",
            ["game_id", "defending_team_id", "position_group"])
    log.info(f"    Upserted {len(final_df)} matchup position rows")


# ---------------------------------------------------------------------------
# Players reference upsert (runs once after all games in the batch are loaded)
# Sources from player_box_score_stats which has separate first_name / last_name.
# Wrapped in try/except in case an edge-case team abbreviation violates the FK.
# ---------------------------------------------------------------------------

def upsert_players(engine):
    log.info("Upserting nba.players from box score data")
    sql = """
        MERGE nba.players AS tgt
        USING (
            SELECT DISTINCT
                player_id                        AS nba_player_id,
                LTRIM(first_name + ' ' + last_name) AS player_name,
                team_abbreviation                AS nba_team,
                position
            FROM nba.player_box_score_stats
            WHERE player_id IS NOT NULL
              AND quarter = 'GAME'
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
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
        log.info(f"  Players merge complete ({result.rowcount} rows affected)")
    except Exception as exc:
        log.warning(f"  Players merge failed (likely FK violation on team abbr): {exc}")


# ---------------------------------------------------------------------------
# Generic upsert via MERGE (one MERGE per row via executemany)
# ---------------------------------------------------------------------------

def _upsert(df: pd.DataFrame, engine, schema: str, table: str, pk_cols: list):
    if df.empty:
        return

    # Convert all NaN/NaT values to None so pyodbc sends NULL
    # Use applymap (pandas < 2.1) or map (pandas >= 2.1) for element-wise conversion
    def _to_none(val):
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        return val

    df = df.apply(lambda col: col.map(_to_none))

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
        game_pairs.sort(key=lambda x: x[1])   # oldest first
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
    game_meta    = fetch_scoreboard_metadata(target_dates)

    log.info(f"Processing {len(game_pairs)} game(s)")
    for game_id, game_date in game_pairs:
        process_game(game_id, game_date, game_meta, engine)

    upsert_players(engine)
    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
