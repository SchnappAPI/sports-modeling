"""
mlb_etl.py

Loads all MLB data from the MLB Stats API into Azure SQL in a single pass.

Load order (respects foreign key dependencies):
    1. mlb.teams                  - Team reference. Truncate and reload each run.
    2. mlb.players                - Player reference. Truncate and reload each run.
    3. mlb.games                  - One row per game. Upsert on game_pk.
    4. mlb.batting_stats          - Per-batter per-game box score. Upsert on batter_game_id.
    5. mlb.pitching_stats         - Per-pitcher per-game box score. Upsert on pitcher_game_id.
    6. mlb.player_season_batting  - Season cumulative batting snapshot. Truncate and reload.
    7. mlb.pitcher_season_stats   - Season cumulative pitching snapshot. Truncate and reload.

Source endpoint for box scores (steps 3-5):
    https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics

    Using /withMetrics instead of the statsapi.boxscore_data wrapper gives us
    access to liveData.boxscore.teams.{side}.players, which contains additional
    per-batter fields not available in the summary endpoint:
      fly_outs, ground_outs, air_outs, pop_outs, line_outs,
      total_bases, games_played, plate_appearances

    The migration script etl/mlb_batting_stats_migration.sql must be run once
    before deploying this version to add those columns to the table.

Teams and players are always fully rebuilt from the API. Foreign key constraints on
the child tables (games, batting_stats, pitching_stats, season snapshots) have been
dropped via drop_mlb_fk_constraints.sql, so teams and players can be reloaded without
clearing box score history.

Box score tables use upsert logic. Games already present in mlb.batting_stats are
skipped, so only new games are fetched from the API on each run.

Historical box score load: 2023, 2024, and current season.
Season snapshot tables always reflect the current season only.

Runs exclusively in GitHub Actions. Never run on a local machine.
Credentials are injected as environment variables from GitHub Secrets.
"""

import os
import sys
import time
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
import statsapi
from sqlalchemy import text

_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from etl.db import get_engine, upsert

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
# Helpers
# ---------------------------------------------------------------------------

def api_get(endpoint, params, retries=3, pause=5):
    """Wrap statsapi.get with retry on transient failures."""
    for attempt in range(1, retries + 1):
        try:
            return statsapi.get(endpoint, params)
        except Exception as exc:
            log.warning("API call failed (attempt %d/%d): %s", attempt, retries, exc)
            if attempt < retries:
                time.sleep(pause)
    raise RuntimeError(f"API call to {endpoint} failed after {retries} attempts.")


def fetch_game_json(game_pk, retries=3, pause=5):
    """Fetch the full /withMetrics JSON for a game. Returns None on failure."""
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("withMetrics fetch failed for game_pk %d (attempt %d/%d): %s",
                        game_pk, attempt, retries, exc)
            if attempt < retries:
                time.sleep(pause)
    return None


def safe_int(val):
    try:
        return int(val) if val is not None and str(val).strip() not in ("", "-") else None
    except (ValueError, TypeError):
        return None


def safe_float(val):
    try:
        s = str(val).strip()
        if s in ("", "-", ".---"):
            return None
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_innings_pitched(ip_str):
    """
    Convert fractional innings string to a true decimal.
    MLB notation: .1 = 1 out = 1/3 inning, .2 = 2 outs = 2/3 inning.
    """
    if ip_str is None or str(ip_str).strip() == "":
        return None
    try:
        s = str(ip_str)
        whole, frac = s.split(".") if "." in s else (s, "0")
        return int(whole) + int(frac) / 3.0
    except (ValueError, TypeError):
        return None


def validate_dataframe(df, required_cols, context):
    if df.empty:
        log.warning("No data returned for %s, skipping load.", context)
        return False
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        log.warning(
            "Unexpected API response for %s: missing columns %s, skipping load.",
            context, missing
        )
        return False
    return True


def truncate_and_load(engine, df, schema, table):
    """Reload a table entirely. Used for reference and snapshot tables."""
    if df.empty:
        log.warning("Truncate/load skipped: empty dataframe for %s.%s", schema, table)
        return
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM [{schema}].[{table}]"))
    df.to_sql(
        table, engine, schema=schema,
        if_exists="append", index=False,
        chunksize=200
    )
    log.info("Loaded %d rows into %s.%s", len(df), schema, table)


def fetch_schedule_months(season):
    """
    Fetch all Final regular season games for a season, month by month,
    to avoid 503 errors on large date ranges. Returns a list of game dicts.
    """
    log.info("Fetching schedule for %d season", season)
    months = [
        (f"{season}-03-01", f"{season}-03-31"),
        (f"{season}-04-01", f"{season}-04-30"),
        (f"{season}-05-01", f"{season}-05-31"),
        (f"{season}-06-01", f"{season}-06-30"),
        (f"{season}-07-01", f"{season}-07-31"),
        (f"{season}-08-01", f"{season}-08-31"),
        (f"{season}-09-01", f"{season}-09-30"),
        (f"{season}-10-01", f"{season}-10-31"),
    ]
    games = []
    for start, end in months:
        try:
            chunk = statsapi.schedule(start_date=start, end_date=end, sportId=1)
            final_regular = [
                g for g in chunk
                if g.get("game_type") == "R" and g.get("status") == "Final"
            ]
            games.extend(final_regular)
            time.sleep(0.5)
        except Exception as exc:
            log.warning("Schedule fetch failed for %s to %s: %s", start, end, exc)
    log.info("Found %d final regular season games for %d", len(games), season)
    return games

# ---------------------------------------------------------------------------
# 1. Teams
# ---------------------------------------------------------------------------

def load_teams(engine, season):
    log.info("Loading teams for season %d", season)
    data = api_get("teams", {"sportId": 1, "season": season})
    rows = []
    for t in data.get("teams", []):
        rows.append({
            "team_id":           t["id"],
            "team_abbreviation": t.get("abbreviation", ""),
            "full_name":         t.get("name", ""),
            "venue_id":          t.get("venue", {}).get("id"),
        })

    df = pd.DataFrame(rows)
    if not validate_dataframe(df, ["team_id"], "teams"):
        log.warning("Teams load skipped. ETL cannot continue without team reference data.")
        raise RuntimeError("Teams API returned no usable data.")

    df = df.drop_duplicates(subset=["team_id"])
    truncate_and_load(engine, df, "mlb", "teams")

    return {row["team_id"]: row["team_abbreviation"] for row in rows}

# ---------------------------------------------------------------------------
# 2. Players
# ---------------------------------------------------------------------------

def load_players(engine, seasons):
    rows = []
    seen = set()
    for season in seasons:
        log.info("Loading players for season %d", season)
        data = api_get("sports_players", {"sport_id": 1, "season": season})
        for p in data.get("people", []):
            pid = p["id"]
            if pid in seen:
                continue
            seen.add(pid)
            rows.append({
                "player_id":   pid,
                "player_name": p.get("fullName", ""),
                "team_id":     p.get("currentTeam", {}).get("id"),
                "position":    p.get("primaryPosition", {}).get("abbreviation"),
                "bat_side":    p.get("batSide", {}).get("code"),
                "pitch_hand":  p.get("pitchHand", {}).get("code"),
            })

    df = pd.DataFrame(rows)
    if not validate_dataframe(df, ["player_id"], "players"):
        log.warning("Players load skipped. ETL cannot continue without player reference data.")
        raise RuntimeError("Players API returned no usable data.")

    truncate_and_load(engine, df, "mlb", "players")

# ---------------------------------------------------------------------------
# 3 + 4 + 5. Games, batting_stats, pitching_stats via /withMetrics
# ---------------------------------------------------------------------------

def build_game_row(game, team_abbr, away_pitcher_id, home_pitcher_id):
    """Build a single row for mlb.games from a schedule game dict."""
    away_id   = game["away_id"]
    home_id   = game["home_id"]
    away_abbr = team_abbr.get(away_id, "")
    home_abbr = team_abbr.get(home_id, "")

    away_score = safe_int(game.get("away_score"))
    home_score = safe_int(game.get("home_score"))

    if away_score is not None and home_score is not None:
        away_is_winner = 1 if away_score > home_score else 0
        home_is_winner = 1 if home_score > away_score else 0
        is_tie         = 1 if away_score == home_score else 0
    else:
        away_is_winner = None
        home_is_winner = None
        is_tie         = None

    return {
        "game_pk":             game["game_id"],
        "game_date":           game["game_date"],
        "game_datetime":       game.get("game_datetime"),
        "game_type":           game.get("game_type", "R"),
        "game_status":         "F" if game.get("status") == "Final" else game.get("status"),
        "abstract_game_state": game.get("status"),
        "day_night":           None,
        "double_header":       game.get("doubleheader"),
        "game_number":         safe_int(game.get("game_num")),
        "game_display":        f"{away_abbr}@{home_abbr}",
        "venue_id":            safe_int(game.get("venue_id")),
        "venue_name":          game.get("venue_name"),
        "away_team_id":        away_id,
        "away_team_score":     away_score,
        "away_is_winner":      away_is_winner,
        "away_pitcher_id":     away_pitcher_id,
        "away_pitcher_name":   game.get("away_probable_pitcher") or None,
        "away_pitcher_hand":   None,
        "home_team_id":        home_id,
        "home_team_score":     home_score,
        "home_is_winner":      home_is_winner,
        "home_pitcher_id":     home_pitcher_id,
        "home_pitcher_name":   game.get("home_probable_pitcher") or None,
        "home_pitcher_hand":   None,
        "is_tie":              is_tie,
        "games_in_series":     None,
        "series_game_number":  None,
        "game_date_index":     None,
    }


def _clean_float(val):
    """Return None for dashes/empty, otherwise float."""
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "-", ".---", "-.--"):
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_boxscore_from_json(game, game_json):
    """
    Extract pitcher IDs, batter rows, and pitcher rows from the full /withMetrics JSON.

    Batting data sourced from liveData.boxscore.teams.{side}.players — richer than
    the summary boxscore endpoint. Includes fly_outs, ground_outs, air_outs, pop_outs,
    line_outs, total_bases, games_played, plate_appearances.

    Pitching data sourced from liveData.boxscore.teams.{side}.pitchers list.

    Returns (away_starter_id, home_starter_id, batter_rows, pitcher_rows).
    """
    game_pk   = game["game_id"]
    game_date = game["game_date"]

    try:
        live_boxscore = game_json["liveData"]["boxscore"]["teams"]
    except (KeyError, TypeError):
        log.warning("game_pk %d: liveData.boxscore not found in /withMetrics response.", game_pk)
        return None, None, [], []

    batter_rows  = []
    pitcher_rows = []
    away_starter_id = None
    home_starter_id = None

    sides = [
        ("away", game["away_id"], "A"),
        ("home", game["home_id"], "H"),
    ]

    for side_key, team_id, side_label in sides:
        side_data = live_boxscore.get(side_key, {})
        players   = side_data.get("players", {})  # dict keyed by "ID{player_id}"
        pitchers  = side_data.get("pitchers", [])  # list of player_id ints

        # Starting pitcher = first pitcher_id in the list
        starter_id = pitchers[0] if pitchers else None
        if side_label == "A":
            away_starter_id = starter_id
        else:
            home_starter_id = starter_id

        # --- Batters ---
        for player_key, player_data in players.items():
            batting_order = player_data.get("battingOrder")
            if batting_order is None:
                continue  # pitchers and bench-only players have no batting order

            stats   = player_data.get("stats", {}).get("batting", {})
            person  = player_data.get("person", {})
            pos     = player_data.get("position", {})
            pid     = person.get("id")
            if not pid:
                continue

            batter_rows.append({
                "batter_game_id":  f"{pid}-{game_pk}-{team_id}",
                "game_pk":         game_pk,
                "game_date":       game_date,
                "player_id":       pid,
                "team_id":         team_id,
                "side":            side_label,
                "position":        pos.get("abbreviation"),
                "batting_order":   safe_int(batting_order),
                "games_played":    safe_int(stats.get("gamesPlayed")),
                "plate_appearances": safe_int(stats.get("plateAppearances")),
                "at_bats":         safe_int(stats.get("atBats")),
                "runs":            safe_int(stats.get("runs")),
                "hits":            safe_int(stats.get("hits")),
                "doubles":         safe_int(stats.get("doubles")),
                "triples":         safe_int(stats.get("triples")),
                "home_runs":       safe_int(stats.get("homeRuns")),
                "total_bases":     safe_int(stats.get("totalBases")),
                "rbi":             safe_int(stats.get("rbi")),
                "stolen_bases":    safe_int(stats.get("stolenBases")),
                "walks":           safe_int(stats.get("baseOnBalls")),
                "intentional_walks": safe_int(stats.get("intentionalWalks")),
                "strikeouts":      safe_int(stats.get("strikeOuts")),
                "hit_by_pitch":    safe_int(stats.get("hitByPitch")),
                "left_on_base":    safe_int(stats.get("leftOnBase")),
                "sac_bunts":       safe_int(stats.get("sacBunts")),
                "sac_flies":       safe_int(stats.get("sacFlies")),
                "fly_outs":        safe_int(stats.get("flyOuts")),
                "ground_outs":     safe_int(stats.get("groundOuts")),
                "air_outs":        safe_int(stats.get("airOuts")),
                "pop_outs":        safe_int(stats.get("popOuts")),
                "line_outs":       safe_int(stats.get("lineOuts")),
                "batting_avg":     _clean_float(stats.get("avg")),
                "obp":             _clean_float(stats.get("obp")),
                "slg":             _clean_float(stats.get("slg")),
                "ops":             _clean_float(stats.get("ops")),
            })

        # --- Pitchers ---
        # Pitcher stats are in the same players dict, but we use the pitchers list
        # to identify who pitched and in what order.
        for pid in pitchers:
            pkey  = f"ID{pid}"
            pdata = players.get(pkey, {})
            if not pdata:
                continue
            stats = pdata.get("stats", {}).get("pitching", {})

            note = pdata.get("gameStatus", {})
            # Determine if starter (first in list) for the note field
            is_starter = (pid == starter_id)

            pitcher_rows.append({
                "pitcher_game_id": f"{pid}-{game_pk}",
                "game_pk":         game_pk,
                "game_date":       game_date,
                "player_id":       pid,
                "team_id":         team_id,
                "side":            side_label,
                "innings_pitched": parse_innings_pitched(stats.get("inningsPitched")),
                "hits_allowed":    safe_int(stats.get("hits")),
                "runs_allowed":    safe_int(stats.get("runs")),
                "earned_runs":     safe_int(stats.get("earnedRuns")),
                "walks":           safe_int(stats.get("baseOnBalls")),
                "strikeouts":      safe_int(stats.get("strikeOuts")),
                "hr_allowed":      safe_int(stats.get("homeRuns")),
                "era":             _clean_float(stats.get("era")),
                "pitches":         safe_int(stats.get("numberOfPitches")),
                "strikes":         safe_int(stats.get("strikes")),
                "note":            "SP" if is_starter else None,
            })

    return away_starter_id, home_starter_id, batter_rows, pitcher_rows


def load_games_and_box_scores(engine, seasons, team_abbr):
    """
    For each season, fetch the schedule and /withMetrics JSON for every Final
    regular season game not yet loaded. Upsert into mlb.games, mlb.batting_stats,
    and mlb.pitching_stats in batches.
    """
    with engine.connect() as conn:
        existing_game_pks = set(
            row[0] for row in conn.execute(
                text("SELECT DISTINCT game_pk FROM [mlb].[batting_stats]")
            )
        )
    log.info("Existing game_pk count in batting_stats: %d", len(existing_game_pks))

    for season in seasons:
        games     = fetch_schedule_months(season)
        new_games = [g for g in games if g["game_id"] not in existing_game_pks]

        seen_ids = set()
        deduped  = []
        for g in new_games:
            if g["game_id"] not in seen_ids:
                seen_ids.add(g["game_id"])
                deduped.append(g)
        new_games = deduped

        log.info(
            "New games to process for %d: %d of %d total",
            season, len(new_games), len(games)
        )

        game_rows    = []
        batter_rows  = []
        pitcher_rows = []
        batch_size   = 100

        for i, game in enumerate(new_games, 1):
            game_pk = game["game_id"]
            game_json = fetch_game_json(game_pk)
            if game_json is None:
                log.warning("Skipping game_pk %d: /withMetrics returned no data.", game_pk)
                time.sleep(0.25)
                continue

            away_id, home_id, batters, pitchers = parse_boxscore_from_json(game, game_json)
            game_rows.append(build_game_row(game, team_abbr, away_id, home_id))
            batter_rows.extend(batters)
            pitcher_rows.extend(pitchers)

            if i % batch_size == 0 or i == len(new_games):
                if game_rows:
                    upsert(engine, pd.DataFrame(game_rows).where(pd.notna(pd.DataFrame(game_rows)), other=None),
                           "mlb", "games", ["game_pk"])
                    game_rows = []
                if batter_rows:
                    df = pd.DataFrame(batter_rows)
                    upsert(engine, df.where(pd.notna(df), other=None),
                           "mlb", "batting_stats", ["batter_game_id"])
                    batter_rows = []
                if pitcher_rows:
                    df = pd.DataFrame(pitcher_rows)
                    upsert(engine, df.where(pd.notna(df), other=None),
                           "mlb", "pitching_stats", ["pitcher_game_id"])
                    pitcher_rows = []
                log.info(
                    "Flushed batch at game %d of %d for season %d",
                    i, len(new_games), season
                )

            time.sleep(0.25)

        log.info("Season %d complete", season)

# ---------------------------------------------------------------------------
# 6. Season snapshot: player_season_batting
# ---------------------------------------------------------------------------

def load_player_season_batting(engine, season):
    current_year = datetime.utcnow().year
    if season > current_year:
        log.info("Season %d has not started yet, skipping player season batting snapshot.", season)
        return

    log.info("Loading player season batting snapshot for %d", season)
    data = api_get("stats", {
        "stats":      "season",
        "group":      "hitting",
        "season":     season,
        "sportId":    1,
        "playerPool": "ALL",
        "limit":      2000,
        "offset":     0,
    })

    rows = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        s = split.get("stat", {})
        p = split.get("player", {})
        t = split.get("team", {})
        cs_pct_raw = s.get("caughtStealingPercentage")
        rows.append({
            "player_id":               p.get("id"),
            "player_name":             p.get("fullName"),
            "team_id":                 t.get("id"),
            "season_year":             season,
            "age":                     safe_int(s.get("age")),
            "games_played":            safe_int(s.get("gamesPlayed")),
            "at_bats":                 safe_int(s.get("atBats")),
            "plate_appearances":       safe_int(s.get("plateAppearances")),
            "hits":                    safe_int(s.get("hits")),
            "doubles":                 safe_int(s.get("doubles")),
            "triples":                 safe_int(s.get("triples")),
            "home_runs":               safe_int(s.get("homeRuns")),
            "runs":                    safe_int(s.get("runs")),
            "rbi":                     safe_int(s.get("rbi")),
            "walks":                   safe_int(s.get("baseOnBalls")),
            "intentional_walks":       safe_int(s.get("intentionalWalks")),
            "strikeouts":              safe_int(s.get("strikeOuts")),
            "hit_by_pitch":            safe_int(s.get("hitByPitch")),
            "stolen_bases":            safe_int(s.get("stolenBases")),
            "caught_stealing":         safe_int(s.get("caughtStealing")),
            "stolen_base_pct":         safe_float(s.get("stolenBasePercentage")),
            "caught_stealing_pct":     safe_float(cs_pct_raw) if cs_pct_raw not in (None, ".---") else None,
            "ground_into_double_play": safe_int(s.get("groundIntoDoublePlay")),
            "total_bases":             safe_int(s.get("totalBases")),
            "left_on_base":            safe_int(s.get("leftOnBase")),
            "sac_bunts":               safe_int(s.get("sacBunts")),
            "sac_flies":               safe_int(s.get("sacFlies")),
            "ground_outs":             safe_int(s.get("groundOuts")),
            "air_outs":                safe_int(s.get("airOuts")),
            "pitches_seen":            safe_int(s.get("numberOfPitches")),
            "batting_avg":             safe_float(s.get("avg")),
            "obp":                     safe_float(s.get("obp")),
            "slg":                     safe_float(s.get("slg")),
            "ops":                     safe_float(s.get("ops")),
            "babip":                   safe_float(s.get("babip")),
            "ground_outs_to_air_outs": safe_float(s.get("groundOutsToAirouts")),
            "at_bats_per_hr":          safe_float(s.get("atBatsPerHomeRun")),
            "catchers_interference":   safe_int(s.get("catchersInterference")),
        })

    df = pd.DataFrame(rows)
    if not validate_dataframe(df, ["player_id"], f"player_season_batting season={season}"):
        return

    df = df.dropna(subset=["player_id"])
    truncate_and_load(engine, df, "mlb", "player_season_batting")

# ---------------------------------------------------------------------------
# 7. Season snapshot: pitcher_season_stats
# ---------------------------------------------------------------------------

def load_pitcher_season_stats(engine, season):
    current_year = datetime.utcnow().year
    if season > current_year:
        log.info("Season %d has not started yet, skipping pitcher season stats snapshot.", season)
        return

    log.info("Loading pitcher season stats snapshot for %d", season)
    data = api_get("stats", {
        "stats":      "season",
        "group":      "pitching",
        "season":     season,
        "sportId":    1,
        "playerPool": "ALL",
        "limit":      2000,
        "offset":     0,
    })

    rows = []
    for split in data.get("stats", [{}])[0].get("splits", []):
        s = split.get("stat", {})
        p = split.get("player", {})
        t = split.get("team", {})
        cs_pct_raw = s.get("caughtStealingPercentage")
        rows.append({
            "player_id":                  p.get("id"),
            "player_name":                p.get("fullName"),
            "team_id":                    t.get("id"),
            "season_year":                season,
            "age":                        safe_int(s.get("age")),
            "games_played":               safe_int(s.get("gamesPlayed")),
            "games_started":              safe_int(s.get("gamesStarted")),
            "ground_outs":                safe_int(s.get("groundOuts")),
            "air_outs":                   safe_int(s.get("airOuts")),
            "runs_allowed":               safe_int(s.get("runs")),
            "doubles_allowed":            safe_int(s.get("doubles")),
            "triples_allowed":            safe_int(s.get("triples")),
            "hr_allowed":                 safe_int(s.get("homeRuns")),
            "strikeouts":                 safe_int(s.get("strikeOuts")),
            "walks":                      safe_int(s.get("baseOnBalls")),
            "intentional_walks":          safe_int(s.get("intentionalWalks")),
            "hits_allowed":               safe_int(s.get("hits")),
            "hit_by_pitch":               safe_int(s.get("hitByPitch")),
            "batting_avg_against":        safe_float(s.get("avg")),
            "at_bats_faced":              safe_int(s.get("atBats")),
            "obp_against":                safe_float(s.get("obp")),
            "slg_against":                safe_float(s.get("slg")),
            "ops_against":                safe_float(s.get("ops")),
            "caught_stealing":            safe_int(s.get("caughtStealing")),
            "stolen_bases_allowed":       safe_int(s.get("stolenBases")),
            "stolen_base_pct_against":    safe_float(s.get("stolenBasePercentage")),
            "caught_stealing_pct":        safe_float(cs_pct_raw) if cs_pct_raw not in (None, ".---") else None,
            "ground_into_double_play":    safe_int(s.get("groundIntoDoublePlay")),
            "total_pitches":              safe_int(s.get("numberOfPitches")),
            "era":                        safe_float(s.get("era")),
            "innings_pitched":            parse_innings_pitched(s.get("inningsPitched")),
            "wins":                       safe_int(s.get("wins")),
            "losses":                     safe_int(s.get("losses")),
            "saves":                      safe_int(s.get("saves")),
            "save_opportunities":         safe_int(s.get("saveOpportunities")),
            "holds":                      safe_int(s.get("holds")),
            "blown_saves":                safe_int(s.get("blownSaves")),
            "earned_runs":                safe_int(s.get("earnedRuns")),
            "whip":                       safe_float(s.get("whip")),
            "batters_faced":              safe_int(s.get("battersFaced")),
            "outs_recorded":              safe_int(s.get("outs")),
            "games_pitched":              safe_int(s.get("gamesPitched")),
            "complete_games":             safe_int(s.get("completeGames")),
            "shutouts":                   safe_int(s.get("shutouts")),
            "strikes_thrown":             safe_int(s.get("strikes")),
            "strike_pct":                 safe_float(s.get("strikePercentage")),
            "hit_batsmen":                safe_int(s.get("hitBatsmen")),
            "balks":                      safe_int(s.get("balks")),
            "wild_pitches":               safe_int(s.get("wildPitches")),
            "pickoffs":                   safe_int(s.get("pickoffs")),
            "total_bases_allowed":        safe_int(s.get("totalBases")),
            "ground_outs_to_air_outs":    safe_float(s.get("groundOutsToAirouts")),
            "win_pct":                    safe_float(s.get("winPercentage")),
            "pitches_per_inning":         safe_float(s.get("pitchesPerInning")),
            "games_finished":             safe_int(s.get("gamesFinished")),
            "strikeout_walk_ratio":       safe_float(s.get("strikeoutWalkRatio")),
            "k_per_9":                    safe_float(s.get("strikeoutsPer9Inn")),
            "bb_per_9":                   safe_float(s.get("walksPer9Inn")),
            "h_per_9":                    safe_float(s.get("hitsPer9Inn")),
            "runs_per_9":                 safe_float(s.get("runsScoredPer9")),
            "hr_per_9":                   safe_float(s.get("homeRunsPer9")),
            "inherited_runners":          safe_int(s.get("inheritedRunners")),
            "inherited_runners_scored":   safe_int(s.get("inheritedRunnersScored")),
            "catchers_interference":      safe_int(s.get("catchersInterference")),
            "sac_bunts":                  safe_int(s.get("sacBunts")),
            "sac_flies":                  safe_int(s.get("sacFlies")),
        })

    df = pd.DataFrame(rows)
    if not validate_dataframe(df, ["player_id"], f"pitcher_season_stats season={season}"):
        return

    df = df.dropna(subset=["player_id"])
    truncate_and_load(engine, df, "mlb", "pitcher_season_stats")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    log.info("=== MLB ETL started ===")
    engine = get_engine()
    current_season     = date.today().year
    historical_seasons = [2023, 2024, 2025, current_season]

    team_abbr = load_teams(engine, season=current_season)
    load_players(engine, seasons=historical_seasons)
    load_games_and_box_scores(engine, historical_seasons, team_abbr)
    load_player_season_batting(engine, season=current_season)
    load_pitcher_season_stats(engine, season=current_season)

    log.info("=== MLB ETL complete ===")


if __name__ == "__main__":
    main()
