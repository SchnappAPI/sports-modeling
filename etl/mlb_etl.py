"""
mlb_etl.py

Loads all MLB data from the mlb-statsapi library into Azure SQL in a single pass.

Load order (respects foreign key dependencies):
    1. mlb.teams                  - Team reference. Truncate and reload each run.
    2. mlb.players                - Player reference. Truncate and reload each run.
    3. mlb.games                  - One row per game. Upsert on game_pk.
    4. mlb.batting_stats          - Per-batter per-game box score. Upsert on batter_game_id.
    5. mlb.pitching_stats         - Per-pitcher per-game box score. Upsert on pitcher_game_id.
    6. mlb.player_season_batting  - Season cumulative batting snapshot. Truncate and reload.
    7. mlb.pitcher_season_stats   - Season cumulative pitching snapshot. Truncate and reload.

Teams and players are always fully rebuilt from the API. Foreign key constraints on
the child tables (games, batting_stats, pitching_stats, season snapshots) have been
dropped via drop_mlb_fk_constraints.sql, so teams and players can be reloaded without
clearing box score history.

Box score tables use upsert logic. Games already present in batting_stats are skipped,
so only new games are fetched from the API on each run.

Historical box score load: 2023, 2024, and current season.
Season snapshot tables always reflect the current season only.

Runs exclusively in GitHub Actions. Never run on a local machine.
Credentials are injected as environment variables from GitHub Secrets.
"""

import os
import time
import logging
from datetime import date, datetime

import pandas as pd
import statsapi
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
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}?driver={driver}"
    )
    return create_engine(conn_str)

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
    """
    Return True if df is non-empty and contains all required columns.
    Logs a descriptive warning and returns False otherwise.
    Called before any dropna or load operation.
    """
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


def upsert(engine, df, schema, table, key_cols):
    """
    Upsert rows from df into schema.table.
    Rows matching key_cols are updated. New rows are inserted.
    Uses a named staging table in dbo that is dropped after the MERGE.
    """
    if df.empty:
        log.info("Upsert skipped: empty dataframe for %s.%s", schema, table)
        return

    staging_name = f"stage_{table}"
    full_table   = f"[{schema}].[{table}]"

    df.to_sql(
        staging_name,
        engine,
        schema="dbo",
        if_exists="replace",
        index=False,
        chunksize=200,
    )

    non_key_cols = [c for c in df.columns if c not in key_cols and c != "created_at"]
    key_join     = " AND ".join(f"t.[{c}] = s.[{c}]" for c in key_cols)
    update_set   = ", ".join(f"t.[{c}] = s.[{c}]" for c in non_key_cols)
    insert_cols  = ", ".join(f"[{c}]" for c in df.columns if c != "created_at")
    insert_vals  = ", ".join(f"s.[{c}]" for c in df.columns if c != "created_at")

    merge_sql = f"""
        MERGE {full_table} AS t
        USING [dbo].[{staging_name}] AS s
            ON {key_join}
        WHEN MATCHED THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({insert_cols})
            VALUES ({insert_vals});
    """

    with engine.begin() as conn:
        conn.execute(text(merge_sql))
        conn.execute(text(f"DROP TABLE IF EXISTS [dbo].[{staging_name}]"))

    log.info("Upserted %d rows into %s.%s", len(df), schema, table)


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
# 3 + 4 + 5. Games, batting_stats, pitching_stats in a single pass per season
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


def parse_boxscore(game, boxscore):
    """
    Extract pitcher IDs, batter rows, and pitcher rows from a boxscore_data result.
    Starting pitcher IDs come from the first non-header row in each pitcher list.
    Returns (away_starter_id, home_starter_id, batter_rows, pitcher_rows).
    """
    game_pk   = game["game_id"]
    game_date = game["game_date"]

    batter_rows  = []
    pitcher_rows = []

    away_starter_id = None
    home_starter_id = None

    for side_key, is_away in [("awayPitchers", True), ("homePitchers", False)]:
        for p in boxscore.get(side_key, []):
            if p.get("personId", 0) != 0:
                if is_away:
                    away_starter_id = p["personId"]
                else:
                    home_starter_id = p["personId"]
                break

    for side_key, team_id_key, side_label in [
        ("awayPitchers", "away_id", "A"),
        ("homePitchers", "home_id", "H"),
    ]:
        team_id = game[team_id_key]
        for p in boxscore.get(side_key, []):
            if p.get("personId", 0) == 0:
                continue
            pitcher_rows.append({
                "pitcher_game_id": f"{p['personId']}-{game_pk}",
                "game_pk":         game_pk,
                "game_date":       game_date,
                "player_id":       p["personId"],
                "team_id":         team_id,
                "side":            side_label,
                "innings_pitched": parse_innings_pitched(p.get("ip")),
                "hits_allowed":    safe_int(p.get("h")),
                "runs_allowed":    safe_int(p.get("r")),
                "earned_runs":     safe_int(p.get("er")),
                "walks":           safe_int(p.get("bb")),
                "strikeouts":      safe_int(p.get("k")),
                "hr_allowed":      safe_int(p.get("hr")),
                "era":             safe_float(p.get("era")),
                "pitches":         safe_int(p.get("p")),
                "strikes":         safe_int(p.get("s")),
                "note":            p.get("note", "").strip() or None,
            })

    for side_key, team_id_key, side_label in [
        ("awayBatters", "away_id", "A"),
        ("homeBatters", "home_id", "H"),
    ]:
        team_id = game[team_id_key]
        for b in boxscore.get(side_key, []):
            if b.get("personId", 0) == 0:
                continue
            batter_rows.append({
                "batter_game_id":  f"{b['personId']}-{game_pk}-{team_id}",
                "game_pk":         game_pk,
                "game_date":       game_date,
                "player_id":       b["personId"],
                "team_id":         team_id,
                "side":            side_label,
                "position":        b.get("position"),
                "batting_order":   safe_int(b.get("battingOrder")),
                "at_bats":         safe_int(b.get("ab")),
                "runs":            safe_int(b.get("r")),
                "hits":            safe_int(b.get("h")),
                "doubles":         safe_int(b.get("doubles")),
                "triples":         safe_int(b.get("triples")),
                "home_runs":       safe_int(b.get("hr")),
                "rbi":             safe_int(b.get("rbi")),
                "stolen_bases":    safe_int(b.get("sb")),
                "walks":           safe_int(b.get("bb")),
                "strikeouts":      safe_int(b.get("k")),
                "left_on_base":    safe_int(b.get("lob")),
                "batting_avg":     safe_float(b.get("avg")),
                "obp":             safe_float(b.get("obp")),
                "slg":             safe_float(b.get("slg")),
                "ops":             safe_float(b.get("ops")),
            })

    return away_starter_id, home_starter_id, batter_rows, pitcher_rows


def load_games_and_box_scores(engine, seasons, team_abbr):
    """
    For each season, fetch the schedule and boxscore for every Final regular season game.
    In a single pass per game, upsert into mlb.games, mlb.batting_stats, and mlb.pitching_stats.
    Games already present in mlb.batting_stats are skipped to avoid redundant API calls.
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

        # Deduplicate: API can return the same game_pk in multiple month chunks
        seen_ids  = set()
        deduped   = []
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
            try:
                boxscore = statsapi.boxscore_data(game_pk)
                away_id, home_id, batters, pitchers = parse_boxscore(game, boxscore)
                game_rows.append(build_game_row(game, team_abbr, away_id, home_id))
                batter_rows.extend(batters)
                pitcher_rows.extend(pitchers)
            except Exception as exc:
                log.warning("Boxscore fetch failed for game_pk %d: %s", game_pk, exc)

            if i % batch_size == 0 or i == len(new_games):
                if game_rows:
                    upsert(engine, pd.DataFrame(game_rows), "mlb", "games", ["game_pk"])
                    game_rows = []
                if batter_rows:
                    upsert(engine, pd.DataFrame(batter_rows), "mlb", "batting_stats", ["batter_game_id"])
                    batter_rows = []
                if pitcher_rows:
                    upsert(engine, pd.DataFrame(pitcher_rows), "mlb", "pitching_stats", ["pitcher_game_id"])
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

    # Steps 1 and 2: rebuild reference tables fresh every run.
    # FK constraints on child tables have been dropped via drop_mlb_fk_constraints.sql,
    # so reloading teams and players does not affect box score history.
    team_abbr = load_teams(engine, season=current_season)
    load_players(engine, seasons=historical_seasons)

    # Steps 3, 4, 5: upsert only new games. Already-loaded game_pks are skipped.
    load_games_and_box_scores(engine, historical_seasons, team_abbr)

    # Steps 6 and 7: season snapshots for current season only.
    load_player_season_batting(engine, season=current_season)
    load_pitcher_season_stats(engine, season=current_season)

    log.info("=== MLB ETL complete ===")


if __name__ == "__main__":
    main()
