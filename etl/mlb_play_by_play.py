"""
mlb_play_by_play.py

Loads pitch-level play-by-play data for MLB games into mlb.play_by_play.

Source: https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics

One row per play event (pitch, pickoff attempt, stolen base, etc.) per game.
The composite key is play_event_id = '{game_pk}-{at_bat_number}-{play_event_index}'.

Incremental logic:
  - Desired game_pk set: all Final regular season games for the configured seasons
    pulled from the mlb.games table already loaded by mlb_etl.py.
  - Existing game_pk set: SELECT DISTINCT game_pk FROM mlb.play_by_play.
  - Delta = desired minus existing.
  - Oldest --batch games processed per run to stay within GitHub Actions time limits.

Backfill scope:
  Currently configured for 2025 only. Extend SEASONS list to go further back.

Runs exclusively in GitHub Actions. Credentials injected as environment variables.
"""

import os
import sys
import time
import logging
from datetime import date

import requests
import pandas as pd
from sqlalchemy import text

# ---------------------------------------------------------------------------
# Shared db module (same engine/upsert used by mlb_etl.py)
# ---------------------------------------------------------------------------

from pathlib import Path
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
# Config
# ---------------------------------------------------------------------------

SEASONS = [2025]   # Extend to [2023, 2024, 2025] for full historical backfill
DEFAULT_BATCH = 50  # games per run; raise for backfill, lower for nightly
API_PAUSE = 0.25    # seconds between game fetches
API_BASE  = "https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics"

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'play_by_play'
)
CREATE TABLE mlb.play_by_play (
    -- Primary key
    play_event_id              VARCHAR(50)   NOT NULL PRIMARY KEY,

    -- Game context
    game_pk                    INT           NOT NULL,
    game_date                  DATE          NULL,
    at_bat_number              INT           NULL,
    play_event_index           INT           NULL,
    inning                     INT           NULL,
    is_top_inning              BIT           NULL,

    -- Team context
    team_id                    INT           NULL,
    vs_team_id                 INT           NULL,
    away_team_id               INT           NULL,
    home_team_id               INT           NULL,
    venue_id                   INT           NULL,

    -- At-bat result (populated only on the final pitch of each at-bat)
    result_event_type          VARCHAR(50)   NULL,
    result_description         VARCHAR(500)  NULL,
    result_rbi                 INT           NULL,
    result_is_out              BIT           NULL,
    at_bat_is_complete         BIT           NULL,
    at_bat_is_scoring_play     BIT           NULL,
    at_bat_has_out             BIT           NULL,
    at_bat_end_time            DATETIME2     NULL,
    play_end_time              DATETIME2     NULL,

    -- Batter info
    batter_id                  INT           NULL,
    batter_hand_code           CHAR(1)       NULL,
    batter_split               VARCHAR(30)   NULL,

    -- Pitcher info
    pitcher_id                 INT           NULL,
    pitcher_hand_code          CHAR(1)       NULL,
    pitcher_split              VARCHAR(30)   NULL,

    -- Play event details
    play_id                    VARCHAR(50)   NULL,
    play_event_type            VARCHAR(30)   NULL,
    is_pitch                   BIT           NULL,
    is_base_running_play       BIT           NULL,
    pitch_number               INT           NULL,
    pitch_call_code            VARCHAR(5)    NULL,
    pitch_type_code            VARCHAR(5)    NULL,
    play_event_description     VARCHAR(500)  NULL,
    is_hit_into_play           BIT           NULL,
    is_strike                  BIT           NULL,
    is_ball                    BIT           NULL,
    is_out                     BIT           NULL,
    runner_going               BIT           NULL,
    count_balls_strikes        VARCHAR(5)    NULL,
    count_outs                 INT           NULL,
    is_last_pitch              BIT           NULL,
    is_at_bat                  BIT           NULL,
    is_plate_appearance        BIT           NULL,
    play_event_end_time        DATETIME2     NULL,

    -- Pitch metrics
    pitch_start_speed          DECIMAL(5,1)  NULL,
    pitch_end_speed            DECIMAL(5,1)  NULL,
    pitch_zone                 INT           NULL,
    strike_zone_top            DECIMAL(5,2)  NULL,
    strike_zone_bottom         DECIMAL(5,2)  NULL,

    -- Hit data
    hit_launch_speed           DECIMAL(5,1)  NULL,
    hit_launch_angle           INT           NULL,
    hit_total_distance         INT           NULL,
    hit_trajectory             VARCHAR(30)   NULL,
    hit_hardness               VARCHAR(20)   NULL,
    hit_location               INT           NULL,
    hit_probability            DECIMAL(5,2)  NULL,
    hit_bat_speed              DECIMAL(5,1)  NULL,
    home_run_ballparks         INT           NULL,

    created_at                 DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
"""


def ensure_table(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL))
    log.info("mlb.play_by_play table ensured.")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def safe_int(val):
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def safe_float(val):
    try:
        s = str(val).strip()
        return float(s) if s not in ("", "None") else None
    except (ValueError, TypeError):
        return None


def safe_bool(val):
    if val is None:
        return None
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, int):
        return 1 if val else 0
    if isinstance(val, str):
        return 1 if val.lower() in ("true", "1", "yes") else 0
    return None


def fetch_game_json(game_pk, retries=3, pause=5):
    url = API_BASE.format(game_pk=game_pk)
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Fetch failed for game_pk %d (attempt %d/%d): %s", game_pk, attempt, retries, exc)
            if attempt < retries:
                time.sleep(pause)
    return None


# ---------------------------------------------------------------------------
# Parse a single game JSON into play_by_play rows
# ---------------------------------------------------------------------------

def parse_play_by_play(game_json, game_pk, game_date):
    """
    Translate the allPlays list from the /withMetrics response into a flat
    list of dicts matching the mlb.play_by_play schema.

    Mirrors the logic in fnGetPlayByPlay.pq:
      - One row per play event (pitch or action).
      - At-bat result fields (result_*, at_bat_*, play_end_time) are only
        populated on the final play event of each at-bat.
      - is_at_bat and is_plate_appearance only populated on the final event.
      - Events with no play_id are excluded (incomplete/in-progress events).
    """
    try:
        all_plays = game_json["liveData"]["plays"]["allPlays"]
    except (KeyError, TypeError):
        return []

    game_data  = game_json.get("gameData", {})
    away_id    = game_data.get("teams", {}).get("away", {}).get("id")
    home_id    = game_data.get("teams", {}).get("home", {}).get("id")
    venue_id   = game_data.get("venue", {}).get("id")

    rows = []

    for play in all_plays:
        about    = play.get("about", {})
        matchup  = play.get("matchup", {})
        result   = play.get("result", {})
        credits  = [c.get("credit") for c in play.get("credits", [])]

        is_top       = about.get("isTopInning")
        at_bat_num   = safe_int(about.get("atBatIndex", -1)) + 1 if about.get("atBatIndex") is not None else None
        batter_id    = matchup.get("batter", {}).get("id")
        pitcher_id   = matchup.get("pitcher", {}).get("id")

        team_id    = away_id if is_top else home_id
        vs_team_id = home_id if is_top else away_id

        is_ab  = 1 if "b_ab" in credits else 0
        is_pa  = 1 if "b_pa" in credits else 0

        # At-bat level fields only written on the last pitch of the at-bat
        play_events = play.get("playEvents", [])
        max_index   = max((e.get("index", -1) for e in play_events), default=-1)

        for event in play_events:
            play_id = event.get("playId")
            if play_id is None:
                continue  # incomplete/in-progress events have no playId

            ev_index  = event.get("index")
            is_last   = (ev_index == max_index)

            details    = event.get("details", {})
            pitch_data = event.get("pitchData", {})
            hit_data   = event.get("hitData", {})
            ctx        = event.get("contextMetrics", {})
            count      = event.get("count", {})

            play_event_id = f"{game_pk}-{at_bat_num}-{ev_index}"

            row = {
                "play_event_id":          play_event_id,
                "game_pk":                game_pk,
                "game_date":              str(game_date) if game_date else None,
                "at_bat_number":          at_bat_num,
                "play_event_index":       ev_index,
                "inning":                 safe_int(about.get("inning")),
                "is_top_inning":          safe_bool(is_top),
                "team_id":                team_id,
                "vs_team_id":             vs_team_id,
                "away_team_id":           away_id,
                "home_team_id":           home_id,
                "venue_id":               venue_id,

                # At-bat result: only on final pitch
                "result_event_type":      result.get("eventType")       if is_last else None,
                "result_description":     result.get("description")     if is_last else None,
                "result_rbi":             safe_int(result.get("rbi"))   if is_last else None,
                "result_is_out":          safe_bool(result.get("isOut")) if is_last else None,
                "at_bat_is_complete":     safe_bool(about.get("isComplete"))    if is_last else None,
                "at_bat_is_scoring_play": safe_bool(about.get("isScoringPlay")) if is_last else None,
                "at_bat_has_out":         safe_bool(about.get("hasOut"))         if is_last else None,
                "at_bat_end_time":        about.get("endTime")          if is_last else None,
                "play_end_time":          play.get("playEndTime")       if is_last else None,
                "is_at_bat":              is_ab if is_last else None,
                "is_plate_appearance":    is_pa if is_last else None,

                # Batter / pitcher
                "batter_id":              batter_id,
                "batter_hand_code":       matchup.get("batSide", {}).get("code"),
                "batter_split":           matchup.get("splits", {}).get("batter"),
                "pitcher_id":             pitcher_id,
                "pitcher_hand_code":      matchup.get("pitchHand", {}).get("code"),
                "pitcher_split":          matchup.get("splits", {}).get("pitcher"),

                # Play event details
                "play_id":                play_id,
                "play_event_type":        event.get("type"),
                "is_pitch":               safe_bool(event.get("isPitch")),
                "is_base_running_play":   safe_bool(event.get("isBaseRunningPlay")),
                "pitch_number":           safe_int(event.get("pitchNumber")),
                "pitch_call_code":        details.get("call", {}).get("code") if isinstance(details.get("call"), dict) else None,
                "pitch_type_code":        details.get("type", {}).get("code") if isinstance(details.get("type"), dict) else None,
                "play_event_description": details.get("description"),
                "is_hit_into_play":       safe_bool(details.get("isInPlay")),
                "is_strike":              safe_bool(details.get("isStrike")),
                "is_ball":                safe_bool(details.get("isBall")),
                "is_out":                 safe_bool(details.get("isOut")),
                "runner_going":           safe_bool(details.get("runnerGoing")),
                "count_balls_strikes":    f"{count.get('balls', '')}-{count.get('strikes', '')}" if count else None,
                "count_outs":             safe_int(count.get("outs")),
                "is_last_pitch":          safe_bool(is_last),
                "play_event_end_time":    event.get("endTime"),

                # Pitch metrics
                "pitch_start_speed":      safe_float(pitch_data.get("startSpeed")),
                "pitch_end_speed":        safe_float(pitch_data.get("endSpeed")),
                "pitch_zone":             safe_int(pitch_data.get("zone")),
                "strike_zone_top":        safe_float(pitch_data.get("strikeZoneTop")),
                "strike_zone_bottom":     safe_float(pitch_data.get("strikeZoneBottom")),

                # Hit data
                "hit_launch_speed":       safe_float(hit_data.get("launchSpeed")),
                "hit_launch_angle":       safe_int(hit_data.get("launchAngle")),
                "hit_total_distance":     safe_int(hit_data.get("totalDistance")),
                "hit_trajectory":         hit_data.get("trajectory"),
                "hit_hardness":           hit_data.get("hardness"),
                "hit_location":           safe_int(hit_data.get("location")),
                "hit_probability":        safe_float(hit_data.get("hitProbability")),
                "hit_bat_speed":          safe_float(hit_data.get("batSpeed")),
                "home_run_ballparks":     safe_int(ctx.get("homeRunBallparks")),
            }
            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Main load function
# ---------------------------------------------------------------------------

def load_play_by_play(engine, seasons, batch_size):
    """
    Fetch the list of Final regular season games from mlb.games for the given
    seasons, diff against existing play_by_play keys, then process the oldest
    batch_size games not yet loaded.
    """
    # Build desired set from mlb.games (already loaded by mlb_etl.py)
    season_list = ", ".join(str(s) for s in seasons)
    with engine.connect() as conn:
        desired = [
            (row[0], row[1]) for row in conn.execute(text(
                f"""
                SELECT game_pk, game_date
                FROM mlb.games
                WHERE game_status = 'F'
                  AND game_type = 'R'
                  AND YEAR(game_date) IN ({season_list})
                ORDER BY game_date ASC
                """
            )).fetchall()
        ]

    if not desired:
        log.info("No Final regular season games found in mlb.games for seasons %s.", seasons)
        return

    # Existing keys
    with engine.connect() as conn:
        existing = {
            row[0] for row in conn.execute(
                text("SELECT DISTINCT game_pk FROM mlb.play_by_play")
            ).fetchall()
        }

    new_games = [(pk, gd) for pk, gd in desired if pk not in existing]
    log.info(
        "play_by_play: %d desired, %d existing, %d new. Processing oldest %d.",
        len(desired), len(existing), len(new_games), min(batch_size, len(new_games))
    )

    if not new_games:
        log.info("No new games to process. Done.")
        return

    work = new_games[:batch_size]
    flush_rows = []
    flush_every = 25  # write to DB every N games

    for i, (game_pk, game_date) in enumerate(work, 1):
        game_json = fetch_game_json(game_pk)
        if game_json is None:
            log.warning("Skipping game_pk %d: no data returned.", game_pk)
            time.sleep(API_PAUSE)
            continue

        rows = parse_play_by_play(game_json, game_pk, game_date)
        if not rows:
            log.warning("game_pk %d: no play events parsed (postponed or no data).", game_pk)
            time.sleep(API_PAUSE)
            continue

        flush_rows.extend(rows)
        log.info("game_pk %d: %d events parsed (%d/%d).", game_pk, len(rows), i, len(work))

        if i % flush_every == 0 or i == len(work):
            df = pd.DataFrame(flush_rows)
            df = df.where(pd.notna(df), other=None)
            upsert(engine, df, schema="mlb", table="play_by_play", keys=["play_event_id"])
            log.info("Flushed %d rows after game %d of %d.", len(flush_rows), i, len(work))
            flush_rows = []

        time.sleep(API_PAUSE)

    log.info("play_by_play load complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch",   type=int, default=DEFAULT_BATCH,
                        help="Max games to process per run (default: 50).")
    parser.add_argument("--seasons", type=int, nargs="+", default=None,
                        help="Override season list. E.g. --seasons 2023 2024 2025.")
    args = parser.parse_args()

    seasons = args.seasons or SEASONS
    log.info("=== MLB Play-by-Play ETL started ===")
    log.info("Seasons: %s  Batch: %d", seasons, args.batch)

    engine = get_engine()
    ensure_table(engine)
    load_play_by_play(engine, seasons, args.batch)

    log.info("=== MLB Play-by-Play ETL complete ===")


if __name__ == "__main__":
    main()
