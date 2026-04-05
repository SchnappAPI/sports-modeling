"""
nba_live.py

Intra-day NBA updater. Called by nba-game-day.yml on every cycle.

Two responsibilities:
  1. update_schedule()  -- Always runs. Fetches the public CDN scoreboard
                           to sync game_status / scores for ALL today's games.
                           This is what flips status 1->2->3 in the DB.
                           No proxy required -- CDN is public.

  2. update_box_scores() -- Gates on game_status=2 (in-progress).
                            Calls NBA CDN for each live game and logs player
                            counts. DB write is skipped -- the Flask runner
                            serves live box scores directly from CDN to the UI.

CDN endpoints (both public, no proxy):
  Scoreboard: https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json
  Box score:  https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json

Box score top-level key: "game"
statistics: single dict per player (cumulative game total)
"""

import os
import sys
import re
import time
import logging
from datetime import date

import requests
from sqlalchemy import text

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.nba_etl import (
    get_engine,
    safe_int,
    safe_str,
    API_DELAY,
    RETRY_COUNT,
    RETRY_WAIT,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CDN_SCOREBOARD = "https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json"


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _request(url, params, label, proxies=None, headers=None):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers=headers,
                params=params,
                proxies=proxies,
                timeout=60,
            )
            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}")
            time.sleep(API_DELAY)
            return resp.json()
        except Exception as exc:
            log.warning(f"  {label} attempt {attempt}/{RETRY_COUNT} failed: {exc}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_WAIT)
    log.error(f"  {label} failed after {RETRY_COUNT} attempts")
    return None


# ---------------------------------------------------------------------------
# Step 1: Update schedule status and scores (always runs)
# ---------------------------------------------------------------------------

def update_schedule(engine):
    """
    Fetch the public CDN scoreboard and update game_status, game_status_text,
    home_score, and away_score for every game in nba.schedule.

    Uses todaysScoreboard_00.json -- no proxy, no special headers required.
    Response shape: scoreboard.games[] with gameId, gameStatus, gameStatusText,
    homeTeam.score, awayTeam.score (identical field names to ScoreboardV3).
    """
    data = _request(CDN_SCOREBOARD, None, "CDN scoreboard")
    if data is None:
        log.warning("CDN scoreboard fetch failed -- schedule not updated this cycle.")
        return 0

    try:
        games = data["scoreboard"]["games"]
    except (KeyError, TypeError) as exc:
        log.warning(f"CDN scoreboard unexpected shape: {exc}")
        return 0

    today_str = date.today().isoformat()
    log.info(f"CDN scoreboard returned {len(games)} game(s) for {today_str}")

    updated = 0
    with engine.begin() as conn:
        for g in games:
            gid = safe_str(g.get("gameId"))
            if not gid:
                continue
            home = g.get("homeTeam", {})
            away = g.get("awayTeam", {})
            conn.execute(
                text(
                    "UPDATE nba.schedule "
                    "SET home_score = :hs, away_score = :as, "
                    "    game_status = :gs, game_status_text = :gst "
                    "WHERE game_id = :gid"
                ),
                {
                    "hs":  safe_int(home.get("score")),
                    "as":  safe_int(away.get("score")),
                    "gs":  safe_int(g.get("gameStatus")),
                    "gst": safe_str(g.get("gameStatusText")),
                    "gid": gid,
                },
            )
            updated += 1
    log.info(f"Schedule updated for {updated} game(s).")
    return updated


# ---------------------------------------------------------------------------
# Step 2: Verify live box score availability (gates on game_status = 2)
# ---------------------------------------------------------------------------

def get_live_game_ids(engine):
    today = date.today()
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT game_id FROM nba.schedule "
                "WHERE game_date = :today AND game_status = 2"
            ),
            {"today": today},
        ).fetchall()
    return [str(r[0]) for r in rows]


def verify_live_box_scores(engine):
    """
    Verify CDN box score availability for in-progress games.
    Does not write to DB -- the Flask runner serves live box scores
    directly from CDN to the UI on each request.
    """
    live_ids = get_live_game_ids(engine)
    if not live_ids:
        log.info("No in-progress games -- box score check skipped.")
        return 0

    log.info(f"{len(live_ids)} in-progress game(s): {live_ids}")
    total_players = 0
    for game_id in live_ids:
        url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        data = _request(url, None, f"CDN boxscore {game_id}")
        if data is None:
            log.warning(f"  {game_id}: CDN fetch failed.")
            continue
        try:
            game_data = data["game"]
            home_count = len(game_data["homeTeam"].get("players", []))
            away_count = len(game_data["awayTeam"].get("players", []))
            status     = game_data.get("gameStatusText", "")
            total_players += home_count + away_count
            log.info(f"  {game_id}: {home_count + away_count} players available ({status})")
        except (KeyError, TypeError) as exc:
            log.warning(f"  {game_id}: unexpected CDN response shape: {exc}")
    return total_players


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    engine = get_engine()

    # Step 1: always update schedule status/scores via CDN (no proxy needed)
    update_schedule(engine)

    # Step 2: verify live box score CDN availability (no DB write)
    verify_live_box_scores(engine)
    log.info("Live update complete.")


if __name__ == "__main__":
    main()
