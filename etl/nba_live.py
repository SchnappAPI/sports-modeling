"""
nba_live.py

Intra-day NBA updater. Called by nba-game-day.yml on every cycle.

Two responsibilities:
  1. update_schedule()  -- Always runs. Calls ScoreboardV3 to sync
                           game_status / scores for ALL today's games.
                           This is what flips status 1->2->3 in the DB.

  2. update_box_scores() -- Gates on game_status=2 (in-progress).
                            Calls NBA CDN for each live game and upserts
                            cumulative stats (one row per player, period='G').

CDN endpoint (public, no proxy):
  https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json
  Top-level key: "game" (not "boxScoreTraditional")
  statistics: single dict per player (cumulative game total)

period='G' is used for cumulative live rows to fit the VARCHAR(2) column.
Quarter rows from backfill use '1Q','2Q','3Q','4Q','OT'.

Proxy
-----
ScoreboardV3 still requires the Webshare rotating residential proxy.
CDN boxscore endpoint is public and requires no proxy.
"""

import os
import sys
import re
import time
import logging
from datetime import date

import pandas as pd
import requests
from sqlalchemy import text

from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.nba_etl import (
    get_engine,
    upsert,
    safe_int,
    safe_float,
    safe_str,
    safe_date,
    NBA_HEADERS,
    get_proxies,
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
    Call ScoreboardV3 for today and update game_status, game_status_text,
    home_score, and away_score for every game in nba.schedule.
    Runs unconditionally — this is what drives status transitions.
    """
    today  = date.today()
    url    = "https://stats.nba.com/stats/scoreboardv3"
    params = {"GameDate": today.strftime("%m/%d/%Y"), "LeagueID": "00"}
    data   = _request(url, params, f"ScoreboardV3 {today}",
                      proxies=get_proxies(), headers=NBA_HEADERS)
    if data is None:
        log.warning("ScoreboardV3 call failed — schedule not updated this cycle.")
        return 0

    try:
        games = data["scoreboard"]["games"]
    except (KeyError, TypeError) as exc:
        log.warning(f"ScoreboardV3 unexpected shape: {exc}")
        return 0

    updated = 0
    with engine.begin() as conn:
        for g in games:
            gid  = safe_str(g.get("gameId"))
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
# Step 2: Live box score (gates on game_status = 2)
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


def _parse_minutes(clock_str):
    if not clock_str:
        return None
    m = re.match(r"PT(\d+)M([\d.]+)S", clock_str)
    if not m:
        return None
    try:
        return round(int(m.group(1)) + float(m.group(2)) / 60, 4)
    except (ValueError, TypeError):
        return None


def fetch_live_box_score(game_id):
    """
    Fetch cumulative live box score from NBA CDN (public, no proxy needed).
    Returns one row per player with period='G' for live upserts.
    period='G' fits the VARCHAR(2) column sized for '1Q','2Q','3Q','4Q','OT'.
    """
    url  = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    data = _request(url, None, f"CDN boxscore {game_id}")
    if data is None:
        return []

    try:
        game_data     = data["game"]
        home_team     = game_data["homeTeam"]
        away_team     = game_data["awayTeam"]
        game_date_raw = game_data.get("gameTimeLocal", "")[:10] or None
    except (KeyError, TypeError) as exc:
        log.warning(f"  {game_id}: unexpected CDN response shape: {exc}")
        return []

    rows = []
    for team_obj in (home_team, away_team):
        team_id      = safe_int(team_obj.get("teamId"))
        team_tricode = safe_str(team_obj.get("teamTricode"))
        for player_obj in team_obj.get("players", []):
            pid   = safe_int(player_obj.get("personId"))
            pname = safe_str(player_obj.get("name"))
            if pid is None:
                continue
            # statistics is a single cumulative dict on CDN (not a list)
            s = player_obj.get("statistics", {})
            if not isinstance(s, dict):
                s = {}
            rows.append({
                "game_id":        game_id,
                "player_id":      pid,
                "period":         "G",      # cumulative live row, fits VARCHAR(2)
                "season_year":    None,
                "player_name":    pname,
                "team_id":        team_id,
                "team_tricode":   team_tricode,
                "game_date":      safe_date(game_date_raw),
                "matchup":        None,
                "minutes":        _parse_minutes(safe_str(s.get("minutes"))),
                "minutes_sec":    safe_str(s.get("minutes")),
                "fgm":            safe_int(s.get("fieldGoalsMade")),
                "fga":            safe_int(s.get("fieldGoalsAttempted")),
                "fg_pct":         safe_float(s.get("fieldGoalsPercentage")),
                "fg3m":           safe_int(s.get("threePointersMade")),
                "fg3a":           safe_int(s.get("threePointersAttempted")),
                "fg3_pct":        safe_float(s.get("threePointersPercentage")),
                "ftm":            safe_int(s.get("freeThrowsMade")),
                "fta":            safe_int(s.get("freeThrowsAttempted")),
                "ft_pct":         safe_float(s.get("freeThrowsPercentage")),
                "oreb":           safe_int(s.get("reboundsOffensive")),
                "dreb":           safe_int(s.get("reboundsDefensive")),
                "reb":            safe_int(s.get("reboundsTotal")),
                "ast":            safe_int(s.get("assists")),
                "tov":            safe_int(s.get("turnovers")),
                "stl":            safe_int(s.get("steals")),
                "blk":            safe_int(s.get("blocks")),
                "blka":           safe_int(s.get("blocksReceived")),
                "pf":             safe_int(s.get("foulsPersonal")),
                "pfd":            safe_int(s.get("foulsDrawn")),
                "pts":            safe_int(s.get("points")),
                "plus_minus":     safe_int(s.get("plusMinusPoints")),
                "dd2":            None,
                "td3":            None,
                "available_flag": None,
            })
    return rows


def update_box_scores(engine):
    live_ids = get_live_game_ids(engine)
    if not live_ids:
        log.info("No in-progress games — box score update skipped.")
        return 0

    log.info(f"{len(live_ids)} in-progress game(s): {live_ids}")
    total = 0
    for game_id in live_ids:
        rows = fetch_live_box_score(game_id)
        if not rows:
            log.warning(f"  {game_id}: no rows returned.")
            continue
        upsert(
            pd.DataFrame(rows), engine,
            "nba", "player_box_score_stats",
            ["game_id", "player_id", "period"],
        )
        log.info(f"  {game_id}: {len(rows)} rows upserted.")
        total += len(rows)
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    engine = get_engine()

    # Step 1: always update schedule status/scores
    update_schedule(engine)

    # Step 2: update live box scores if any games are in progress
    total = update_box_scores(engine)
    log.info(f"Live update complete. {total} box score rows upserted.")


if __name__ == "__main__":
    main()
