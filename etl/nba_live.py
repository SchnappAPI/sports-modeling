"""
nba_live.py

Intra-day live box score updater for the sports modeling database.
Runs every 5 minutes during game hours via nba-live.yml.

Design
------
Gate:   Queries nba.schedule for games with game_status = 2 (in progress).
        Exits immediately if none found.
Fetch:  Calls BoxScoreTraditionalV3 for each in-progress game via proxy.
        This endpoint returns current cumulative stats for all periods
        that have completed or are in progress.
Write:  Upserts into nba.player_box_score_stats using the same schema
        as the nightly ETL. Existing rows are overwritten in place.
        The nightly ETL will do a final clean write once games complete.

Note on periods
---------------
BoxScoreTraditionalV3 returns one row per player per period played so far.
Period labels in the response are integers (1, 2, 3, 4, 5+).
We map: 1->1Q, 2->2Q, 3->3Q, 4->4Q, 5+->OT.
Only these five labels are stored; any OT period collapses into OT.

Proxy
-----
All stats.nba.com calls require the Webshare rotating residential proxy.
Secret: NBA_PROXY_URL.
"""

import os
import sys
import time
import math
import logging
from datetime import date

import pandas as pd
import requests
from sqlalchemy import text

# Re-use shared helpers from nba_etl.py
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

PERIOD_MAP = {1: "1Q", 2: "2Q", 3: "3Q", 4: "4Q"}


# ---------------------------------------------------------------------------
# Gate: find in-progress games
# ---------------------------------------------------------------------------

def get_live_game_ids(engine):
    """Return list of game_ids currently in progress (game_status = 2)."""
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


# ---------------------------------------------------------------------------
# Fetch BoxScoreTraditionalV3
# ---------------------------------------------------------------------------

def _request(url, params, label):
    proxies = get_proxies()
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers=NBA_HEADERS,
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


def fetch_live_box_score(game_id):
    """
    Fetch BoxScoreTraditionalV3 for a single game_id.
    Returns a list of row dicts ready for upsert, or empty list.
    """
    url = "https://stats.nba.com/stats/boxscoretraditionalv3"
    params = {
        "GameID":       game_id,
        "StartPeriod":  0,
        "EndPeriod":    0,
        "StartRange":   0,
        "EndRange":     0,
        "RangeType":    0,
    }
    data = _request(url, params, f"BoxScoreTraditionalV3 {game_id}")
    if data is None:
        return []

    try:
        game_data     = data["boxScoreTraditional"]
        home_team     = game_data["homeTeam"]
        away_team     = game_data["awayTeam"]
        game_date_raw = game_data.get("gameTimeLocal", "")[:10] or None
    except (KeyError, TypeError) as exc:
        log.warning(f"  {game_id}: unexpected response shape: {exc}")
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

            for period_stats in player_obj.get("statistics", []):
                period_num = safe_int(period_stats.get("period"))
                if period_num is None:
                    continue
                period_label = PERIOD_MAP.get(period_num, "OT")

                s = period_stats
                rows.append({
                    "game_id":        game_id,
                    "player_id":      pid,
                    "period":         period_label,
                    "season_year":    None,
                    "player_name":    pname,
                    "team_id":        team_id,
                    "team_tricode":   team_tricode,
                    "game_date":      safe_date(game_date_raw),
                    "matchup":        None,
                    "minutes":        _parse_minutes(safe_str(s.get("clock"))),
                    "minutes_sec":    safe_str(s.get("clock")),
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


def _parse_minutes(clock_str):
    if not clock_str:
        return None
    import re
    m = re.match(r'PT(\d+)M([\d.]+)S', clock_str)
    if not m:
        return None
    try:
        minutes = int(m.group(1))
        seconds = float(m.group(2))
        return round(minutes + seconds / 60, 4)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Update nba.schedule scores via ScoreboardV3
# ---------------------------------------------------------------------------

def update_live_schedule(engine, game_id):
    url    = "https://stats.nba.com/stats/scoreboardv3"
    params = {"GameDate": date.today().strftime("%m/%d/%Y"), "LeagueID": "00"}
    data   = _request(url, params, f"ScoreboardV3 {date.today()}")
    if data is None:
        return

    try:
        games = data["scoreboard"]["games"]
    except (KeyError, TypeError):
        return

    rows = []
    for g in games:
        gid = safe_str(g.get("gameId"))
        if gid is None:
            continue
        home = g.get("homeTeam", {})
        away = g.get("awayTeam", {})
        rows.append({
            "game_id":          gid,
            "game_status":      safe_int(g.get("gameStatus")),
            "game_status_text": safe_str(g.get("gameStatusText")),
            "home_score":       safe_int(home.get("score")),
            "away_score":       safe_int(away.get("score")),
        })

    if not rows:
        return

    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    "UPDATE nba.schedule "
                    "SET home_score = :hs, away_score = :as, "
                    "    game_status = :gs, game_status_text = :gst "
                    "WHERE game_id = :gid"
                ),
                {"hs": row["home_score"], "as": row["away_score"],
                 "gs": row["game_status"], "gst": row["game_status_text"],
                 "gid": row["game_id"]},
            )
    log.info(f"  Schedule scores updated for {len(rows)} game(s).")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    engine = get_engine()

    live_ids = get_live_game_ids(engine)
    if not live_ids:
        log.info("Gate: no in-progress games. Nothing to do.")
        return

    log.info(f"Gate: {len(live_ids)} in-progress game(s): {live_ids}")

    update_live_schedule(engine, live_ids[0])

    total_rows = 0
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
        total_rows += len(rows)

    log.info(f"Live update complete. {total_rows} total rows upserted.")


if __name__ == "__main__":
    main()
