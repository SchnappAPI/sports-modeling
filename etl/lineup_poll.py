"""
lineup_poll.py

Fetches today's NBA lineups and upserts them into nba.daily_lineups.
Runs every 15 minutes during the game window via lineup-poll.yml.

Two-stage lineup strategy
--------------------------
Stage 1 (Confirmed): Fetch the NBA's official daily lineups JSON.
  URL: stats.nba.com/js/data/leaders/00_daily_lineups_{YYYYMMDD}.json
  This file is published 30-60 minutes before tip with confirmed starters.
  Players written with lineup_status = 'Confirmed'.

Stage 2 (Projected): For any qualifying game that returned NO rows from
  stage 1, call boxscorepreviewv3 per game and parse the predicted lineup.
  Players written with lineup_status = 'Projected'.
  This gives the Roster tab useful data hours before confirmed lineups drop.

Confirmed always beats projected: the upsert PK is (game_id, team_tricode,
player_name). Once confirmed rows are written in stage 1, a subsequent stage 2
call for the same game_id will be skipped entirely (game excluded from stage 2
because it had rows in stage 1). So projected rows are never written for a
game that already has confirmed data.

Secrets required
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import sys
import time
import logging
from datetime import date, datetime, timezone, timedelta
import re

import pandas as pd
from sqlalchemy import text
from pathlib import Path

# ---------------------------------------------------------------------------
# Import shared infrastructure from nba_etl.
# nba_etl guards main() under if __name__ == "__main__" so importing it
# does not trigger argparse or any side effects.
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.nba_etl import (
    get_engine,
    upsert,
    safe_int,
    safe_str,
    NBA_HEADERS,
    get_proxies,
    API_DELAY,
    RETRY_COUNT,
    RETRY_WAIT,
)

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PROXY_URL = __import__('os').environ.get("NBA_PROXY_URL")


# ---------------------------------------------------------------------------
# Game start time parsing
# ---------------------------------------------------------------------------
_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(am|pm)", re.IGNORECASE)

def parse_game_start_utc(game_status_text):
    """
    Parse a game_status_text like '7:30 pm ET' into a UTC datetime for today.
    Returns None if parsing fails.
    """
    if not game_status_text:
        return None
    m = _TIME_RE.search(game_status_text)
    if not m:
        return None
    try:
        hour   = int(m.group(1))
        minute = int(m.group(2))
        ampm   = m.group(3).lower()
        if ampm == "pm" and hour != 12:
            hour += 12
        elif ampm == "am" and hour == 12:
            hour = 0
        today_et   = date.today()
        et_offset  = timedelta(hours=-4)  # EDT; acceptable error during NBA season
        et_tz      = timezone(et_offset)
        start_et   = datetime(today_et.year, today_et.month, today_et.day,
                              hour, minute, tzinfo=et_tz)
        return start_et.astimezone(timezone.utc)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Schedule query
# ---------------------------------------------------------------------------
def get_todays_nonfinal_games(engine, hours_ahead):
    today   = date.today()
    now_utc = datetime.now(timezone.utc)
    cutoff  = now_utc + timedelta(hours=hours_ahead)

    with engine.connect() as conn:
        rows = [
            dict(row._mapping)
            for row in conn.execute(
                text(
                    "SELECT game_id, game_date, game_status, game_status_text, "
                    "home_team_tricode, away_team_tricode "
                    "FROM nba.schedule "
                    "WHERE game_date = :today AND (game_status IS NULL OR game_status != 3)"
                ),
                {"today": today},
            )
        ]

    qualified = []
    for r in rows:
        start_utc = parse_game_start_utc(r.get("game_status_text"))
        if start_utc is None:
            log.info(f"  Game {r['game_id']}: start time unparseable, including conservatively.")
            qualified.append(r)
        elif start_utc <= cutoff:
            log.info(
                f"  Game {r['game_id']} ({r.get('away_team_tricode')} @ {r.get('home_team_tricode')}): "
                f"starts {start_utc.strftime('%H:%M UTC')}, within window."
            )
            qualified.append(r)
        else:
            log.info(
                f"  Game {r['game_id']}: starts {start_utc.strftime('%H:%M UTC')}, outside window."
            )

    return qualified


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------
def _direct_get(url, label, timeout=30):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers=NBA_HEADERS,
                proxies=get_proxies(),
                timeout=timeout,
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
# Stage 1: Confirmed lineups from official daily lineups JSON
# ---------------------------------------------------------------------------
def fetch_confirmed_lineups(game_date):
    """
    Fetch the NBA's official daily lineups JSON for game_date.
    Returns list of row dicts with lineup_status = 'Confirmed'.
    """
    date_key = game_date.strftime("%Y%m%d")
    url      = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date_key}.json"
    data     = _direct_get(url, f"daily_lineups {date_key}")
    if data is None:
        return []
    rows = []
    for g in data.get("games", []):
        game_id = safe_str(g.get("gameId"))
        if game_id is None:
            continue
        for side, home_away in (("homeTeam", "Home"), ("awayTeam", "Away")):
            team    = g.get(side, {})
            tricode = safe_str(team.get("teamAbbreviation"))
            for p in team.get("players", []):
                pos     = safe_str(p.get("position"))
                roster  = safe_str(p.get("rosterStatus"))
                starter = "Starter" if pos else ("Bench" if roster == "Active" else "Inactive")
                rows.append({
                    "game_id":        game_id,
                    "game_date":      game_date,
                    "home_away":      home_away,
                    "team_tricode":   tricode,
                    "player_name":    safe_str(p.get("playerName")),
                    "position":       pos,
                    "lineup_status":  "Confirmed",
                    "roster_status":  roster,
                    "starter_status": starter,
                })
    return rows


# ---------------------------------------------------------------------------
# Stage 2: Projected lineups from boxscorepreviewv3
# ---------------------------------------------------------------------------
def fetch_projected_lineups(game_id, game_date):
    """
    Fetch boxscorepreviewv3 for a single game and extract the predicted lineup.
    Returns list of row dicts with lineup_status = 'Projected'.

    boxscorepreviewv3 returns a 'homeTeam'/'awayTeam' structure with
    'players' arrays. The 'position' field indicates a predicted starter;
    players with no position are predicted bench.
    """
    url  = "https://stats.nba.com/stats/boxscorepreviewv3"
    data = _direct_get(url, f"boxscorepreviewv3 {game_id}", timeout=60)
    if data is None:
        return []

    try:
        game_data = data.get("game", {})
        home_obj  = game_data.get("homeTeam", {})
        away_obj  = game_data.get("awayTeam", {})
    except Exception as exc:
        log.warning(f"  {game_id}: unexpected preview response shape: {exc}")
        return []

    rows = []
    for team_obj, home_away in ((home_obj, "Home"), (away_obj, "Away")):
        tricode = safe_str(team_obj.get("teamTricode"))
        for p in team_obj.get("players", []):
            name    = safe_str(p.get("name"))
            pos     = safe_str(p.get("position"))
            status  = safe_str(p.get("status"))  # 'Active', 'Inactive', etc.
            if name is None:
                continue
            # Players with a position in the preview are predicted starters;
            # active players without a position are projected bench.
            if status and status.lower() not in ("active", "actv", ""):
                starter = "Inactive"
            elif pos:
                starter = "Starter"
            else:
                starter = "Bench"
            rows.append({
                "game_id":        game_id,
                "game_date":      game_date,
                "home_away":      home_away,
                "team_tricode":   tricode,
                "player_name":    name,
                "position":       pos,
                "lineup_status":  "Projected",
                "roster_status":  status,
                "starter_status": starter,
            })
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA lineup poller")
    parser.add_argument(
        "--hours-ahead", type=float, default=4.0,
        help="Include games starting within this many hours from now. Default: 4."
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set.")

    engine = get_engine()

    qualified_games = get_todays_nonfinal_games(engine, args.hours_ahead)
    if not qualified_games:
        log.info(f"No non-final games starting within {args.hours_ahead}h. Nothing to do.")
        return

    game_ids_to_update = {r["game_id"] for r in qualified_games}
    log.info(f"{len(game_ids_to_update)} game(s) qualify for lineup refresh.")

    today = date.today()

    # ------------------------------------------------------------------
    # Stage 1: Confirmed lineups from the official daily lineups JSON.
    # One HTTP call returns all games for the day.
    # ------------------------------------------------------------------
    log.info("Stage 1: fetching confirmed lineups...")
    all_confirmed = fetch_confirmed_lineups(today)
    confirmed_by_game = {}
    for row in all_confirmed:
        gid = row["game_id"]
        if gid in game_ids_to_update:
            confirmed_by_game.setdefault(gid, []).append(row)

    confirmed_rows = []
    confirmed_game_ids = set()
    for gid, rows in confirmed_by_game.items():
        if rows:
            confirmed_rows.extend(rows)
            confirmed_game_ids.add(gid)

    if confirmed_rows:
        # Delete then upsert so scratches are cleared.
        with engine.begin() as conn:
            for gid in confirmed_game_ids:
                conn.execute(
                    text("DELETE FROM nba.daily_lineups WHERE game_id = :gid"),
                    {"gid": gid}
                )
        upsert(
            pd.DataFrame(confirmed_rows),
            engine, "nba", "daily_lineups",
            ["game_id", "team_tricode", "player_name"]
        )
        log.info(f"  Stage 1 complete: {len(confirmed_rows)} confirmed rows for "
                 f"{len(confirmed_game_ids)} game(s).")
    else:
        log.info("  Stage 1: confirmed lineup JSON returned no rows for qualifying games yet.")

    # ------------------------------------------------------------------
    # Stage 2: Projected lineups for games with no confirmed data.
    # Calls boxscorepreviewv3 once per game.
    # ------------------------------------------------------------------
    needs_projected = game_ids_to_update - confirmed_game_ids
    if not needs_projected:
        log.info("Stage 2: all qualifying games have confirmed lineups. Skipping projected fetch.")
        return

    log.info(f"Stage 2: fetching projected lineups for {len(needs_projected)} game(s): {needs_projected}")
    projected_rows = []
    for gid in sorted(needs_projected):
        rows = fetch_projected_lineups(gid, today)
        if not rows:
            log.warning(f"  {gid}: boxscorepreviewv3 returned no data.")
            continue
        projected_rows.extend(rows)
        log.info(f"  {gid}: {len(rows)} projected rows.")
        time.sleep(API_DELAY)

    if projected_rows:
        # For projected, only delete existing projected rows (not confirmed).
        # This preserves any confirmed data for other games that might be in
        # the table from prior runs.
        with engine.begin() as conn:
            for gid in needs_projected:
                conn.execute(
                    text("DELETE FROM nba.daily_lineups "
                         "WHERE game_id = :gid AND lineup_status = 'Projected'"),
                    {"gid": gid}
                )
        upsert(
            pd.DataFrame(projected_rows),
            engine, "nba", "daily_lineups",
            ["game_id", "team_tricode", "player_name"]
        )
        log.info(f"  Stage 2 complete: {len(projected_rows)} projected rows for "
                 f"{len(needs_projected)} game(s).")
    else:
        log.info("  Stage 2: no projected rows returned.")

    log.info("Lineup poll complete.")


if __name__ == "__main__":
    main()
