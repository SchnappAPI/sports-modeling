"""
lineup_poll.py

Fetches today's NBA lineups and upserts them into nba.daily_lineups.
Runs every 15 minutes during the game window via lineup-poll.yml.

Two-stage lineup strategy
--------------------------
Stage 1 (Official starters JSON): Fetch the NBA's official daily lineups JSON.
  URL: stats.nba.com/js/data/leaders/00_daily_lineups_{YYYYMMDD}.json
  This file only contains the projected or confirmed starting five per team.
  It does NOT include bench or inactive players.

  Players from this JSON are written as 'Starter' with lineup_status
  'Confirmed' (within 30 min of tip) or 'Projected' (further out).

Stage 2 (Full roster from boxscorepreviewv3): Always runs for every qualifying
  game, regardless of Stage 1 results.
  - Provides bench and inactive players that Stage 1 omits.
  - If Stage 1 already wrote a player as a Starter, Stage 2 does not
    overwrite them (the upsert PK is game_id + team_tricode + player_name,
    and Stage 1 rows are written first, so Stage 2 only inserts net-new rows).
  - Players from Stage 2 that are NOT in Stage 1 keep the starter_status
    derived from the preview endpoint (Starter/Bench/Inactive).

This approach gives us:
  - Accurate starters from the official JSON (most reliable source)
  - Full bench + inactive roster from the preview (fills the gap)

Secrets required
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import sys
import time
import logging
from datetime import date, datetime, timezone, timedelta
from zoneinfo import ZoneInfo
import re

import pandas as pd
from sqlalchemy import text
from pathlib import Path

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PROXY_URL = __import__('os').environ.get("NBA_PROXY_URL")

ET_TZ = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Game start time parsing
# ---------------------------------------------------------------------------
_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(am|pm)", re.IGNORECASE)

def parse_game_start_utc(game_status_text):
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
        today_et = datetime.now(ET_TZ).date()
        start_et = datetime(today_et.year, today_et.month, today_et.day,
                            hour, minute, tzinfo=ET_TZ)
        return start_et.astimezone(timezone.utc)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Schedule query
# ---------------------------------------------------------------------------
def get_todays_nonfinal_games(engine, hours_ahead):
    today_et = datetime.now(ET_TZ).date()
    now_utc  = datetime.now(timezone.utc)
    cutoff   = now_utc + timedelta(hours=hours_ahead)

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
                {"today": today_et},
            )
        ]

    qualified = []
    for r in rows:
        start_utc = parse_game_start_utc(r.get("game_status_text"))
        r["start_utc"] = start_utc
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
# Stage 1: Official daily lineups JSON (starters only)
# ---------------------------------------------------------------------------
def fetch_official_lineups(game_date):
    """
    Returns dict keyed by game_id. Each value is a dict of:
      { player_name: { lineup_status, starter_status, position, ... } }
    Only starters appear in this data.
    """
    date_key = game_date.strftime("%Y%m%d")
    url      = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date_key}.json"
    data     = _direct_get(url, f"daily_lineups {date_key}")
    if data is None:
        return {}

    by_game = {}
    for g in data.get("games", []):
        game_id = safe_str(g.get("gameId"))
        if game_id is None:
            continue

        meta = {k: g[k] for k in g if k not in ("homeTeam", "awayTeam")}
        log.info(f"  Official JSON meta {game_id}: {meta}")

        rows = {}
        for side, home_away in (("homeTeam", "Home"), ("awayTeam", "Away")):
            team    = g.get(side, {})
            tricode = safe_str(team.get("teamAbbreviation"))
            for p in team.get("players", []):
                name    = safe_str(p.get("playerName"))
                pos     = safe_str(p.get("position"))
                roster  = safe_str(p.get("rosterStatus"))
                starter = "Starter" if pos else ("Bench" if roster == "Active" else "Inactive")
                if name:
                    rows[name] = {
                        "game_id":        game_id,
                        "game_date":      game_date,
                        "home_away":      home_away,
                        "team_tricode":   tricode,
                        "player_name":    name,
                        "position":       pos,
                        "roster_status":  roster,
                        "starter_status": starter,
                        "lineup_status":  None,  # set by caller
                    }
        if rows:
            by_game[game_id] = rows
    return by_game


# ---------------------------------------------------------------------------
# Stage 2: Full roster from boxscorepreviewv3
# ---------------------------------------------------------------------------
def fetch_preview_roster(game_id, game_date):
    """
    Returns list of row dicts for all players in the game preview.
    lineup_status is always 'Projected' here.
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
            name   = safe_str(p.get("name"))
            pos    = safe_str(p.get("position"))
            status = safe_str(p.get("status"))
            if name is None:
                continue
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

    game_start_map     = {r["game_id"]: r.get("start_utc") for r in qualified_games}
    game_ids_to_update = set(game_start_map.keys())
    log.info(f"{len(game_ids_to_update)} game(s) qualify for lineup refresh.")

    today   = datetime.now(ET_TZ).date()
    now_utc = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Stage 1: Official daily lineups JSON.
    # Only contains starters. Determines Confirmed vs Projected label.
    # ------------------------------------------------------------------
    log.info("Stage 1: fetching official lineups JSON...")
    official_by_game = fetch_official_lineups(today)

    # Determine label per game.
    game_labels = {}
    for gid in game_ids_to_update:
        start_utc = game_start_map.get(gid)
        if start_utc is not None and (start_utc - now_utc) <= timedelta(minutes=30):
            game_labels[gid] = "Confirmed"
        elif gid in official_by_game:
            # Official JSON has data but tip is more than 30 min out.
            game_labels[gid] = "Projected"
        else:
            # No official data at all yet.
            game_labels[gid] = "Projected"

    # Apply label to Stage 1 rows.
    stage1_by_game = {}
    for gid, player_dict in official_by_game.items():
        if gid not in game_ids_to_update:
            continue
        label = game_labels[gid]
        for name, row in player_dict.items():
            row["lineup_status"] = label
        stage1_by_game[gid] = player_dict
        log.info(f"  {gid}: {len(player_dict)} starters from official JSON, label '{label}'.")

    # ------------------------------------------------------------------
    # Stage 2: Full roster from boxscorepreviewv3.
    # Always runs for all qualifying games.
    # Merges with Stage 1: Stage 1 starters take priority.
    # Stage 2 provides bench + inactive players not in Stage 1.
    # ------------------------------------------------------------------
    log.info("Stage 2: fetching full rosters from boxscorepreviewv3...")

    all_rows = []
    for gid in sorted(game_ids_to_update):
        preview_rows = fetch_preview_roster(gid, today)
        label = game_labels.get(gid, "Projected")

        if not preview_rows:
            log.warning(f"  {gid}: boxscorepreviewv3 returned no data.")
            # Fall back to Stage 1 only if we have it.
            if gid in stage1_by_game:
                all_rows.extend(stage1_by_game[gid].values())
                log.info(f"  {gid}: using {len(stage1_by_game[gid])} Stage 1 rows only.")
            continue

        # Build merged roster:
        # - Start with Stage 2 (full roster, all players).
        # - For any player that also appears in Stage 1, override with
        #   Stage 1's starter_status and lineup_status (more authoritative).
        stage1_players = stage1_by_game.get(gid, {})
        merged = []
        for row in preview_rows:
            name = row["player_name"]
            if name in stage1_players:
                # Use Stage 1's authoritative starter/confirmation data.
                s1 = stage1_players[name]
                row["starter_status"] = s1["starter_status"]
                row["lineup_status"]  = s1["lineup_status"]
                row["position"]       = s1["position"]
            else:
                # Player not in official JSON — keep Stage 2 data but
                # use the game's label for lineup_status.
                row["lineup_status"] = label
            merged.append(row)

        # Add any Stage 1 starters NOT found in Stage 2 preview
        # (shouldn't happen often, but handles edge cases).
        preview_names = {r["player_name"] for r in preview_rows}
        for name, s1row in stage1_players.items():
            if name not in preview_names:
                log.info(f"  {gid}: {name} in official JSON but not preview — adding from Stage 1.")
                merged.append(s1row)

        all_rows.extend(merged)
        s1_count = len(stage1_players)
        s2_only  = len([r for r in merged if r["player_name"] not in stage1_players])
        log.info(f"  {gid}: {len(merged)} total ({s1_count} from official JSON, {s2_only} from preview only), label '{label}'.")
        time.sleep(API_DELAY)

    if not all_rows:
        log.info("No rows to write.")
        return

    # Delete all existing rows for qualifying games, then write merged set.
    with engine.begin() as conn:
        for gid in game_ids_to_update:
            conn.execute(
                text("DELETE FROM nba.daily_lineups WHERE game_id = :gid"),
                {"gid": gid}
            )

    upsert(
        pd.DataFrame(all_rows),
        engine, "nba", "daily_lineups",
        ["game_id", "team_tricode", "player_name"]
    )
    log.info(f"  Written {len(all_rows)} total rows for {len(game_ids_to_update)} game(s).")
    log.info("Lineup poll complete.")


if __name__ == "__main__":
    main()
