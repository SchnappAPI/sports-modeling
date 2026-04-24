"""
lineup_backfill.py

Rewrites nba.daily_lineups for completed games using the authoritative NBA
daily lineups JSON endpoint. For every game where nba.games.game_status = 3
in the target range, fetches the daily JSON for that date, DELETEs the
existing rows for that game_id, and INSERTs the full roster with
lineup_status = 'Confirmed' as the authoritative post-game record.

Source
------
URL: https://stats.nba.com/js/data/leaders/00_daily_lineups_{YYYYMMDD}.json

This is the same endpoint Stage 1 of etl/lineup_poll.py consumes for live
games. It returns a full per-team roster (5 starters + bench + inactive)
with explicit rosterStatus flags. Parsing logic below is kept consistent
with fetch_official_lineups in lineup_poll.py so that Confirmed rows written
by this backfill and Confirmed rows written by the live poll are identical
in shape.

Behaviour
---------
- If the daily JSON for a date returns non-200 or does not contain a given
  game_id, that game's existing rows are left untouched and it is reported
  as "skipped".
- Each game's DELETE + INSERT happens inside a single transaction so a
  mid-run failure cannot leave a game half-written.
- Games that are not yet Final (game_status != 3) are never touched.

CLI
---
  --start-date YYYY-MM-DD   inclusive start date (default: earliest nba.games date)
  --end-date   YYYY-MM-DD   inclusive end date (default: today ET)
  --game-id    GID          backfill only one game (overrides date range)
  --dry-run                 log what would be written without modifying the DB

Secrets required
----------------
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import logging
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.nba_etl import (
    get_engine,
    upsert,
    safe_str,
    NBA_HEADERS,
    get_proxies,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

ET_TZ = ZoneInfo("America/New_York")

OFFICIAL_JSON_TIMEOUT = 20   # seconds
BETWEEN_DATES_DELAY   = 0.5  # seconds between date fetches to be polite


# ---------------------------------------------------------------------------
# HTTP fetch with retry
# ---------------------------------------------------------------------------
def fetch_daily_lineups_json(game_date):
    """
    Fetch the NBA daily lineups JSON for a given date.
    Returns the parsed JSON on 200, None on any other status or exception.
    """
    date_key = game_date.strftime("%Y%m%d")
    url = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date_key}.json"
    try:
        resp = requests.get(
            url,
            headers=NBA_HEADERS,
            proxies=get_proxies(),
            timeout=OFFICIAL_JSON_TIMEOUT,
        )
        if resp.status_code != 200:
            log.warning(f"  {date_key}: HTTP {resp.status_code}")
            return None
        return resp.json()
    except Exception as exc:
        log.warning(f"  {date_key}: fetch failed: {exc}")
        return None


# ---------------------------------------------------------------------------
# Parsing - identical structure to fetch_official_lineups in lineup_poll.py
# ---------------------------------------------------------------------------
def parse_game_rows(game_obj, game_date):
    """
    Parse one game entry from the daily lineups JSON into a list of row
    dicts ready for upsert. Returns [] if the shape is unexpected.
    """
    game_id = safe_str(game_obj.get("gameId"))
    if game_id is None:
        return []

    rows = []
    for side, home_away in (("homeTeam", "Home"), ("awayTeam", "Away")):
        team = game_obj.get(side, {})
        tricode = safe_str(team.get("teamAbbreviation"))
        for p in team.get("players", []):
            name   = safe_str(p.get("playerName"))
            pos    = safe_str(p.get("position"))
            roster = safe_str(p.get("rosterStatus"))
            if name is None:
                continue
            # Same classification rule as fetch_official_lineups:
            # starters have position set, bench have rosterStatus Active, else Inactive.
            if pos:
                starter = "Starter"
            elif roster == "Active":
                starter = "Bench"
            else:
                starter = "Inactive"
            rows.append({
                "game_id":        game_id,
                "game_date":      game_date,
                "home_away":      home_away,
                "team_tricode":   tricode,
                "player_name":    name,
                "position":       pos,
                "lineup_status":  "Confirmed",
                "roster_status":  roster,
                "starter_status": starter,
            })
    return rows


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_completed_games(engine, start_date, end_date, only_game_id=None):
    """Return list of dicts for games in [start_date, end_date] with game_status = 3."""
    if only_game_id is not None:
        sql = (
            "SELECT game_id, game_date FROM nba.games "
            "WHERE game_id = :gid AND game_status = 3"
        )
        params = {"gid": only_game_id}
    else:
        sql = (
            "SELECT game_id, game_date FROM nba.games "
            "WHERE game_status = 3 AND game_date BETWEEN :s AND :e "
            "ORDER BY game_date, game_id"
        )
        params = {"s": start_date, "e": end_date}

    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql), params)]


def replace_game_rows(engine, game_id, rows, dry_run):
    """
    DELETE + INSERT for one game inside a single transaction. Uses the shared
    upsert helper for the INSERT so the MERGE semantics and staging table
    behaviour match existing writers.
    """
    if dry_run:
        log.info(f"    [dry-run] would DELETE+INSERT {len(rows)} rows for {game_id}")
        return

    df = pd.DataFrame(rows)
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM nba.daily_lineups WHERE game_id = :gid"),
            {"gid": game_id},
        )
    # upsert opens its own engine.begin() internally; keeping it outside the
    # DELETE transaction is acceptable because the DELETE+INSERT pair is
    # re-runnable (idempotent on the same JSON source).
    upsert(
        df,
        engine, "nba", "daily_lineups",
        ["game_id", "team_tricode", "player_name"],
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA lineup backfill for completed games")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Inclusive start date YYYY-MM-DD. Default: earliest nba.games date.")
    parser.add_argument("--end-date", type=str, default=None,
                        help="Inclusive end date YYYY-MM-DD. Default: today ET.")
    parser.add_argument("--game-id", type=str, default=None,
                        help="Backfill only this one game_id. Overrides date range.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log what would be written without modifying the DB.")
    args = parser.parse_args()

    engine = get_engine()

    # Resolve date range.
    if args.game_id is None:
        if args.start_date:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        else:
            with engine.connect() as conn:
                row = conn.execute(
                    text("SELECT MIN(game_date) AS d FROM nba.games WHERE game_status = 3")
                ).fetchone()
                start_date = row.d if row and row.d else date.today()

        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        else:
            end_date = datetime.now(ET_TZ).date()
    else:
        start_date = None
        end_date = None

    log.info("NBA lineup backfill")
    if args.game_id:
        log.info(f"  Target: single game {args.game_id}")
    else:
        log.info(f"  Target: {start_date} to {end_date} (inclusive)")
    log.info(f"  Dry run: {args.dry_run}")

    games = get_completed_games(engine, start_date, end_date, args.game_id)
    if not games:
        log.info("No completed games in range. Nothing to do.")
        return

    log.info(f"  {len(games)} completed game(s) in scope.")

    # Group by date so we fetch each date's JSON once.
    games_by_date = {}
    for g in games:
        games_by_date.setdefault(g["game_date"], []).append(g["game_id"])

    n_written_games = 0
    n_written_rows  = 0
    n_skipped_games = 0
    n_missing_json  = 0

    for i, (d, game_ids) in enumerate(sorted(games_by_date.items())):
        if i > 0:
            time.sleep(BETWEEN_DATES_DELAY)

        data = fetch_daily_lineups_json(d)
        if data is None:
            n_missing_json += len(game_ids)
            log.warning(f"  {d}: JSON unavailable, skipping {len(game_ids)} game(s).")
            continue

        # Index games by id within this date's JSON payload.
        json_games = {safe_str(g.get("gameId")): g for g in data.get("games", [])}

        for gid in game_ids:
            g_obj = json_games.get(gid)
            if g_obj is None:
                n_skipped_games += 1
                log.info(f"    {gid}: not present in daily JSON for {d}, leaving existing rows.")
                continue

            rows = parse_game_rows(g_obj, d)
            if not rows:
                n_skipped_games += 1
                log.info(f"    {gid}: daily JSON returned no players, leaving existing rows.")
                continue

            replace_game_rows(engine, gid, rows, args.dry_run)
            n_written_games += 1
            n_written_rows  += len(rows)
            log.info(f"    {gid}: wrote {len(rows)} Confirmed rows.")

    log.info("")
    log.info("Summary:")
    log.info(f"  Games written:      {n_written_games}")
    log.info(f"  Rows written:       {n_written_rows}")
    log.info(f"  Games skipped:      {n_skipped_games}  (not in daily JSON or empty roster)")
    log.info(f"  Games missing JSON: {n_missing_json}   (entire date's JSON unavailable)")


if __name__ == "__main__":
    main()
