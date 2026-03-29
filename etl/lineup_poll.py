"""
lineup_poll.py

Fetches today's NBA lineups from the NBA daily lineups JSON endpoint and
upserts them into nba.daily_lineups. Runs every 15 minutes during the
game window via lineup-poll.yml.

Design
------
- Queries nba.schedule for today's games (game_status != 3, i.e. not final).
- Filters to games whose game_status_text indicates a start within
  --hours-ahead hours (default: 4).  Uses the schedule's game_status_text
  ("7:30 pm ET" style) to determine start time rather than hitting another
  endpoint.  If parse fails for a game it is included conservatively.
- Fetches the daily lineups JSON for today (single HTTP call covers all games).
- Upserts matching rows into nba.daily_lineups.
- Exits cleanly with no error if no games qualify (normal overnight state).

Secrets required
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import math
import os
import re
import time
import logging
from datetime import date, datetime, timezone, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXY_URL   = os.environ.get("NBA_PROXY_URL")
API_DELAY   = 1.5
RETRY_WAIT  = 30
RETRY_COUNT = 3

NBA_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Origin":             "https://www.nba.com",
    "Referer":            "https://www.nba.com/",
}

def get_proxies():
    if not PROXY_URL:
        return None
    return {"http": PROXY_URL, "https": PROXY_URL}


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(f"DB connection attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                log.info("Waiting 60s for Azure SQL to resume...")
                time.sleep(60)
    raise RuntimeError("Could not connect to Azure SQL after 3 attempts.")


# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------
def safe_int(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)
    except (ValueError, TypeError):
        return None

def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


# ---------------------------------------------------------------------------
# MERGE upsert
# ---------------------------------------------------------------------------
def _clean_val(v):
    import numpy as np
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    return v

def upsert(df, engine, schema, table, pk_cols):
    if df is None or df.empty:
        return
    records = [
        {col: _clean_val(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]
    non_pk    = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = (
        ", ".join(f"tgt.{c} = src.{c}" for c in non_pk)
        if non_pk else f"tgt.{pk_cols[0]} = tgt.{pk_cols[0]}"
    )
    merge_sql = f"""
        MERGE {schema}.{table} AS tgt
        USING (VALUES ({val_list})) AS src ({col_list})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
    """
    with engine.begin() as conn:
        conn.execute(text(merge_sql), records)


# ---------------------------------------------------------------------------
# Game start time parsing
# ---------------------------------------------------------------------------
_TIME_RE = re.compile(r"(\d{1,2}):(\d{2})\s*(am|pm)", re.IGNORECASE)

def parse_game_start_utc(game_status_text):
    """
    Parse a game_status_text like '7:30 pm ET' into a UTC datetime for today.
    Returns None if parsing fails.
    NBA games are always displayed in Eastern time.
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
        today_et = date.today()
        # Eastern time is UTC-5 (EST) or UTC-4 (EDT).
        # Use UTC-4 during NBA season (games run Oct-Jun, mostly EDT).
        # A 30-minute offset error is acceptable for a polling window gate.
        et_offset  = timedelta(hours=-4)
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
    """
    Returns schedule rows for today's games that have not yet finished
    (game_status != 3) and whose start time is within hours_ahead hours
    from now, OR whose start time could not be parsed (conservative inclusion).
    """
    today = date.today()
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
            # Cannot parse, include conservatively.
            log.info(f"  Game {r['game_id']}: start time unparseable, including.")
            qualified.append(r)
        elif start_utc <= cutoff:
            log.info(
                f"  Game {r['game_id']} ({r.get('away_team_tricode')} @ {r.get('home_team_tricode')}): "
                f"starts {start_utc.strftime('%H:%M UTC')}, within window."
            )
            qualified.append(r)
        else:
            log.info(
                f"  Game {r['game_id']}: starts {start_utc.strftime('%H:%M UTC')}, outside window, skipping."
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
# Lineup fetch + parse
# ---------------------------------------------------------------------------
def fetch_lineups_for_date(game_date):
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
                pos    = safe_str(p.get("position"))
                roster = safe_str(p.get("rosterStatus"))
                starter = "Starter" if pos else ("Bench" if roster == "Active" else "Inactive")
                rows.append({
                    "game_id":        game_id,
                    "game_date":      game_date,
                    "home_away":      home_away,
                    "team_tricode":   tricode,
                    "player_name":    safe_str(p.get("playerName")),
                    "position":       pos,
                    "lineup_status":  safe_str(p.get("lineupStatus")),
                    "roster_status":  roster,
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

    today      = date.today()
    all_rows   = fetch_lineups_for_date(today)
    if not all_rows:
        log.warning(f"Lineup endpoint returned no data for {today}. NBA may not have published yet.")
        return

    filtered = [r for r in all_rows if r["game_id"] in game_ids_to_update]
    if not filtered:
        log.warning("Lineup JSON returned data but none matched qualifying game IDs.")
        return

    # Delete existing rows for these games before upserting so scratches are removed.
    with engine.begin() as conn:
        for gid in game_ids_to_update:
            conn.execute(
                text("DELETE FROM nba.daily_lineups WHERE game_id = :gid"),
                {"gid": gid}
            )

    upsert(
        pd.DataFrame(filtered),
        engine, "nba", "daily_lineups",
        ["game_id", "team_tricode", "player_name"]
    )
    log.info(f"Lineup poll complete: {len(filtered)} rows upserted for {len(game_ids_to_update)} game(s).")


if __name__ == "__main__":
    main()
