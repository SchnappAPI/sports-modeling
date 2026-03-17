"""
nba_daily_test.py

NBA daily test ETL. Given a game date, discovers all games scheduled for
that date via ScoreboardV3, fetches quarter-level box score data for every
game, then fetches passing and rebounding tracking stats for that date using
direct HTTP requests with browser-mimicking headers (same approach as the
Excel Power Query).

Run modes:
  Yesterday (default):
    python nba_daily_test.py
  Specific date:
    python nba_daily_test.py --date 2025-03-14
  Box scores only (skip pt stats):
    python nba_daily_test.py --date 2025-03-14 --skip-pt-stats
  With explicit season override:
    python nba_daily_test.py --date 2025-03-14 --season 2024-25

Flow per run:
  1. Call ScoreboardV3 for --date to discover all game IDs on that day.
  2. For each game ID, fetch BoxScoreTraditionalV3 Q1-Q4 + OT with range_type=2.
     Write player_box_score_stats and team_box_score_stats via MERGE upsert.
  3. Call LeagueDashPtStats PtMeasureType=Passing via direct requests
     with DateFrom=DateTo=<date> and PerMode=Totals.
  4. Call LeagueDashPtStats PtMeasureType=Rebounding the same way.
  5. Write passing and rebounding rows to CSV logs.
  6. Write a text summary artifact.

Output files (uploaded as GitHub Actions artifacts):
  nba_daily_test_YYYY-MM-DD.txt      Per-quarter box score tables + sanity checks.
  nba_passing_stats_log.csv          Cumulative daily passing stats log.
  nba_rebound_chances_log.csv        Cumulative daily rebound chances log.

Secrets required:
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import os
import time
import logging
import math
from datetime import date, timedelta

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
    scoreboardv3,
)

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

# Retry waits for the direct HTTP pt stats calls.
RETRY_WAIT_TIMEOUT = 30
RETRY_WAIT_500     = 60

# Between-call pause for pt stats to mimic natural request cadence.
PT_STATS_BETWEEN_DELAY = 15

# Headers that mirror the Excel Power Query and the NBA stats site exactly.
# The nba_api wrapper does not send these, which is why it gets throttled.
# Direct requests with these headers work reliably.
NBA_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Origin":             "https://www.nba.com",
    "Referer":            "https://www.nba.com/",
}

PERIOD_RANGES = [
    (1, "Q1", 0,     7200),
    (2, "Q2", 7200,  14400),
    (3, "Q3", 14400, 21600),
    (4, "Q4", 21600, 28800),
]
OT_START_RANGE = 28800
OT_PERIOD_LEN  = 3000

# ---------------------------------------------------------------------------
# Column specs for display tables
# ---------------------------------------------------------------------------
PLAYER_COLS = [
    "game_id", "player_id", "team_id", "opponent_team_id",
    "quarter", "first_name", "last_name", "team_abbreviation",
    "position", "minutes",
    "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "reb", "ast", "stl", "blk", "tov", "pts", "plus_minus",
]

TEAM_COLS = [
    "game_id", "team_id", "opponent_team_id",
    "quarter", "team_abbreviation",
    "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "reb", "ast", "pts", "plus_minus",
]

PASSING_LOG_COLS = [
    "game_date", "player_id", "player_name", "team_id", "team_abbreviation",
    "potential_ast", "ast", "ft_ast", "secondary_ast",
    "passes_made", "passes_received",
    "ast_points_created", "ast_adj", "ast_to_pass_pct", "ast_to_pass_pct_adj",
]

REB_LOG_COLS = [
    "game_date", "player_id", "player_name", "team_id", "team_abbreviation",
    "oreb", "oreb_chances", "dreb", "dreb_chances", "reb_chances",
]

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
def safe_float(val):
    try:
        if val is None:
            return None
        if isinstance(val, float):
            return None if (math.isnan(val) or math.isinf(val)) else val
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None

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

def safe_pct(num, den):
    n, d = safe_int(num), safe_int(den)
    if n is None or d is None or d == 0:
        return None
    return round(n / d, 4)

# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------
def get_proxies():
    if not PROXY_URL:
        return None
    return {"http": PROXY_URL, "https": PROXY_URL}

# ---------------------------------------------------------------------------
# API retry wrapper (for nba_api calls)
# ---------------------------------------------------------------------------
def api_call(fn, label):
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            result = fn()
            time.sleep(API_DELAY)
            return result
        except Exception as exc:
            log.warning(f"  {label} attempt {attempt}/{RETRY_COUNT} failed: {exc}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_WAIT)
    log.error(f"  {label} failed after {RETRY_COUNT} attempts, skipping")
    return None

# ---------------------------------------------------------------------------
# Clean value helper (for MERGE upsert)
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

# ---------------------------------------------------------------------------
# MERGE upsert
# ---------------------------------------------------------------------------
def upsert(df, engine, schema, table, pk_cols, exclude_cols=None):
    if df is None or df.empty:
        return
    exclude_cols = set(exclude_cols or [])
    db_cols = [c for c in df.columns if c not in exclude_cols]
    df = df[db_cols]
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
# _seed_players
# ---------------------------------------------------------------------------
def _seed_players(rows, engine):
    seed_sql = """
        MERGE nba.players AS tgt
        USING (VALUES (:nba_player_id, :player_name))
              AS src (nba_player_id, player_name)
        ON tgt.nba_player_id = src.nba_player_id
        WHEN NOT MATCHED THEN INSERT
            (nba_player_id, player_name, created_at)
        VALUES (src.nba_player_id, src.player_name, GETUTCDATE());
    """
    seed_rows = []
    for r in rows:
        pid = r.get("player_id") or r.get("nba_player_id")
        if pid is None:
            continue
        fn   = r.get("first_name") or ""
        ln   = r.get("last_name") or ""
        name = (fn + " " + ln).strip() or "Unknown"
        seed_rows.append({"nba_player_id": pid, "player_name": name})
    if not seed_rows:
        return
    with engine.begin() as conn:
        conn.execute(text(seed_sql), seed_rows)

# ---------------------------------------------------------------------------
# Opponent map
# ---------------------------------------------------------------------------
def _build_opponent_map(player_rows):
    team_ids = list({r["team_id"] for r in player_rows if r.get("team_id")})
    if len(team_ids) != 2:
        return {}
    return {team_ids[0]: team_ids[1], team_ids[1]: team_ids[0]}

# ---------------------------------------------------------------------------
# Discover game IDs for a date via ScoreboardV3
# ---------------------------------------------------------------------------
def fetch_game_ids_for_date(game_date):
    """
    Returns a list of game ID strings for all games on game_date.
    ScoreboardV3 line score rows are ordered away-first, home-second per game.
    We only need the game IDs, so we pull from the games dataset directly.
    """
    date_str = game_date.strftime("%m/%d/%Y")
    log.info(f"Fetching game IDs for {game_date} via ScoreboardV3...")

    ep = api_call(
        lambda: scoreboardv3.ScoreboardV3(
            game_date=date_str,
            league_id="00",
            proxy=PROXY_URL,
        ),
        f"ScoreboardV3 {game_date}",
    )
    if ep is None:
        log.warning(f"  ScoreboardV3 failed for {game_date}. No games loaded.")
        return []

    try:
        games_df = ep.game_header.get_data_frame()
    except Exception as exc:
        log.warning(f"  Could not parse ScoreboardV3 game header for {game_date}: {exc}")
        return []

    if games_df is None or games_df.empty:
        log.info(f"  No games found on {game_date}.")
        return []

    game_ids = games_df["GAME_ID"].astype(str).tolist()
    log.info(f"  Found {len(game_ids)} game(s): {game_ids}")
    return game_ids

# ---------------------------------------------------------------------------
# Box score row builders
# ---------------------------------------------------------------------------
def _trad_player_rows(game_id, quarter_label, df):
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        comment = safe_str(row.get("comment")) or ""
        if comment:
            continue
        pid = safe_int(row.get("personId"))
        if pid is None:
            continue
        pos_raw  = safe_str(row.get("position"))
        position = pos_raw if pos_raw and pos_raw.lower() != "nan" else "BENCH"
        rows.append({
            "game_id":           game_id,
            "player_id":         pid,
            "quarter":           quarter_label,
            "first_name":        safe_str(row.get("firstName")),
            "last_name":         safe_str(row.get("familyName")),
            "team_id":           safe_int(row.get("teamId")),
            "team_abbreviation": safe_str(row.get("teamTricode")),
            "position":          position,
            "minutes":           safe_str(row.get("minutes")),
            "fgm":               safe_int(row.get("fieldGoalsMade")),
            "fga":               safe_int(row.get("fieldGoalsAttempted")),
            "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
            "fg3m":              safe_int(row.get("threePointersMade")),
            "fg3a":              safe_int(row.get("threePointersAttempted")),
            "fg3_pct":           safe_float(row.get("threePointersPercentage")),
            "ftm":               safe_int(row.get("freeThrowsMade")),
            "fta":               safe_int(row.get("freeThrowsAttempted")),
            "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
            "oreb":              safe_int(row.get("reboundsOffensive")),
            "dreb":              safe_int(row.get("reboundsDefensive")),
            "reb":               safe_int(row.get("reboundsTotal")),
            "ast":               safe_int(row.get("assists")),
            "stl":               safe_int(row.get("steals")),
            "blk":               safe_int(row.get("blocks")),
            "tov":               safe_int(row.get("turnovers")),
            "pf":                safe_int(row.get("foulsPersonal")),
            "pts":               safe_int(row.get("points")),
            "plus_minus":        safe_float(row.get("plusMinusPoints")),
        })
    return rows


def _trad_team_rows(game_id, quarter_label, df):
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        tid = safe_int(row.get("teamId"))
        if tid is None:
            continue
        rows.append({
            "game_id":           game_id,
            "team_id":           tid,
            "quarter":           quarter_label,
            "team_abbreviation": safe_str(row.get("teamTricode")),
            "fgm":               safe_int(row.get("fieldGoalsMade")),
            "fga":               safe_int(row.get("fieldGoalsAttempted")),
            "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
            "fg3m":              safe_int(row.get("threePointersMade")),
            "fg3a":              safe_int(row.get("threePointersAttempted")),
            "fg3_pct":           safe_float(row.get("threePointersPercentage")),
            "ftm":               safe_int(row.get("freeThrowsMade")),
            "fta":               safe_int(row.get("freeThrowsAttempted")),
            "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
            "oreb":              safe_int(row.get("reboundsOffensive")),
            "dreb":              safe_int(row.get("reboundsDefensive")),
            "reb":               safe_int(row.get("reboundsTotal")),
            "ast":               safe_int(row.get("assists")),
            "stl":               safe_int(row.get("steals")),
            "blk":               safe_int(row.get("blocks")),
            "tov":               safe_int(row.get("turnovers")),
            "pf":                safe_int(row.get("foulsPersonal")),
            "pts":               safe_int(row.get("points")),
            "plus_minus":        safe_float(row.get("plusMinusPoints")),
        })
    return rows


def _sum_ot_player_rows(game_id, ot_periods_data):
    if not ot_periods_data:
        return []
    all_rows = []
    for player_df, _ in ot_periods_data:
        all_rows.extend(_trad_player_rows(game_id, "OT", player_df))
    if not all_rows:
        return []
    df = pd.DataFrame(all_rows)
    count_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                  "oreb","dreb","reb","ast","stl","blk","tov","pf","pts","plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    meta_cols  = ["game_id","player_id","quarter","first_name","last_name",
                  "team_id","team_abbreviation","position","minutes"]
    agg_meta   = df.groupby("player_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("player_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"player_id": "_pid"}, inplace=True)
    agg_meta["_pid"] = agg_meta["player_id"]
    merged = agg_meta.merge(agg_counts, on="_pid").drop(columns=["_pid"])
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"
    return merged.to_dict(orient="records")


def _sum_ot_team_rows(game_id, ot_periods_data):
    if not ot_periods_data:
        return []
    all_rows = []
    for _, team_df in ot_periods_data:
        all_rows.extend(_trad_team_rows(game_id, "OT", team_df))
    if not all_rows:
        return []
    df = pd.DataFrame(all_rows)
    count_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                  "oreb","dreb","reb","ast","stl","blk","tov","pf","pts","plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    meta_cols  = ["game_id","team_id","quarter","team_abbreviation"]
    agg_meta   = df.groupby("team_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("team_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"team_id": "_tid"}, inplace=True)
    agg_meta["_tid"] = agg_meta["team_id"]
    merged = agg_meta.merge(agg_counts, on="_tid").drop(columns=["_tid"])
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"
    return merged.to_dict(orient="records")

# ---------------------------------------------------------------------------
# Table formatter
# ---------------------------------------------------------------------------
def format_table(title, rows, columns):
    if not rows:
        return f"\n{'='*60}\n{title}\n{'='*60}\nNo rows.\n"
    df = pd.DataFrame(rows)
    columns = [c for c in columns if c in df.columns]
    df = df[columns].copy()
    df = df.fillna("").astype(str)
    for col in df.columns:
        df[col] = df[col].str.replace("nan", "", regex=False).str[:22]
    col_widths = {
        col: max(len(str(col)), df[col].str.len().max())
        for col in df.columns
    }
    def fmt_row(r):
        return "| " + " | ".join(
            str(r[col]).ljust(col_widths[col]) for col in df.columns
        ) + " |"
    header    = "| " + " | ".join(col.ljust(col_widths[col]) for col in df.columns) + " |"
    separator = "|-" + "-|-".join("-" * col_widths[col] for col in df.columns) + "-|"
    data_rows = "\n".join(fmt_row(row) for _, row in df.iterrows())
    return (
        f"\n{'='*60}\n"
        f"{title}  ({len(df)} rows)\n"
        f"{'='*60}\n"
        f"{header}\n"
        f"{separator}\n"
        f"{data_rows}\n"
    )

# ---------------------------------------------------------------------------
# CSV log helper
# ---------------------------------------------------------------------------
def _append_to_log(rows, columns, log_path, date_str):
    """
    Writes rows to a cumulative CSV log. Drops any existing rows for
    date_str first so re-runs on the same date overwrite cleanly.
    """
    if not rows:
        return
    new_df = pd.DataFrame(rows, columns=columns).astype(str)
    if os.path.exists(log_path):
        existing = pd.read_csv(log_path, dtype=str)
        existing = existing[existing["game_date"] != date_str]
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_csv(log_path, index=False)

# ---------------------------------------------------------------------------
# Direct HTTP fetch for LeagueDashPtStats
# (replicates the Excel Power Query approach that works reliably)
# ---------------------------------------------------------------------------
def _fetch_pt_stats_direct(game_date, pt_measure_type, season, timeout=60):
    """
    Fetches LeagueDashPtStats for a single date using a direct HTTP request
    with browser-mimicking headers. This is required because the nba_api
    wrapper does not send these headers and gets throttled or returns empty
    results when called from cloud IP ranges.

    DateFrom=DateTo=<date> with PerMode=Totals returns counting totals for
    that specific day only, matching the Excel fetchDate function behavior.
    """
    date_str = game_date.strftime("%m/%d/%Y")
    encoded  = requests.utils.quote(date_str)

    url = (
        "https://stats.nba.com/stats/leaguedashptstats"
        f"?Season={season}"
        "&SeasonType=Regular+Season"
        "&PlayerOrTeam=Player"
        f"&PtMeasureType={pt_measure_type}"
        "&PerMode=Totals"
        "&LastNGames=0&Month=0&OpponentTeamID=0"
        f"&DateFrom={encoded}"
        f"&DateTo={encoded}"
    )

    log.info(f"  Fetching {pt_measure_type} for {date_str} via direct request")

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(
                url,
                headers=NBA_HEADERS,
                proxies=get_proxies(),
                timeout=timeout,
            )

            if resp.status_code == 500:
                raise ValueError("HTTP 500")

            if resp.status_code != 200:
                raise ValueError(f"HTTP {resp.status_code}: {resp.text[:200]}")

            data      = resp.json()
            row_set   = data["resultSets"][0]["rowSet"]
            col_names = data["resultSets"][0]["headers"]
            row_count = len(row_set)

            log.info(f"  HTTP 200 — {row_count} rows returned")

            if row_count == 0:
                log.warning(f"  No {pt_measure_type} rows for {date_str} (no games or off day)")
                return None

            df = pd.DataFrame(row_set, columns=col_names)
            time.sleep(API_DELAY)
            return df

        except Exception as exc:
            exc_str = str(exc)
            is_500  = "500" in exc_str
            wait    = RETRY_WAIT_500 if is_500 else RETRY_WAIT_TIMEOUT

            log.warning(
                f"  {pt_measure_type} {date_str} attempt {attempt}/{RETRY_COUNT} failed: {exc_str}"
            )
            if attempt < RETRY_COUNT:
                log.info(f"  Waiting {wait}s before retry...")
                time.sleep(wait)

    log.error(f"  {pt_measure_type} {date_str} failed after {RETRY_COUNT} attempts, skipping")
    return None

# ---------------------------------------------------------------------------
# Passing and rebounding stats for a single date
# ---------------------------------------------------------------------------
def fetch_and_log_passing_stats(game_date, season, log_path):
    df = _fetch_pt_stats_direct(game_date, "Passing", season)
    if df is None or df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PLAYER_ID"))
        if pid is None:
            continue
        rows.append({
            "game_date":           str(game_date),
            "player_id":           pid,
            "player_name":         safe_str(row.get("PLAYER_NAME")),
            "team_id":             safe_int(row.get("TEAM_ID")),
            "team_abbreviation":   safe_str(row.get("TEAM_ABBREVIATION")),
            "potential_ast":       safe_float(row.get("POTENTIAL_AST")),
            "ast":                 safe_float(row.get("AST")),
            "ft_ast":              safe_float(row.get("FT_AST")),
            "secondary_ast":       safe_float(row.get("SECONDARY_AST")),
            "passes_made":         safe_float(row.get("PASSES_MADE")),
            "passes_received":     safe_float(row.get("PASSES_RECEIVED")),
            "ast_points_created":  safe_float(row.get("AST_POINTS_CREATED")),
            "ast_adj":             safe_float(row.get("AST_ADJ")),
            "ast_to_pass_pct":     safe_float(row.get("AST_TO_PASS_PCT")),
            "ast_to_pass_pct_adj": safe_float(row.get("AST_TO_PASS_PCT_ADJ")),
        })

    _append_to_log(rows, PASSING_LOG_COLS, log_path, str(game_date))
    log.info(f"  Passing log: {len(rows)} rows for {game_date} -> {log_path}")
    return rows


def fetch_and_log_rebound_chances(game_date, season, log_path):
    df = _fetch_pt_stats_direct(game_date, "Rebounding", season)
    if df is None or df.empty:
        return []

    rows = []
    for _, row in df.iterrows():
        pid = safe_int(row.get("PLAYER_ID"))
        if pid is None:
            continue
        rows.append({
            "game_date":         str(game_date),
            "player_id":         pid,
            "player_name":       safe_str(row.get("PLAYER_NAME")),
            "team_id":           safe_int(row.get("TEAM_ID")),
            "team_abbreviation": safe_str(row.get("TEAM_ABBREVIATION")),
            "oreb":              safe_float(row.get("OREB")),
            "oreb_chances":      safe_float(row.get("OREB_CHANCES")),
            "dreb":              safe_float(row.get("DREB")),
            "dreb_chances":      safe_float(row.get("DREB_CHANCES")),
            "reb_chances":       safe_float(row.get("REB_CHANCES")),
        })

    _append_to_log(rows, REB_LOG_COLS, log_path, str(game_date))
    log.info(f"  Rebound log: {len(rows)} rows for {game_date} -> {log_path}")
    return rows

# ---------------------------------------------------------------------------
# Per-game box score processor
# ---------------------------------------------------------------------------
def process_game_box_score(game_id, engine, sections):
    """
    Fetches Q1-Q4 and OT box score data for a single game.
    Writes to Azure SQL via MERGE upsert.
    Returns (all_player_rows, all_team_rows) for display and sanity check.
    """
    log.info(f"\nProcessing box score for {game_id}")
    all_player_rows = []
    all_team_rows   = []

    def log_section(title, rows, columns):
        block = format_table(title, rows, columns)
        log.info(block)
        sections.append(block)

    # Q1 through Q4
    for _period_num, quarter_label, start_range, end_range in PERIOD_RANGES:
        ep = api_call(
            lambda s=start_range, e=end_range: boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=0,
                end_period=0,
                range_type=2,
                start_range=s,
                end_range=e,
                proxy=PROXY_URL,
            ),
            f"BoxScoreTraditionalV3 {game_id} {quarter_label}",
        )
        if ep is None:
            continue
        try:
            p_df = ep.player_stats.get_data_frame()
            t_df = ep.team_stats.get_data_frame()
        except Exception as exc:
            log.warning(f"  Parse failed {game_id} {quarter_label}: {exc}")
            continue

        p_rows = _trad_player_rows(game_id, quarter_label, p_df)
        t_rows = _trad_team_rows(game_id, quarter_label, t_df)

        opp_map = _build_opponent_map(p_rows)
        for r in p_rows:
            r["opponent_team_id"] = opp_map.get(r["team_id"])
        for r in t_rows:
            r["opponent_team_id"] = opp_map.get(r["team_id"])

        if p_rows:
            _seed_players(p_rows, engine)
            upsert(pd.DataFrame(p_rows), engine,
                   "nba", "player_box_score_stats", ["game_id","player_id","quarter"],
                   exclude_cols=["opponent_team_id"])
            all_player_rows.extend(p_rows)

        if t_rows:
            upsert(pd.DataFrame(t_rows), engine,
                   "nba", "team_box_score_stats", ["game_id","team_id","quarter"],
                   exclude_cols=["opponent_team_id"])
            all_team_rows.extend(t_rows)

        log_section(f"Player Box Score  --  {game_id} {quarter_label}", p_rows, PLAYER_COLS)
        log_section(f"Team Box Score  --  {game_id} {quarter_label}", t_rows, TEAM_COLS)

    # OT periods
    ot_periods_data = []
    ot_start = OT_START_RANGE

    while True:
        ot_end = ot_start + OT_PERIOD_LEN
        try:
            ep_ot = boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=0,
                end_period=0,
                range_type=2,
                start_range=ot_start,
                end_range=ot_end,
                proxy=PROXY_URL,
            )
            ot_p_df = ep_ot.player_stats.get_data_frame()
            ot_t_df = ep_ot.team_stats.get_data_frame()
        except Exception:
            break

        if ot_p_df is None or ot_p_df.empty:
            break
        has_data = (
            ot_p_df["minutes"].notna().any()
            if "minutes" in ot_p_df.columns else False
        )
        if not has_data:
            break

        time.sleep(API_DELAY)
        ot_periods_data.append((ot_p_df, ot_t_df))
        ot_start += OT_PERIOD_LEN

    if ot_periods_data:
        ot_p_rows = _sum_ot_player_rows(game_id, ot_periods_data)
        ot_t_rows = _sum_ot_team_rows(game_id, ot_periods_data)

        opp_map_ot = _build_opponent_map(all_player_rows) if all_player_rows else {}
        for r in ot_p_rows:
            r["opponent_team_id"] = opp_map_ot.get(r["team_id"])
        for r in ot_t_rows:
            r["opponent_team_id"] = opp_map_ot.get(r["team_id"])

        if ot_p_rows:
            _seed_players(ot_p_rows, engine)
            upsert(pd.DataFrame(ot_p_rows), engine,
                   "nba", "player_box_score_stats", ["game_id","player_id","quarter"],
                   exclude_cols=["opponent_team_id"])
            all_player_rows.extend(ot_p_rows)

        if ot_t_rows:
            upsert(pd.DataFrame(ot_t_rows), engine,
                   "nba", "team_box_score_stats", ["game_id","team_id","quarter"],
                   exclude_cols=["opponent_team_id"])
            all_team_rows.extend(ot_t_rows)

        log_section(
            f"Player Box Score  --  {game_id} OT ({len(ot_periods_data)} period(s))",
            ot_p_rows,
            PLAYER_COLS,
        )
        log_section(f"Team Box Score  --  {game_id} OT", ot_t_rows, TEAM_COLS)
    else:
        sections.append(
            f"\n{'='*60}\n{game_id} OT\n{'='*60}\nNo overtime detected.\n"
        )

    # Combined display table for this game
    if all_player_rows:
        log_section(
            f"Player Box Score  --  {game_id} All Quarters Combined",
            all_player_rows,
            PLAYER_COLS,
        )
    if all_team_rows:
        log_section(
            f"Team Box Score  --  {game_id} All Quarters Combined",
            all_team_rows,
            TEAM_COLS,
        )

    return all_player_rows, all_team_rows

# ---------------------------------------------------------------------------
# Sanity check for one game
# ---------------------------------------------------------------------------
def run_sanity_check(game_id, all_player_rows, sections):
    sanity_ep = api_call(
        lambda: boxscoretraditionalv3.BoxScoreTraditionalV3(
            game_id=game_id,
            start_period=0,
            end_period=0,
            range_type=0,
            start_range=0,
            end_range=0,
            proxy=PROXY_URL,
        ),
        f"BoxScoreTraditionalV3 full-game sanity {game_id}",
    )
    if sanity_ep is None or not all_player_rows:
        return

    try:
        full_rows = _trad_player_rows(
            game_id, "FULL", sanity_ep.player_stats.get_data_frame()
        )
        stat_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                     "oreb","dreb","reb","ast","stl","blk","tov","pf","pts"]

        q_df = pd.DataFrame(all_player_rows)
        for c in stat_cols:
            q_df[c] = pd.to_numeric(q_df[c], errors="coerce").fillna(0)
        q_sums = (
            q_df.groupby("player_id")[stat_cols].sum()
            .reset_index()
            .rename(columns={c: f"q_{c}" for c in stat_cols})
        )

        f_df = pd.DataFrame(full_rows)
        for c in stat_cols:
            f_df[c] = pd.to_numeric(f_df[c], errors="coerce").fillna(0)
        f_sums = f_df.groupby("player_id")[
            stat_cols + ["first_name","last_name"]
        ].first().reset_index()

        merged = f_sums.merge(q_sums, on="player_id", how="left")
        sanity_rows = []
        for _, r in merged.iterrows():
            for c in ["pts","reb","ast"]:
                fv = int(r.get(c, 0) or 0)
                qv = int(r.get(f"q_{c}", 0) or 0)
                sanity_rows.append({
                    "player_id":    int(r["player_id"]),
                    "player":       f"{r.get('first_name','')} {r.get('last_name','')}".strip(),
                    "stat":         c,
                    "full_game":    fv,
                    "sum_quarters": qv,
                    "result":       "OK" if fv == qv else "MISMATCH",
                })

        block = format_table(
            f"Sanity Check  --  {game_id}",
            sanity_rows,
            ["player_id","player","stat","full_game","sum_quarters","result"],
        )
        log.info(block)
        sections.append(block)

        mismatches = [r for r in sanity_rows if r["result"] == "MISMATCH"]
        if mismatches:
            log.warning(f"  {len(mismatches)} MISMATCH(ES) found for {game_id}.")
        else:
            log.info(f"  All sanity checks passed for {game_id}.")

    except Exception as exc:
        log.warning(f"  Sanity check failed for {game_id}: {exc}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="NBA daily ETL: box scores for all games on a date plus passing and rebounding tracking stats"
    )
    parser.add_argument(
        "--date",
        default=None,
        help=(
            "Game date in YYYY-MM-DD format. "
            "Defaults to yesterday if not provided."
        ),
    )
    parser.add_argument(
        "--season", default="2024-25",
        help="NBA season string for pt stats calls (default: 2024-25)",
    )
    parser.add_argument(
        "--skip-pt-stats", action="store_true",
        help="Skip passing and rebounding tracking stats (box scores only)",
    )
    parser.add_argument(
        "--pt-stats-timeout", type=int, default=60,
        help="Timeout in seconds for pt stats HTTP requests (default: 60)",
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set.")

    # Resolve game date
    if args.date:
        try:
            game_date = date.fromisoformat(args.date)
        except ValueError:
            raise ValueError(f"Invalid --date format: {args.date}. Use YYYY-MM-DD.")
    else:
        game_date = date.today() - timedelta(days=1)

    log.info(f"Game date: {game_date}")
    log.info(f"Season:    {args.season}")

    engine = get_engine()

    sections  = []
    date_str  = str(game_date)
    out_path  = f"nba_daily_test_{date_str}.txt"
    pass_log  = "nba_passing_stats_log.csv"
    reb_log   = "nba_rebound_chances_log.csv"

    # Step 1: Discover all game IDs for this date
    game_ids = fetch_game_ids_for_date(game_date)

    if not game_ids:
        log.warning(f"No games found on {game_date}. Nothing to load.")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(f"NBA Daily Test  --  {date_str}\nNo games found.\n")
        return

    log.info(f"\nProcessing {len(game_ids)} game(s) for {game_date}...")

    # Step 2: Process box scores for every game on this date
    batch_player_rows = []
    for game_id in game_ids:
        p_rows, t_rows = process_game_box_score(game_id, engine, sections)
        run_sanity_check(game_id, p_rows, sections)
        batch_player_rows.extend(p_rows)

    log.info(
        f"\nBox score phase complete. "
        f"{len(game_ids)} game(s), {len(batch_player_rows)} total player-quarter rows."
    )

    # Step 3: Fetch passing and rebounding tracking stats for the date
    # These are called once per date (not once per game) because the endpoint
    # returns all players across all games on that date in a single response.
    if not args.skip_pt_stats:
        log.info(f"\nFetching passing tracking stats for {game_date}...")
        passing_rows = fetch_and_log_passing_stats(game_date, args.season, pass_log)

        # Pause between passing and rebounding to avoid triggering rate limits.
        # The Excel Power Query never fires both back to back, so a gap here
        # mimics a more natural request cadence.
        if passing_rows:
            log.info(f"Waiting {PT_STATS_BETWEEN_DELAY}s before rebounding call...")
            time.sleep(PT_STATS_BETWEEN_DELAY)

        log.info(f"Fetching rebounding tracking stats for {game_date}...")
        reb_rows = fetch_and_log_rebound_chances(game_date, args.season, reb_log)

        # Append display tables to the text artifact
        block = format_table(
            f"Passing Stats  --  {date_str}  (top 30 by potential_ast)",
            sorted(passing_rows, key=lambda r: r.get("potential_ast") or 0, reverse=True)[:30],
            PASSING_LOG_COLS,
        )
        log.info(block)
        sections.append(block)

        block = format_table(
            f"Rebound Chances  --  {date_str}  (top 30 by reb_chances)",
            sorted(reb_rows, key=lambda r: r.get("reb_chances") or 0, reverse=True)[:30],
            REB_LOG_COLS,
        )
        log.info(block)
        sections.append(block)

        log.info(
            f"\nPt stats phase complete. "
            f"Passing: {len(passing_rows)} rows. Rebounding: {len(reb_rows)} rows."
        )
    else:
        log.info("Skipping pt stats (--skip-pt-stats flag set).")

    # Write text summary artifact
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"NBA Daily Test  --  {date_str}\n")
        f.write(f"Games: {', '.join(game_ids)}\n")
        f.write(f"Season: {args.season}\n")
        f.write("=" * 60 + "\n")
        for section in sections:
            f.write(section)

    log.info(f"\nSummary written to {out_path}")
    log.info(f"Passing log:  {pass_log}")
    log.info(f"Rebound log:  {reb_log}")


if __name__ == "__main__":
    main()
