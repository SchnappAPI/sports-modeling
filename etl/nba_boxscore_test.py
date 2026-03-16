"""
nba_boxscore_test.py

Isolated test script for the box score quarter-split and matchup fixes.
Processes a single game ID passed via --game-id argument.

Output formatting rules:
  - Team tables exclude minutes column.
  - Position null/nan = bench player (did not start), displayed as BENCH.
  - OT columns are a subset of Q1-Q4 columns (no position, minutes, stl, blk, tov).
  - All tables include game_id, team_id, and opponent_team_id.
  - A combined table of all quarters appended together is shown at the end.

Run:
  python nba_boxscore_test.py --game-id 0022400061

Secrets required:
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import os
import time
import logging
import math

import pandas as pd
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import (
    boxscoretraditionalv3,
    boxscorematchupsv3,
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

# Period boundaries in tenths of a second from tip-off.
# range_type=2 uses these to slice exactly one period.
# Each regulation quarter = 7200 tenths (12 min x 60s x 10).
# Each OT period          = 3000 tenths (5 min x 60s x 10).
PERIOD_RANGES = [
    (1, "Q1", 0,     7200),
    (2, "Q2", 7200,  14400),
    (3, "Q3", 14400, 21600),
    (4, "Q4", 21600, 28800),
]
OT_START_RANGE = 28800
OT_PERIOD_LEN  = 3000

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
# API retry wrapper
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
# Clean value helper
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
def upsert(df, engine, schema, table, pk_cols):
    if df is None or df.empty:
        return
    # Drop any display-only columns not in the DB schema before upserting
    db_cols = [c for c in df.columns if c != "opponent_team_id"]
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
# Derive opponent_team_id for each row
# Returns {team_id: opponent_team_id}
# ---------------------------------------------------------------------------
def _build_opponent_map(player_rows):
    team_ids = list({r["team_id"] for r in player_rows if r.get("team_id")})
    if len(team_ids) != 2:
        return {}
    return {team_ids[0]: team_ids[1], team_ids[1]: team_ids[0]}

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
    """Team rows. Minutes excluded per spec."""
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
# Column specs
# ---------------------------------------------------------------------------
PLAYER_Q_COLS = [
    "game_id","team_id","opponent_team_id",
    "quarter","first_name","last_name","team_abbreviation","position","minutes",
    "fgm","fga","fg3m","fg3a","ftm","fta","reb","ast","stl","blk","tov","pts","plus_minus",
]

PLAYER_OT_COLS = [
    "game_id","team_id","opponent_team_id",
    "quarter","first_name","last_name","team_abbreviation",
    "fgm","fga","fg3m","fg3a","ftm","fta","reb","ast","pts","plus_minus",
]

TEAM_COLS = [
    "game_id","team_id","opponent_team_id",
    "quarter","team_abbreviation",
    "fgm","fga","fg3m","fg3a","ftm","fta","reb","ast","pts","plus_minus",
]

COMBINED_PLAYER_COLS = [
    "game_id","team_id","opponent_team_id",
    "quarter","first_name","last_name","team_abbreviation","position","minutes",
    "fgm","fga","fg3m","fg3a","ftm","fta","reb","ast","stl","blk","tov","pts","plus_minus",
]

COMBINED_TEAM_COLS = [
    "game_id","team_id","opponent_team_id",
    "quarter","team_abbreviation",
    "fgm","fga","fg3m","fg3a","ftm","fta","reb","ast","pts","plus_minus",
]

# ---------------------------------------------------------------------------
# Main test logic
# ---------------------------------------------------------------------------
def run_test(game_id, engine, out_path):
    sections = []

    def log_section(title, rows, columns):
        block = format_table(title, rows, columns)
        log.info(block)
        sections.append(block)

    all_player_rows = []
    all_team_rows   = []

    # ------------------------------------------------------------------
    # Q1 through Q4
    # ------------------------------------------------------------------
    for period_num, quarter_label, start_range, end_range in PERIOD_RANGES:
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
                   "nba", "player_box_score_stats", ["game_id","player_id","quarter"])
            all_player_rows.extend(p_rows)

        if t_rows:
            upsert(pd.DataFrame(t_rows), engine,
                   "nba", "team_box_score_stats", ["game_id","team_id","quarter"])
            all_team_rows.extend(t_rows)

        log_section(f"Player Box Score  —  {quarter_label}", p_rows, PLAYER_Q_COLS)
        log_section(f"Team Box Score  —  {quarter_label}", t_rows, TEAM_COLS)

    # ------------------------------------------------------------------
    # OT periods
    # ------------------------------------------------------------------
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
                   "nba", "player_box_score_stats", ["game_id","player_id","quarter"])
            all_player_rows.extend(ot_p_rows)

        if ot_t_rows:
            upsert(pd.DataFrame(ot_t_rows), engine,
                   "nba", "team_box_score_stats", ["game_id","team_id","quarter"])
            all_team_rows.extend(ot_t_rows)

        log_section(
            f"Player Box Score  —  OT (summed across {len(ot_periods_data)} period(s))",
            ot_p_rows,
            PLAYER_OT_COLS,
        )
        log_section("Team Box Score  —  OT", ot_t_rows, TEAM_COLS)
    else:
        no_ot = f"\n{'='*60}\nOT\n{'='*60}\nNo overtime periods detected.\n"
        log.info(no_ot)
        sections.append(no_ot)

    # ------------------------------------------------------------------
    # Combined tables: all quarters appended
    # OT rows will have blank position/minutes/stl/blk/tov cells since
    # those are not tracked at the OT summary level
    # ------------------------------------------------------------------
    if all_player_rows:
        log_section(
            "Player Box Score  —  All Quarters Combined",
            all_player_rows,
            COMBINED_PLAYER_COLS,
        )
    if all_team_rows:
        log_section(
            "Team Box Score  —  All Quarters Combined",
            all_team_rows,
            COMBINED_TEAM_COLS,
        )

    # ------------------------------------------------------------------
    # Sanity check: quarter sums vs full-game pull
    # ------------------------------------------------------------------
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

    sanity_rows = []
    if sanity_ep is not None and all_player_rows:
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
            f_sums = f_df.groupby("player_id")[stat_cols + ["first_name","last_name"]].first().reset_index()

            merged = f_sums.merge(q_sums, on="player_id", how="left")
            for _, r in merged.iterrows():
                for c in ["pts","reb","ast"]:
                    fv = int(r.get(c, 0) or 0)
                    qv = int(r.get(f"q_{c}", 0) or 0)
                    sanity_rows.append({
                        "player":       f"{r.get('first_name','')} {r.get('last_name','')}".strip(),
                        "stat":         c,
                        "full_game":    fv,
                        "sum_quarters": qv,
                        "result":       "OK" if fv == qv else "MISMATCH",
                    })
        except Exception as exc:
            log.warning(f"  Sanity check failed: {exc}")

    log_section(
        "Sanity Check  —  Full-game totals vs sum of quarter rows (pts/reb/ast)",
        sanity_rows,
        ["player","stat","full_game","sum_quarters","result"],
    )

    # ------------------------------------------------------------------
    # Matchup column probe (informational only)
    # ------------------------------------------------------------------
    probe_ep = api_call(
        lambda: boxscorematchupsv3.BoxScoreMatchupsV3(
            game_id=game_id, proxy=PROXY_URL),
        f"BoxScoreMatchupsV3 column probe {game_id}",
    )
    if probe_ep is not None:
        try:
            probe_cols = probe_ep.player_stats.get_data_frame().columns.tolist()
            col_section = (
                f"\n{'='*60}\n"
                f"BoxScoreMatchupsV3 raw column names\n"
                f"{'='*60}\n"
                + "\n".join(f"  {c}" for c in probe_cols)
                + "\n"
            )
            log.info(col_section)
            sections.append(col_section)
        except Exception as exc:
            log.warning(f"  Column probe failed: {exc}")

    # ------------------------------------------------------------------
    # Write output file
    # ------------------------------------------------------------------
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"NBA Box Score Fix Test  —  game_id: {game_id}\n")
        f.write("=" * 60 + "\n")
        for section in sections:
            f.write(section)

    log.info(f"\nOutput written to {out_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Test script for NBA box score quarter-split fix"
    )
    parser.add_argument(
        "--game-id", required=True,
        help="NBA game ID to test (e.g. 0022400061)",
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set.")

    engine   = get_engine()
    out_path = f"nba_boxscore_test_{args.game_id}.txt"
    run_test(args.game_id, engine, out_path)


if __name__ == "__main__":
    main()
