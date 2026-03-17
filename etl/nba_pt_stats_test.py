"""
nba_pt_stats_test.py

Standalone test script for LeagueDashPtStats passing and rebounding stats.
Fetches data for each of the last 5 days (excluding today), one date at a time,
using DateFrom=DateTo=<date> and PerMode=Totals. This replicates the exact
logic from the Excel Power Query scripts.

Run:
  python nba_pt_stats_test.py

Secrets required:
  NBA_PROXY_URL

Optional:
  --season    NBA season string (default: 2024-25)
  --days      Number of days to look back (default: 5)
  --timeout   API timeout in seconds (default: 60)
"""

import argparse
import math
import os
import time
import logging
from datetime import date, timedelta

import pandas as pd

from nba_api.stats.endpoints import leaguedashptstats

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

PASSING_COLS = [
    "game_date", "player_id", "player_name", "team_id", "team_abbreviation",
    "potential_ast", "ast", "ft_ast", "secondary_ast",
    "passes_made", "passes_received",
    "ast_points_created", "ast_adj", "ast_to_pass_pct", "ast_to_pass_pct_adj",
]

REB_COLS = [
    "game_date", "player_id", "player_name", "team_id", "team_abbreviation",
    "oreb", "oreb_chances", "dreb", "dreb_chances", "reb_chances",
]

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
# Table formatter
# ---------------------------------------------------------------------------
def format_table(title, rows, columns):
    if not rows:
        return f"\n{'='*60}\n{title}\n{'='*60}\nNo rows returned for this date.\n"

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
# Core fetch function — mirrors the Excel fetchDate function exactly
# One call per date, PerMode=Totals, DateFrom=DateTo
# ---------------------------------------------------------------------------
def fetch_pt_stats(game_date, pt_measure_type, season, timeout):
    """
    Fetches LeagueDashPtStats for a single date with PerMode=Totals.
    DateFrom and DateTo are both set to game_date, isolating that day's
    counting totals. This is the exact equivalent of the Excel Power Query
    fetchDate function.
    """
    date_str = game_date.strftime("%m/%d/%Y")
    ep = api_call(
        lambda d=date_str: leaguedashptstats.LeagueDashPtStats(
            pt_measure_type=pt_measure_type,
            per_mode_simple="Totals",
            season=season,
            season_type_all_star="Regular Season",
            league_id_nullable="00",
            date_from_nullable=d,
            date_to_nullable=d,
            proxy=PROXY_URL,
            timeout=timeout,
        ),
        f"LeagueDashPtStats {pt_measure_type} {date_str}",
    )
    if ep is None:
        return None
    try:
        return ep.league_dash_pt_stats.get_data_frame()
    except Exception as exc:
        log.warning(f"  Parse failed for {pt_measure_type} {date_str}: {exc}")
        return None

# ---------------------------------------------------------------------------
# Per-date processors
# ---------------------------------------------------------------------------
def process_passing(game_date, season, timeout):
    df = fetch_pt_stats(game_date, "Passing", season, timeout)
    if df is None or df.empty:
        log.info(f"  No passing data for {game_date}")
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

    log.info(f"  Passing: {len(rows)} player rows for {game_date}")
    return rows


def process_rebounding(game_date, season, timeout):
    df = fetch_pt_stats(game_date, "Rebounding", season, timeout)
    if df is None or df.empty:
        log.info(f"  No rebounding data for {game_date}")
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

    log.info(f"  Rebounding: {len(rows)} player rows for {game_date}")
    return rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Test LeagueDashPtStats passing and rebounding for last N days"
    )
    parser.add_argument(
        "--season", default="2024-25",
        help="NBA season string (default: 2024-25)",
    )
    parser.add_argument(
        "--days", type=int, default=5,
        help="Number of days to look back from yesterday (default: 5)",
    )
    parser.add_argument(
        "--timeout", type=int, default=60,
        help="API timeout in seconds (default: 60)",
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set. Requests will likely be blocked.")

    # Build date range: last N days, excluding today
    yesterday  = date.today() - timedelta(days=1)
    date_range = [yesterday - timedelta(days=i) for i in range(args.days - 1, -1, -1)]

    log.info(f"Season: {args.season}")
    log.info(f"Date range: {date_range[0]} to {date_range[-1]} ({len(date_range)} days)")

    all_passing_rows = []
    all_reb_rows     = []
    sections         = []

    for game_date in date_range:
        log.info(f"\n--- {game_date} ---")

        p_rows = process_passing(game_date, args.season, args.timeout)
        r_rows = process_rebounding(game_date, args.season, args.timeout)

        all_passing_rows.extend(p_rows)
        all_reb_rows.extend(r_rows)

        # Per-date display: top 10 by potential_ast and reb_chances
        block = format_table(
            f"Passing  —  {game_date}  (top 10 by potential_ast)",
            sorted(p_rows, key=lambda r: r.get("potential_ast") or 0, reverse=True)[:10],
            PASSING_COLS,
        )
        log.info(block)
        sections.append(block)

        block = format_table(
            f"Rebounding  —  {game_date}  (top 10 by reb_chances)",
            sorted(r_rows, key=lambda r: r.get("reb_chances") or 0, reverse=True)[:10],
            REB_COLS,
        )
        log.info(block)
        sections.append(block)

    # Summary counts
    summary = (
        f"\n{'='*60}\n"
        f"Run Summary\n"
        f"{'='*60}\n"
        f"  Dates processed : {len(date_range)}\n"
        f"  Dates with data : {len({r['game_date'] for r in all_passing_rows})}\n"
        f"  Passing rows    : {len(all_passing_rows)}\n"
        f"  Rebounding rows : {len(all_reb_rows)}\n"
    )
    log.info(summary)
    sections.append(summary)

    # Write CSVs
    if all_passing_rows:
        pass_path = "nba_passing_stats_log.csv"
        pd.DataFrame(all_passing_rows, columns=PASSING_COLS).to_csv(pass_path, index=False)
        log.info(f"Passing stats written to {pass_path}")

    if all_reb_rows:
        reb_path = "nba_rebound_chances_log.csv"
        pd.DataFrame(all_reb_rows, columns=REB_COLS).to_csv(reb_path, index=False)
        log.info(f"Rebound chances written to {reb_path}")

    # Write text summary
    out_path = "nba_pt_stats_test_output.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"NBA Pt Stats Test  —  {args.season}  —  Last {args.days} days\n")
        f.write("=" * 60 + "\n")
        for section in sections:
            f.write(section)
    log.info(f"Summary written to {out_path}")


if __name__ == "__main__":
    main()
