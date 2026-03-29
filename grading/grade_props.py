"""
grade_props.py

NBA prop grading model.

Architecture
------------
All component grades are precomputed in bulk using vectorized pandas
operations before any row iteration. The per-row assembly loop does only
dict lookups — zero computation. This keeps runtime flat regardless of how
many line values are graded per player per market.

DB round trips per grade date
------------------------------
  1. Fetch FanDuel prop lines for the date.
  2. Fetch 60-day hit-rate history (bulk, all players + markets at once).
     Now also returns opp_team_id per game row via nba.schedule join.
  3. Fetch full-season game totals (bulk, all players at once).
  4. Fetch matchup defense ranks (one query for all opp/position pairs).
  5. Fetch opponent/position info per player (one query).
  6. Write all grade rows in one batched upsert.

Modes
-----
  upcoming   Grades today's standard lines (posted FanDuel line ± 5 bracket)
             plus alternate lines from the static grid filtered to lines
             FanDuel has actually priced. Full component computation.

  intraday   Standard lines only. Checks for line movement since last grade.
             Only re-grades players/markets where the posted line changed.
             Skips alternate markets entirely. Triggered by pregame-refresh.

  backfill   Works through historical game dates, oldest ungraded first.
             Uses posted historical FanDuel lines only (no bracket, no grid).

Standard line bracket
---------------------
  Posted FanDuel line ± 5 increments of 1.0 (integer steps on a half-point
  line). Minimum 0.5. Example: posted 14.5 → grades 9.5–19.5 in 1.0 steps.
  11 line values per player per market.

Alternate line grid (half-points matching FanDuel's actual format)
------------------------------------------------------------------
  pts:  4.5, 9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5
  reb:  3.5, 5.5, 7.5, 9.5, 11.5, 13.5, 15.5
  ast:  1.5, 3.5, 5.5, 7.5, 9.5, 11.5, 13.5
  fg3m: 0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5
  blk:  0.5, 1.5, 2.5, 3.5
  stl:  0.5, 1.5, 2.5, 3.5
  pra:  9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5, 54.5, 59.5
  pr:   9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5
  pa:   9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5
  ra:   4.5, 9.5, 14.5, 19.5, 24.5

  Only lines where FanDuel has posted odds are written. Graded overnight
  only; never re-graded intraday.

Component grades (all 0-100)
-----------------------------
  weighted_hit_rate  Blended 20/60-day hit rate. Primary signal.
  trend_grade        Raw stat mean: last-10 games vs last-30. Centered at 50.
  momentum_grade     Consecutive hit/miss streak, log-scaled, uncapped.
  pattern_grade      Historical reversal rate after runs of the current length.
  matchup_grade      Defense rank for player position vs today's opponent.
  regression_grade   Z-score of recent 10-game mean vs season. Reversion signal.
  composite_grade    Equal-weighted average of all non-NULL components.

Args
----
  --mode      upcoming | intraday | backfill  (default: upcoming)
  --batch N   Backfill mode: max game dates per run (default 10)
  --date      Backfill mode: grade a specific date (YYYY-MM-DD)

Secrets required
----------------
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import math
import os
import time
import logging
from datetime import date

import numpy as np
import pandas as pd
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
BOOKMAKER      = "fanduel"

LOOKBACK_LONG  = 60
LOOKBACK_SHORT = 20
WEIGHT_SHORT   = 0.60
WEIGHT_LONG    = 0.40
MIN_SAMPLE     = 5

SEASON_START        = "2024-10-01"
SEASON_MIN          = 10
RECENT_WINDOW       = 10
TREND_SHORT         = 10
TREND_LONG          = 30
TREND_MIN           = 3
PATTERN_MIN         = 3
BRACKET_STEPS       = 5
BRACKET_INCREMENT   = 1.0

BATCH_DEFAULT = 10

# ---------------------------------------------------------------------------
# Market definitions
# ---------------------------------------------------------------------------
STANDARD_MARKETS = {
    "player_points", "player_rebounds", "player_assists", "player_threes",
    "player_blocks", "player_steals",
    "player_points_rebounds_assists", "player_points_rebounds",
    "player_points_assists", "player_rebounds_assists",
    "player_double_double", "player_triple_double", "player_first_basket",
}

ALTERNATE_MARKETS = {
    "player_points_alternate", "player_rebounds_alternate",
    "player_assists_alternate", "player_threes_alternate",
    "player_blocks_alternate", "player_steals_alternate",
    "player_points_assists_alternate", "player_points_rebounds_alternate",
    "player_rebounds_assists_alternate",
    "player_points_rebounds_assists_alternate",
}

PLAYER_MARKETS = STANDARD_MARKETS | ALTERNATE_MARKETS

ALT_GRIDS = {
    "pts":  [4.5, 9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5],
    "reb":  [3.5, 5.5, 7.5, 9.5, 11.5, 13.5, 15.5],
    "ast":  [1.5, 3.5, 5.5, 7.5, 9.5, 11.5, 13.5],
    "fg3m": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
    "blk":  [0.5, 1.5, 2.5, 3.5],
    "stl":  [0.5, 1.5, 2.5, 3.5],
    "pra":  [9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5, 54.5, 59.5],
    "pr":   [9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5],
    "pa":   [9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5],
    "ra":   [4.5, 9.5, 14.5, 19.5, 24.5],
}

MARKET_STAT_MAP = {
    "player_points":                            "SUM(pts)",
    "player_points_alternate":                  "SUM(pts)",
    "player_rebounds":                          "SUM(reb)",
    "player_rebounds_alternate":                "SUM(reb)",
    "player_assists":                           "SUM(ast)",
    "player_assists_alternate":                 "SUM(ast)",
    "player_threes":                            "SUM(fg3m)",
    "player_threes_alternate":                  "SUM(fg3m)",
    "player_blocks":                            "SUM(blk)",
    "player_blocks_alternate":                  "SUM(blk)",
    "player_steals":                            "SUM(stl)",
    "player_steals_alternate":                  "SUM(stl)",
    "player_points_rebounds_assists":           "SUM(pts) + SUM(reb) + SUM(ast)",
    "player_points_rebounds_assists_alternate": "SUM(pts) + SUM(reb) + SUM(ast)",
    "player_points_rebounds":                   "SUM(pts) + SUM(reb)",
    "player_points_rebounds_alternate":         "SUM(pts) + SUM(reb)",
    "player_points_assists":                    "SUM(pts) + SUM(ast)",
    "player_points_assists_alternate":          "SUM(pts) + SUM(ast)",
    "player_rebounds_assists":                  "SUM(reb) + SUM(ast)",
    "player_rebounds_assists_alternate":        "SUM(reb) + SUM(ast)",
}

MARKET_STAT_COL = {
    "player_points":                            "pts",
    "player_points_alternate":                  "pts",
    "player_rebounds":                          "reb",
    "player_rebounds_alternate":                "reb",
    "player_assists":                           "ast",
    "player_assists_alternate":                 "ast",
    "player_threes":                            "fg3m",
    "player_threes_alternate":                  "fg3m",
    "player_blocks":                            "blk",
    "player_blocks_alternate":                  "blk",
    "player_steals":                            "stl",
    "player_steals_alternate":                  "stl",
    "player_points_rebounds_assists":           "pra",
    "player_points_rebounds_assists_alternate": "pra",
    "player_points_rebounds":                   "pr",
    "player_points_rebounds_alternate":         "pr",
    "player_points_assists":                    "pa",
    "player_points_assists_alternate":          "pa",
    "player_rebounds_assists":                  "ra",
    "player_rebounds_assists_alternate":        "ra",
}

MARKET_DEF_RANK = {
    "player_points":             "rank_pts",
    "player_points_alternate":   "rank_pts",
    "player_rebounds":           "rank_reb",
    "player_rebounds_alternate": "rank_reb",
    "player_assists":            "rank_ast",
    "player_assists_alternate":  "rank_ast",
    "player_threes":             "rank_fg3m",
    "player_threes_alternate":   "rank_fg3m",
    "player_blocks":             "rank_blk",
    "player_blocks_alternate":   "rank_blk",
    "player_steals":             "rank_stl",
    "player_steals_alternate":   "rank_stl",
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
def get_engine(max_retries=3, retry_wait=60):
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=False)
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(f"DB connection attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                log.info(f"Waiting {retry_wait}s...")
                time.sleep(retry_wait)
    raise RuntimeError("Could not connect to Azure SQL after retries.")


# ---------------------------------------------------------------------------
# Schema setup
# ---------------------------------------------------------------------------
def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('common.grade_thresholds', 'U') IS NOT NULL
                DROP TABLE common.grade_thresholds
        """))
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'common')
                EXEC('CREATE SCHEMA common')
        """))
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'common' AND TABLE_NAME = 'daily_grades'
            )
            CREATE TABLE common.daily_grades (
                grade_id          INT IDENTITY(1,1) NOT NULL,
                grade_date        DATE          NOT NULL,
                event_id          VARCHAR(50)   NOT NULL,
                game_id           VARCHAR(15)   NULL,
                player_id         BIGINT        NULL,
                player_name       NVARCHAR(100) NOT NULL,
                market_key        VARCHAR(100)  NOT NULL,
                bookmaker_key     VARCHAR(50)   NOT NULL,
                line_value        DECIMAL(6,1)  NOT NULL,
                hit_rate_60       FLOAT         NULL,
                hit_rate_20       FLOAT         NULL,
                sample_size_60    INT           NULL,
                sample_size_20    INT           NULL,
                weighted_hit_rate FLOAT         NULL,
                grade             FLOAT         NULL,
                trend_grade       FLOAT         NULL,
                momentum_grade    FLOAT         NULL,
                pattern_grade     FLOAT         NULL,
                matchup_grade     FLOAT         NULL,
                regression_grade  FLOAT         NULL,
                composite_grade   FLOAT         NULL,
                hit_rate_opp      FLOAT         NULL,
                sample_size_opp   INT           NULL,
                created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
                CONSTRAINT pk_daily_grades PRIMARY KEY (grade_id),
                CONSTRAINT uq_daily_grades UNIQUE (
                    grade_date, event_id, player_id,
                    market_key, bookmaker_key, line_value
                )
            )
        """))
        # Idempotent migrations for all component + opp columns.
        for col, dtype in [
            ("trend_grade",      "FLOAT"),
            ("momentum_grade",   "FLOAT"),
            ("pattern_grade",    "FLOAT"),
            ("matchup_grade",    "FLOAT"),
            ("regression_grade", "FLOAT"),
            ("composite_grade",  "FLOAT"),
            ("hit_rate_opp",     "FLOAT"),
            ("sample_size_opp",  "INT"),
        ]:
            conn.execute(text(f"""
                IF NOT EXISTS (
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'common' AND TABLE_NAME = 'daily_grades'
                      AND COLUMN_NAME = '{col}'
                )
                ALTER TABLE common.daily_grades ADD {col} {dtype} NULL
            """))
    log.info("Schema verified.")


# ---------------------------------------------------------------------------
# DB fetches
# ---------------------------------------------------------------------------
def fetch_history(engine, player_ids, market_keys, as_of_date):
    """
    Fetch per-game stat history for hit rate computation.
    Returns one row per (player_id, market_key, game_id) with:
      stat_value, in_short_window, opp_team_id.
    The opp_team_id column enables vs-opponent hit rate computation
    without a separate DB round trip.
    """
    gradeable = [m for m in market_keys if m in MARKET_STAT_MAP]
    if not player_ids or not gradeable:
        return pd.DataFrame()

    pid_list = ", ".join(str(int(p)) for p in player_ids)
    expr_to_mkts: dict = {}
    for m in gradeable:
        expr_to_mkts.setdefault(MARKET_STAT_MAP[m], []).append(m)

    branches = []
    for expr, mkts in expr_to_mkts.items():
        mkt_vals = ", ".join(f"('{m}')" for m in mkts)
        branches.append(f"""
            SELECT b.player_id, m.market_key, b.game_date, b.game_id,
                   {expr} AS stat_value,
                   CASE WHEN b.team_id = s.home_team_id
                        THEN s.away_team_id ELSE s.home_team_id
                   END AS opp_team_id
            FROM nba.player_box_score_stats b
            JOIN nba.schedule s ON s.game_id = b.game_id
            CROSS JOIN (SELECT market_key FROM (VALUES {mkt_vals}) AS t(market_key)) m
            WHERE b.player_id IN ({pid_list})
              AND b.game_date < :aod
              AND b.game_date >= DATEADD(day, -:lb_long, :aod)
            GROUP BY b.player_id, b.game_id, b.game_date, b.team_id,
                     s.home_team_id, s.away_team_id, m.market_key
        """)

    union_sql = "\nUNION ALL\n".join(branches)
    sql = text(f"""
        SELECT player_id, market_key, game_date, stat_value, opp_team_id,
               CASE WHEN game_date >= DATEADD(day, -:lb_short, :aod) THEN 1 ELSE 0 END
                   AS in_short_window
        FROM ({union_sql}) AS combined
        WHERE stat_value IS NOT NULL
    """)
    df = pd.read_sql(sql, engine, params={
        "aod": str(as_of_date), "lb_long": LOOKBACK_LONG, "lb_short": LOOKBACK_SHORT,
    })
    log.info(f"  Hit-rate history: {len(df)} rows.")
    return df


def fetch_season_history(engine, player_ids, as_of_date):
    if not player_ids:
        return pd.DataFrame()
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    sql = text(f"""
        SELECT b.player_id, b.game_date, b.game_id,
               SUM(b.pts) AS pts, SUM(b.reb) AS reb, SUM(b.ast) AS ast,
               SUM(b.stl) AS stl, SUM(b.blk) AS blk, SUM(b.fg3m) AS fg3m,
               SUM(b.tov) AS tov
        FROM nba.player_box_score_stats b
        WHERE b.player_id IN ({pid_list})
          AND b.game_date >= :ss AND b.game_date < :aod
        GROUP BY b.player_id, b.game_id, b.game_date
    """)
    df = pd.read_sql(sql, engine, params={"ss": SEASON_START, "aod": str(as_of_date)})
    df["pra"] = df["pts"] + df["reb"] + df["ast"]
    df["pr"]  = df["pts"] + df["reb"]
    df["pa"]  = df["pts"] + df["ast"]
    df["ra"]  = df["reb"] + df["ast"]
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["player_id", "game_date"])
    log.info(f"  Season history: {len(df)} rows.")
    return df


def fetch_opp_info(engine, player_ids, grade_date_str):
    if not player_ids:
        return {}
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    sql = text(f"""
        SELECT p.player_id, p.position,
               CASE WHEN p.team_id = s.home_team_id THEN s.away_team_id
                    ELSE s.home_team_id END AS opp_team_id
        FROM nba.players p
        JOIN nba.schedule s
          ON (s.home_team_id = p.team_id OR s.away_team_id = p.team_id)
         AND CAST(s.game_date AS DATE) = :gd
        WHERE p.player_id IN ({pid_list})
    """)
    df = pd.read_sql(sql, engine, params={"gd": grade_date_str})
    result = {}
    for _, row in df.iterrows():
        result[int(row["player_id"])] = {
            "position":    row["position"] or "",
            "opp_team_id": int(row["opp_team_id"]) if pd.notna(row["opp_team_id"]) else None,
        }
    return result


def fetch_matchup_defense(engine, opp_player_pairs):
    unique = list(set(
        (tid, pg) for tid, pg in opp_player_pairs
        if tid is not None and pg is not None
    ))
    if not unique:
        return {}

    values_rows = ", ".join(f"({tid}, '{pg}')" for tid, pg in unique)
    sql = text(f"""
        WITH season_start AS (
            SELECT CAST(CAST(
                CASE WHEN MONTH(GETUTCDATE()) < 10
                    THEN YEAR(GETUTCDATE()) - 1
                    ELSE YEAR(GETUTCDATE()) END
            AS VARCHAR(4)) + '-10-01' AS DATE) AS dt
        ),
        game_totals AS (
            SELECT pbs.player_id, pbs.game_id,
                   CASE WHEN pbs.team_id = s.home_team_id
                        THEN s.away_team_id ELSE s.home_team_id END AS opp_team_id,
                   SUM(pbs.pts) AS pts, SUM(pbs.reb) AS reb, SUM(pbs.ast) AS ast,
                   SUM(pbs.stl) AS stl, SUM(pbs.blk) AS blk,
                   SUM(pbs.fg3m) AS fg3m, SUM(pbs.tov) AS tov
            FROM nba.player_box_score_stats pbs
            JOIN nba.schedule s ON s.game_id = pbs.game_id
            WHERE s.game_date >= (SELECT dt FROM season_start)
            GROUP BY pbs.player_id, pbs.game_id, pbs.team_id,
                     s.home_team_id, s.away_team_id
        ),
        pos_filtered AS (
            SELECT gt.*, LEFT(p.position, 1) AS pos_group
            FROM game_totals gt
            JOIN nba.players p ON p.player_id = gt.player_id
            WHERE LEFT(p.position, 1) IN ('G','F','C')
        ),
        target_pairs AS (
            SELECT opp_team_id, pos_group
            FROM (VALUES {values_rows}) AS t(opp_team_id, pos_group)
        ),
        team_defense AS (
            SELECT pf.opp_team_id, pf.pos_group,
                   COUNT(*) AS games_defended,
                   AVG(CAST(pf.pts  AS FLOAT)) AS avg_pts,
                   AVG(CAST(pf.reb  AS FLOAT)) AS avg_reb,
                   AVG(CAST(pf.ast  AS FLOAT)) AS avg_ast,
                   AVG(CAST(pf.stl  AS FLOAT)) AS avg_stl,
                   AVG(CAST(pf.blk  AS FLOAT)) AS avg_blk,
                   AVG(CAST(pf.fg3m AS FLOAT)) AS avg_fg3m,
                   AVG(CAST(pf.tov  AS FLOAT)) AS avg_tov
            FROM pos_filtered pf
            JOIN target_pairs tp
              ON tp.opp_team_id = pf.opp_team_id AND tp.pos_group = pf.pos_group
            GROUP BY pf.opp_team_id, pf.pos_group
        ),
        all_teams AS (
            SELECT pos_group, opp_team_id, games_defended,
                   avg_pts,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_pts  DESC) AS rank_pts,
                   avg_reb,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_reb  DESC) AS rank_reb,
                   avg_ast,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_ast  DESC) AS rank_ast,
                   avg_stl,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_stl  DESC) AS rank_stl,
                   avg_blk,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_blk  DESC) AS rank_blk,
                   avg_fg3m, RANK() OVER (PARTITION BY pos_group ORDER BY avg_fg3m DESC) AS rank_fg3m,
                   avg_tov,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_tov  DESC) AS rank_tov
            FROM team_defense
        )
        SELECT * FROM all_teams
    """)
    df = pd.read_sql(sql, engine)
    result = {}
    for _, row in df.iterrows():
        result[(int(row["opp_team_id"]), str(row["pos_group"]))] = row.to_dict()
    log.info(f"  Matchup defense: {len(result)} team-position pairs.")
    return result


# ---------------------------------------------------------------------------
# Props building
# ---------------------------------------------------------------------------
MARKET_LIST_SQL = ", ".join(f"'{m}'" for m in PLAYER_MARKETS)

BASE_PROPS_SELECT = """
    SELECT DISTINCT
        pp.event_id, pm.player_id, pp.player_name, pp.market_key,
        pp.bookmaker_key, pp.outcome_point AS line_value, egm.game_id,
        pp.outcome_price AS over_price
    FROM {props_table} pp
    JOIN odds.event_game_map egm
        ON egm.event_id = pp.event_id AND egm.sport_key = 'basketball_nba'
       AND egm.game_id IS NOT NULL
    JOIN odds.player_map pm
        ON pm.odds_player_name = pp.player_name AND pm.sport_key = pp.sport_key
       AND pm.player_id IS NOT NULL
    WHERE pp.sport_key = 'basketball_nba'
      AND pp.bookmaker_key = :bk AND pp.outcome_name = 'Over'
      AND pp.outcome_point IS NOT NULL
      AND pp.market_key IN ({mkt_list})
      {date_filter}
"""


def fetch_posted_props(engine, table="odds.upcoming_player_props", date_filter="", params=None):
    sql = text(BASE_PROPS_SELECT.format(
        props_table=table, mkt_list=MARKET_LIST_SQL, date_filter=date_filter,
    ))
    return pd.read_sql(sql, engine, params={**(params or {}), "bk": BOOKMAKER})


def build_standard_props(posted_df):
    std = posted_df[posted_df["market_key"].isin(STANDARD_MARKETS)].copy()
    if std.empty:
        return pd.DataFrame()

    std = std.drop_duplicates(subset=["player_id", "market_key"])

    rows = []
    for _, r in std.iterrows():
        center = float(r["line_value"])
        for step in range(-BRACKET_STEPS, BRACKET_STEPS + 1):
            lv = round(center + step * BRACKET_INCREMENT, 1)
            if lv < 0.5:
                continue
            rows.append({
                "event_id":      r["event_id"],
                "player_id":     r["player_id"],
                "player_name":   r["player_name"],
                "market_key":    r["market_key"],
                "bookmaker_key": r["bookmaker_key"],
                "line_value":    lv,
                "game_id":       r["game_id"],
                "over_price":    r["over_price"] if step == 0 else None,
            })
    return pd.DataFrame(rows).drop_duplicates(
        subset=["player_id", "market_key", "line_value"]
    )


def build_alt_props(posted_df, active_players_df, event_map):
    alt_posted = posted_df[posted_df["market_key"].isin(ALTERNATE_MARKETS)].copy()
    if alt_posted.empty:
        return pd.DataFrame()

    posted_set = set(
        zip(alt_posted["player_id"].astype(int),
            alt_posted["market_key"],
            alt_posted["line_value"].astype(float))
    )
    price_lookup = {
        (int(r["player_id"]), r["market_key"], float(r["line_value"])): r["over_price"]
        for _, r in alt_posted.iterrows()
    }

    rows = []
    for _, p in active_players_df.iterrows():
        pid     = int(p["player_id"])
        pname   = p["player_name"]
        team_id = int(p["team_id"])
        ev_info = event_map.get(team_id)
        if ev_info is None:
            continue
        event_id, game_id = ev_info

        for mkt in ALTERNATE_MARKETS:
            stat_col = MARKET_STAT_COL.get(mkt)
            if stat_col is None:
                continue
            for lv in ALT_GRIDS.get(stat_col, []):
                if (pid, mkt, float(lv)) not in posted_set:
                    continue
                rows.append({
                    "event_id":      event_id,
                    "player_id":     pid,
                    "player_name":   pname,
                    "market_key":    mkt,
                    "bookmaker_key": BOOKMAKER,
                    "line_value":    float(lv),
                    "game_id":       game_id,
                    "over_price":    price_lookup.get((pid, mkt, float(lv))),
                })

    return pd.DataFrame(rows).drop_duplicates(
        subset=["player_id", "market_key", "line_value"]
    ) if rows else pd.DataFrame()


def fetch_active_players_today(engine, grade_date_str):
    sql = text("""
        SELECT p.player_id, p.player_name, p.team_id
        FROM nba.players p
        JOIN nba.schedule s
          ON (s.home_team_id = p.team_id OR s.away_team_id = p.team_id)
         AND CAST(s.game_date AS DATE) = :gd
        WHERE p.roster_status = 1
    """)
    return pd.read_sql(sql, engine, params={"gd": grade_date_str})


def fetch_event_map_today(engine, grade_date_str):
    sql = text("""
        SELECT egm.event_id, egm.game_id,
               s.home_team_id, s.away_team_id
        FROM nba.schedule s
        JOIN odds.event_game_map egm ON egm.game_id = s.game_id
        JOIN odds.upcoming_events ue ON ue.event_id = egm.event_id
        WHERE CAST(s.game_date AS DATE) = :gd
          AND egm.game_id IS NOT NULL
    """)
    df = pd.read_sql(sql, engine, params={"gd": grade_date_str})
    result = {}
    for _, row in df.iterrows():
        ev = (row["event_id"], row["game_id"])
        result[int(row["home_team_id"])] = ev
        result[int(row["away_team_id"])] = ev
    return result


# ---------------------------------------------------------------------------
# Hit rate computation
# ---------------------------------------------------------------------------
def compute_all_hit_rates(props_df, history_df, opp_info):
    """
    Compute hit_rate_60, hit_rate_20, weighted_hit_rate, grade (existing),
    and hit_rate_opp, sample_size_opp (new: vs today's opponent only).

    history_df must contain an opp_team_id column (added in fetch_history).
    opp_info is the dict {player_id: {opp_team_id, position}} from fetch_opp_info.
    """
    result = props_df.copy()
    all_grade_cols = (
        "hit_rate_60", "sample_size_60", "hit_rate_20", "sample_size_20",
        "weighted_hit_rate", "grade", "hit_rate_opp", "sample_size_opp",
    )
    if history_df.empty:
        for col in all_grade_cols:
            result[col] = None
        return result

    history = history_df.copy()
    history["stat_value"] = history["stat_value"].astype(float)
    result["line_value"]  = result["line_value"].astype(float)

    lines  = result[["player_id", "market_key", "line_value"]].drop_duplicates()
    merged = history.merge(lines, on=["player_id", "market_key"], how="inner")
    merged["hit"] = (merged["stat_value"] > merged["line_value"]).astype(int)

    # --- 60-day and 20-day hit rates (unchanged) ---
    g60 = (merged.groupby(["player_id", "market_key", "line_value"])
           .agg(sample_size_60=("hit", "count"), hits_60=("hit", "sum")).reset_index())
    g60["hit_rate_60"] = g60["hits_60"] / g60["sample_size_60"]

    g20 = (merged[merged["in_short_window"] == 1]
           .groupby(["player_id", "market_key", "line_value"])
           .agg(sample_size_20=("hit", "count"), hits_20=("hit", "sum")).reset_index())
    g20["hit_rate_20"] = g20["hits_20"] / g20["sample_size_20"]

    result = result.merge(
        g60[["player_id", "market_key", "line_value", "hit_rate_60", "sample_size_60"]],
        on=["player_id", "market_key", "line_value"], how="left")
    result = result.merge(
        g20[["player_id", "market_key", "line_value", "hit_rate_20", "sample_size_20"]],
        on=["player_id", "market_key", "line_value"], how="left")

    result["sample_size_60"] = result["sample_size_60"].fillna(0).astype(int)
    result["sample_size_20"] = result["sample_size_20"].fillna(0).astype(int)

    use_blend = (result["sample_size_20"] >= MIN_SAMPLE) & result["hit_rate_20"].notna()
    result["weighted_hit_rate"] = result["hit_rate_60"]
    result.loc[use_blend, "weighted_hit_rate"] = (
        WEIGHT_SHORT * result.loc[use_blend, "hit_rate_20"]
        + WEIGHT_LONG * result.loc[use_blend, "hit_rate_60"]
    )
    result["grade"] = result["weighted_hit_rate"].apply(
        lambda x: round(x * 100, 1) if pd.notna(x) else None)
    for col in ("weighted_hit_rate", "hit_rate_60", "hit_rate_20"):
        result[col] = result[col].apply(lambda x: round(x, 4) if pd.notna(x) else None)

    # --- vs-opponent hit rate ---
    # Build a lookup: player_id -> today's opp_team_id
    player_opp = {
        pid: int(info["opp_team_id"])
        for pid, info in opp_info.items()
        if info.get("opp_team_id") is not None
    }

    if player_opp and "opp_team_id" in merged.columns:
        # Keep only history rows where the player faced today's opponent
        merged["today_opp"] = merged["player_id"].map(player_opp)
        opp_rows = merged[
            merged["today_opp"].notna() &
            (merged["opp_team_id"] == merged["today_opp"])
        ]
        if not opp_rows.empty:
            g_opp = (
                opp_rows.groupby(["player_id", "market_key", "line_value"])
                .agg(sample_size_opp=("hit", "count"), hits_opp=("hit", "sum"))
                .reset_index()
            )
            g_opp["hit_rate_opp"] = (
                g_opp["hits_opp"] / g_opp["sample_size_opp"]
            ).apply(lambda x: round(x, 4) if pd.notna(x) else None)
            result = result.merge(
                g_opp[["player_id", "market_key", "line_value",
                        "hit_rate_opp", "sample_size_opp"]],
                on=["player_id", "market_key", "line_value"], how="left"
            )
            result["sample_size_opp"] = result["sample_size_opp"].fillna(0).astype(int)
        else:
            result["hit_rate_opp"]   = None
            result["sample_size_opp"] = 0
    else:
        result["hit_rate_opp"]   = None
        result["sample_size_opp"] = 0

    # Replace 0-sample opp rows with NULL for cleaner display
    if "sample_size_opp" in result.columns:
        result.loc[result["sample_size_opp"] == 0, "hit_rate_opp"]   = None
        result.loc[result["sample_size_opp"] == 0, "sample_size_opp"] = None

    return result


# ---------------------------------------------------------------------------
# Vectorized component precomputation
# ---------------------------------------------------------------------------
def _safe(v):
    if v is None:
        return None
    try:
        if math.isnan(v) or math.isinf(v):
            return None
        return round(float(v), 1)
    except Exception:
        return None


def precompute_player_market_grades(season_df, props_df):
    combos = props_df[["player_id", "market_key"]].drop_duplicates()
    result = {}

    for _, row in combos.iterrows():
        pid      = int(row["player_id"])
        mkt      = row["market_key"]
        stat_col = MARKET_STAT_COL.get(mkt)
        if stat_col is None:
            result[(pid, mkt)] = {"trend_grade": None, "regression_grade": None}
            continue

        pdf = season_df[season_df["player_id"] == pid].sort_values("game_date")
        if pdf.empty or stat_col not in pdf.columns:
            result[(pid, mkt)] = {"trend_grade": None, "regression_grade": None}
            continue

        vals = pdf[stat_col].dropna().values

        trend = None
        if len(vals) >= TREND_MIN:
            short = vals[-TREND_SHORT:] if len(vals) >= TREND_SHORT else vals
            long  = vals[-TREND_LONG:]  if len(vals) >= TREND_LONG  else vals
            if len(short) >= TREND_MIN:
                sm, lm = float(np.mean(short)), float(np.mean(long))
                if lm != 0:
                    trend = _safe(max(0.0, min(100.0, 50.0 + (sm - lm) / lm * 150.0)))

        regression = None
        if len(vals) >= SEASON_MIN:
            recent = vals[-RECENT_WINDOW:] if len(vals) >= RECENT_WINDOW else vals[-max(1, len(vals)//2):]
            if len(recent) >= 3:
                s_std = float(np.std(vals))
                if s_std >= 0.01:
                    z = (float(np.mean(recent)) - float(np.mean(vals))) / s_std
                    regression = _safe(max(0.0, min(100.0, 50.0 - z * 25.0)))

        result[(pid, mkt)] = {"trend_grade": trend, "regression_grade": regression}

    return result


def precompute_line_grades(season_df, props_df):
    combos = props_df[["player_id", "market_key", "line_value"]].drop_duplicates()
    result = {}

    player_groups = {
        pid: grp.sort_values("game_date")
        for pid, grp in season_df.groupby("player_id")
    }

    for _, row in combos.iterrows():
        pid      = int(row["player_id"])
        mkt      = row["market_key"]
        lv       = float(row["line_value"])
        key      = (pid, mkt, lv)
        stat_col = MARKET_STAT_COL.get(mkt)
        pdf      = player_groups.get(pid)

        if stat_col is None or pdf is None or pdf.empty or stat_col not in pdf.columns:
            result[key] = {"momentum_grade": None, "pattern_grade": None}
            continue

        vals = pdf[stat_col].dropna().values
        if len(vals) == 0:
            result[key] = {"momentum_grade": None, "pattern_grade": None}
            continue

        hits = [bool(v > lv) for v in vals]

        momentum = None
        if hits:
            last   = hits[-1]
            streak = 0
            for h in reversed(hits):
                if h == last:
                    streak += 1
                else:
                    break
            direction = 1 if last else -1
            momentum = _safe(max(0.0, min(100.0, 50.0 + direction * 25.0 * math.log2(1 + streak))))

        pattern = None
        if len(hits) >= 2:
            last = hits[-1]
            current_streak = 0
            for h in reversed(hits):
                if h == last:
                    current_streak += 1
                else:
                    break

            reversals = 0
            instances = 0
            for end_idx in range(current_streak - 1, len(hits) - current_streak):
                window = hits[end_idx - current_streak + 1:end_idx + 1]
                if len(window) != current_streak:
                    continue
                if all(h == last for h in window):
                    before_idx = end_idx - current_streak
                    if before_idx >= 0 and hits[before_idx] == last:
                        continue
                    next_idx = end_idx + 1
                    if next_idx < len(hits):
                        instances += 1
                        if hits[next_idx] != last:
                            reversals += 1

            if instances >= PATTERN_MIN:
                rate  = reversals / instances
                score = 50.0 - (rate - 0.5) * 100.0 if last else 50.0 + (rate - 0.5) * 100.0
                pattern = _safe(max(0.0, min(100.0, score)))

        result[key] = {"momentum_grade": momentum, "pattern_grade": pattern}

    return result


def compute_matchup_grade(market_key, opp_team_id, position, matchup_cache):
    if opp_team_id is None or not position:
        return None
    rank_col = MARKET_DEF_RANK.get(market_key)
    if rank_col is None:
        return None
    pg = ("G" if position.startswith("G") else
          "F" if position.startswith("F") else
          "C" if position.startswith("C") else None)
    if pg is None:
        return None
    defense = matchup_cache.get((int(opp_team_id), pg))
    if defense is None:
        return None
    rank = defense.get(rank_col)
    if rank is None or (isinstance(rank, float) and math.isnan(rank)):
        return None
    return _safe(max(0.0, min(100.0, (30 - int(rank) + 1) / 30.0 * 100.0)))


def compute_composite(whr, trend, momentum, pattern, matchup, regression):
    parts = []
    if whr is not None:
        parts.append(whr * 100.0)
    for v in (trend, momentum, pattern, matchup, regression):
        if v is not None:
            parts.append(float(v))
    return _safe(sum(parts) / len(parts)) if parts else None


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
MERGE_KEY = ["grade_date", "event_id", "player_id", "market_key", "bookmaker_key", "line_value"]


def upsert_grades(engine, rows):
    if not rows:
        return 0

    seen = {}
    for r in rows:
        k = (r["grade_date"], r["event_id"], r["player_id"],
             r["market_key"], r["bookmaker_key"], r["line_value"])
        seen[k] = r
    rows = list(seen.values())

    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#stage_grades') IS NOT NULL
                DROP TABLE #stage_grades
        """))
        conn.execute(text("""
            CREATE TABLE #stage_grades (
                grade_date DATE, event_id VARCHAR(50), game_id VARCHAR(15),
                player_id BIGINT, player_name NVARCHAR(100),
                market_key VARCHAR(100), bookmaker_key VARCHAR(50),
                line_value DECIMAL(6,1),
                hit_rate_60 FLOAT, hit_rate_20 FLOAT,
                sample_size_60 INT, sample_size_20 INT,
                weighted_hit_rate FLOAT, grade FLOAT,
                trend_grade FLOAT, momentum_grade FLOAT, pattern_grade FLOAT,
                matchup_grade FLOAT, regression_grade FLOAT, composite_grade FLOAT,
                hit_rate_opp FLOAT, sample_size_opp INT
            )
        """))
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            conn.exec_driver_sql(
                "INSERT INTO #stage_grades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(r["grade_date"], r["event_id"], r["game_id"],
                  r["player_id"], r["player_name"], r["market_key"],
                  r["bookmaker_key"], r["line_value"],
                  r["hit_rate_60"], r["hit_rate_20"],
                  r["sample_size_60"], r["sample_size_20"],
                  r["weighted_hit_rate"], r["grade"],
                  r["trend_grade"], r["momentum_grade"], r["pattern_grade"],
                  r["matchup_grade"], r["regression_grade"], r["composite_grade"],
                  r["hit_rate_opp"], r["sample_size_opp"])
                 for r in chunk]
            )
        conn.execute(text("""
            MERGE common.daily_grades AS t
            USING #stage_grades AS s
            ON (t.grade_date = s.grade_date AND t.event_id = s.event_id
                AND t.player_id = s.player_id AND t.market_key = s.market_key
                AND t.bookmaker_key = s.bookmaker_key AND t.line_value = s.line_value)
            WHEN MATCHED THEN UPDATE SET
                t.game_id = s.game_id,
                t.hit_rate_60 = s.hit_rate_60, t.hit_rate_20 = s.hit_rate_20,
                t.sample_size_60 = s.sample_size_60, t.sample_size_20 = s.sample_size_20,
                t.weighted_hit_rate = s.weighted_hit_rate, t.grade = s.grade,
                t.trend_grade = s.trend_grade, t.momentum_grade = s.momentum_grade,
                t.pattern_grade = s.pattern_grade, t.matchup_grade = s.matchup_grade,
                t.regression_grade = s.regression_grade, t.composite_grade = s.composite_grade,
                t.hit_rate_opp = s.hit_rate_opp, t.sample_size_opp = s.sample_size_opp
            WHEN NOT MATCHED THEN INSERT (
                grade_date, event_id, game_id, player_id, player_name,
                market_key, bookmaker_key, line_value,
                hit_rate_60, hit_rate_20, sample_size_60, sample_size_20,
                weighted_hit_rate, grade, trend_grade, momentum_grade,
                pattern_grade, matchup_grade, regression_grade, composite_grade,
                hit_rate_opp, sample_size_opp
            ) VALUES (
                s.grade_date, s.event_id, s.game_id, s.player_id, s.player_name,
                s.market_key, s.bookmaker_key, s.line_value,
                s.hit_rate_60, s.hit_rate_20, s.sample_size_60, s.sample_size_20,
                s.weighted_hit_rate, s.grade, s.trend_grade, s.momentum_grade,
                s.pattern_grade, s.matchup_grade, s.regression_grade, s.composite_grade,
                s.hit_rate_opp, s.sample_size_opp
            );
        """))
    return len(rows)


# ---------------------------------------------------------------------------
# Core: grade a set of props rows
# ---------------------------------------------------------------------------
def grade_props_for_date(engine, grade_date_str, props_df):
    if props_df.empty:
        log.info(f"  {grade_date_str}: no props.")
        return 0

    props_df = props_df.drop_duplicates(
        subset=["player_id", "market_key", "line_value"]
    ).copy()

    player_ids  = props_df["player_id"].dropna().unique().tolist()
    market_keys = props_df["market_key"].dropna().unique().tolist()

    history_df = fetch_history(engine, player_ids, market_keys, grade_date_str)
    season_df  = fetch_season_history(engine, player_ids, grade_date_str)
    opp_info   = fetch_opp_info(engine, player_ids, grade_date_str)

    matchup_pairs = []
    for pid, info in opp_info.items():
        pos = info.get("position", "")
        pg  = ("G" if pos.startswith("G") else "F" if pos.startswith("F") else
               "C" if pos.startswith("C") else None)
        if pg and info.get("opp_team_id"):
            matchup_pairs.append((int(info["opp_team_id"]), pg))
    matchup_cache = fetch_matchup_defense(engine, matchup_pairs)

    # Pass opp_info so compute_all_hit_rates can compute vs-opp rates
    graded_df   = compute_all_hit_rates(props_df, history_df, opp_info)
    pm_grades   = precompute_player_market_grades(season_df, graded_df)
    line_grades = precompute_line_grades(season_df, graded_df)

    rows = []
    for _, r in graded_df.iterrows():
        pid = r["player_id"]
        if pd.isna(pid):
            continue
        pid_int  = int(pid)
        mkt      = r["market_key"]
        lv       = float(r["line_value"])
        info     = opp_info.get(pid_int, {})
        position = info.get("position", "")
        opp_id   = info.get("opp_team_id")
        whr      = r.get("weighted_hit_rate")
        whr      = whr if pd.notna(whr) else None

        pm = pm_grades.get((pid_int, mkt), {})
        lk = line_grades.get((pid_int, mkt, lv), {})

        trend      = pm.get("trend_grade")
        regression = pm.get("regression_grade")
        momentum   = lk.get("momentum_grade")
        pattern    = lk.get("pattern_grade")
        matchup    = compute_matchup_grade(mkt, opp_id, position, matchup_cache)
        composite  = compute_composite(whr, trend, momentum, pattern, matchup, regression)

        hr_opp  = r.get("hit_rate_opp")
        hr_opp  = hr_opp if pd.notna(hr_opp) else None
        n_opp   = r.get("sample_size_opp")
        n_opp   = int(n_opp) if pd.notna(n_opp) and n_opp else None

        rows.append({
            "grade_date":        grade_date_str,
            "event_id":          r["event_id"],
            "game_id":           r.get("game_id"),
            "player_id":         pid_int,
            "player_name":       r["player_name"],
            "market_key":        mkt,
            "bookmaker_key":     r["bookmaker_key"],
            "line_value":        lv,
            "hit_rate_60":       r.get("hit_rate_60")     if pd.notna(r.get("hit_rate_60"))     else None,
            "hit_rate_20":       r.get("hit_rate_20")     if pd.notna(r.get("hit_rate_20"))     else None,
            "sample_size_60":    int(r["sample_size_60"]) if pd.notna(r.get("sample_size_60")) else 0,
            "sample_size_20":    int(r["sample_size_20"]) if pd.notna(r.get("sample_size_20")) else 0,
            "weighted_hit_rate": whr,
            "grade":             r.get("grade")           if pd.notna(r.get("grade"))           else None,
            "trend_grade":       trend,
            "momentum_grade":    momentum,
            "pattern_grade":     pattern,
            "matchup_grade":     matchup,
            "regression_grade":  regression,
            "composite_grade":   composite,
            "hit_rate_opp":      hr_opp,
            "sample_size_opp":   n_opp,
        })

    written = upsert_grades(engine, rows)
    graded  = sum(1 for r in rows if r["composite_grade"] is not None)
    with_opp = sum(1 for r in rows if r["hit_rate_opp"] is not None)
    log.info(f"  {grade_date_str}: {written} rows written, {graded} with composite, {with_opp} with opp rate.")
    return written


# ---------------------------------------------------------------------------
# Mode runners
# ---------------------------------------------------------------------------
def run_upcoming(engine):
    today = str(date.today())
    log.info(f"Upcoming mode: {today}")

    posted    = fetch_posted_props(engine)
    std_props = build_standard_props(posted)

    active    = fetch_active_players_today(engine, today)
    event_map = fetch_event_map_today(engine, today)
    alt_props = build_alt_props(posted, active, event_map)

    all_props = pd.concat(
        [p for p in [std_props, alt_props] if not p.empty], ignore_index=True
    )
    if all_props.empty:
        log.info("No props to grade.")
        return

    all_props = all_props.drop_duplicates(subset=["player_id", "market_key", "line_value"])
    log.info(f"  {len(all_props)} prop rows ({len(std_props)} standard, {len(alt_props)} alternate).")
    grade_props_for_date(engine, today, all_props)


def run_intraday(engine):
    today = str(date.today())
    log.info(f"Intraday mode: {today}")

    posted     = fetch_posted_props(engine)
    std_posted = posted[posted["market_key"].isin(STANDARD_MARKETS)].copy()
    if std_posted.empty:
        log.info("  No standard lines posted.")
        return

    player_ids = std_posted["player_id"].dropna().unique().tolist()
    if not player_ids:
        return
    pid_list     = ", ".join(str(int(p)) for p in player_ids)
    std_mkt_list = ", ".join(f"'{m}'" for m in STANDARD_MARKETS)

    last_graded = pd.read_sql(text(f"""
        SELECT player_id, market_key, line_value AS last_line
        FROM (
            SELECT player_id, market_key, line_value,
                   ROW_NUMBER() OVER (
                       PARTITION BY player_id, market_key ORDER BY grade_id DESC
                   ) AS rn
            FROM common.daily_grades
            WHERE grade_date = :gd AND player_id IN ({pid_list})
              AND market_key IN ({std_mkt_list}) AND bookmaker_key = :bk
        ) ranked
        WHERE rn = 1
    """), engine, params={"gd": today, "bk": BOOKMAKER})

    current = std_posted[["player_id", "market_key", "line_value"]].rename(
        columns={"line_value": "current_line"}
    )
    if not last_graded.empty:
        merged = current.merge(last_graded, on=["player_id", "market_key"], how="left")
        moved  = merged[
            merged["last_line"].isna() |
            (merged["current_line"].astype(float) != merged["last_line"].astype(float))
        ]
    else:
        moved = current.copy()

    if moved.empty:
        log.info("  No line movement. Nothing to do.")
        return

    log.info(f"  {len(moved)} player-market pairs with movement.")
    moved_posted = std_posted.merge(
        moved[["player_id", "market_key"]], on=["player_id", "market_key"], how="inner"
    )
    bracket = build_standard_props(moved_posted)
    if bracket.empty:
        return
    grade_props_for_date(engine, today, bracket)


def run_backfill(engine, batch_size, specific_date=None):
    if specific_date:
        work_dates = [specific_date]
    else:
        df = pd.read_sql(text("""
            SELECT DISTINCT CAST(egm.game_date AS DATE) AS game_date
            FROM odds.player_props pp
            JOIN odds.event_game_map egm
              ON egm.event_id = pp.event_id AND egm.game_id IS NOT NULL
            WHERE pp.sport_key = 'basketball_nba' AND pp.bookmaker_key = :bk
              AND pp.outcome_name = 'Over' AND pp.outcome_point IS NOT NULL
              AND egm.game_date IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM common.daily_grades g
                  WHERE g.grade_date = CAST(egm.game_date AS DATE)
              )
            ORDER BY game_date ASC
        """), engine, params={"bk": BOOKMAKER})
        work_dates = df["game_date"].astype(str).tolist()[:batch_size]

    if not work_dates:
        log.info("Backfill: nothing to do.")
        return

    log.info(f"Backfill: {len(work_dates)} date(s): {work_dates[0]} to {work_dates[-1]}")
    total = 0
    for gd in work_dates:
        props = fetch_posted_props(
            engine,
            table="odds.player_props",
            date_filter="AND CAST(egm.game_date AS DATE) = :gd",
            params={"gd": gd},
        )
        total += grade_props_for_date(engine, gd, props)
    log.info(f"Backfill complete. {total} total rows written.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA prop grading model")
    parser.add_argument("--mode", choices=["upcoming", "intraday", "backfill"],
                        default="upcoming")
    parser.add_argument("--batch", type=int, default=BATCH_DEFAULT)
    parser.add_argument("--date",  type=str, default=None)
    args = parser.parse_args()

    engine = get_engine()
    ensure_tables(engine)

    if args.mode == "upcoming":
        run_upcoming(engine)
    elif args.mode == "intraday":
        run_intraday(engine)
    else:
        run_backfill(engine, batch_size=args.batch, specific_date=args.date)

    log.info("Done.")


if __name__ == "__main__":
    main()
