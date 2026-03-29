"""
grade_props.py

NBA prop grading model.

Grades are computed per player, per market, per bookmaker, per line value,
anchored to the actual odds posted for a specific game. Every grade row ties
back to an event_id and game_id so results can be evaluated against what
actually happened.

Component grades
----------------
  weighted_hit_rate  Blended 20/60-day hit rate (existing). Primary signal.
  trend_grade        Direction of hit rate: last-10 vs last-30 window.
                     Centered at 50. Above 50 = improving trend.
  momentum_grade     Uncapped consecutive game streak on the specific line.
                     Diminishing returns curve (log-scaled). 50 = neutral.
  pattern_grade      Historical recurrence: how often does a run of this
                     length end with a reversal? Noisy until season+ history
                     accumulates per player per market.
  matchup_grade      Defense rank for the player's position group vs today's
                     opponent for the relevant stat. Rank 1 = 100 (best
                     matchup for overs). Rank 30 = 0.
  regression_grade   Z-score of recent 10-game mean vs full season.
                     High z-score (running hot) = low regression grade.
  composite_grade    Equal-weighted average of all non-NULL components
                     including weighted_hit_rate. NULL components excluded
                     from the denominator.

Grade formula (hit rate component)
-----------------------------------
  hit_rate_60  = hits / games where stat > line, over prior 60 calendar days
  hit_rate_20  = hits / games where stat > line, over prior 20 calendar days
  weighted_hit_rate = (0.60 * hit_rate_20) + (0.40 * hit_rate_60)
  grade        = weighted_hit_rate * 100, rounded to 1 decimal

  If sample_size_20 < MIN_SAMPLE, weighted blend falls back to hit_rate_60
  only. Grade is always written regardless of sample size.

Performance design
------------------
Per grade date, the script issues exactly these database round trips:

  1. Fetch FanDuel player prop lines (deduplicated).
  2. Fetch 60-day hit-rate history (existing bulk query).
  3. Fetch full-season game totals (for regression + pattern + momentum).
  4. Fetch matchup defense ranks for all unique (opp_team_id, pos_group)
     pairs in the grade set. One query, results joined in pandas.
  5. Write all grade rows in one batched upsert.

All component computation is in-memory pandas after those fetches.

Modes
-----
  upcoming  Grades today's lines from odds.upcoming_player_props.
  backfill  Works through historical game dates, oldest first.

Tables written
--------------
  common.daily_grades

Args
----
  --mode      upcoming | backfill  (default: upcoming)
  --batch N   Max game dates per run in backfill mode (default: 10)
  --date      Grade a specific date only (YYYY-MM-DD, backfill mode only)

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

LOOKBACK_LONG  = 60    # calendar days for hit rate long window
LOOKBACK_SHORT = 20    # calendar days for hit rate short window
WEIGHT_SHORT   = 0.60
WEIGHT_LONG    = 0.40
MIN_SAMPLE     = 5     # minimum games for short-window blend

TREND_LONG     = 30    # days for trend baseline
TREND_SHORT    = 10    # days for trend recent window
TREND_MIN      = 3     # minimum games in short window to compute trend

SEASON_START   = "2024-10-01"   # hardcoded current NBA season
SEASON_MIN     = 10             # minimum season games for regression grade
RECENT_WINDOW  = 10             # games for regression z-score and trend
PATTERN_MIN_INSTANCES = 3       # minimum prior occurrences for pattern grade

BATCH_DEFAULT  = 10

PLAYER_MARKETS = {
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
    "player_points_rebounds_assists",
    "player_points_rebounds",
    "player_points_assists",
    "player_rebounds_assists",
    "player_double_double",
    "player_triple_double",
    "player_first_basket",
    "player_points_alternate",
    "player_rebounds_alternate",
    "player_assists_alternate",
    "player_threes_alternate",
    "player_blocks_alternate",
    "player_steals_alternate",
    "player_points_assists_alternate",
    "player_points_rebounds_alternate",
    "player_rebounds_assists_alternate",
    "player_points_rebounds_assists_alternate",
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

# Maps market_key to the stat column name used in the season history frame.
# season_history has columns: player_id, game_date, game_id, pts, reb, ast,
# stl, blk, fg3m, tov — one row per player per game (season totals).
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

# Map each market to its defense stat column in the matchup defense results.
MARKET_DEF_STAT = {
    "player_points":                            "avg_pts",
    "player_points_alternate":                  "avg_pts",
    "player_rebounds":                          "avg_reb",
    "player_rebounds_alternate":                "avg_reb",
    "player_assists":                           "avg_ast",
    "player_assists_alternate":                 "avg_ast",
    "player_threes":                            "avg_fg3m",
    "player_threes_alternate":                  "avg_fg3m",
    "player_blocks":                            "avg_blk",
    "player_blocks_alternate":                  "avg_blk",
    "player_steals":                            "avg_stl",
    "player_steals_alternate":                  "avg_stl",
    "player_points_rebounds_assists":           None,
    "player_points_rebounds_assists_alternate": None,
    "player_points_rebounds":                   None,
    "player_points_rebounds_alternate":         None,
    "player_points_assists":                    None,
    "player_points_assists_alternate":          None,
    "player_rebounds_assists":                  None,
    "player_rebounds_assists_alternate":        None,
}

MARKET_DEF_RANK = {
    "player_points":            "rank_pts",
    "player_points_alternate":  "rank_pts",
    "player_rebounds":          "rank_reb",
    "player_rebounds_alternate":"rank_reb",
    "player_assists":           "rank_ast",
    "player_assists_alternate": "rank_ast",
    "player_threes":            "rank_fg3m",
    "player_threes_alternate":  "rank_fg3m",
    "player_blocks":            "rank_blk",
    "player_blocks_alternate":  "rank_blk",
    "player_steals":            "rank_stl",
    "player_steals_alternate":  "rank_stl",
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
                log.info(f"Waiting {retry_wait}s for Azure SQL to resume...")
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
                created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
                CONSTRAINT pk_daily_grades PRIMARY KEY (grade_id),
                CONSTRAINT uq_daily_grades UNIQUE (
                    grade_date, event_id, player_id,
                    market_key, bookmaker_key, line_value
                )
            )
        """))
    log.info("Schema verified: common.daily_grades ready.")


# ---------------------------------------------------------------------------
# History fetches
# ---------------------------------------------------------------------------
def _build_union_branches(gradeable_markets, pid_list, date_filter, params_note):
    """Build UNION ALL SQL branches grouped by stat expression."""
    expr_to_markets: dict = {}
    for mkt in gradeable_markets:
        expr = MARKET_STAT_MAP[mkt]
        expr_to_markets.setdefault(expr, []).append(mkt)

    branches = []
    for expr, mkts in expr_to_markets.items():
        mkt_values = ", ".join(f"('{m}')" for m in mkts)
        branches.append(f"""
            SELECT
                b.player_id,
                m.market_key,
                b.game_date,
                b.game_id,
                {expr} AS stat_value
            FROM nba.player_box_score_stats b
            CROSS JOIN (SELECT market_key FROM (VALUES {mkt_values})
                        AS t(market_key)) m
            WHERE b.player_id IN ({pid_list})
              AND {date_filter}
            GROUP BY b.player_id, b.game_id, b.game_date, m.market_key
        """)
    return branches


def fetch_history(engine, player_ids, market_keys, as_of_date):
    """
    60-day hit rate history. Returns one row per (player_id, market_key,
    game_date) with stat_value and in_short_window flag.
    Used exclusively for hit rate computation.
    """
    gradeable_markets = [m for m in market_keys if m in MARKET_STAT_MAP]
    if not player_ids or not gradeable_markets:
        return pd.DataFrame()

    pid_list = ", ".join(str(int(p)) for p in player_ids)
    date_filter = (
        "b.game_date < :aod "
        "AND b.game_date >= DATEADD(day, -:lb_long, :aod)"
    )
    branches = _build_union_branches(gradeable_markets, pid_list, date_filter, "60d")
    union_sql = "\nUNION ALL\n".join(branches)

    sql = text(f"""
        SELECT
            player_id,
            market_key,
            game_date,
            stat_value,
            CASE
                WHEN game_date >= DATEADD(day, -:lb_short, :aod) THEN 1
                ELSE 0
            END AS in_short_window
        FROM (
            {union_sql}
        ) AS combined
        WHERE stat_value IS NOT NULL
    """)

    df = pd.read_sql(
        sql, engine,
        params={
            "aod":      str(as_of_date),
            "lb_long":  LOOKBACK_LONG,
            "lb_short": LOOKBACK_SHORT,
        }
    )
    log.info(f"  Hit-rate history loaded: {len(df)} rows.")
    return df


def fetch_season_history(engine, player_ids, as_of_date):
    """
    Full-season game totals for all players from SEASON_START through
    as_of_date (exclusive). Returns one row per (player_id, game_date,
    game_id) with all stat columns plus derived combination columns.
    Used for trend, momentum, pattern, and regression grade computation.
    """
    if not player_ids:
        return pd.DataFrame()

    pid_list = ", ".join(str(int(p)) for p in player_ids)
    sql = text(f"""
        SELECT
            b.player_id,
            b.game_date,
            b.game_id,
            SUM(b.pts)  AS pts,
            SUM(b.reb)  AS reb,
            SUM(b.ast)  AS ast,
            SUM(b.stl)  AS stl,
            SUM(b.blk)  AS blk,
            SUM(b.fg3m) AS fg3m,
            SUM(b.tov)  AS tov
        FROM nba.player_box_score_stats b
        WHERE b.player_id IN ({pid_list})
          AND b.game_date >= :season_start
          AND b.game_date <  :aod
        GROUP BY b.player_id, b.game_id, b.game_date
    """)

    df = pd.read_sql(sql, engine, params={"season_start": SEASON_START, "aod": str(as_of_date)})

    # Derived combination stats
    df["pra"] = df["pts"] + df["reb"] + df["ast"]
    df["pr"]  = df["pts"] + df["reb"]
    df["pa"]  = df["pts"] + df["ast"]
    df["ra"]  = df["reb"] + df["ast"]

    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["player_id", "game_date"])
    log.info(f"  Season history loaded: {len(df)} rows.")
    return df


def fetch_matchup_defense(engine, opp_player_pairs):
    """
    Bulk fetch defense ranks for a set of (opp_team_id, pos_group) pairs.
    opp_player_pairs: list of (opp_team_id, pos_group) tuples.

    Returns a dict keyed by (opp_team_id, pos_group) with value being a dict
    of rank columns: {rank_pts, rank_reb, rank_ast, rank_stl, rank_blk,
    rank_fg3m, rank_tov}.
    """
    if not opp_player_pairs:
        return {}

    unique_pairs = list(set(opp_player_pairs))

    # Build a VALUES table of (opp_team_id, pos_group) pairs to JOIN against.
    values_rows = ", ".join(
        f"({tid}, '{pg}')" for tid, pg in unique_pairs if tid is not None and pg is not None
    )
    if not values_rows:
        return {}

    sql = text(f"""
        WITH season_start AS (
            SELECT CAST(
                CAST(
                    CASE WHEN MONTH(GETUTCDATE()) < 10
                        THEN YEAR(GETUTCDATE()) - 1
                        ELSE YEAR(GETUTCDATE())
                    END
                AS VARCHAR(4)) + '-10-01'
            AS DATE) AS dt
        ),
        game_totals AS (
            SELECT
                pbs.player_id,
                pbs.game_id,
                CASE
                    WHEN pbs.team_id = s.home_team_id THEN s.away_team_id
                    ELSE s.home_team_id
                END AS opp_team_id,
                SUM(pbs.pts)  AS pts,
                SUM(pbs.reb)  AS reb,
                SUM(pbs.ast)  AS ast,
                SUM(pbs.stl)  AS stl,
                SUM(pbs.blk)  AS blk,
                SUM(pbs.fg3m) AS fg3m,
                SUM(pbs.tov)  AS tov
            FROM nba.player_box_score_stats pbs
            JOIN nba.schedule s ON s.game_id = pbs.game_id
            WHERE s.game_date >= (SELECT dt FROM season_start)
            GROUP BY pbs.player_id, pbs.game_id, pbs.team_id, s.home_team_id, s.away_team_id
        ),
        pos_filtered AS (
            SELECT gt.*, LEFT(p.position, 1) AS pos_group
            FROM game_totals gt
            JOIN nba.players p ON p.player_id = gt.player_id
            WHERE LEFT(p.position, 1) IN ('G', 'F', 'C')
        ),
        target_pairs AS (
            SELECT opp_team_id, pos_group
            FROM (VALUES {values_rows}) AS t(opp_team_id, pos_group)
        ),
        team_defense AS (
            SELECT
                pf.opp_team_id,
                pf.pos_group,
                COUNT(*)                 AS games_defended,
                AVG(CAST(pf.pts  AS FLOAT)) AS avg_pts,
                AVG(CAST(pf.reb  AS FLOAT)) AS avg_reb,
                AVG(CAST(pf.ast  AS FLOAT)) AS avg_ast,
                AVG(CAST(pf.stl  AS FLOAT)) AS avg_stl,
                AVG(CAST(pf.blk  AS FLOAT)) AS avg_blk,
                AVG(CAST(pf.fg3m AS FLOAT)) AS avg_fg3m,
                AVG(CAST(pf.tov  AS FLOAT)) AS avg_tov
            FROM pos_filtered pf
            JOIN target_pairs tp
              ON tp.opp_team_id = pf.opp_team_id
             AND tp.pos_group   = pf.pos_group
            GROUP BY pf.opp_team_id, pf.pos_group
        ),
        all_teams AS (
            SELECT
                pos_group,
                opp_team_id,
                games_defended,
                avg_pts,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_pts  DESC) AS rank_pts,
                avg_reb,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_reb  DESC) AS rank_reb,
                avg_ast,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_ast  DESC) AS rank_ast,
                avg_stl,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_stl  DESC) AS rank_stl,
                avg_blk,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_blk  DESC) AS rank_blk,
                avg_fg3m, RANK() OVER (PARTITION BY pos_group ORDER BY avg_fg3m DESC) AS rank_fg3m,
                avg_tov,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_tov  DESC) AS rank_tov
            FROM team_defense
        )
        SELECT *
        FROM all_teams
    """)

    df = pd.read_sql(sql, engine)
    result = {}
    for _, row in df.iterrows():
        key = (int(row["opp_team_id"]), str(row["pos_group"]))
        result[key] = row.to_dict()
    log.info(f"  Matchup defense loaded: {len(result)} team-position pairs.")
    return result


# ---------------------------------------------------------------------------
# Hit rate computation (unchanged)
# ---------------------------------------------------------------------------
def compute_all_hit_rates(props_df, history_df):
    result = props_df.copy()
    grade_cols = ("hit_rate_60", "sample_size_60", "hit_rate_20",
                  "sample_size_20", "weighted_hit_rate", "grade")

    if history_df.empty:
        for col in grade_cols:
            result[col] = None
        return result

    history = history_df.copy()
    history["stat_value"] = history["stat_value"].astype(float)
    result["line_value"]  = result["line_value"].astype(float)

    lines  = result[["player_id", "market_key", "line_value"]].drop_duplicates()
    merged = history.merge(lines, on=["player_id", "market_key"], how="inner")
    merged["hit"] = (merged["stat_value"] > merged["line_value"]).astype(int)

    g60 = (
        merged
        .groupby(["player_id", "market_key", "line_value"])
        .agg(sample_size_60=("hit", "count"), hits_60=("hit", "sum"))
        .reset_index()
    )
    g60["hit_rate_60"] = g60["hits_60"] / g60["sample_size_60"]

    g20 = (
        merged[merged["in_short_window"] == 1]
        .groupby(["player_id", "market_key", "line_value"])
        .agg(sample_size_20=("hit", "count"), hits_20=("hit", "sum"))
        .reset_index()
    )
    g20["hit_rate_20"] = g20["hits_20"] / g20["sample_size_20"]

    result = result.merge(
        g60[["player_id", "market_key", "line_value", "hit_rate_60", "sample_size_60"]],
        on=["player_id", "market_key", "line_value"], how="left",
    )
    result = result.merge(
        g20[["player_id", "market_key", "line_value", "hit_rate_20", "sample_size_20"]],
        on=["player_id", "market_key", "line_value"], how="left",
    )

    result["sample_size_60"] = result["sample_size_60"].fillna(0).astype(int)
    result["sample_size_20"] = result["sample_size_20"].fillna(0).astype(int)

    use_blend = (result["sample_size_20"] >= MIN_SAMPLE) & result["hit_rate_20"].notna()
    result["weighted_hit_rate"] = result["hit_rate_60"]
    result.loc[use_blend, "weighted_hit_rate"] = (
        WEIGHT_SHORT * result.loc[use_blend, "hit_rate_20"]
        + WEIGHT_LONG  * result.loc[use_blend, "hit_rate_60"]
    )

    result["grade"] = result["weighted_hit_rate"].apply(
        lambda x: round(x * 100, 1) if pd.notna(x) else None
    )
    for col in ("weighted_hit_rate", "hit_rate_60", "hit_rate_20"):
        result[col] = result[col].apply(
            lambda x: round(x, 4) if pd.notna(x) else None
        )

    return result


# ---------------------------------------------------------------------------
# Component grade computation
# ---------------------------------------------------------------------------

def _safe(v):
    """Return None if v is NaN or non-finite, else round to 1 decimal."""
    if v is None:
        return None
    try:
        if math.isnan(v) or math.isinf(v):
            return None
        return round(float(v), 1)
    except Exception:
        return None


def compute_trend_grade(player_id, market_key, as_of_date, season_df):
    """
    Compares hit rate over the last TREND_SHORT games vs the last TREND_LONG
    games (by game count, not calendar days) for a specific line.

    Because the line_value is not in season_df, trend measures directional
    momentum of the raw stat value rather than a specific line. We compute
    the mean stat in the short window vs the long window.

    Formula: score = CLIP(50 + delta * 150, 0, 100)
    where delta = (short_mean - long_mean) / long_mean.
    A 20% improvement in the stat yields 50 + 0.2 * 150 = 80.

    Returns None if insufficient data.
    """
    stat_col = MARKET_STAT_COL.get(market_key)
    if stat_col is None:
        return None

    pdf = season_df[season_df["player_id"] == player_id].sort_values("game_date")
    if pdf.empty or stat_col not in pdf.columns:
        return None

    vals = pdf[stat_col].dropna().values
    if len(vals) < TREND_MIN:
        return None

    short_vals = vals[-TREND_SHORT:] if len(vals) >= TREND_SHORT else vals
    long_vals  = vals[-TREND_LONG:]  if len(vals) >= TREND_LONG  else vals

    if len(short_vals) < TREND_MIN:
        return None

    short_mean = float(np.mean(short_vals))
    long_mean  = float(np.mean(long_vals))

    if long_mean == 0:
        return None

    delta = (short_mean - long_mean) / long_mean
    score = 50.0 + delta * 150.0
    return _safe(max(0.0, min(100.0, score)))


def compute_momentum_grade(player_id, market_key, line_value, season_df):
    """
    Counts the current consecutive streak of hitting (stat > line) or missing
    the line going into today's game. No cap on streak length.

    Scoring uses a log curve so early games in a streak produce meaningful
    jumps and later games produce diminishing but nonzero increments:

      score = 50 + direction * 25 * log2(1 + |streak|)

    A 5-game hitting streak: 50 + 25 * log2(6) = 50 + 64.4 -> capped at 100.
    A 3-game hitting streak: 50 + 25 * log2(4) = 50 + 50 = 100 -> capped.
    A 1-game hitting streak: 50 + 25 * log2(2) = 50 + 25 = 75.
    A 2-game miss streak:    50 - 25 * log2(3) = 50 - 39.6 = 10.4.

    direction: +1 for hitting streaks, -1 for miss streaks.
    Returns None if fewer than 1 game of history.
    """
    stat_col = MARKET_STAT_COL.get(market_key)
    if stat_col is None:
        return None

    pdf = season_df[season_df["player_id"] == player_id].sort_values("game_date")
    if pdf.empty or stat_col not in pdf.columns:
        return None

    vals = pdf[stat_col].dropna().values
    if len(vals) == 0:
        return None

    line = float(line_value)
    hits = [v > line for v in vals]

    if not hits:
        return None

    # Walk backward from most recent game to find streak.
    last_result = hits[-1]
    streak = 0
    for h in reversed(hits):
        if h == last_result:
            streak += 1
        else:
            break

    direction = 1 if last_result else -1
    raw = 50.0 + direction * 25.0 * math.log2(1 + streak)
    return _safe(max(0.0, min(100.0, raw)))


def compute_pattern_grade(player_id, market_key, line_value, season_df):
    """
    Recurrence pattern signal: given the current streak (N consecutive hits
    or misses), what fraction of prior identical-length streaks in this
    player's history ended with a reversal on the very next game?

    A high reversal rate on a hitting streak is a regression warning (low
    grade). A high reversal rate on a miss streak is a rebound signal (high
    grade).

    Score = 50 if no prior instances, because we have no signal.
    Score approaches 100 if near-certain reversal is coming after a miss streak.
    Score approaches 0 if near-certain reversal is coming after a hitting streak.

    Falls back to None if fewer than PATTERN_MIN_INSTANCES prior streaks found.
    """
    stat_col = MARKET_STAT_COL.get(market_key)
    if stat_col is None:
        return None

    pdf = season_df[season_df["player_id"] == player_id].sort_values("game_date")
    if pdf.empty or stat_col not in pdf.columns:
        return None

    vals = pdf[stat_col].dropna().values
    if len(vals) < 2:
        return None

    line = float(line_value)
    hits = [v > line for v in vals]

    # Current streak length and direction from the end of history.
    last_result = hits[-1]
    current_streak = 0
    for h in reversed(hits):
        if h == last_result:
            current_streak += 1
        else:
            break

    # Search all earlier positions in history where the same streak length
    # in the same direction occurred, then check the outcome on game+1.
    reversals = 0
    instances = 0

    # We need at least current_streak + 1 games to find a comparable ending.
    for end_idx in range(current_streak - 1, len(hits) - current_streak):
        # Check if hits[end_idx - current_streak + 1 : end_idx + 1] is a
        # streak of exactly current_streak in the same direction.
        window = hits[end_idx - current_streak + 1:end_idx + 1]
        if len(window) != current_streak:
            continue
        if all(h == last_result for h in window):
            # Confirm this is exactly the streak length (not longer).
            before_idx = end_idx - current_streak
            if before_idx >= 0 and hits[before_idx] == last_result:
                continue  # streak was longer at this position, skip
            next_idx = end_idx + 1
            if next_idx < len(hits):
                instances += 1
                if hits[next_idx] != last_result:
                    reversals += 1

    if instances < PATTERN_MIN_INSTANCES:
        return None

    reversal_rate = reversals / instances

    # A reversal after a miss streak is good for the over -> high grade.
    # A reversal after a hitting streak means the run ends -> low grade.
    if last_result:
        # Currently on a hitting streak. High reversal rate = bad.
        score = 50.0 - (reversal_rate - 0.5) * 100.0
    else:
        # Currently on a miss streak. High reversal rate = good.
        score = 50.0 + (reversal_rate - 0.5) * 100.0

    return _safe(max(0.0, min(100.0, score)))


def compute_matchup_grade(player_id, market_key, opp_team_id, position, matchup_cache):
    """
    Defense rank for this player's position group vs the opponent.
    Rank 1 = most allowed = 100 (best matchup). Rank 30 = 0.
    Linear scale: score = (30 - rank + 1) / 30 * 100.

    Combination markets (PRA, PR, PA, RA) have no single defense stat and
    return None.
    """
    if opp_team_id is None or position is None:
        return None

    rank_col = MARKET_DEF_RANK.get(market_key)
    if rank_col is None:
        return None

    pos_group = (
        "G" if position.startswith("G") else
        "F" if position.startswith("F") else
        "C" if position.startswith("C") else None
    )
    if pos_group is None:
        return None

    key = (int(opp_team_id), pos_group)
    defense = matchup_cache.get(key)
    if defense is None:
        return None

    rank = defense.get(rank_col)
    if rank is None or (isinstance(rank, float) and math.isnan(rank)):
        return None

    score = (30 - int(rank) + 1) / 30.0 * 100.0
    return _safe(max(0.0, min(100.0, score)))


def compute_regression_grade(player_id, market_key, line_value, season_df):
    """
    Z-score of the player's last RECENT_WINDOW games vs their full season
    distribution for the relevant stat.

    A z-score above 0 means the player has been running above their season
    average recently. High positive z-score -> regression risk -> low grade.
    Low/negative z-score -> been cold -> mean reversion -> high grade.

    Formula: score = CLIP(50 - z * 25, 0, 100)
    z = +2: score = 0   (very hot, high regression risk)
    z = 0:  score = 50  (neutral)
    z = -2: score = 100 (very cold, strong reversion signal)

    Returns None if fewer than SEASON_MIN season games or RECENT_WINDOW//2 recent games.
    """
    stat_col = MARKET_STAT_COL.get(market_key)
    if stat_col is None:
        return None

    pdf = season_df[season_df["player_id"] == player_id].sort_values("game_date")
    if pdf.empty or stat_col not in pdf.columns:
        return None

    vals = pdf[stat_col].dropna().values
    if len(vals) < SEASON_MIN:
        return None

    recent = vals[-RECENT_WINDOW:] if len(vals) >= RECENT_WINDOW else vals[-len(vals)//2:]
    if len(recent) < 3:
        return None

    season_mean = float(np.mean(vals))
    season_std  = float(np.std(vals))

    if season_std < 0.01:
        return None

    recent_mean = float(np.mean(recent))
    z = (recent_mean - season_mean) / season_std

    score = 50.0 - z * 25.0
    return _safe(max(0.0, min(100.0, score)))


def compute_composite_grade(weighted_hit_rate, trend, momentum, pattern, matchup, regression):
    """
    Equal-weighted average of all non-NULL components.
    weighted_hit_rate is included scaled to 0-100 (same as grade).
    Returns None if no components are available.
    """
    components = []
    if weighted_hit_rate is not None:
        components.append(weighted_hit_rate * 100.0)
    for v in (trend, momentum, pattern, matchup, regression):
        if v is not None:
            components.append(float(v))
    if not components:
        return None
    return _safe(sum(components) / len(components))


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
def upsert_grades(engine, rows):
    if not rows:
        return 0

    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#stage_grades') IS NOT NULL
                DROP TABLE #stage_grades
        """))
        conn.execute(text("""
            CREATE TABLE #stage_grades (
                grade_date        DATE,
                event_id          VARCHAR(50),
                game_id           VARCHAR(15),
                player_id         BIGINT,
                player_name       NVARCHAR(100),
                market_key        VARCHAR(100),
                bookmaker_key     VARCHAR(50),
                line_value        DECIMAL(6,1),
                hit_rate_60       FLOAT,
                hit_rate_20       FLOAT,
                sample_size_60    INT,
                sample_size_20    INT,
                weighted_hit_rate FLOAT,
                grade             FLOAT,
                trend_grade       FLOAT,
                momentum_grade    FLOAT,
                pattern_grade     FLOAT,
                matchup_grade     FLOAT,
                regression_grade  FLOAT,
                composite_grade   FLOAT
            )
        """))

        chunk_size = 500
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            conn.exec_driver_sql(
                "INSERT INTO #stage_grades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        r["grade_date"], r["event_id"], r["game_id"],
                        r["player_id"], r["player_name"], r["market_key"],
                        r["bookmaker_key"], r["line_value"],
                        r["hit_rate_60"], r["hit_rate_20"],
                        r["sample_size_60"], r["sample_size_20"],
                        r["weighted_hit_rate"], r["grade"],
                        r["trend_grade"], r["momentum_grade"],
                        r["pattern_grade"], r["matchup_grade"],
                        r["regression_grade"], r["composite_grade"],
                    )
                    for r in chunk
                ]
            )

        conn.execute(text("""
            MERGE common.daily_grades AS t
            USING #stage_grades AS s
            ON (
                    t.grade_date    = s.grade_date
                AND t.event_id      = s.event_id
                AND t.player_id     = s.player_id
                AND t.market_key    = s.market_key
                AND t.bookmaker_key = s.bookmaker_key
                AND t.line_value    = s.line_value
            )
            WHEN MATCHED THEN UPDATE SET
                t.game_id           = s.game_id,
                t.hit_rate_60       = s.hit_rate_60,
                t.hit_rate_20       = s.hit_rate_20,
                t.sample_size_60    = s.sample_size_60,
                t.sample_size_20    = s.sample_size_20,
                t.weighted_hit_rate = s.weighted_hit_rate,
                t.grade             = s.grade,
                t.trend_grade       = s.trend_grade,
                t.momentum_grade    = s.momentum_grade,
                t.pattern_grade     = s.pattern_grade,
                t.matchup_grade     = s.matchup_grade,
                t.regression_grade  = s.regression_grade,
                t.composite_grade   = s.composite_grade
            WHEN NOT MATCHED THEN INSERT (
                grade_date, event_id, game_id, player_id, player_name,
                market_key, bookmaker_key, line_value,
                hit_rate_60, hit_rate_20, sample_size_60, sample_size_20,
                weighted_hit_rate, grade,
                trend_grade, momentum_grade, pattern_grade,
                matchup_grade, regression_grade, composite_grade
            ) VALUES (
                s.grade_date, s.event_id, s.game_id, s.player_id, s.player_name,
                s.market_key, s.bookmaker_key, s.line_value,
                s.hit_rate_60, s.hit_rate_20, s.sample_size_60, s.sample_size_20,
                s.weighted_hit_rate, s.grade,
                s.trend_grade, s.momentum_grade, s.pattern_grade,
                s.matchup_grade, s.regression_grade, s.composite_grade
            );
        """))

    return len(rows)


# ---------------------------------------------------------------------------
# Core grading logic
# ---------------------------------------------------------------------------
def grade_props_for_date(engine, grade_date_str, props_df):
    if props_df.empty:
        log.info(f"  {grade_date_str}: no props. Skipping.")
        return 0

    player_ids  = props_df["player_id"].dropna().unique().tolist()
    market_keys = props_df["market_key"].dropna().unique().tolist()

    # Fetch all three data sources.
    history_df = fetch_history(engine, player_ids, market_keys, grade_date_str)
    season_df  = fetch_season_history(engine, player_ids, grade_date_str)

    # Build matchup pairs: need opp_team_id and position per player.
    # props_df may not have these; fetch from nba.players + nba.schedule.
    opp_info = _fetch_opp_info(engine, props_df, grade_date_str)

    # Build (opp_team_id, pos_group) pairs for bulk matchup fetch.
    matchup_pairs = []
    for pid, info in opp_info.items():
        pos = info.get("position", "")
        if pos:
            pg = "G" if pos.startswith("G") else "F" if pos.startswith("F") else "C" if pos.startswith("C") else None
            if pg and info.get("opp_team_id"):
                matchup_pairs.append((int(info["opp_team_id"]), pg))

    matchup_cache = fetch_matchup_defense(engine, matchup_pairs)

    graded_df = compute_all_hit_rates(props_df, history_df)

    rows = []
    for _, r in graded_df.iterrows():
        pid = r["player_id"]
        if pd.isna(pid):
            continue
        pid_int = int(pid)
        mkt     = r["market_key"]
        line    = float(r["line_value"])

        info     = opp_info.get(pid_int, {})
        opp_id   = info.get("opp_team_id")
        position = info.get("position", "")

        whr = r.get("weighted_hit_rate")
        whr = whr if pd.notna(whr) else None

        trend      = compute_trend_grade(pid_int, mkt, grade_date_str, season_df)
        momentum   = compute_momentum_grade(pid_int, mkt, line, season_df)
        pattern    = compute_pattern_grade(pid_int, mkt, line, season_df)
        matchup    = compute_matchup_grade(pid_int, mkt, opp_id, position, matchup_cache)
        regression = compute_regression_grade(pid_int, mkt, line, season_df)
        composite  = compute_composite_grade(whr, trend, momentum, pattern, matchup, regression)

        rows.append({
            "grade_date":        grade_date_str,
            "event_id":          r["event_id"],
            "game_id":           r.get("game_id"),
            "player_id":         pid_int,
            "player_name":       r["player_name"],
            "market_key":        mkt,
            "bookmaker_key":     r["bookmaker_key"],
            "line_value":        line,
            "hit_rate_60":       r.get("hit_rate_60")       if pd.notna(r.get("hit_rate_60"))       else None,
            "hit_rate_20":       r.get("hit_rate_20")       if pd.notna(r.get("hit_rate_20"))       else None,
            "sample_size_60":    int(r["sample_size_60"])   if pd.notna(r.get("sample_size_60"))    else 0,
            "sample_size_20":    int(r["sample_size_20"])   if pd.notna(r.get("sample_size_20"))    else 0,
            "weighted_hit_rate": whr,
            "grade":             r.get("grade")             if pd.notna(r.get("grade"))             else None,
            "trend_grade":       trend,
            "momentum_grade":    momentum,
            "pattern_grade":     pattern,
            "matchup_grade":     matchup,
            "regression_grade":  regression,
            "composite_grade":   composite,
        })

    written = upsert_grades(engine, rows)
    graded  = sum(1 for r in rows if r["composite_grade"] is not None)
    log.info(f"  {grade_date_str}: {written} rows written, {graded} with composite grade.")
    return written


def _fetch_opp_info(engine, props_df, grade_date_str):
    """
    For each player_id in props_df, fetch their position and opponent team_id
    for the given grade date. Returns dict: {player_id: {opp_team_id, position}}.
    """
    player_ids = props_df["player_id"].dropna().unique().tolist()
    if not player_ids:
        return {}

    pid_list = ", ".join(str(int(p)) for p in player_ids)

    sql = text(f"""
        SELECT
            p.player_id,
            p.position,
            CASE
                WHEN p.team_id = s.home_team_id THEN s.away_team_id
                ELSE s.home_team_id
            END AS opp_team_id
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
            "position":   row["position"] or "",
            "opp_team_id": int(row["opp_team_id"]) if pd.notna(row["opp_team_id"]) else None,
        }
    return result


# ---------------------------------------------------------------------------
# Props fetch helpers (unchanged)
# ---------------------------------------------------------------------------
PROPS_SELECT = """
    SELECT DISTINCT
        pp.event_id,
        pm.player_id,
        pp.player_name,
        pp.market_key,
        pp.bookmaker_key,
        pp.outcome_point AS line_value,
        egm.game_id
    FROM {props_table} pp
    JOIN odds.event_game_map egm
        ON  egm.event_id  = pp.event_id
        AND egm.sport_key = 'basketball_nba'
        AND egm.game_id   IS NOT NULL
    JOIN odds.player_map pm
        ON  pm.odds_player_name = pp.player_name
        AND pm.sport_key        = pp.sport_key
        AND pm.player_id        IS NOT NULL
    WHERE pp.sport_key      = 'basketball_nba'
      AND pp.bookmaker_key  = :bk
      AND pp.outcome_name   = 'Over'
      AND pp.outcome_point  IS NOT NULL
      AND pp.market_key     IN ({mkt_list})
      {date_filter}
"""

MARKET_LIST_SQL = ", ".join(f"'{m}'" for m in PLAYER_MARKETS)


def fetch_props_for_date(engine, game_date):
    sql = text(
        PROPS_SELECT.format(
            props_table="odds.player_props",
            mkt_list=MARKET_LIST_SQL,
            date_filter="AND CAST(egm.game_date AS DATE) = :gd",
        )
    )
    return pd.read_sql(sql, engine, params={"bk": BOOKMAKER, "gd": game_date})


def fetch_upcoming_props(engine):
    sql = text(
        PROPS_SELECT.format(
            props_table="odds.upcoming_player_props",
            mkt_list=MARKET_LIST_SQL,
            date_filter="",
        )
    )
    return pd.read_sql(sql, engine, params={"bk": BOOKMAKER})


# ---------------------------------------------------------------------------
# Mode runners
# ---------------------------------------------------------------------------
def run_upcoming(engine):
    today = str(date.today())
    log.info(f"Upcoming mode: grading FanDuel lines for {today}")
    props = fetch_upcoming_props(engine)
    if props.empty:
        log.info("No upcoming props found. Nothing to grade.")
        return
    log.info(f"Found {len(props)} prop lines across {props['player_id'].nunique()} players.")
    grade_props_for_date(engine, today, props)


def get_backfill_dates(engine, batch_size, specific_date=None):
    if specific_date:
        return [specific_date]
    df = pd.read_sql(
        text("""
            SELECT DISTINCT CAST(egm.game_date AS DATE) AS game_date
            FROM odds.player_props pp
            JOIN odds.event_game_map egm
                ON  egm.event_id = pp.event_id
                AND egm.game_id  IS NOT NULL
            WHERE pp.sport_key     = 'basketball_nba'
              AND pp.bookmaker_key = :bk
              AND pp.outcome_name  = 'Over'
              AND pp.outcome_point IS NOT NULL
              AND egm.game_date    IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM common.daily_grades g
                  WHERE g.grade_date = CAST(egm.game_date AS DATE)
              )
            ORDER BY game_date ASC
        """),
        engine,
        params={"bk": BOOKMAKER}
    )
    return df["game_date"].astype(str).tolist()[:batch_size]


def run_backfill(engine, batch_size, specific_date=None):
    work_dates = get_backfill_dates(engine, batch_size, specific_date)
    if not work_dates:
        log.info("Backfill: all dates already graded. Nothing to do.")
        return
    log.info(f"Backfill: {len(work_dates)} date(s): {work_dates[0]} to {work_dates[-1]}")
    total = 0
    for gd in work_dates:
        props = fetch_props_for_date(engine, gd)
        total += grade_props_for_date(engine, gd, props)
    log.info(f"Backfill complete. {total} total rows written.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA prop grading model")
    parser.add_argument("--mode", choices=["upcoming", "backfill"], default="upcoming")
    parser.add_argument("--batch", type=int, default=BATCH_DEFAULT)
    parser.add_argument("--date", type=str, default=None)
    args = parser.parse_args()

    engine = get_engine()
    ensure_tables(engine)

    if args.mode == "upcoming":
        run_upcoming(engine)
    else:
        run_backfill(engine, batch_size=args.batch, specific_date=args.date)

    log.info("Done.")


if __name__ == "__main__":
    main()
