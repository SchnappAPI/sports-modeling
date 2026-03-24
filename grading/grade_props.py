"""
grade_props.py

NBA prop grading model.

Grades are computed per player, per market, per bookmaker, per line value,
anchored to the actual odds posted for a specific game. Every grade row ties
back to an event_id and game_id so results can be evaluated against what
actually happened.

Grade formula
-------------
  hit_rate_60  = hits / games where stat > line, over prior 60 calendar days
  hit_rate_20  = hits / games where stat > line, over prior 20 calendar days
  weighted_hit_rate = (0.60 * hit_rate_20) + (0.40 * hit_rate_60)
  grade        = weighted_hit_rate * 100, rounded to 1 decimal

  If sample_size_20 < MIN_SAMPLE, weighted blend falls back to hit_rate_60
  only. Grade is always written regardless of sample size so thin-sample
  rows are visible rather than silently omitted.

Performance design
------------------
All hit rate computation is set-based. For each game date being graded the
script issues exactly three database round trips:

  1. Fetch FanDuel player prop lines for the date, deduplicated on
     (player_id, market_key, line_value).
  2. Fetch all historical stat totals for every relevant player and market
     over the prior 60 days in a single bulk query directly against
     nba.player_box_score_stats. No view join required.
  3. Write all grade rows in one batched upsert.

Hit rates are computed entirely in pandas against the in-memory history
dataframe using vectorized operations. There are zero per-row database
calls. This keeps a full-season backfill well within GitHub Actions time
limits.

Bookmaker
---------
All grading uses FanDuel lines only. FanDuel is the reference bookmaker.
The bookmaker_key column is retained in the schema for future extension.

Markets graded
--------------
All player-level prop and alt-prop markets are graded, including standard,
alternate, and combination markets. Team totals, game lines (h2h, spreads,
totals), and all half/quarter game lines are excluded.

Modes
-----
  upcoming  Grades today's lines from odds.upcoming_player_props.
            This is the nightly production mode.

  backfill  Works through historical game dates using odds.player_props,
            oldest ungraded date first, bounded by --batch N.

Tables written
--------------
  common.daily_grades   One row per (grade_date, event_id, player_id,
                        market_key, bookmaker_key, line_value).

Tables read
-----------
  odds.upcoming_player_props   Today's FanDuel lines (upcoming mode)
  odds.player_props            Historical FanDuel lines (backfill mode)
  odds.event_game_map          Resolves event_id -> game_id + game_date
  nba.player_box_score_stats   Per-quarter box scores for hit rate computation

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
import os
import time
import logging
from datetime import date

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

LOOKBACK_LONG  = 60    # calendar days for full window
LOOKBACK_SHORT = 20    # calendar days for recent window
WEIGHT_SHORT   = 0.60  # weight applied to recent 20-day hit rate
WEIGHT_LONG    = 0.40  # weight applied to full 60-day hit rate
MIN_SAMPLE     = 5     # minimum games required to use the short window blend
BATCH_DEFAULT  = 10

# All player-level prop and alt-prop markets. Excludes team totals, h2h,
# spreads, totals, and all half/quarter game lines.
PLAYER_MARKETS = {
    # Standard
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
    # Alternate
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

# Maps each market_key to the stat expression needed from the box score.
# All stats are summed across periods (1Q+2Q+3Q+4Q+OT) to get game totals.
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
    # double_double / triple_double / first_basket have no direct stat
    # expression and are excluded from history computation.
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
    # fast_executemany=False prevents NVARCHAR(MAX) truncation on wide rows
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
    """
    Idempotent. Drops legacy grade_thresholds if it exists. Creates
    common.daily_grades if not already present. Never drops daily_grades
    so partial backfill progress is preserved across runs.
    """
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
# Bulk history fetch
# ---------------------------------------------------------------------------
def fetch_history(engine, player_ids, market_keys, as_of_date):
    """
    Single bulk query: compute game-total stat values directly from
    nba.player_box_score_stats for every (player_id, market_key) combination
    over the prior LOOKBACK_LONG days, strictly before as_of_date.

    Stats are summed across all periods (1Q+2Q+3Q+4Q+OT) per game to
    produce true game totals. Each market maps to a specific stat expression
    via MARKET_STAT_MAP. Markets without a stat expression (double_double,
    triple_double, first_basket) are skipped.

    Returns a DataFrame with columns:
        player_id, market_key, game_date, stat_value, in_short_window

    One row per (player_id, market_key, game_date). No further DB calls
    are needed after this point.
    """
    gradeable_markets = [m for m in market_keys if m in MARKET_STAT_MAP]
    if not player_ids or not gradeable_markets:
        return pd.DataFrame()

    pid_list = ", ".join(str(int(p)) for p in player_ids)

    # Group markets by stat expression to minimise UNION ALL branches.
    expr_to_markets: dict[str, list[str]] = {}
    for mkt in gradeable_markets:
        expr = MARKET_STAT_MAP[mkt]
        expr_to_markets.setdefault(expr, []).append(mkt)

    union_branches = []
    for expr, mkts in expr_to_markets.items():
        union_branches.append(f"""
            SELECT
                b.player_id,
                m.market_key,
                b.game_date,
                {expr} AS stat_value
            FROM nba.player_box_score_stats b
            CROSS JOIN (SELECT market_key FROM (VALUES {', '.join(f"('{m}')" for m in mkts)})
                        AS t(market_key)) m
            WHERE b.player_id IN ({pid_list})
              AND b.game_date  <  :aod
              AND b.game_date  >= DATEADD(day, -:lb_long, :aod)
            GROUP BY b.player_id, b.game_id, b.game_date, m.market_key
        """)

    union_sql = "\nUNION ALL\n".join(union_branches)

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
        sql,
        engine,
        params={
            "aod":      str(as_of_date),
            "lb_long":  LOOKBACK_LONG,
            "lb_short": LOOKBACK_SHORT,
        }
    )
    log.info(f"  History loaded: {len(df)} rows for {len(player_ids)} players, "
             f"{len(gradeable_markets)} markets.")
    return df


# ---------------------------------------------------------------------------
# In-memory hit rate computation
# ---------------------------------------------------------------------------
def compute_all_hit_rates(props_df, history_df):
    """
    Compute hit rates for every (player_id, market_key, line_value)
    combination in props_df using the pre-loaded history_df.

    For each historical game row, a hit is recorded if stat_value > line_value.
    Aggregation is done in pandas with vectorized groupby operations.

    Returns props_df with columns added:
        hit_rate_60, sample_size_60, hit_rate_20, sample_size_20,
        weighted_hit_rate, grade
    """
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

    # 60-day window
    g60 = (
        merged
        .groupby(["player_id", "market_key", "line_value"])
        .agg(sample_size_60=("hit", "count"), hits_60=("hit", "sum"))
        .reset_index()
    )
    g60["hit_rate_60"] = g60["hits_60"] / g60["sample_size_60"]

    # 20-day window
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
# Upsert
# ---------------------------------------------------------------------------
def upsert_grades(engine, rows):
    if not rows:
        return 0

    with engine.begin() as conn:
        # Drop and recreate the staging table each call so this function is
        # safe to call multiple times within the same connection lifetime.
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
                grade             FLOAT
            )
        """))

        chunk_size = 500
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            conn.exec_driver_sql(
                "INSERT INTO #stage_grades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    (
                        r["grade_date"], r["event_id"], r["game_id"],
                        r["player_id"], r["player_name"], r["market_key"],
                        r["bookmaker_key"], r["line_value"],
                        r["hit_rate_60"], r["hit_rate_20"],
                        r["sample_size_60"], r["sample_size_20"],
                        r["weighted_hit_rate"], r["grade"],
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
                t.grade             = s.grade
            WHEN NOT MATCHED THEN INSERT (
                grade_date, event_id, game_id, player_id, player_name,
                market_key, bookmaker_key, line_value,
                hit_rate_60, hit_rate_20, sample_size_60, sample_size_20,
                weighted_hit_rate, grade
            ) VALUES (
                s.grade_date, s.event_id, s.game_id, s.player_id, s.player_name,
                s.market_key, s.bookmaker_key, s.line_value,
                s.hit_rate_60, s.hit_rate_20, s.sample_size_60, s.sample_size_20,
                s.weighted_hit_rate, s.grade
            );
        """))

    return len(rows)


# ---------------------------------------------------------------------------
# Core grading logic (shared by both modes)
# ---------------------------------------------------------------------------
def grade_props_for_date(engine, grade_date_str, props_df):
    if props_df.empty:
        log.info(f"  {grade_date_str}: no props. Skipping.")
        return 0

    player_ids  = props_df["player_id"].dropna().unique().tolist()
    market_keys = props_df["market_key"].dropna().unique().tolist()

    history_df = fetch_history(engine, player_ids, market_keys, grade_date_str)
    graded_df  = compute_all_hit_rates(props_df, history_df)

    rows = []
    for _, r in graded_df.iterrows():
        pid = r["player_id"]
        rows.append({
            "grade_date":        grade_date_str,
            "event_id":          r["event_id"],
            "game_id":           r.get("game_id"),
            "player_id":         int(pid) if pd.notna(pid) else None,
            "player_name":       r["player_name"],
            "market_key":        r["market_key"],
            "bookmaker_key":     r["bookmaker_key"],
            "line_value":        float(r["line_value"]),
            "hit_rate_60":       r["hit_rate_60"]       if pd.notna(r.get("hit_rate_60"))       else None,
            "hit_rate_20":       r["hit_rate_20"]       if pd.notna(r.get("hit_rate_20"))       else None,
            "sample_size_60":    int(r["sample_size_60"]) if pd.notna(r.get("sample_size_60")) else 0,
            "sample_size_20":    int(r["sample_size_20"]) if pd.notna(r.get("sample_size_20")) else 0,
            "weighted_hit_rate": r["weighted_hit_rate"] if pd.notna(r.get("weighted_hit_rate")) else None,
            "grade":             r["grade"]             if pd.notna(r.get("grade"))             else None,
        })

    written = upsert_grades(engine, rows)
    graded  = sum(1 for r in rows if r["grade"] is not None)
    log.info(f"  {grade_date_str}: {written} rows written, {graded} with grade.")
    return written


# ---------------------------------------------------------------------------
# Props fetch helpers
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
# Upcoming mode
# ---------------------------------------------------------------------------
def run_upcoming(engine):
    today = str(date.today())
    log.info(f"Upcoming mode: grading FanDuel lines for {today}")

    props = fetch_upcoming_props(engine)

    if props.empty:
        log.info("No upcoming props found. Nothing to grade.")
        return

    log.info(f"Found {len(props)} prop lines across "
             f"{props['player_id'].nunique()} players.")
    grade_props_for_date(engine, today, props)


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------
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

    log.info(f"Backfill: {len(work_dates)} date(s): "
             f"{work_dates[0]} to {work_dates[-1]}")

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
    parser.add_argument(
        "--mode", choices=["upcoming", "backfill"], default="upcoming",
    )
    parser.add_argument(
        "--batch", type=int, default=BATCH_DEFAULT,
        help=f"Backfill mode: max game dates per run (default {BATCH_DEFAULT})."
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Grade a specific date (YYYY-MM-DD). Backfill mode only."
    )
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
