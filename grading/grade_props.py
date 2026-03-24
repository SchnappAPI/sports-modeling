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

If sample_size_20 < MIN_SAMPLE the weighted blend falls back to hit_rate_60
only, since 20-game window is too thin to be meaningful. If sample_size_60 is
also below MIN_SAMPLE the grade is still written but flagged with low sample.

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
  odds.upcoming_player_props    Today's lines (upcoming mode)
  odds.player_props             Historical lines (backfill mode)
  odds.event_game_map           Resolves event_id -> game_id + game_date
  odds.vw_nba_player_prop_results  Resolved over/under outcomes with stat values
  nba.player_box_score_stats    Used to compute rolling hit rates

Args
----
  --mode      upcoming | backfill  (default: upcoming)
  --batch N   Max game dates per run in backfill mode (default: 10)
  --date      Grade a specific date only (YYYY-MM-DD, overrides batch)

Secrets required
----------------
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import os
import sys
import time
import logging
from datetime import date, timedelta

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
LOOKBACK_LONG  = 60   # calendar days for full window
LOOKBACK_SHORT = 20   # calendar days for recent window
WEIGHT_SHORT   = 0.60 # weight applied to recent 20-game hit rate
WEIGHT_LONG    = 0.40 # weight applied to full 60-day hit rate
MIN_SAMPLE     = 5    # minimum games to trust either window
BATCH_DEFAULT  = 10


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
    # fast_executemany=False: prevents NVARCHAR(MAX) truncation on wide rows
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
        # Drop legacy objects if they exist
        conn.execute(text("""
            IF OBJECT_ID('common.grade_thresholds', 'U') IS NOT NULL
                DROP TABLE common.grade_thresholds
        """))
        conn.execute(text("""
            IF OBJECT_ID('common.daily_grades', 'U') IS NOT NULL
                DROP TABLE common.daily_grades
        """))
        # Ensure common schema exists
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'common')
                EXEC('CREATE SCHEMA common')
        """))
        # Create new daily_grades
        conn.execute(text("""
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
# Hit rate computation
# ---------------------------------------------------------------------------
def compute_hit_rates(engine, player_id, market_key, line_value, as_of_date):
    """
    Return (hit_rate_60, sample_size_60, hit_rate_20, sample_size_20)
    using only games strictly before as_of_date.

    Pulls directly from vw_nba_player_prop_results which already joins
    box scores to odds outcomes and resolves over_hit per game.

    We filter to Over rows only (one row per game per bookmaker at this line)
    and deduplicate to one result per game_id to avoid double-counting
    when multiple bookmakers post the same line.
    """
    sql = text("""
        WITH per_game AS (
            SELECT
                game_date,
                MAX(over_hit) AS over_hit
            FROM odds.vw_nba_player_prop_results
            WHERE player_id    = :pid
              AND market_key   = :mkt
              AND line         = :line
              AND outcome_name = 'Over'
              AND game_date    < :aod
              AND game_date   >= DATEADD(day, -:lb_long, :aod)
            GROUP BY game_date
        )
        SELECT
            game_date,
            over_hit,
            CASE WHEN game_date >= DATEADD(day, -:lb_short, :aod) THEN 1 ELSE 0 END
                AS in_short_window
        FROM per_game
        WHERE over_hit IS NOT NULL
    """)

    df = pd.read_sql(
        sql,
        engine,
        params={
            "pid":      int(player_id),
            "mkt":      market_key,
            "line":     float(line_value),
            "aod":      str(as_of_date),
            "lb_long":  LOOKBACK_LONG,
            "lb_short": LOOKBACK_SHORT,
        }
    )

    if df.empty:
        return None, 0, None, 0

    n60 = len(df)
    h60 = int(df["over_hit"].sum())
    hr60 = h60 / n60

    short = df[df["in_short_window"] == 1]
    n20 = len(short)
    h20 = int(short["over_hit"].sum()) if n20 > 0 else 0
    hr20 = (h20 / n20) if n20 > 0 else None

    return hr60, n60, hr20, n20


def compute_weighted_grade(hr60, n60, hr20, n20):
    """
    Blend hit rates into a single grade value 0-100.
    Falls back to hr60 only if short window is too thin.
    Returns (weighted_hit_rate, grade).
    """
    if hr60 is None:
        return None, None

    if hr20 is not None and n20 >= MIN_SAMPLE:
        whr = (WEIGHT_SHORT * hr20) + (WEIGHT_LONG * hr60)
    else:
        # Short window too thin; use long window only
        whr = hr60

    grade = round(whr * 100, 1)
    return round(whr, 4), grade


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
def upsert_grades(engine, rows):
    if not rows:
        return

    with engine.begin() as conn:
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

        chunk_size = 200
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            tuples = [
                (
                    r["grade_date"],
                    r["event_id"],
                    r["game_id"],
                    r["player_id"],
                    r["player_name"],
                    r["market_key"],
                    r["bookmaker_key"],
                    r["line_value"],
                    r["hit_rate_60"],
                    r["hit_rate_20"],
                    r["sample_size_60"],
                    r["sample_size_20"],
                    r["weighted_hit_rate"],
                    r["grade"],
                )
                for r in chunk
            ]
            conn.exec_driver_sql(
                "INSERT INTO #stage_grades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuples
            )

        conn.execute(text("""
            MERGE common.daily_grades AS t
            USING #stage_grades AS s
            ON (
                t.grade_date    = s.grade_date
                AND t.event_id  = s.event_id
                AND t.player_id = s.player_id
                AND t.market_key     = s.market_key
                AND t.bookmaker_key  = s.bookmaker_key
                AND t.line_value     = s.line_value
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


# ---------------------------------------------------------------------------
# Upcoming mode
# ---------------------------------------------------------------------------
def run_upcoming(engine):
    """
    Grade all lines in odds.upcoming_player_props.
    Uses today as the as_of_date so only historical games feed the hit rates.
    """
    today = str(date.today())
    log.info(f"Upcoming mode: grading lines for {today}")

    props = pd.read_sql(
        text("""
            SELECT
                up.event_id,
                up.player_name,
                up.player_id,
                up.market_key,
                up.bookmaker_key,
                up.outcome_point  AS line_value,
                egm.game_id,
                egm.game_date
            FROM odds.upcoming_player_props up
            LEFT JOIN odds.event_game_map egm
                ON egm.event_id  = up.event_id
                AND egm.sport_key = 'basketball_nba'
            WHERE up.sport_key    = 'basketball_nba'
              AND up.outcome_name = 'Over'
              AND up.outcome_point IS NOT NULL
              AND up.player_id    IS NOT NULL
        """),
        engine
    )

    if props.empty:
        log.info("No upcoming props found. Nothing to grade.")
        return

    log.info(f"Found {len(props)} upcoming prop lines across "
             f"{props['player_id'].nunique()} players.")

    rows = []
    for _, p in props.iterrows():
        hr60, n60, hr20, n20 = compute_hit_rates(
            engine,
            player_id   = p["player_id"],
            market_key  = p["market_key"],
            line_value  = p["line_value"],
            as_of_date  = today,
        )
        whr, grade = compute_weighted_grade(hr60, n60, hr20, n20)
        rows.append({
            "grade_date":        today,
            "event_id":          p["event_id"],
            "game_id":           p["game_id"],
            "player_id":         int(p["player_id"]) if p["player_id"] is not None else None,
            "player_name":       p["player_name"],
            "market_key":        p["market_key"],
            "bookmaker_key":     p["bookmaker_key"],
            "line_value":        float(p["line_value"]),
            "hit_rate_60":       round(hr60, 4) if hr60 is not None else None,
            "hit_rate_20":       round(hr20, 4) if hr20 is not None else None,
            "sample_size_60":    int(n60),
            "sample_size_20":    int(n20),
            "weighted_hit_rate": whr,
            "grade":             grade,
        })

    upsert_grades(engine, rows)
    graded = sum(1 for r in rows if r["grade"] is not None)
    log.info(f"Upcoming grades written: {len(rows)} rows, {graded} with grade.")


# ---------------------------------------------------------------------------
# Backfill mode
# ---------------------------------------------------------------------------
def get_backfill_dates(engine, batch_size, specific_date=None):
    """
    Return up to batch_size historical game dates that:
      - have odds in odds.player_props (basketball_nba, Over lines)
      - have a resolved event_game_map entry
      - are not yet present in common.daily_grades
    Oldest first.
    """
    if specific_date:
        return [specific_date]

    df = pd.read_sql(
        text("""
            SELECT DISTINCT CAST(egm.game_date AS DATE) AS game_date
            FROM odds.player_props pp
            JOIN odds.event_game_map egm
                ON egm.event_id  = pp.event_id
               AND egm.game_id   IS NOT NULL
            WHERE pp.sport_key    = 'basketball_nba'
              AND pp.outcome_name = 'Over'
              AND pp.outcome_point IS NOT NULL
              AND egm.game_date IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM common.daily_grades g
                  WHERE g.grade_date = CAST(egm.game_date AS DATE)
              )
            ORDER BY game_date ASC
        """),
        engine
    )

    return df["game_date"].astype(str).tolist()[:batch_size]


def run_backfill_date(engine, game_date):
    """
    Grade all player prop lines for a single historical game date.
    as_of_date is set to game_date itself so only prior games count.
    """
    props = pd.read_sql(
        text("""
            SELECT DISTINCT
                pp.event_id,
                pm.player_id,
                pp.player_name,
                pp.market_key,
                pp.bookmaker_key,
                pp.outcome_point  AS line_value,
                egm.game_id,
                egm.game_date
            FROM odds.player_props pp
            JOIN odds.event_game_map egm
                ON egm.event_id   = pp.event_id
               AND egm.sport_key  = 'basketball_nba'
               AND egm.game_id    IS NOT NULL
            JOIN odds.player_map pm
                ON pm.odds_player_name = pp.player_name
               AND pm.sport_key        = pp.sport_key
               AND pm.player_id        IS NOT NULL
            WHERE pp.sport_key     = 'basketball_nba'
              AND pp.outcome_name  = 'Over'
              AND pp.outcome_point IS NOT NULL
              AND CAST(egm.game_date AS DATE) = :gd
        """),
        engine,
        params={"gd": game_date}
    )

    if props.empty:
        log.info(f"  {game_date}: no props found. Skipping.")
        return 0

    rows = []
    for _, p in props.iterrows():
        hr60, n60, hr20, n20 = compute_hit_rates(
            engine,
            player_id  = p["player_id"],
            market_key = p["market_key"],
            line_value = p["line_value"],
            as_of_date = game_date,
        )
        whr, grade = compute_weighted_grade(hr60, n60, hr20, n20)
        rows.append({
            "grade_date":        game_date,
            "event_id":          p["event_id"],
            "game_id":           p["game_id"],
            "player_id":         int(p["player_id"]),
            "player_name":       p["player_name"],
            "market_key":        p["market_key"],
            "bookmaker_key":     p["bookmaker_key"],
            "line_value":        float(p["line_value"]),
            "hit_rate_60":       round(hr60, 4) if hr60 is not None else None,
            "hit_rate_20":       round(hr20, 4) if hr20 is not None else None,
            "sample_size_60":    int(n60),
            "sample_size_20":    int(n20),
            "weighted_hit_rate": whr,
            "grade":             grade,
        })

    upsert_grades(engine, rows)
    graded = sum(1 for r in rows if r["grade"] is not None)
    log.info(f"  {game_date}: {len(rows)} rows written, {graded} with grade.")
    return len(rows)


def run_backfill(engine, batch_size, specific_date=None):
    work_dates = get_backfill_dates(engine, batch_size, specific_date)

    if not work_dates:
        log.info("Backfill: all dates already graded. Nothing to do.")
        return

    log.info(f"Backfill: {len(work_dates)} date(s) to process: "
             f"{work_dates[0]} to {work_dates[-1]}")

    total = 0
    for gd in work_dates:
        total += run_backfill_date(engine, gd)

    log.info(f"Backfill complete. {total} total rows written.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="NBA prop grading model")
    parser.add_argument(
        "--mode", choices=["upcoming", "backfill"], default="upcoming",
        help="upcoming: grade today's lines. backfill: process historical dates."
    )
    parser.add_argument(
        "--batch", type=int, default=BATCH_DEFAULT,
        help=f"Backfill mode: max game dates per run (default {BATCH_DEFAULT})."
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Grade a specific date only (YYYY-MM-DD). Backfill mode only."
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
