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

  1. Fetch prop lines for the date (props query).
  2. Fetch all historical over/under results for every relevant player and
     market over the prior 60 days in a single bulk query (history query).
  3. Write all grade rows in one batched upsert.

Hit rates are computed entirely in pandas against the in-memory history
dataframe using vectorized groupby operations. There are zero per-row
database calls. This keeps a full-season backfill well within GitHub
Actions time limits.

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
  odds.upcoming_player_props       Today's lines (upcoming mode)
  odds.player_props                Historical lines (backfill mode)
  odds.event_game_map              Resolves event_id -> game_id + game_date
  odds.vw_nba_player_prop_results  Resolved over/under outcomes with stat values

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
LOOKBACK_LONG  = 60    # calendar days for full window
LOOKBACK_SHORT = 20    # calendar days for recent window
WEIGHT_SHORT   = 0.60  # weight applied to recent 20-day hit rate
WEIGHT_LONG    = 0.40  # weight applied to full 60-day hit rate
MIN_SAMPLE     = 5     # minimum games required to use the short window blend
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
    Idempotent schema setup. Drops legacy tables from the old threshold-based
    model if they exist, then creates common.daily_grades if not present.
    Does NOT drop daily_grades if it already exists so partial backfills
    are preserved across runs.
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
    Single bulk query: load all resolved over/under results for every
    combination of player_id and market_key over the prior LOOKBACK_LONG days,
    strictly before as_of_date.

    Returns a DataFrame with columns:
        player_id, market_key, line, game_date, over_hit, in_short_window

    One row per (player_id, market_key, line, game_date) after deduplication
    across bookmakers. This is the only database read for hit rate computation.
    """
    if not player_ids or not market_keys:
        return pd.DataFrame()

    # SQL Server does not support binding lists directly so we inline them.
    # player_ids are integers and market_keys are controlled internal strings
    # (never user input), so this is safe.
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    mkt_list = ", ".join(f"'{m}'" for m in market_keys)

    sql = text(f"""
        WITH deduped AS (
            -- One result per (player, market, line, game_date).
            -- Multiple bookmakers may post the same line; MAX(over_hit)
            -- collapses them to a single row without losing any hit.
            SELECT
                player_id,
                market_key,
                line,
                game_date,
                MAX(over_hit) AS over_hit
            FROM odds.vw_nba_player_prop_results
            WHERE player_id  IN ({pid_list})
              AND market_key IN ({mkt_list})
              AND outcome_name = 'Over'
              AND game_date    < :aod
              AND game_date   >= DATEADD(day, -:lb_long, :aod)
              AND over_hit     IS NOT NULL
            GROUP BY player_id, market_key, line, game_date
        )
        SELECT
            player_id,
            market_key,
            line,
            game_date,
            over_hit,
            CASE
                WHEN game_date >= DATEADD(day, -:lb_short, :aod) THEN 1
                ELSE 0
            END AS in_short_window
        FROM deduped
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
             f"{len(market_keys)} markets.")
    return df


# ---------------------------------------------------------------------------
# In-memory hit rate computation
# ---------------------------------------------------------------------------
def compute_all_hit_rates(props_df, history_df):
    """
    Compute hit rates for every (player_id, market_key, line_value)
    combination in props_df using the pre-loaded history_df.

    Returns props_df with five new columns added:
        hit_rate_60, sample_size_60, hit_rate_20, sample_size_20,
        weighted_hit_rate, grade

    No database calls. Pure pandas vectorized operations.
    """
    if history_df.empty:
        for col in ("hit_rate_60", "sample_size_60", "hit_rate_20",
                    "sample_size_20", "weighted_hit_rate", "grade"):
            props_df[col] = None
        return props_df

    # Ensure line types match for the merge
    history_df = history_df.copy()
    history_df["line"] = history_df["line"].astype(float)
    props_df = props_df.copy()
    props_df["line_value"] = props_df["line_value"].astype(float)

    # --- 60-day window ---
    g60 = (
        history_df
        .groupby(["player_id", "market_key", "line"])
        .agg(
            sample_size_60=("over_hit", "count"),
            hits_60=("over_hit", "sum"),
        )
        .reset_index()
    )
    g60["hit_rate_60"] = g60["hits_60"] / g60["sample_size_60"]

    # --- 20-day window ---
    short = history_df[history_df["in_short_window"] == 1]
    g20 = (
        short
        .groupby(["player_id", "market_key", "line"])
        .agg(
            sample_size_20=("over_hit", "count"),
            hits_20=("over_hit", "sum"),
        )
        .reset_index()
    )
    g20["hit_rate_20"] = g20["hits_20"] / g20["sample_size_20"]

    # --- Merge onto props ---
    result = props_df.merge(
        g60[["player_id", "market_key", "line", "hit_rate_60", "sample_size_60"]],
        left_on=["player_id", "market_key", "line_value"],
        right_on=["player_id", "market_key", "line"],
        how="left",
    ).drop(columns=["line"])

    result = result.merge(
        g20[["player_id", "market_key", "line", "hit_rate_20", "sample_size_20"]],
        left_on=["player_id", "market_key", "line_value"],
        right_on=["player_id", "market_key", "line"],
        how="left",
    ).drop(columns=["line"])

    result["sample_size_60"] = result["sample_size_60"].fillna(0).astype(int)
    result["sample_size_20"] = result["sample_size_20"].fillna(0).astype(int)

    # --- Weighted hit rate ---
    # Use blend when short window has enough sample, otherwise fall back to long.
    use_blend = (result["sample_size_20"] >= MIN_SAMPLE) & result["hit_rate_20"].notna()

    result["weighted_hit_rate"] = None
    result.loc[result["hit_rate_60"].notna(), "weighted_hit_rate"] = (
        result.loc[result["hit_rate_60"].notna(), "hit_rate_60"]
    )
    result.loc[use_blend, "weighted_hit_rate"] = (
        WEIGHT_SHORT * result.loc[use_blend, "hit_rate_20"]
        + WEIGHT_LONG  * result.loc[use_blend, "hit_rate_60"]
    )

    result["grade"] = result["weighted_hit_rate"].apply(
        lambda x: round(x * 100, 1) if pd.notna(x) else None
    )
    result["weighted_hit_rate"] = result["weighted_hit_rate"].apply(
        lambda x: round(x, 4) if pd.notna(x) else None
    )
    result["hit_rate_60"] = result["hit_rate_60"].apply(
        lambda x: round(x, 4) if pd.notna(x) else None
    )
    result["hit_rate_20"] = result["hit_rate_20"].apply(
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
                    t.grade_date   = s.grade_date
                AND t.event_id     = s.event_id
                AND t.player_id    = s.player_id
                AND t.market_key   = s.market_key
                AND t.bookmaker_key = s.bookmaker_key
                AND t.line_value   = s.line_value
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
def grade_date(engine, grade_date_str, props_df):
    """
    Grade all prop lines in props_df for a single game date.

    props_df must have columns:
        event_id, player_id, player_name, market_key, bookmaker_key,
        line_value, game_id

    Returns number of rows written.
    """
    if props_df.empty:
        log.info(f"  {grade_date_str}: no props. Skipping.")
        return 0

    player_ids  = props_df["player_id"].dropna().unique().tolist()
    market_keys = props_df["market_key"].dropna().unique().tolist()

    history_df = fetch_history(engine, player_ids, market_keys, grade_date_str)

    graded_df = compute_all_hit_rates(props_df, history_df)

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
            "hit_rate_60":       r["hit_rate_60"] if pd.notna(r.get("hit_rate_60")) else None,
            "hit_rate_20":       r["hit_rate_20"] if pd.notna(r.get("hit_rate_20")) else None,
            "sample_size_60":    int(r["sample_size_60"]) if pd.notna(r.get("sample_size_60")) else 0,
            "sample_size_20":    int(r["sample_size_20"]) if pd.notna(r.get("sample_size_20")) else 0,
            "weighted_hit_rate": r["weighted_hit_rate"] if pd.notna(r.get("weighted_hit_rate")) else None,
            "grade":             r["grade"] if pd.notna(r.get("grade")) else None,
        })

    written = upsert_grades(engine, rows)
    graded  = sum(1 for r in rows if r["grade"] is not None)
    log.info(f"  {grade_date_str}: {written} rows written, {graded} with grade.")
    return written


# ---------------------------------------------------------------------------
# Upcoming mode
# ---------------------------------------------------------------------------
def run_upcoming(engine):
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
                egm.game_id
            FROM odds.upcoming_player_props up
            LEFT JOIN odds.event_game_map egm
                ON  egm.event_id  = up.event_id
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

    log.info(f"Found {len(props)} prop lines across "
             f"{props['player_id'].nunique()} players.")

    grade_date(engine, today, props)


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
            WHERE pp.sport_key    = 'basketball_nba'
              AND pp.outcome_name = 'Over'
              AND pp.outcome_point IS NOT NULL
              AND egm.game_date   IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM common.daily_grades g
                  WHERE g.grade_date = CAST(egm.game_date AS DATE)
              )
            ORDER BY game_date ASC
        """),
        engine
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
        props = pd.read_sql(
            text("""
                SELECT DISTINCT
                    pp.event_id,
                    pm.player_id,
                    pp.player_name,
                    pp.market_key,
                    pp.bookmaker_key,
                    pp.outcome_point AS line_value,
                    egm.game_id
                FROM odds.player_props pp
                JOIN odds.event_game_map egm
                    ON  egm.event_id  = pp.event_id
                    AND egm.sport_key = 'basketball_nba'
                    AND egm.game_id   IS NOT NULL
                JOIN odds.player_map pm
                    ON  pm.odds_player_name = pp.player_name
                    AND pm.sport_key        = pp.sport_key
                    AND pm.player_id        IS NOT NULL
                WHERE pp.sport_key     = 'basketball_nba'
                  AND pp.outcome_name  = 'Over'
                  AND pp.outcome_point IS NOT NULL
                  AND CAST(egm.game_date AS DATE) = :gd
            """),
            engine,
            params={"gd": gd}
        )
        total += grade_date(engine, gd, props)

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
