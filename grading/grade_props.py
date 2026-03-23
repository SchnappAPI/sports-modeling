import argparse
import json
import os
import sys
import time
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

LOOKBACK_DAYS = 60
BATCH_SIZE    = 10   # game dates processed per run (increase for backfill)


def get_engine(max_retries=3, retry_wait=45):
    """
    Grading-specific engine with fast_executemany=False.
    fast_executemany pre-allocates a fixed buffer from the first row which
    truncates NVARCHAR(MAX) columns when later rows have longer strings.
    """
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=False)
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            if i == max_retries - 1:
                raise
            print(f"  Connection attempt {i + 1} failed. Retrying in {retry_wait}s...")
            time.sleep(retry_wait)


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'common' AND TABLE_NAME = 'daily_grades'
            )
            CREATE TABLE common.daily_grades (
                grade_id            INT IDENTITY(1,1) PRIMARY KEY,
                grade_date          DATE          NOT NULL,
                player_name         NVARCHAR(100) NOT NULL,
                stat_code           NVARCHAR(10)  NOT NULL,
                line_value          FLOAT         NOT NULL,
                hit_rate            FLOAT,
                sample_size         INT,
                grade               FLOAT,
                all_line_hit_rates  NVARCHAR(MAX),
                created_at          DATETIME2 DEFAULT GETUTCDATE(),
                CONSTRAINT uq_daily_grade
                    UNIQUE (grade_date, player_name, stat_code, line_value)
            );
        """))


def get_work_dates(engine, grade_date, batch_size, lookback_days):
    """
    Return up to batch_size game dates that:
      - exist in the box score table
      - have enough lookback history (at least lookback_days of prior data)
      - are not yet in daily_grades
    Oldest dates first.
    """
    earliest_gradeable = pd.read_sql(
        text("""
            SELECT DATEADD(day, :lb, MIN(game_date)) AS earliest
            FROM nba.player_box_score_stats
        """),
        engine,
        params={"lb": lookback_days}
    ).iloc[0]["earliest"]

    if earliest_gradeable is None:
        return []

    cutoff = str(grade_date) if grade_date else "9999-12-31"

    missing = pd.read_sql(
        text("""
            SELECT DISTINCT b.game_date
            FROM nba.player_box_score_stats b
            WHERE b.game_date >= :earliest
              AND b.game_date <= :cutoff
              AND NOT EXISTS (
                  SELECT 1 FROM common.daily_grades g
                  WHERE g.grade_date = b.game_date
              )
            ORDER BY b.game_date ASC
        """),
        engine,
        params={"earliest": str(earliest_gradeable), "cutoff": cutoff}
    )

    return missing["game_date"].astype(str).tolist()[:batch_size]


def grade_date_set_based(engine, grade_date):
    """
    Compute all hit rates for all active players on a single grade_date
    using one set-based SQL query.

    Active players = confirmed/expected from daily_lineups if available,
    otherwise all players who appeared in the lookback window.

    Returns a list of row dicts ready to upsert.
    """
    # Determine active players for this date
    lineup_players = pd.read_sql(
        text("""
            SELECT DISTINCT player_name
            FROM nba.daily_lineups
            WHERE game_date   = :gd
              AND roster_status = 'Active'
              AND lineup_status IN ('Confirmed', 'Expected')
        """),
        engine,
        params={"gd": grade_date}
    )

    if not lineup_players.empty:
        player_filter_sql = """
            JOIN (
                SELECT DISTINCT player_name
                FROM nba.daily_lineups
                WHERE game_date    = :gd
                  AND roster_status = 'Active'
                  AND lineup_status IN ('Confirmed', 'Expected')
            ) lp ON g.player_name = lp.player_name
        """
        source = "lineup"
    else:
        player_filter_sql = """
            JOIN (
                SELECT DISTINCT player_name
                FROM nba.player_box_score_stats
                WHERE game_date >= DATEADD(day, -:lb, :gd)
                  AND game_date <  :gd
            ) lp ON g.player_name = lp.player_name
        """
        source = "fallback"

    # One query: aggregate per-period rows into game totals, derive combo stats,
    # cross join with thresholds, compute hit rates — all in the database.
    sql = f"""
        WITH game_totals AS (
            SELECT
                player_name,
                game_date,
                SUM(CAST(pts  AS FLOAT)) AS pts,
                SUM(CAST(ast  AS FLOAT)) AS ast,
                SUM(CAST(reb  AS FLOAT)) AS reb,
                SUM(CAST(fg3m AS FLOAT)) AS fg3m,
                SUM(CAST(stl  AS FLOAT)) AS stl,
                SUM(CAST(blk  AS FLOAT)) AS blk
            FROM nba.player_box_score_stats
            WHERE game_date >= DATEADD(day, -:lb, :gd)
              AND game_date <  :gd
            GROUP BY player_name, game_date
        ),
        game_totals_with_combos AS (
            SELECT
                player_name,
                game_date,
                pts,
                ast,
                reb,
                fg3m,
                stl,
                blk,
                pts + reb            AS pr,
                pts + ast            AS pa,
                pts + reb + ast      AS pra,
                reb + ast            AS ra
            FROM game_totals
        ),
        unpivoted AS (
            SELECT player_name, game_date, 'PTS' AS stat_code, pts  AS stat_value FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'AST',  ast  FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'REB',  reb  FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, '3PM',  fg3m FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'STL',  stl  FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'BLK',  blk  FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'PR',   pr   FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'PA',   pa   FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'PRA',  pra  FROM game_totals_with_combos
            UNION ALL
            SELECT player_name, game_date, 'RA',   ra   FROM game_totals_with_combos
        ),
        player_stats AS (
            SELECT u.player_name, u.stat_code, u.game_date, u.stat_value
            FROM unpivoted u
            {player_filter_sql}
        ),
        hit_counts AS (
            SELECT
                ps.player_name,
                ps.stat_code,
                t.line_value,
                COUNT(*)                                                        AS sample_size,
                SUM(CASE WHEN ps.stat_value > t.line_value THEN 1 ELSE 0 END)  AS hits
            FROM player_stats ps
            JOIN common.grade_thresholds t ON t.stat_code = ps.stat_code
            GROUP BY ps.player_name, ps.stat_code, t.line_value
        )
        SELECT
            player_name,
            stat_code,
            line_value,
            sample_size,
            hits,
            CAST(hits AS FLOAT) / NULLIF(sample_size, 0) AS hit_rate
        FROM hit_counts
        ORDER BY player_name, stat_code, line_value;
    """

    df = pd.read_sql(
        text(sql),
        engine,
        params={"gd": grade_date, "lb": LOOKBACK_DAYS}
    )

    if df.empty:
        return [], source

    # Build the all_line_hit_rates JSON per player+stat using the full result
    # already in memory — no additional queries needed.
    json_map = {}
    for (player, stat), group in df.groupby(["player_name", "stat_code"]):
        json_map[(player, stat)] = json.dumps([
            {
                "line":        float(row["line_value"]),
                "hit_rate":    round(float(row["hit_rate"]), 4) if row["hit_rate"] is not None else None,
                "sample_size": int(row["sample_size"])
            }
            for _, row in group.iterrows()
        ])

    rows = []
    for _, row in df.iterrows():
        hr = float(row["hit_rate"]) if row["hit_rate"] is not None else None
        rows.append({
            "grade_date":         grade_date,
            "player_name":        row["player_name"],
            "stat_code":          row["stat_code"],
            "line_value":         float(row["line_value"]),
            "hit_rate":           round(hr, 4) if hr is not None else None,
            "sample_size":        int(row["sample_size"]),
            "grade":              round(hr * 100, 1) if hr is not None else None,
            "all_line_hit_rates": json_map[(row["player_name"], row["stat_code"])],
        })

    return rows, source


def upsert_grades(engine, rows):
    if not rows:
        return

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE #stage_daily_grades (
                grade_date         DATE,
                player_name        NVARCHAR(100),
                stat_code          NVARCHAR(10),
                line_value         FLOAT,
                hit_rate           FLOAT,
                sample_size        INT,
                grade              FLOAT,
                all_line_hit_rates NVARCHAR(MAX)
            )
        """))

        chunk_size = 200
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i + chunk_size]
            tuples = [
                (
                    r["grade_date"],
                    r["player_name"],
                    r["stat_code"],
                    r["line_value"],
                    r["hit_rate"],
                    r["sample_size"],
                    r["grade"],
                    r["all_line_hit_rates"],
                )
                for r in chunk
            ]
            conn.exec_driver_sql(
                "INSERT INTO #stage_daily_grades VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                tuples
            )

        conn.execute(text("""
            MERGE common.daily_grades AS t
            USING #stage_daily_grades AS s
            ON (    t.grade_date  = s.grade_date
                AND t.player_name = s.player_name
                AND t.stat_code   = s.stat_code
                AND t.line_value  = s.line_value)
            WHEN MATCHED THEN UPDATE SET
                t.hit_rate           = s.hit_rate,
                t.sample_size        = s.sample_size,
                t.grade              = s.grade,
                t.all_line_hit_rates = s.all_line_hit_rates
            WHEN NOT MATCHED THEN INSERT (
                grade_date, player_name, stat_code, line_value,
                hit_rate, sample_size, grade, all_line_hit_rates
            ) VALUES (
                s.grade_date, s.player_name, s.stat_code, s.line_value,
                s.hit_rate, s.sample_size, s.grade, s.all_line_hit_rates
            );
        """))


def run(grade_date=None, batch_size=BATCH_SIZE):
    engine = get_engine()
    ensure_tables(engine)

    # Determine the upper bound date for grading.
    # If a specific date is passed, grade only that date.
    # If no date is passed, grade all missing dates up to yesterday
    # (today's games have not been played yet).
    if grade_date:
        target_date = str(pd.to_datetime(grade_date).date())
        work_dates = [target_date]
    else:
        yesterday = str(date.today() - timedelta(days=1))
        work_dates = get_work_dates(engine, yesterday, batch_size, LOOKBACK_DAYS)

    if not work_dates:
        print("All dates already graded. Nothing to do.")
        return

    print(f"Grading {len(work_dates)} date(s): {work_dates[0]} to {work_dates[-1]}")

    total_rows = 0
    for gd in work_dates:
        rows, source = grade_date_set_based(engine, gd)

        if not rows:
            print(f"  {gd}: no data. Skipping.")
            continue

        upsert_grades(engine, rows)
        total_rows += len(rows)
        print(f"  {gd}: {len(rows)} grades written ({source}).")

    print(f"Done. {total_rows} total grade rows written.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date", type=str, default=None,
        help="Grade a specific date (YYYY-MM-DD). Omit to process missing dates automatically."
    )
    parser.add_argument(
        "--batch", type=int, default=BATCH_SIZE,
        help=f"Max dates to process per run (default {BATCH_SIZE}). Use higher values for backfill."
    )
    args = parser.parse_args()
    run(grade_date=args.date, batch_size=args.batch)
