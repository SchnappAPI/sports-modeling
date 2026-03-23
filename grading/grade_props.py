import argparse
import json
import os
import sys
import pandas as pd
from datetime import date, timedelta
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from etl.db import get_engine

LOOKBACK_DAYS = 60

# Columns to sum from per-period rows to get full-game totals.
# dd2 and td3 are excluded because they must be derived after aggregation.
SUM_COLS = [
    "fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
    "oreb", "dreb", "reb", "ast", "tov", "stl", "blk", "pts", "minutes_sec"
]

PROP_COL_MAP = {
    "points":    "pts",
    "rebounds":  "reb",
    "assists":   "ast",
    "threes":    "fg3m",
    "steals":    "stl",
    "blocks":    "blk",
    "turnovers": "tov",
    "pra":       "pra",   # derived
    "pr":        "pr",    # derived
    "ra":        "ra",    # derived
}

ALT_SPREAD = [-5.0, -2.5, 0.0, 2.5, 5.0]  # offsets from primary line


def get_player_game_totals(engine, player_id, game_date, lookback_days):
    """
    Aggregate per-period rows into full-game totals for a player
    over the lookback window. Returns one row per game_id.
    OT is included in all sums (matches sportsbook convention).
    """
    cutoff = (pd.to_datetime(game_date) - timedelta(days=lookback_days)).date()

    sum_expr = ", ".join([f"SUM({c}) AS {c}" for c in SUM_COLS])

    sql = f"""
        SELECT
            game_id,
            game_date,
            {sum_expr}
        FROM nba.player_box_score_stats
        WHERE player_id = :pid
          AND game_date >= :cutoff
          AND game_date < :gd
        GROUP BY game_id, game_date
        ORDER BY game_date ASC
    """

    df = pd.read_sql(
        text(sql),
        engine,
        params={"pid": player_id, "cutoff": str(cutoff), "gd": str(game_date)}
    )

    if df.empty:
        return df

    # Derive combo props after aggregation
    df["pra"] = df["pts"] + df["reb"] + df["ast"]
    df["pr"]  = df["pts"] + df["reb"]
    df["ra"]  = df["reb"] + df["ast"]

    return df


def hit_rate(df, col, line):
    """Fraction of games where col exceeded line. Returns (rate, sample_size)."""
    if df.empty or col not in df.columns:
        return None, 0
    valid = df[col].dropna()
    if valid.empty:
        return None, 0
    n = len(valid)
    rate = round((valid > line).sum() / n, 4)
    return rate, n


def alternate_hit_rates(df, col, primary_line, spreads):
    results = []
    for offset in spreads:
        threshold = primary_line + offset
        rate, n = hit_rate(df, col, threshold)
        results.append({
            "threshold": threshold,
            "hit_rate": rate,
            "sample_size": n
        })
    return results


def scale_grade(hit_rate_value):
    """
    Phase 1: grade is the hit rate scaled to 0-100.
    Later phases blend additional components in here.
    """
    if hit_rate_value is None:
        return None
    return round(hit_rate_value * 100, 1)


def get_lines(engine, grade_date):
    """Pull active prop lines for the given date from common.prop_lines."""
    sql = """
        SELECT player_id, player_name, prop_type, line_value
        FROM common.prop_lines
        WHERE grade_date = :gd AND active = 1
    """
    df = pd.read_sql(text(sql), engine, params={"gd": str(grade_date)})
    return df.to_dict("records")


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'common' AND TABLE_NAME = 'daily_grades'
            )
            CREATE TABLE common.daily_grades (
                grade_id             INT IDENTITY(1,1) PRIMARY KEY,
                grade_date           DATE NOT NULL,
                player_id            INT NOT NULL,
                player_name          NVARCHAR(100) NOT NULL,
                prop_type            NVARCHAR(50) NOT NULL,
                line_value           FLOAT NOT NULL,
                final_grade          FLOAT,
                hit_rate_score       FLOAT,
                sample_size          INT,
                alternate_hit_rates  NVARCHAR(MAX),
                created_at           DATETIME2 DEFAULT GETUTCDATE(),
                CONSTRAINT uq_daily_grade UNIQUE (grade_date, player_id, prop_type, line_value)
            )
        """))


def upsert_grades(engine, rows):
    if not rows:
        print("No grades to write.")
        return

    df = pd.DataFrame(rows)
    df = df.where(pd.notna(df), other=None)

    df.to_sql("#stage_daily_grades", engine, index=False, if_exists="replace", chunksize=200)

    with engine.begin() as conn:
        conn.execute(text("""
            MERGE common.daily_grades AS t
            USING #stage_daily_grades AS s
            ON (    t.grade_date = s.grade_date
                AND t.player_id  = s.player_id
                AND t.prop_type  = s.prop_type
                AND t.line_value = s.line_value)
            WHEN MATCHED THEN UPDATE SET
                t.final_grade         = s.final_grade,
                t.hit_rate_score      = s.hit_rate_score,
                t.sample_size         = s.sample_size,
                t.alternate_hit_rates = s.alternate_hit_rates
            WHEN NOT MATCHED THEN INSERT (
                grade_date, player_id, player_name, prop_type, line_value,
                final_grade, hit_rate_score, sample_size, alternate_hit_rates
            ) VALUES (
                s.grade_date, s.player_id, s.player_name, s.prop_type, s.line_value,
                s.final_grade, s.hit_rate_score, s.sample_size, s.alternate_hit_rates
            );
        """))


def run(grade_date=None):
    if grade_date is None:
        grade_date = date.today()
    else:
        grade_date = pd.to_datetime(grade_date).date()

    engine = get_engine()
    ensure_tables(engine)

    lines = get_lines(engine, grade_date)
    if not lines:
        print(f"No active prop lines found for {grade_date}. "
              f"Insert rows into common.prop_lines and re-run.")
        return

    print(f"Grading {len(lines)} props for {grade_date}.")
    rows = []

    for item in lines:
        pid       = item["player_id"]
        name      = item["player_name"]
        prop      = item["prop_type"]
        line      = item["line_value"]
        col       = PROP_COL_MAP.get(prop)

        if col is None:
            print(f"  SKIP: unknown prop_type '{prop}' for {name}")
            continue

        df = get_player_game_totals(engine, pid, grade_date, LOOKBACK_DAYS)
        hr, n = hit_rate(df, col, line)
        grade = scale_grade(hr)
        alt   = alternate_hit_rates(df, col, line, ALT_SPREAD)

        print(f"  {name} | {prop} {line} | hit_rate={hr} n={n} | grade={grade}")

        rows.append({
            "grade_date":          str(grade_date),
            "player_id":           pid,
            "player_name":         name,
            "prop_type":           prop,
            "line_value":          line,
            "final_grade":         grade,
            "hit_rate_score":      round(hr * 100, 1) if hr is not None else None,
            "sample_size":         n,
            "alternate_hit_rates": json.dumps(alt),
        })

    upsert_grades(engine, rows)
    print(f"Done. Wrote {len(rows)} grades for {grade_date}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None,
                        help="Grade date in YYYY-MM-DD format. Defaults to today.")
    args = parser.parse_args()
    run(grade_date=args.date)
