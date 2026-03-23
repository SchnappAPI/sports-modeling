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
FALLBACK_DAYS = 14  # days of recent play used when no lineup data exists

# Generic lines per stat code
GENERIC_LINES = {
    "PTS": [0, 10, 15, 20, 25, 30, 35, 40],
    "AST": [0, 2, 4, 6, 8, 10, 12, 14],
    "REB": [0, 4, 6, 8, 10, 12, 14, 16],
    "3PM": [0, 1, 2, 3, 4, 5, 6, 7],
    "STL": [0, 1, 2, 3, 4, 5, 6, 7],
    "BLK": [0, 1, 2, 3, 4, 5, 6, 7],
    "PR":  [0, 10, 15, 20, 25, 30, 35, 40, 45, 50],
    "PA":  [0, 10, 15, 20, 25, 30, 35, 40, 45, 50],
    "PRA": [0, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65],
    "RA":  [0, 10, 15, 20, 25, 30, 35],
}

# Maps stat code to the aggregated column name used after summation
STAT_COL_MAP = {
    "PTS": "pts",
    "AST": "ast",
    "REB": "reb",
    "3PM": "fg3m",
    "STL": "stl",
    "BLK": "blk",
    "PR":  "pr",
    "PA":  "pa",
    "PRA": "pra",
    "RA":  "ra",
}

# Numeric columns to SUM from per-period rows to get full-game totals.
# minutes_sec is varchar in the source table and is excluded.
SUM_COLS = [
    "pts", "ast", "reb", "fg3m", "stl", "blk",
    "fgm", "fga", "fg3a", "ftm", "fta",
    "oreb", "dreb", "tov",
]


def get_engine(max_retries=3, retry_wait=45):
    """
    Grading-specific engine with fast_executemany=False.
    fast_executemany pre-allocates a fixed buffer from the first row, which
    truncates NVARCHAR(MAX) columns like all_line_hit_rates when later rows
    contain longer strings (e.g. PRA with 13 thresholds vs PTS with 8).
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


def get_active_players(engine, grade_date):
    """
    Return a list of player_name strings for players expected to play today.

    Priority:
      1. Confirmed active players from daily_lineups
      2. Expected active players from daily_lineups
      3. Fallback: distinct players who appeared in a game in the last FALLBACK_DAYS
    """
    date_str = str(grade_date)

    confirmed = pd.read_sql(
        text("""
            SELECT DISTINCT player_name
            FROM nba.daily_lineups
            WHERE game_date = :gd
              AND lineup_status = 'Confirmed'
              AND roster_status = 'Active'
        """),
        engine,
        params={"gd": date_str}
    )

    if not confirmed.empty:
        print(f"  Using {len(confirmed)} confirmed active players from daily_lineups.")
        return confirmed["player_name"].tolist()

    expected = pd.read_sql(
        text("""
            SELECT DISTINCT player_name
            FROM nba.daily_lineups
            WHERE game_date = :gd
              AND lineup_status = 'Expected'
              AND roster_status = 'Active'
        """),
        engine,
        params={"gd": date_str}
    )

    if not expected.empty:
        print(f"  Using {len(expected)} expected active players from daily_lineups.")
        return expected["player_name"].tolist()

    cutoff = (pd.to_datetime(grade_date) - timedelta(days=FALLBACK_DAYS)).date()
    recent = pd.read_sql(
        text("""
            SELECT DISTINCT player_name
            FROM nba.player_box_score_stats
            WHERE game_date >= :cutoff
        """),
        engine,
        params={"cutoff": str(cutoff)}
    )

    print(f"  No lineup data for {grade_date}. "
          f"Falling back to {len(recent)} players active in last {FALLBACK_DAYS} days.")
    return recent["player_name"].tolist()


def get_player_game_totals(engine, player_name, grade_date, lookback_days):
    """
    Aggregate per-period rows into full-game totals for a player
    over the lookback window. Returns one row per game_id.
    OT is included in all sums.
    """
    cutoff = (pd.to_datetime(grade_date) - timedelta(days=lookback_days)).date()

    sum_expr = ", ".join([f"SUM(CAST({c} AS FLOAT)) AS {c}" for c in SUM_COLS])

    df = pd.read_sql(
        text(f"""
            SELECT game_id, game_date, {sum_expr}
            FROM nba.player_box_score_stats
            WHERE player_name = :name
              AND game_date >= :cutoff
              AND game_date < :gd
            GROUP BY game_id, game_date
            ORDER BY game_date ASC
        """),
        engine,
        params={"name": player_name, "cutoff": str(cutoff), "gd": str(grade_date)}
    )

    if df.empty:
        return df

    df["pr"]  = df["pts"] + df["reb"]
    df["pa"]  = df["pts"] + df["ast"]
    df["pra"] = df["pts"] + df["reb"] + df["ast"]
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
    rate = round(float((valid > line).sum()) / n, 4)
    return rate, n


def build_grade_rows(player_name, grade_date, df):
    """
    For a single player's game history DataFrame, compute hit rates
    across every stat code and every generic line. Returns a list of
    row dicts ready to upsert into common.daily_grades.
    """
    rows = []

    for stat_code, lines in GENERIC_LINES.items():
        col = STAT_COL_MAP[stat_code]

        if df.empty or col not in df.columns:
            continue

        line_results = []
        for line in lines:
            hr, n = hit_rate(df, col, line)
            line_results.append({
                "line": line,
                "hit_rate": hr,
                "sample_size": n
            })

        for result in line_results:
            hr_val = result["hit_rate"]
            rows.append({
                "grade_date":         str(grade_date),
                "player_name":        player_name,
                "stat_code":          stat_code,
                "line_value":         float(result["line"]),
                "hit_rate":           float(hr_val) if hr_val is not None else None,
                "sample_size":        int(result["sample_size"]),
                "grade":              round(float(hr_val) * 100, 1) if hr_val is not None else None,
                "all_line_hit_rates": json.dumps(line_results),
            })

    return rows


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = 'common' AND TABLE_NAME = 'daily_grades'
            )
            CREATE TABLE common.daily_grades (
                grade_id            INT IDENTITY(1,1) PRIMARY KEY,
                grade_date          DATE NOT NULL,
                player_name         NVARCHAR(100) NOT NULL,
                stat_code           NVARCHAR(10) NOT NULL,
                line_value          FLOAT NOT NULL,
                hit_rate            FLOAT,
                sample_size         INT,
                grade               FLOAT,
                all_line_hit_rates  NVARCHAR(MAX),
                created_at          DATETIME2 DEFAULT GETUTCDATE(),
                CONSTRAINT uq_daily_grade UNIQUE (grade_date, player_name, stat_code, line_value)
            )
        """))


def upsert_grades(engine, rows):
    if not rows:
        print("No grades to write.")
        return

    # All staging work happens inside a single connection so the local temp
    # table remains visible for the INSERT chunks and the MERGE that follows.
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
            )
        """))


def run(grade_date=None):
    if grade_date is None:
        grade_date = date.today()
    else:
        grade_date = pd.to_datetime(grade_date).date()

    engine = get_engine()
    ensure_tables(engine)

    print(f"Grading date: {grade_date}")
    players = get_active_players(engine, grade_date)

    if not players:
        print("No active players found. Exiting.")
        return

    all_rows = []
    skipped = 0

    for player_name in players:
        df = get_player_game_totals(engine, player_name, grade_date, LOOKBACK_DAYS)

        if df.empty:
            skipped += 1
            continue

        rows = build_grade_rows(player_name, grade_date, df)
        all_rows.extend(rows)

    print(f"  {len(players) - skipped} players graded, {skipped} skipped (no history).")
    print(f"  Writing {len(all_rows)} grade rows...")

    upsert_grades(engine, all_rows)
    print(f"Done. Wrote {len(all_rows)} grades for {grade_date}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None,
                        help="Grade date in YYYY-MM-DD format. Defaults to today.")
    args = parser.parse_args()
    run(grade_date=args.date)
