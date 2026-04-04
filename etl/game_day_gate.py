"""
game_day_gate.py

Single gate script for nba-game-day.yml.
Queries nba.schedule and writes GitHub Actions outputs:

  has_pregame       true/false  -- any game_status = 1 today
  has_live          true/false  -- any game_status = 2 today
  has_final         true/false  -- any game_status = 3 today with no grade rows yet
  any_active        true/false  -- has_pregame OR has_live
  final_date        YYYY-MM-DD  -- ET game date for newly-final grading
  run_odds_grading  true/false  -- true if >= 14 minutes since last intraday grade

The run_odds_grading flag replaces the fragile run_number % 3 throttle.
It checks the MAX created_at timestamp in common.daily_grades for today
and returns true if more than 14 minutes have elapsed, giving a reliable
~15-minute cadence that is drift-proof and self-healing.

Exit code is always 0.
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta

import pyodbc

DRIVER   = "ODBC Driver 18 for SQL Server"
SERVER   = os.environ["AZURE_SQL_SERVER"]
DATABASE = os.environ["AZURE_SQL_DATABASE"]
USERNAME = os.environ["AZURE_SQL_USERNAME"]
PASSWORD = os.environ["AZURE_SQL_PASSWORD"]

CONN_STR = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=90;"
)

# Use Eastern time (UTC-4 during EDT) for the game date.
ET_OFFSET = timedelta(hours=-4)
NOW_UTC   = datetime.now(timezone.utc)
TODAY_ET  = NOW_UTC.astimezone(timezone(ET_OFFSET)).strftime("%Y-%m-%d")

# Minimum minutes between intraday odds+grading runs.
ODDS_INTERVAL_MINUTES = 14

QUERY = f"""
SELECT
    SUM(CASE WHEN game_status = 1 THEN 1 ELSE 0 END) AS pregame_count,
    SUM(CASE WHEN game_status = 2 THEN 1 ELSE 0 END) AS live_count,
    SUM(CASE WHEN game_status = 3 THEN 1 ELSE 0 END) AS final_count,
    SUM(CASE
        WHEN s.game_status = 3
         AND NOT EXISTS (
             SELECT 1 FROM common.daily_grades g
             WHERE g.grade_date = CAST(s.game_date AS DATE)
               AND g.bookmaker_key = 'fanduel'
         )
        THEN 1 ELSE 0 END) AS newly_final_count,
    -- Minutes since last intraday grade written today (NULL if none yet)
    DATEDIFF(minute,
        MAX(CASE WHEN CAST(g2.grade_date AS DATE) = '{TODAY_ET}'
                 THEN g2.created_at END),
        GETUTCDATE()
    ) AS minutes_since_last_grade
FROM nba.schedule s
CROSS JOIN (SELECT MAX(created_at) AS created_at, grade_date
             FROM common.daily_grades
             GROUP BY grade_date) g2
WHERE CONVERT(VARCHAR(10), s.game_date, 120) = '{TODAY_ET}'
"""

# Simpler fallback query if cross join causes issues
QUERY_GATE = f"""
SELECT
    SUM(CASE WHEN game_status = 1 THEN 1 ELSE 0 END) AS pregame_count,
    SUM(CASE WHEN game_status = 2 THEN 1 ELSE 0 END) AS live_count,
    SUM(CASE WHEN game_status = 3 THEN 1 ELSE 0 END) AS final_count,
    SUM(CASE
        WHEN s.game_status = 3
         AND NOT EXISTS (
             SELECT 1 FROM common.daily_grades g
             WHERE g.grade_date = CAST(s.game_date AS DATE)
               AND g.bookmaker_key = 'fanduel'
         )
        THEN 1 ELSE 0 END) AS newly_final_count
FROM nba.schedule s
WHERE CONVERT(VARCHAR(10), s.game_date, 120) = '{TODAY_ET}'
"""

QUERY_LAST_GRADE = f"""
SELECT DATEDIFF(minute, MAX(created_at), GETUTCDATE()) AS minutes_since
FROM common.daily_grades
WHERE grade_date = '{TODAY_ET}'
"""


def run():
    for attempt in range(1, 4):
        try:
            conn   = pyodbc.connect(CONN_STR, timeout=90)
            cursor = conn.cursor()

            # Gate query
            cursor.execute(QUERY_GATE)
            row = cursor.fetchone()

            pregame     = int(row[0] or 0)
            live        = int(row[1] or 0)
            newly_final = int(row[3] or 0)

            # Timestamp-based odds/grading gate
            cursor.execute(QUERY_LAST_GRADE)
            trow = cursor.fetchone()
            minutes_since = trow[0] if trow and trow[0] is not None else 9999
            run_odds_grading = "true" if minutes_since >= ODDS_INTERVAL_MINUTES else "false"

            conn.close()

            has_pregame = "true" if pregame > 0 else "false"
            has_live    = "true" if live    > 0 else "false"
            has_final   = "true" if newly_final > 0 else "false"
            any_active  = "true" if (pregame + live) > 0 else "false"
            final_date  = TODAY_ET if newly_final > 0 else ""

            lines = [
                f"has_pregame={has_pregame}",
                f"has_live={has_live}",
                f"has_final={has_final}",
                f"any_active={any_active}",
                f"final_date={final_date}",
                f"run_odds_grading={run_odds_grading}",
            ]

            output_file = os.environ.get("GITHUB_OUTPUT")
            if output_file:
                with open(output_file, "a") as f:
                    f.write("\n".join(lines) + "\n")
            for line in lines:
                print(line, file=sys.stderr)

            print(
                f"Gate: pregame={pregame} live={live} newly_final={newly_final} "
                f"minutes_since_grade={minutes_since} run_odds={run_odds_grading} today_et={TODAY_ET}",
                file=sys.stderr,
            )
            return

        except Exception as exc:
            print(f"DB attempt {attempt}/3 failed: {exc}", file=sys.stderr)
            if attempt < 3:
                time.sleep(45)

    # Fallback: default everything to safe values
    print("Gate check failed — defaulting to false.", file=sys.stderr)
    output_file = os.environ.get("GITHUB_OUTPUT")
    lines = [
        "has_pregame=false",
        "has_live=false",
        "has_final=false",
        "any_active=false",
        "final_date=",
        "run_odds_grading=false",
    ]
    if output_file:
        with open(output_file, "a") as f:
            f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    run()
