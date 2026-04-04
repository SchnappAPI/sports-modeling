"""
game_day_gate.py

Single gate script for nba-game-day.yml.
Queries nba.schedule and writes GitHub Actions outputs:

  has_pregame       true/false  -- any game_status = 1 today
  has_live          true/false  -- any game_status = 2 today
  has_final         true/false  -- any game_status = 3 today with no grade rows yet
  any_active        true/false  -- has_pregame OR has_live
  final_date        YYYY-MM-DD  -- ET game date for newly-final grading
  run_odds_grading  true/false  -- true if enough time has elapsed since last
                                   intraday grade (interval depends on game state)

Odds/grading interval:
  Pre-game only:  14 minutes  (lines can move; grade fresh often)
  Any game live:  30 minutes  (lines are locked; less need to re-grade)

Exit code is always 0.
"""

import os
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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

ET_TZ    = ZoneInfo("America/New_York")
TODAY_ET = datetime.now(ET_TZ).strftime("%Y-%m-%d")

# Minutes between odds+grading runs depending on game state.
INTERVAL_PREGAME = 14   # lines can still move pre-game
INTERVAL_LIVE    = 30   # lines locked once games start; less urgency

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

            cursor.execute(QUERY_GATE)
            row = cursor.fetchone()

            pregame     = int(row[0] or 0)
            live        = int(row[1] or 0)
            newly_final = int(row[3] or 0)

            cursor.execute(QUERY_LAST_GRADE)
            trow = cursor.fetchone()
            minutes_since = trow[0] if trow and trow[0] is not None else 9999

            conn.close()

            # Use a longer interval once any game is live — lines are locked
            # and re-grading every 14 minutes adds no value mid-game.
            interval = INTERVAL_LIVE if live > 0 else INTERVAL_PREGAME
            run_odds_grading = "true" if minutes_since >= interval else "false"

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
                f"minutes_since_grade={minutes_since} interval={interval} "
                f"run_odds={run_odds_grading} today_et={TODAY_ET}",
                file=sys.stderr,
            )
            return

        except Exception as exc:
            print(f"DB attempt {attempt}/3 failed: {exc}", file=sys.stderr)
            if attempt < 3:
                time.sleep(45)

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
