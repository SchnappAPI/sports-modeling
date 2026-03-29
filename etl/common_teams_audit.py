"""
common_teams_audit.py

Writes the full contents of common.teams, nba.teams, mlb.teams,
and distinct NFL team abbreviations from nfl.games to CSV files
so nothing gets truncated in the Actions log.

Output: audit_output/common_teams_*.csv
"""

import os
import csv
import time
from pathlib import Path
from sqlalchemy import create_engine, text

OUT = Path("audit_output")
OUT.mkdir(exist_ok=True)


def get_engine():
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception as exc:
            print(f"Connection attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                time.sleep(45)
    raise RuntimeError("Could not connect after 3 attempts.")


def dump_csv(conn, filename, sql):
    result = conn.execute(text(sql))
    headers = list(result.keys())
    rows = result.fetchall()
    path = OUT / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"  {filename}: {len(rows)} rows -> {path}")
    return rows


def main():
    engine = get_engine()
    print("Connected.\n")

    with engine.connect() as conn:

        dump_csv(conn, "common_teams.csv",
                 "SELECT * FROM common.teams ORDER BY league, team_id")

        dump_csv(conn, "nba_teams.csv",
                 "SELECT * FROM nba.teams ORDER BY team_id")

        dump_csv(conn, "mlb_teams.csv",
                 "SELECT * FROM mlb.teams ORDER BY team_id")

        dump_csv(conn, "nfl_game_team_abbrs.csv",
                 """
                 SELECT DISTINCT team_abbr
                 FROM (
                     SELECT home_team AS team_abbr FROM nfl.games
                     UNION
                     SELECT away_team AS team_abbr FROM nfl.games
                 ) t
                 WHERE team_abbr IS NOT NULL
                 ORDER BY team_abbr
                 """)

        dump_csv(conn, "nfl_games_columns.csv",
                 """
                 SELECT column_name, data_type
                 FROM information_schema.columns
                 WHERE table_schema = 'nfl' AND table_name = 'games'
                 ORDER BY ordinal_position
                 """)

    print("\n=== AUDIT COMPLETE ===")
    print(f"All files written to: {OUT.resolve()}")


if __name__ == "__main__":
    main()
