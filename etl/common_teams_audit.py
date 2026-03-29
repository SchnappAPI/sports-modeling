"""
common_teams_audit.py

Prints the current contents of common.teams, nba.teams, mlb.teams,
and the distinct team abbreviations found in nfl.games so we can
plan the common.teams migration.
"""

import os
import time
from sqlalchemy import create_engine, text


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


def dump(conn, label, sql):
    print(f"\n=== {label} ===")
    result = conn.execute(text(sql))
    headers = list(result.keys())
    rows = result.fetchall()
    print("  " + " | ".join(f"{h:<25}" for h in headers))
    print("  " + "-" * (28 * len(headers)))
    for row in rows:
        print("  " + " | ".join(f"{str(v) if v is not None else 'NULL':<25}" for v in row))
    print(f"  ({len(rows)} rows)")


def main():
    engine = get_engine()
    print("Connected.")

    with engine.connect() as conn:

        # common.teams - full dump
        dump(conn, "common.teams (all columns, all rows)",
             "SELECT * FROM common.teams ORDER BY 1")

        # nba.teams - full dump
        dump(conn, "nba.teams (all columns)",
             "SELECT * FROM nba.teams ORDER BY team_id")

        # mlb.teams - full dump
        dump(conn, "mlb.teams (all columns)",
             "SELECT * FROM mlb.teams ORDER BY team_id")

        # NFL team abbreviations from nfl.games
        dump(conn, "NFL team abbreviations from nfl.games (home_team + away_team)",
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

        # nfl.games column names so we know the exact field names
        dump(conn, "nfl.games columns",
             """
             SELECT column_name, data_type
             FROM information_schema.columns
             WHERE table_schema = 'nfl' AND table_name = 'games'
             ORDER BY ordinal_position
             """)

    print("\n=== AUDIT COMPLETE ===")


if __name__ == "__main__":
    main()
