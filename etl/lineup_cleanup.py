"""
lineup_cleanup.py

One-shot script: deletes all nba.daily_lineups rows for game dates
on or after 2026-04-01 so they can be repulled cleanly by the
updated lineup_poll.py (which now includes bench + inactive players).

Run once via db_inventory.yml, then trigger refresh-data.yml.
"""

import os
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
    "Connection Timeout=60;"
)

conn = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

# How many rows are we about to delete?
cursor.execute(
    "SELECT COUNT(*) FROM nba.daily_lineups WHERE game_date >= '2026-04-01'"
)
count = cursor.fetchone()[0]
print(f"Rows to delete: {count}")

# Show which games are affected.
cursor.execute(
    """
    SELECT game_id, game_date,
           COUNT(*) AS row_count,
           SUM(CASE WHEN starter_status = 'Starter'  THEN 1 ELSE 0 END) AS starters,
           SUM(CASE WHEN starter_status = 'Bench'    THEN 1 ELSE 0 END) AS bench,
           SUM(CASE WHEN starter_status = 'Inactive' THEN 1 ELSE 0 END) AS inactive
    FROM nba.daily_lineups
    WHERE game_date >= '2026-04-01'
    GROUP BY game_id, game_date
    ORDER BY game_date, game_id
    """
)
for row in cursor.fetchall():
    print(f"  {row.game_id}  {row.game_date}  total={row.row_count}  "
          f"S={row.starters}  B={row.bench}  I={row.inactive}")

# Delete.
cursor.execute(
    "DELETE FROM nba.daily_lineups WHERE game_date >= '2026-04-01'"
)
conn.commit()
print(f"Deleted {cursor.rowcount} rows.")

conn.close()
print("Done.")
