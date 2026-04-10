import os, subprocess, sys
out = "/tmp/backfill_check.txt"
script = """
import os
from sqlalchemy import create_engine, text

conn_str = (
    f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
    f"{os.environ['AZURE_SQL_PASSWORD']}@"
    f"{os.environ['AZURE_SQL_SERVER']}/"
    f"{os.environ['AZURE_SQL_DATABASE']}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=90"
)
engine = create_engine(conn_str, fast_executemany=False)

with engine.connect() as conn:
    # Get actual column names for odds.player_props
    cols = conn.execute(text(\"\"\"
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='player_props'
        ORDER BY ORDINAL_POSITION
    \"\"\"))
    print("odds.player_props columns:")
    for c in cols:
        print(f"  {c[0]}")

    # Latest entries and date coverage
    r = conn.execute(text(\"\"\"
        SELECT TOP 5 COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='player_props'
          AND COLUMN_NAME LIKE '%date%' OR COLUMN_NAME LIKE '%time%'
    \"\"\"))

    # Check unmapped players
    r2 = conn.execute(text(\"\"\"
        SELECT COUNT(*) AS unmapped
        FROM odds.player_map
        WHERE sport_key = 'basketball_nba' AND player_id IS NULL
    \"\"\"))
    print(f"\\nUnmapped NBA players: {r2.scalar():,}")

    # Latest grade date
    r3 = conn.execute(text(\"\"\"
        SELECT MAX(grade_date) AS latest, COUNT(DISTINCT grade_date) AS days
        FROM common.daily_grades WHERE outcome_name = 'Over'
    \"\"\"))
    row = r3.fetchone()
    print(f"Latest grade: {row[0]}  Days graded: {row[1]}")

print("Done.")
"""
with open(out, "w") as f:
    subprocess.run([sys.executable, "-c", script], stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
