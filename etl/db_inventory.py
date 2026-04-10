import os, subprocess, sys
out = "/tmp/mappings_debug2.txt"
script = """
import os
from sqlalchemy import create_engine, text

conn_str = (
    f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
    f"{os.environ['AZURE_SQL_PASSWORD']}@"
    f"{os.environ['AZURE_SQL_SERVER']}/"
    f"{os.environ['AZURE_SQL_DATABASE']}"
    "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=90"
)
engine = create_engine(conn_str, fast_executemany=False)

with engine.connect() as conn:
    r = conn.execute(text(\"\"\"
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='nba' AND TABLE_NAME='players'
        ORDER BY ORDINAL_POSITION
    \"\"\"))
    cols = [row[0] for row in r]
    print(f"nba.players columns: {cols}")

with engine.connect() as conn:
    # Check what name column exists and find sample players
    r = conn.execute(text("SELECT TOP 3 * FROM nba.players"))
    cols = [d[0] for d in r.cursor.description]
    rows = list(r)
    print(f"\\nSample rows, cols={cols}")
    for row in rows:
        print(f"  {dict(zip(cols, row))}")

with engine.connect() as conn:
    # Now check how odds_etl does its matching - look at player_map
    r = conn.execute(text("SELECT TOP 3 * FROM odds.player_map WHERE sport_key='basketball_nba'"))
    cols2 = [d[0] for d in r.cursor.description]
    rows2 = list(r)
    print(f"\\nodds.player_map cols: {cols2}")
    for row in rows2:
        print(f"  {dict(zip(cols2, row))}")

print("Done.")
"""
with open(out, "w") as f:
    subprocess.run([sys.executable, "-c", script], stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
