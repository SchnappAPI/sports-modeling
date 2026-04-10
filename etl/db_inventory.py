import os, subprocess, sys
out = "/tmp/mappings_debug.txt"
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

# Check if these names exist in nba.players at all
with engine.connect() as conn:
    r = conn.execute(text(\"\"\"
        SELECT display_name, player_id FROM nba.players
        WHERE display_name IN (
            'Spencer Dinwiddie','Malik Beasley','Malcolm Brogdon',
            'Ben Simmons','Georges Niang','Cam Johnson','Alec Burks'
        )
    \"\"\"))
    found = list(r)
    print(f"Players found in nba.players matching unmapped names: {len(found)}")
    for row in found:
        print(f"  {row[0]} -> pid={row[1]}")

with engine.connect() as conn:
    # Check what columns odds.player_map has
    r = conn.execute(text(\"\"\"
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='player_map'
        ORDER BY ORDINAL_POSITION
    \"\"\"))
    print(f"\\nodds.player_map columns: {[row[0] for row in r]}")

with engine.connect() as conn:
    # Check if there's a name_normalized or match_attempted column
    r = conn.execute(text("SELECT TOP 3 * FROM odds.player_map WHERE sport_key='basketball_nba' AND player_id IS NOT NULL"))
    cols = [d[0] for d in r.cursor.description]
    print(f"Sample mapped row columns: {cols}")

print("Done.")
"""
with open(out, "w") as f:
    subprocess.run([sys.executable, "-c", script], stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
