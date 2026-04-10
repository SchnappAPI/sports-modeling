import os, subprocess, sys
out = "/tmp/mappings_verify.txt"
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
    r = conn.execute(text("SELECT COUNT(*) FROM odds.player_map WHERE sport_key='basketball_nba' AND player_id IS NULL"))
    print(f"Still unmapped: {r.scalar():,}")

with engine.connect() as conn:
    r = conn.execute(text("SELECT COUNT(*) FROM odds.player_map WHERE sport_key='basketball_nba' AND player_id IS NOT NULL"))
    print(f"Mapped:         {r.scalar():,}")

with engine.connect() as conn:
    r = conn.execute(text(\"\"\"
        SELECT TOP 10 pm.odds_player_name, pm.player_id
        FROM odds.player_map pm
        WHERE pm.sport_key='basketball_nba' AND pm.player_id IS NULL
        ORDER BY pm.odds_player_name
    \"\"\"))
    print("\\nRemaining unmapped (sample):")
    for row in r:
        print(f"  {row[0]}")

print("Done.")
"""
with open(out, "w") as f:
    subprocess.run([sys.executable, "-c", script], stdout=f, stderr=subprocess.STDOUT)
with open(out) as f:
    print(f.read())
