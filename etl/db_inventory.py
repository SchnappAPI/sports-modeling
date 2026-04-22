import os, pyodbc
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
cur = conn.cursor()

# Delete the NBA 2025 discover cursor so the next discover run starts from
# today and walks backward through Apr 4-22 (the gap).
cur.execute("""
    DELETE FROM odds.discover_cursors
    WHERE sport_key = 'basketball_nba' AND season_year = 2025
""")
print(f"Deleted {cur.rowcount} cursor row(s).")

conn.commit()

# Confirm
cur.execute("SELECT sport_key, season_year, oldest_snapshot_ts FROM odds.discover_cursors WHERE sport_key='basketball_nba'")
for r in cur.fetchall():
    print(r)

conn.close()
print("Done.")
