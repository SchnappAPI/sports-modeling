"""
db_inventory.py — verify link column populated in upcoming_player_props
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

out = []
def p(line=""): print(line); out.append(line)

conn   = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

p("=== link column presence ===")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'upcoming_player_props'
      AND COLUMN_NAME = 'link'
""")
row = cursor.fetchone()
if row:
    p(f"  link column exists: {row[0]} {row[1]}({row[2]})")
else:
    p("  link column NOT found")

p()
p("=== link population summary ===")
cursor.execute("""
    SELECT
        COUNT(*)                                          AS total_rows,
        SUM(CASE WHEN link IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_link,
        SUM(CASE WHEN link IS NULL     THEN 1 ELSE 0 END) AS rows_without_link
    FROM odds.upcoming_player_props
    WHERE sport_key = 'basketball_nba'
""")
row = cursor.fetchone()
cols = [d[0] for d in cursor.description]
for col, val in zip(cols, row):
    p(f"  {col}: {val}")

p()
p("=== sample links (5 rows) ===")
cursor.execute("""
    SELECT TOP 5
        player_name, market_key, outcome_name, outcome_point, outcome_price, link
    FROM odds.upcoming_player_props
    WHERE sport_key = 'basketball_nba'
      AND link IS NOT NULL
      AND outcome_name = 'Over'
    ORDER BY player_name, market_key, outcome_point
""")
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(cols))
for row in cursor.fetchall():
    p("  " + " | ".join(str(v) for v in row))

conn.close()
p()
p("Done.")

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
