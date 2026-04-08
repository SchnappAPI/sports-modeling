"""
db_inventory.py — last 7 days of raw player props from odds.player_props
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

p("=== odds.player_props — TABLE SUMMARY ===")
cursor.execute("""
    SELECT
        COUNT(*)                                                    AS total_rows,
        COUNT(DISTINCT CAST(egm.game_date AS DATE))                 AS distinct_dates,
        CONVERT(VARCHAR(10), MIN(CAST(egm.game_date AS DATE)), 120) AS earliest_date,
        CONVERT(VARCHAR(10), MAX(CAST(egm.game_date AS DATE)), 120) AS latest_date,
        COUNT(DISTINCT pp.market_key)                               AS distinct_markets,
        COUNT(DISTINCT pp.bookmaker_key)                            AS distinct_bookmakers
    FROM odds.player_props pp
    JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
    WHERE pp.sport_key = 'basketball_nba'
""")
row = cursor.fetchone()
cols = [d[0] for d in cursor.description]
for col, val in zip(cols, row):
    p(f"  {col}: {val}")

p()
p("=== LAST 7 DAYS — ROW COUNT BY DATE AND BOOKMAKER ===")
cursor.execute("""
    SELECT
        CONVERT(VARCHAR(10), CAST(egm.game_date AS DATE), 120) AS game_date,
        pp.bookmaker_key,
        COUNT(*)                                               AS rows,
        COUNT(DISTINCT pp.player_name)                         AS players,
        COUNT(DISTINCT pp.market_key)                          AS markets
    FROM odds.player_props pp
    JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
    WHERE pp.sport_key = 'basketball_nba'
      AND CAST(egm.game_date AS DATE) >= CAST(DATEADD(day, -7, GETUTCDATE()) AS DATE)
    GROUP BY CAST(egm.game_date AS DATE), pp.bookmaker_key
    ORDER BY game_date DESC, rows DESC
""")
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:>15}" for c in cols))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):>15}" for v in row))

p()
p("=== LAST 7 DAYS — FANDUEL SAMPLE (20 rows, Over lines only) ===")
cursor.execute("""
    SELECT TOP 20
        CONVERT(VARCHAR(10), CAST(egm.game_date AS DATE), 120) AS game_date,
        pp.player_name,
        pp.market_key,
        pp.outcome_name,
        pp.outcome_point  AS line_value,
        pp.outcome_price  AS price
    FROM odds.player_props pp
    JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
    WHERE pp.sport_key = 'basketball_nba'
      AND pp.bookmaker_key = 'fanduel'
      AND pp.outcome_name = 'Over'
      AND CAST(egm.game_date AS DATE) >= CAST(DATEADD(day, -7, GETUTCDATE()) AS DATE)
    ORDER BY egm.game_date DESC, pp.player_name, pp.market_key
""")
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(cols))
for row in cursor.fetchall():
    p("  " + " | ".join(str(v) for v in row))

p()
p("=== odds.upcoming_player_props — CURRENT LINES SUMMARY ===")
cursor.execute("""
    SELECT
        COUNT(*)                         AS total_rows,
        COUNT(DISTINCT pp.player_name)   AS players,
        COUNT(DISTINCT pp.market_key)    AS markets,
        COUNT(DISTINCT pp.bookmaker_key) AS bookmakers
    FROM odds.upcoming_player_props pp
    WHERE pp.sport_key = 'basketball_nba'
""")
row = cursor.fetchone()
cols = [d[0] for d in cursor.description]
for col, val in zip(cols, row):
    p(f"  {col}: {val}")

conn.close()
p()
p("Done.")

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
