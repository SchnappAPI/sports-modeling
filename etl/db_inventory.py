import os, pyodbc
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
cur = conn.cursor()
def p(label, q):
    print(f"\n=== {label} ===")
    cur.execute(q)
    for r in cur.fetchall(): print(r)

p("both archives: exist?", """
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE (TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive')
   OR (TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props_archive')
""")

p("row counts", """
SELECT 'upp' AS t, COUNT(*) FROM odds.upcoming_player_props
UNION ALL
SELECT 'player_props', COUNT(*) FROM odds.player_props
UNION ALL
SELECT 'dg', COUNT(*) FROM common.daily_grades
UNION ALL
SELECT 'dg_archive', COUNT(*) FROM common.daily_grades_archive
""")

p("player_props coverage by sport", """
SELECT sport_key,
    MIN(CAST(egm.game_date AS DATE)) AS min_date,
    MAX(CAST(egm.game_date AS DATE)) AS max_date,
    COUNT(DISTINCT egm.game_id) AS games
FROM odds.player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.bookmaker_key = 'fanduel'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
GROUP BY pp.sport_key
ORDER BY pp.sport_key
""")

conn.close()
