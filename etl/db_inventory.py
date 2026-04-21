import os, pyodbc

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
cur = conn.cursor()

# The offending key
print("=== Offending key detail ===")
cur.execute("""
SELECT grade_date, event_id, player_id, market_key, bookmaker_key, line_value,
       outcome_name, over_price, grade_id
FROM common.daily_grades
WHERE grade_date='2026-04-02' AND event_id='289fb44a6171fe5e7365b05dca97889e'
  AND player_id=1628970 AND market_key='player_points_assists'
  AND bookmaker_key='fanduel' AND outcome_name='Over' AND over_price IS NOT NULL
ORDER BY line_value
""")
for r in cur.fetchall():
    print(r)

print("\n=== Full conflict-key scan: how many (grade_date, event, player, market, book, outcome_name) groups have >1 Over rows with over_price IS NOT NULL? ===")
cur.execute("""
WITH conflicts AS (
  SELECT grade_date, event_id, player_id, market_key, bookmaker_key, outcome_name, COUNT(*) AS n
  FROM common.daily_grades
  WHERE outcome_name='Over' AND over_price IS NOT NULL
    AND market_key IN ('player_points','player_rebounds','player_assists','player_threes',
      'player_blocks','player_steals','player_points_rebounds_assists','player_points_rebounds',
      'player_points_assists','player_rebounds_assists','player_double_double',
      'player_triple_double','player_first_basket')
  GROUP BY grade_date, event_id, player_id, market_key, bookmaker_key, outcome_name
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) AS conflict_groups, SUM(n) AS total_rows_in_conflicts FROM conflicts
""")
print(cur.fetchone())

print("\n=== Breakdown of those conflicts (top 20) ===")
cur.execute("""
SELECT TOP 20 grade_date, event_id, player_id, market_key, COUNT(*) AS n,
       MIN(line_value) AS min_lv, MAX(line_value) AS max_lv
FROM common.daily_grades
WHERE outcome_name='Over' AND over_price IS NOT NULL
  AND market_key IN ('player_points','player_rebounds','player_assists','player_threes',
    'player_blocks','player_steals','player_points_rebounds_assists','player_points_rebounds',
    'player_points_assists','player_rebounds_assists','player_double_double',
    'player_triple_double','player_first_basket')
GROUP BY grade_date, event_id, player_id, market_key
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
""")
cols = [c[0] for c in cur.description]
print(" | ".join(cols))
for r in cur.fetchall():
    print(" | ".join(str(x) for x in r))

conn.close()
