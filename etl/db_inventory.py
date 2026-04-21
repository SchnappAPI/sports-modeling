import os, pyodbc

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};"
    f"DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};"
    f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
conn = pyodbc.connect(CONN_STR)
cur = conn.cursor()

def p(label, query, params=None):
    print(f"\n=== {label} ===")
    cur.execute(query, params or ())
    cols = [c[0] for c in cur.description] if cur.description else []
    if cols:
        print(" | ".join(cols))
    for r in cur.fetchall():
        print(" | ".join(str(x) for x in r))

# Look at a single game in detail with all columns for the standard market
p("maxey PTS full row for 04-19 game", """
SELECT dg.grade_date, dg.market_key, dg.line_value, dg.outcome_name,
       dg.over_price, dg.hit_rate_60, dg.hit_rate_20, dg.grade, dg.composite_grade
FROM common.daily_grades dg
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.grade_date = '2026-04-19'
  AND dg.market_key = 'player_points'
ORDER BY dg.line_value, dg.outcome_name
""")

# And 3PM
p("maxey 3PM full row for 04-19 game", """
SELECT dg.grade_date, dg.market_key, dg.line_value, dg.outcome_name,
       dg.over_price, dg.hit_rate_60, dg.grade, dg.composite_grade
FROM common.daily_grades dg
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.grade_date = '2026-04-19'
  AND dg.market_key = 'player_threes'
ORDER BY dg.line_value, dg.outcome_name
""")

# Contrast: what's in odds.upcoming_player_props for that event (the actual posted lines)
p("upcoming_player_props for Maxey event 04-19", """
SELECT TOP 30 upp.event_id, upp.market_key, upp.outcome_point, upp.outcome_name,
       upp.outcome_price, upp.bookmaker_key, upp.snap_ts
FROM odds.upcoming_player_props upp
JOIN nba.players p ON p.player_name = upp.player_name
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND upp.bookmaker_key='fanduel'
  AND upp.market_key IN ('player_points', 'player_threes', 'player_points_alternate', 'player_threes_alternate')
  AND CAST(upp.snap_ts AS DATE) = '2026-04-19'
ORDER BY upp.market_key, upp.outcome_point, upp.outcome_name, upp.snap_ts DESC
""")

# Show the grading script's relevant behavior: does it write one row per standard line or many?
p("count rows per (grade_date, market_key) for Maxey", """
SELECT TOP 20 dg.grade_date, dg.market_key, COUNT(*) AS rows, MIN(dg.line_value) AS min_line, MAX(dg.line_value) AS max_line
FROM common.daily_grades dg
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.market_key NOT LIKE '%_alternate'
GROUP BY dg.grade_date, dg.market_key
ORDER BY dg.grade_date DESC, dg.market_key
""")

conn.close()
print("\nDONE")
