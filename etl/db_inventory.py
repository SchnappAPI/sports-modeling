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

# 1) Find Maxey
p("maxey lookup", """
SELECT player_id, player_name
FROM nba.players WHERE player_name LIKE '%Maxey%'
""")

# 2) How many daily_grades rows exist for Maxey, grouped by grade_date
p("maxey daily_grades by grade_date", """
SELECT dg.grade_date,
       COUNT(*) AS rows,
       COUNT(DISTINCT dg.event_id) AS events,
       COUNT(DISTINCT dg.market_key) AS markets
FROM common.daily_grades dg
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.market_key NOT LIKE '%_alternate'
GROUP BY dg.grade_date
ORDER BY dg.grade_date DESC
""")

# 3) Sample rows — what does a given grade_date look like for Maxey?
p("maxey sample rows (most recent date)", """
SELECT TOP 30 dg.grade_date, dg.event_id, dg.market_key, dg.line_value, dg.outcome_name
FROM common.daily_grades dg
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.market_key NOT LIKE '%_alternate'
  AND dg.grade_date = (
    SELECT MAX(grade_date) FROM common.daily_grades dg2
    JOIN nba.players p2 ON p2.player_id = dg2.player_id
    WHERE p2.player_name LIKE '%Tyrese Maxey%'
  )
ORDER BY dg.event_id, dg.market_key
""")

# 4) Distinct game_ids that the player-grades API would return for Maxey
p("player-grades API output simulation", """
SELECT COUNT(*) AS rows,
       COUNT(DISTINCT egm.game_id) AS distinct_games,
       COUNT(DISTINCT dg.market_key) AS distinct_markets
FROM common.daily_grades dg
JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.market_key NOT LIKE '%_alternate'
""")

# 5) Per-game line for Maxey's 3PM market across all games in API output
p("maxey 3pm lines per game (what gradeMap would see)", """
SELECT egm.game_id, dg.grade_date, dg.market_key, dg.line_value
FROM common.daily_grades dg
JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.market_key = 'player_threes'
ORDER BY egm.game_id DESC
""")

# 6) Per-game PTS line
p("maxey pts lines per game (what gradeMap would see)", """
SELECT egm.game_id, dg.grade_date, dg.market_key, dg.line_value
FROM common.daily_grades dg
JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
JOIN nba.players p ON p.player_id = dg.player_id
WHERE p.player_name LIKE '%Tyrese Maxey%'
  AND dg.bookmaker_key='fanduel'
  AND dg.market_key = 'player_points'
ORDER BY egm.game_id DESC
""")

conn.close()
print("\nDONE")
