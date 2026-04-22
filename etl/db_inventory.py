import os, pyodbc
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
cur = conn.cursor()
def p(label, q, params=None):
    print(f"\n=== {label} ===")
    cur.execute(q) if not params else cur.execute(q, params)
    for r in cur.fetchall(): print(r)

p("player_props standard coverage", """
SELECT
    MIN(CAST(egm.game_date AS DATE)) AS min_date,
    MAX(CAST(egm.game_date AS DATE)) AS max_date,
    COUNT(DISTINCT egm.game_id) AS games
FROM odds.player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.bookmaker_key = 'fanduel'
  AND pp.sport_key = 'basketball_nba'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
""")

p("player_props alternate coverage", """
SELECT
    MIN(CAST(egm.game_date AS DATE)) AS min_date,
    MAX(CAST(egm.game_date AS DATE)) AS max_date,
    COUNT(DISTINCT egm.game_id) AS games
FROM odds.player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.bookmaker_key = 'fanduel'
  AND pp.sport_key = 'basketball_nba'
  AND pp.outcome_name = 'Over'
  AND pp.market_key LIKE '%alternate%'
""")

p("upcoming_player_props standard coverage", """
SELECT
    MIN(CAST(egm.game_date AS DATE)) AS min_date,
    MAX(CAST(egm.game_date AS DATE)) AS max_date,
    COUNT(DISTINCT egm.game_id) AS games
FROM odds.upcoming_player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.bookmaker_key = 'fanduel'
  AND pp.sport_key = 'basketball_nba'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
""")

p("upcoming_player_props alternate coverage", """
SELECT
    MIN(CAST(egm.game_date AS DATE)) AS min_date,
    MAX(CAST(egm.game_date AS DATE)) AS max_date,
    COUNT(DISTINCT egm.game_id) AS games
FROM odds.upcoming_player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.bookmaker_key = 'fanduel'
  AND pp.sport_key = 'basketball_nba'
  AND pp.outcome_name = 'Over'
  AND pp.market_key LIKE '%alternate%'
""")

p("Banchero standard in player_props", """
SELECT TOP 10
    CAST(egm.game_date AS DATE) AS game_date,
    pp.market_key,
    pp.outcome_point,
    pp.outcome_price
FROM odds.player_props pp
JOIN odds.player_map pm ON pm.odds_player_name = pp.player_name AND pm.sport_key = 'basketball_nba'
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pm.player_id = 1629029
  AND pp.bookmaker_key = 'fanduel'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
ORDER BY egm.game_date DESC, pp.market_key
""")

p("Banchero alternates in player_props", """
SELECT TOP 10
    CAST(egm.game_date AS DATE) AS game_date,
    pp.market_key,
    pp.outcome_point,
    pp.outcome_price
FROM odds.player_props pp
JOIN odds.player_map pm ON pm.odds_player_name = pp.player_name AND pm.sport_key = 'basketball_nba'
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pm.player_id = 1629029
  AND pp.bookmaker_key = 'fanduel'
  AND pp.outcome_name = 'Over'
  AND pp.market_key LIKE '%alternate%'
ORDER BY egm.game_date DESC, pp.market_key, pp.outcome_point
""")

p("Banchero standard in upcoming_player_props", """
SELECT TOP 10
    CAST(egm.game_date AS DATE) AS game_date,
    pp.market_key,
    pp.outcome_point,
    pp.outcome_price
FROM odds.upcoming_player_props pp
JOIN odds.player_map pm ON pm.odds_player_name = pp.player_name AND pm.sport_key = 'basketball_nba'
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pm.player_id = 1629029
  AND pp.bookmaker_key = 'fanduel'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
ORDER BY egm.game_date DESC, pp.market_key
""")

conn.close()
