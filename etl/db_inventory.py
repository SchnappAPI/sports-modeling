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

p("NBA 2025: discovered vs loaded", """
SELECT
    (SELECT COUNT(*) FROM odds.discovered_events WHERE sport_key='basketball_nba' AND season_year=2025) AS discovered,
    (SELECT COUNT(*) FROM odds.events WHERE sport_key='basketball_nba' AND season_year=2025) AS loaded,
    (SELECT MAX(CAST(commence_time AS DATE)) FROM odds.discovered_events WHERE sport_key='basketball_nba' AND season_year=2025) AS max_discovered,
    (SELECT MAX(CAST(commence_time AS DATE)) FROM odds.events WHERE sport_key='basketball_nba' AND season_year=2025) AS max_loaded
""")

p("NBA 2025: player_props coverage (standard only)", """
SELECT
    MIN(CAST(egm.game_date AS DATE)) AS min_date,
    MAX(CAST(egm.game_date AS DATE)) AS max_date,
    COUNT(DISTINCT egm.game_id) AS games_with_props
FROM odds.player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.sport_key = 'basketball_nba'
  AND pp.bookmaker_key = 'fanduel'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
  AND egm.game_date >= '2025-10-01'
""")

p("NBA 2025: games in player_props Apr 1+ (standard)", """
SELECT DISTINCT CAST(egm.game_date AS DATE) AS game_date, COUNT(DISTINCT pp.player_name) AS players
FROM odds.player_props pp
JOIN odds.event_game_map egm ON egm.event_id = pp.event_id
WHERE pp.sport_key = 'basketball_nba'
  AND pp.bookmaker_key = 'fanduel'
  AND pp.outcome_name = 'Over'
  AND pp.market_key NOT LIKE '%alternate%'
  AND egm.game_date >= '2026-04-01'
GROUP BY CAST(egm.game_date AS DATE)
ORDER BY game_date
""")

p("NBA 2025: loaded events Apr 3+", """
SELECT TOP 20 CAST(commence_time AS DATE) AS game_date, away_team, home_team
FROM odds.events
WHERE sport_key = 'basketball_nba' AND season_year = 2025
  AND commence_time >= '2026-04-03'
ORDER BY commence_time
""")

conn.close()
