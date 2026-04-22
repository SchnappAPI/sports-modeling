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

p("discover_cursors", """
SELECT sport_key, season_year, oldest_snapshot_ts, snapshots_walked, events_found, last_walked_at
FROM odds.discover_cursors
ORDER BY sport_key, season_year
""")

p("discovered_events date range by sport/season", """
SELECT sport_key, season_year,
    MIN(CAST(commence_time AS DATE)) AS min_date,
    MAX(CAST(commence_time AS DATE)) AS max_date,
    COUNT(*) AS total_events
FROM odds.discovered_events
GROUP BY sport_key, season_year
ORDER BY sport_key, season_year
""")

p("events loaded vs discovered for NBA 2024", """
SELECT
    (SELECT COUNT(*) FROM odds.discovered_events WHERE sport_key='basketball_nba' AND season_year=2024) AS discovered,
    (SELECT COUNT(*) FROM odds.events WHERE sport_key='basketball_nba' AND season_year=2024) AS loaded,
    (SELECT MAX(CAST(commence_time AS DATE)) FROM odds.events WHERE sport_key='basketball_nba' AND season_year=2024) AS max_loaded_date
""")

p("discovered NBA 2024 events NOT yet loaded (oldest 20)", """
SELECT TOP 20 de.event_id, CAST(de.commence_time AS DATE) AS game_date, de.away_team, de.home_team
FROM odds.discovered_events de
WHERE de.sport_key = 'basketball_nba' AND de.season_year = 2024
  AND NOT EXISTS (SELECT 1 FROM odds.events e WHERE e.event_id = de.event_id)
ORDER BY de.commence_time ASC
""")

p("discovered NBA 2024 events NOT yet loaded (newest 20)", """
SELECT TOP 20 de.event_id, CAST(de.commence_time AS DATE) AS game_date, de.away_team, de.home_team
FROM odds.discovered_events de
WHERE de.sport_key = 'basketball_nba' AND de.season_year = 2024
  AND NOT EXISTS (SELECT 1 FROM odds.events e WHERE e.event_id = de.event_id)
ORDER BY de.commence_time DESC
""")

conn.close()
