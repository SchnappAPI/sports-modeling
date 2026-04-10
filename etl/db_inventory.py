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

out = []
def p(line=""): print(line); out.append(line)

cur.execute("""
    SELECT player_id, player_name, team_id, team_tricode, roster_status
    FROM nba.players WHERE player_name LIKE '%McCollum%'
""")
p("=== players ===")
for r in cur.fetchall(): p(str(r))

cur.execute("""
    SELECT COUNT(DISTINCT game_id) AS games, MIN(game_date) AS first, MAX(game_date) AS last
    FROM nba.player_box_score_stats
    WHERE player_id IN (SELECT player_id FROM nba.players WHERE player_name LIKE '%McCollum%')
""")
p("=== pbs game count ===")
for r in cur.fetchall(): p(str(r))

cur.execute("""
    SELECT TOP 5 game_id, CONVERT(VARCHAR(10), game_date, 120) AS game_date, matchup
    FROM nba.player_box_score_stats
    WHERE player_id IN (SELECT player_id FROM nba.players WHERE player_name LIKE '%McCollum%')
    ORDER BY game_date DESC
""")
p("=== recent pbs rows ===")
for r in cur.fetchall(): p(str(r))

cur.execute("""
    SELECT TOP 5 game_id, CONVERT(VARCHAR(10), game_date, 120) AS game_date, matchup
    FROM nba.player_box_score_stats
    WHERE player_id IN (SELECT player_id FROM nba.players WHERE player_name LIKE '%McCollum%')
    ORDER BY game_date ASC
""")
p("=== oldest pbs rows ===")
for r in cur.fetchall(): p(str(r))

conn.close()

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
p("Done.")
