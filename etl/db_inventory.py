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

p("upcoming_player_props cols", """
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props' ORDER BY ORDINAL_POSITION""")

p("upcoming_events cols", """
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_events' ORDER BY ORDINAL_POSITION""")

p("upcoming_player_props row count + distinct events", """
SELECT COUNT(*) AS rows, COUNT(DISTINCT event_id) AS events
FROM odds.upcoming_player_props""")

p("event_game_map cols", """
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='event_game_map' ORDER BY ORDINAL_POSITION""")

p("daily_grades is_standard state", """
SELECT
  SUM(CASE WHEN is_standard IS NOT NULL THEN 1 ELSE 0 END) AS has_col,
  SUM(is_standard) AS sum_flag,
  COUNT(*) AS total
FROM common.daily_grades""")

p("filtered index existence", """
SELECT name, is_unique, has_filter, filter_definition
FROM sys.indexes
WHERE object_id = OBJECT_ID('common.daily_grades')
  AND name='uq_daily_grades_standard'""")

conn.close()
