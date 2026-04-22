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

p("daily_grades_archive grade_id identity?", """
SELECT c.name, c.is_identity, c.is_nullable
FROM sys.columns c
WHERE c.object_id=OBJECT_ID('common.daily_grades_archive')
  AND c.name IN ('grade_id','archived_at')
""")

p("upcoming_player_props_archive col check", """
SELECT COUNT(*) AS col_count FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props_archive'
""")

p("row counts", """
SELECT 'upp_archive' AS t, COUNT(*) FROM odds.upcoming_player_props_archive
UNION ALL
SELECT 'dg_archive', COUNT(*) FROM common.daily_grades_archive
UNION ALL
SELECT 'upp', COUNT(*) FROM odds.upcoming_player_props
UNION ALL
SELECT 'dg', COUNT(*) FROM common.daily_grades
""")

conn.close()
