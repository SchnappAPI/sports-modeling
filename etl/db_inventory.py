import os, pyodbc
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
cur = conn.cursor()

print("=== upcoming_player_props FULL col list ===")
cur.execute("""SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props'
ORDER BY ORDINAL_POSITION""")
for r in cur.fetchall(): print(r)

print("\n=== sample row ===")
cur.execute("SELECT TOP 1 * FROM odds.upcoming_player_props")
cols = [c[0] for c in cur.description]
for name, val in zip(cols, cur.fetchone()):
    print(f"  {name}: {val!r}")

print("\n=== upcoming_player_props distinct snap_ts per event (sample) ===")
cur.execute("""
SELECT TOP 10 event_id, COUNT(DISTINCT snap_ts) AS distinct_snaps,
       MIN(snap_ts) AS first_snap, MAX(snap_ts) AS last_snap, COUNT(*) AS rows
FROM odds.upcoming_player_props
GROUP BY event_id ORDER BY event_id""")
for r in cur.fetchall(): print(r)

print("\n=== commence_time per event (sample) ===")
cur.execute("""
SELECT TOP 10 event_id, commence_time FROM odds.upcoming_events ORDER BY commence_time DESC""")
for r in cur.fetchall(): print(r)

print("\n=== is_standard column state (fixed CAST) ===")
cur.execute("""
SELECT
  COUNT(*) AS total_rows,
  SUM(CAST(is_standard AS INT)) AS sum_flag
FROM common.daily_grades""")
print(cur.fetchone())

print("\n=== daily_grades_archive exists? ===")
cur.execute("""
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive'""")
print(cur.fetchone())

print("\n=== upcoming_player_props_archive exists? ===")
cur.execute("""
SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props_archive'""")
print(cur.fetchone())

conn.close()
