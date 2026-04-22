"""Temporary: verify opportunity grading columns populated."""
import os, pyodbc
pw = os.environ["AZURE_SQL_PASSWORD"]
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=sports-modeling-server.database.windows.net,1433;"
    f"DATABASE=sports-modeling;UID=sqladmin;PWD={pw};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=90"
)
c = conn.cursor()

print("=== Top 5 rows with opportunity grades (today) ===")
c.execute("""
SELECT TOP 5 player_name, market_key, line_value, outcome_name,
       composite_grade,
       opportunity_short_grade AS os,
       opportunity_long_grade  AS ol,
       opportunity_matchup_grade AS om,
       opportunity_streak_grade  AS ok,
       opportunity_volume_grade  AS ov,
       opportunity_expected_grade AS oe
FROM common.daily_grades
WHERE grade_date = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time' AS DATE)
  AND opportunity_short_grade IS NOT NULL
ORDER BY composite_grade DESC
""")
for r in c.fetchall():
    print(r)

print()
print("=== Coverage by market (today) ===")
c.execute("""
SELECT market_key,
       COUNT(*) AS total,
       SUM(CASE WHEN opportunity_short_grade    IS NOT NULL THEN 1 ELSE 0 END) AS short_n,
       SUM(CASE WHEN opportunity_long_grade     IS NOT NULL THEN 1 ELSE 0 END) AS long_n,
       SUM(CASE WHEN opportunity_matchup_grade  IS NOT NULL THEN 1 ELSE 0 END) AS match_n,
       SUM(CASE WHEN opportunity_streak_grade   IS NOT NULL THEN 1 ELSE 0 END) AS streak_n,
       SUM(CASE WHEN opportunity_volume_grade   IS NOT NULL THEN 1 ELSE 0 END) AS vol_n,
       SUM(CASE WHEN opportunity_expected_grade IS NOT NULL THEN 1 ELSE 0 END) AS exp_n
FROM common.daily_grades
WHERE grade_date = CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time' AS DATE)
GROUP BY market_key
ORDER BY total DESC
""")
print(f"{'market':45s} {'total':>6} {'short':>6} {'long':>6} {'match':>6} {'streak':>6} {'vol':>5} {'exp':>5}")
for r in c.fetchall():
    print(f"{r[0]:45s} {r[1]:>6} {r[2]:>6} {r[3]:>6} {r[4]:>6} {r[5]:>6} {r[6]:>5} {r[7]:>5}")

print()
print("=== Schema check ===")
c.execute("""
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades'
  AND COLUMN_NAME LIKE 'opportunity_%'
ORDER BY ORDINAL_POSITION
""")
for r in c.fetchall():
    print(f"  {r[0]:40s} {r[1]}")

c.execute("""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive'
  AND COLUMN_NAME LIKE 'opportunity_%'
ORDER BY ORDINAL_POSITION
""")
arch_cols = [r[0] for r in c.fetchall()]
print(f"Archive opportunity columns: {len(arch_cols)} -> {arch_cols}")
