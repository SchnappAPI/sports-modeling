import os, pyodbc
pw = os.environ["AZURE_SQL_PASSWORD"]
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=sports-modeling-server.database.windows.net,1433;"
    f"DATABASE=sports-modeling;UID=sqladmin;PWD={pw};Encrypt=yes;Connection Timeout=90"
)
c = conn.cursor()
c.execute("""
SELECT MIN(grade_date), MAX(grade_date), COUNT(DISTINCT grade_date), COUNT(*)
FROM common.daily_grades
WHERE grade_date < CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time' AS DATE)
""")
r = c.fetchone()
print(f"Historical range: {r[0]} to {r[1]}")
print(f"Distinct dates: {r[2]}")
print(f"Total rows: {r[3]}")

c.execute("""
SELECT COUNT(*) 
FROM common.daily_grades
WHERE grade_date < CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time' AS DATE)
  AND outcome IS NOT NULL
""")
resolved = c.fetchone()[0]
print(f"Rows with resolved outcomes: {resolved}")

c.execute("""
SELECT COUNT(*) 
FROM common.daily_grades
WHERE grade_date < CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'Eastern Standard Time' AS DATE)
  AND opportunity_short_grade IS NOT NULL
""")
with_opp = c.fetchone()[0]
print(f"Historical rows already with opportunity grades: {with_opp}")
