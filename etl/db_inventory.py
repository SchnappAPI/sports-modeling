import os, pyodbc
conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={os.environ['AZURE_SQL_SERVER']};DATABASE={os.environ['AZURE_SQL_DATABASE']};"
    f"UID={os.environ['AZURE_SQL_USERNAME']};PWD={os.environ['AZURE_SQL_PASSWORD']};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
cur = conn.cursor()

lines = []
def out(s=""):
    print(s)
    lines.append(s)

out("=== Query 1: per-date NULL counts (grade_date >= 2025-10-21) ===")
cur.execute("""
SELECT grade_date,
       COUNT(*) AS total_rows,
       SUM(CASE WHEN opportunity_short_grade IS NULL THEN 1 ELSE 0 END) AS null_opp_short,
       SUM(CASE WHEN opportunity_expected_grade IS NULL THEN 1 ELSE 0 END) AS null_opp_expected,
       SUM(CASE WHEN composite_grade IS NULL THEN 1 ELSE 0 END) AS null_composite
FROM common.daily_grades
WHERE grade_date >= '2025-10-21'
GROUP BY grade_date
ORDER BY grade_date;
""")
rows = cur.fetchall()
out(f"{'grade_date':<12} {'total':>8} {'null_opp_short':>16} {'null_opp_exp':>14} {'null_comp':>11}")
for r in rows:
    out(f"{str(r[0]):<12} {r[1]:>8} {r[2]:>16} {r[3]:>14} {r[4]:>11}")
out(f"TOTAL DATES: {len(rows)}")

out("")
out("=== Query 2: dates with null opp_expected but populated composite ===")
cur.execute("""
SELECT COUNT(DISTINCT grade_date) AS dates_with_null_opp_but_populated_composite
FROM common.daily_grades
WHERE opportunity_expected_grade IS NULL
  AND composite_grade IS NOT NULL
  AND grade_date >= '2025-10-21';
""")
out(f"dates_with_null_opp_but_populated_composite: {cur.fetchone()[0]}")

out("")
out("=== Query 3: overall row-level null_opp_exp vs populated composite counts ===")
cur.execute("""
SELECT
  SUM(CASE WHEN opportunity_expected_grade IS NULL AND composite_grade IS NOT NULL THEN 1 ELSE 0 END) AS rows_null_opp_but_populated_comp,
  SUM(CASE WHEN opportunity_expected_grade IS NOT NULL THEN 1 ELSE 0 END) AS rows_opp_populated,
  SUM(CASE WHEN opportunity_expected_grade IS NULL THEN 1 ELSE 0 END) AS rows_opp_null,
  COUNT(*) AS total_rows
FROM common.daily_grades
WHERE grade_date >= '2025-10-21';
""")
r = cur.fetchone()
out(f"rows_null_opp_but_populated_comp: {r[0]}")
out(f"rows_opp_populated:               {r[1]}")
out(f"rows_opp_null:                    {r[2]}")
out(f"total_rows:                       {r[3]}")

conn.close()

with open("/tmp/step1_audit_output.txt", "w") as f:
    f.write("\n".join(lines) + "\n")
