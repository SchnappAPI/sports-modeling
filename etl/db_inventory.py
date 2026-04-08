"""
db_inventory.py — daily_grades audit
Writes output to /tmp/db_inventory_output.txt for retrieval via shell_exec.
"""
import os
import sys
import pyodbc

DRIVER   = "ODBC Driver 18 for SQL Server"
SERVER   = os.environ["AZURE_SQL_SERVER"]
DATABASE = os.environ["AZURE_SQL_DATABASE"]
USERNAME = os.environ["AZURE_SQL_USERNAME"]
PASSWORD = os.environ["AZURE_SQL_PASSWORD"]

CONN_STR = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)

out = []

def p(line=""):
    print(line)
    out.append(line)

conn   = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

p("=== SUMMARY ===")
cursor.execute("""
    SELECT
        COUNT(*)                                             AS total_rows,
        COUNT(DISTINCT grade_date)                          AS distinct_dates,
        CONVERT(VARCHAR(10), MIN(grade_date), 120)          AS earliest_date,
        CONVERT(VARCHAR(10), MAX(grade_date), 120)          AS latest_date,
        COUNT(DISTINCT player_id)                           AS distinct_players,
        COUNT(DISTINCT market_key)                          AS distinct_markets,
        SUM(CASE WHEN outcome = 'Won'  THEN 1 ELSE 0 END)  AS won,
        SUM(CASE WHEN outcome = 'Lost' THEN 1 ELSE 0 END)  AS lost,
        SUM(CASE WHEN outcome IS NULL  THEN 1 ELSE 0 END)  AS unresolved
    FROM common.daily_grades
""")
row = cursor.fetchone()
cols = [d[0] for d in cursor.description]
for col, val in zip(cols, row):
    p(f"  {col}: {val}")

p()
p("=== ROWS BY MARKET ===")
cursor.execute("""
    SELECT market_key,
           COUNT(*)                                            AS total,
           SUM(CASE WHEN outcome='Won'  THEN 1 ELSE 0 END)   AS won,
           SUM(CASE WHEN outcome='Lost' THEN 1 ELSE 0 END)   AS lost,
           SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END)  AS unresolved
    FROM common.daily_grades
    GROUP BY market_key
    ORDER BY total DESC
""")
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:<55}" if i == 0 else f"{c:>10}" for i, c in enumerate(cols)))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):<55}" if i == 0 else f"{str(v):>10}" for i, v in enumerate(row)))

p()
p("=== ROWS BY DATE (last 10 graded dates) ===")
cursor.execute("""
    SELECT TOP 10
        CONVERT(VARCHAR(10), grade_date, 120)               AS grade_date,
        COUNT(*)                                            AS total,
        SUM(CASE WHEN outcome='Won'  THEN 1 ELSE 0 END)    AS won,
        SUM(CASE WHEN outcome='Lost' THEN 1 ELSE 0 END)    AS lost,
        SUM(CASE WHEN outcome IS NULL THEN 1 ELSE 0 END)   AS unresolved
    FROM common.daily_grades
    GROUP BY grade_date
    ORDER BY grade_date DESC
""")
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:>12}" for c in cols))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):>12}" for v in row))

p()
p("=== TOP 15 PROPS (most recent date, Over, by composite grade) ===")
cursor.execute("""
    SELECT TOP 15
        CONVERT(VARCHAR(10), dg.grade_date, 120) AS grade_date,
        dg.player_name,
        dg.market_key,
        dg.line_value,
        dg.over_price,
        ROUND(dg.composite_grade, 1)             AS composite_grade,
        dg.outcome
    FROM common.daily_grades dg
    WHERE dg.grade_date = (SELECT MAX(grade_date) FROM common.daily_grades)
      AND dg.outcome_name = 'Over'
      AND dg.over_price IS NOT NULL
    ORDER BY dg.composite_grade DESC
""")
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(f"{c}" for c in cols))
for row in cursor.fetchall():
    p("  " + " | ".join(f"{str(v)}" for v in row))

conn.close()
p()
p("Done.")

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
