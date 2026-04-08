"""
db_inventory.py — audit null over_price rows in common.daily_grades
"""
import os
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
def p(line=""): print(line); out.append(line)

conn   = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

p("=== NULL vs NON-NULL over_price breakdown ===")
cursor.execute("""
    SELECT
        SUM(CASE WHEN over_price IS NULL THEN 1 ELSE 0 END)     AS null_price,
        SUM(CASE WHEN over_price IS NOT NULL THEN 1 ELSE 0 END) AS has_price,
        COUNT(*)                                                 AS total
    FROM common.daily_grades
""")
row = cursor.fetchone()
for col, val in zip([d[0] for d in cursor.description], row):
    p(f"  {col}: {val}")

p()
p("=== NULL over_price breakdown by outcome_name ===")
cursor.execute("""
    SELECT outcome_name,
           COUNT(*)                                                 AS total,
           SUM(CASE WHEN over_price IS NULL THEN 1 ELSE 0 END)     AS null_price,
           SUM(CASE WHEN over_price IS NOT NULL THEN 1 ELSE 0 END) AS has_price
    FROM common.daily_grades
    GROUP BY outcome_name
    ORDER BY total DESC
""")
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:>15}" for c in cols))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):>15}" for v in row))

p()
p("=== NULL over_price breakdown by market_key (top 10) ===")
cursor.execute("""
    SELECT TOP 10
        market_key,
        SUM(CASE WHEN over_price IS NULL THEN 1 ELSE 0 END)     AS null_price,
        SUM(CASE WHEN over_price IS NOT NULL THEN 1 ELSE 0 END) AS has_price,
        COUNT(*)                                                 AS total
    FROM common.daily_grades
    GROUP BY market_key
    ORDER BY null_price DESC
""")
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:>45}" if i == 0 else f"{c:>12}" for i, c in enumerate(cols)))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):>45}" if i == 0 else f"{str(v):>12}" for i, v in enumerate(row)))

p()
p("=== ARE NULL-PRICE ROWS REFERENCED ANYWHERE? ===")
p("  Checking if any null-price rows have hit_rate or grade data (i.e. do they carry useful signal)...")
cursor.execute("""
    SELECT
        SUM(CASE WHEN composite_grade IS NOT NULL THEN 1 ELSE 0 END) AS has_composite,
        SUM(CASE WHEN grade IS NOT NULL           THEN 1 ELSE 0 END) AS has_grade,
        SUM(CASE WHEN hit_rate_60 IS NOT NULL     THEN 1 ELSE 0 END) AS has_hit_rate_60,
        SUM(CASE WHEN outcome IS NOT NULL         THEN 1 ELSE 0 END) AS has_outcome,
        COUNT(*)                                                      AS total_null_price
    FROM common.daily_grades
    WHERE over_price IS NULL
""")
row = cursor.fetchone()
for col, val in zip([d[0] for d in cursor.description], row):
    p(f"  {col}: {val}")

p()
p("=== WHERE DO NULL-PRICE ROWS COME FROM? ===")
p("  Sample of null-price rows with their market_key and outcome_name...")
cursor.execute("""
    SELECT TOP 20
        CONVERT(VARCHAR(10), grade_date, 120) AS grade_date,
        player_name,
        market_key,
        outcome_name,
        line_value,
        over_price,
        composite_grade,
        outcome
    FROM common.daily_grades
    WHERE over_price IS NULL
    ORDER BY grade_date DESC, player_name
""")
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(cols))
for row in cursor.fetchall():
    p("  " + " | ".join(str(v) for v in row))

p()
p("=== DO ANY NULL-PRICE ROWS HAVE outcome = Won OR Lost? ===")
cursor.execute("""
    SELECT outcome, COUNT(*) AS cnt
    FROM common.daily_grades
    WHERE over_price IS NULL
    GROUP BY outcome
""")
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:>10}" for c in cols))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):>10}" for v in row))

p()
p("=== UI IMPACT CHECK ===")
p("  GradesPageInner filters: rows = grades.filter(r => r.overPrice != null)")
p("  So null-price rows are already excluded from the At a Glance view.")
p()
p("=== WHAT ARE THE NON-STANDARD BRACKET LINES? ===")
p("  For standard markets, bracket grading generates 11 lines per player-market.")
p("  Only the center line (step=0) gets over_price stored. The other 10 do not.")
p("  Checking this assumption...")
cursor.execute("""
    SELECT TOP 5
        CONVERT(VARCHAR(10), grade_date, 120) AS grade_date,
        player_name,
        market_key,
        outcome_name,
        line_value,
        over_price
    FROM common.daily_grades
    WHERE over_price IS NULL
      AND market_key NOT LIKE '%_alternate'
      AND outcome_name = 'Over'
    ORDER BY grade_date DESC, player_name, market_key, line_value
""")
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(cols))
for row in cursor.fetchall():
    p("  " + " | ".join(str(v) for v in row))

conn.close()
p()
p("Done.")

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
