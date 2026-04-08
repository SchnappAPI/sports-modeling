"""
db_inventory.py — over_price distribution check
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

p("=== OVER_PRICE DISTRIBUTION (Over rows with a price) ===")
cursor.execute("""
    SELECT
        SUM(CASE WHEN over_price < -1000               THEN 1 ELSE 0 END) AS below_neg1000,
        SUM(CASE WHEN over_price BETWEEN -1000 AND -500 THEN 1 ELSE 0 END) AS neg1000_to_neg500,
        SUM(CASE WHEN over_price BETWEEN -499  AND -200 THEN 1 ELSE 0 END) AS neg499_to_neg200,
        SUM(CASE WHEN over_price BETWEEN -199  AND -101 THEN 1 ELSE 0 END) AS neg199_to_neg101,
        SUM(CASE WHEN over_price BETWEEN -100  AND  100 THEN 1 ELSE 0 END) AS neg100_to_pos100,
        SUM(CASE WHEN over_price BETWEEN  101  AND  200 THEN 1 ELSE 0 END) AS pos101_to_pos200,
        SUM(CASE WHEN over_price BETWEEN  201  AND  400 THEN 1 ELSE 0 END) AS pos201_to_pos400,
        SUM(CASE WHEN over_price >  400                 THEN 1 ELSE 0 END) AS above_pos400,
        MAX(over_price)                                                     AS max_price,
        COUNT(*)                                                            AS total_with_price
    FROM common.daily_grades
    WHERE outcome_name = 'Over'
      AND over_price IS NOT NULL
""")
row = cursor.fetchone()
cols = [d[0] for d in cursor.description]
for col, val in zip(cols, row):
    p(f"  {col}: {val}")

p()
p("=== SAMPLE OF ROWS WITH PRICE > +200 (today or most recent date) ===")
cursor.execute("""
    SELECT TOP 20
        CONVERT(VARCHAR(10), grade_date, 120) AS grade_date,
        player_name, market_key, line_value, over_price,
        ROUND(composite_grade, 1) AS composite_grade
    FROM common.daily_grades
    WHERE outcome_name = 'Over'
      AND over_price > 200
      AND grade_date = (SELECT MAX(grade_date) FROM common.daily_grades WHERE over_price > 200)
    ORDER BY over_price DESC
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
