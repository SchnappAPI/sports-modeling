"""
db_inventory.py — understand alternate line structure in daily_grades
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

# Pick a recent date with good data
cursor.execute("""
    SELECT TOP 1 CONVERT(VARCHAR(10), grade_date, 120)
    FROM common.daily_grades
    WHERE grade_date < CAST(GETUTCDATE() AS DATE)
    GROUP BY grade_date
    HAVING COUNT(*) > 500
    ORDER BY grade_date DESC
""")
sample_date = cursor.fetchone()[0]
p(f"Sample date: {sample_date}")

p()
p("=== player_points line values for one player on sample date ===")
p("  (showing both standard and alternate to understand structure)")
cursor.execute("""
    SELECT TOP 1 player_name
    FROM common.daily_grades
    WHERE CONVERT(VARCHAR(10), grade_date, 120) = ?
      AND market_key IN ('player_points', 'player_points_alternate')
      AND outcome_name = 'Over'
      AND over_price IS NOT NULL
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
""", sample_date)
sample_player = cursor.fetchone()[0]
p(f"Sample player: {sample_player}")

cursor.execute("""
    SELECT market_key, outcome_name, line_value, over_price,
           CASE WHEN line_value = FLOOR(line_value) THEN 'whole' ELSE 'decimal' END AS num_type
    FROM common.daily_grades
    WHERE CONVERT(VARCHAR(10), grade_date, 120) = ?
      AND player_name = ?
      AND market_key IN ('player_points', 'player_points_alternate')
    ORDER BY market_key, outcome_name, line_value
""", sample_date, sample_player)
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(cols))
for row in cursor.fetchall():
    p("  " + " | ".join(str(v) for v in row))

p()
p("=== player_threes line values for one player ===")
p("  (threes is clearest — 0.5 increments vs whole number)")
cursor.execute("""
    SELECT TOP 1 player_name
    FROM common.daily_grades
    WHERE CONVERT(VARCHAR(10), grade_date, 120) = ?
      AND market_key IN ('player_threes', 'player_threes_alternate')
      AND outcome_name = 'Over'
      AND over_price IS NOT NULL
    GROUP BY player_name
    ORDER BY COUNT(*) DESC
""", sample_date)
row = cursor.fetchone()
if row:
    sample_player2 = row[0]
    p(f"Sample player: {sample_player2}")
    cursor.execute("""
        SELECT market_key, outcome_name, line_value, over_price,
               CASE WHEN line_value = FLOOR(line_value) THEN 'whole' ELSE 'decimal' END AS num_type
        FROM common.daily_grades
        WHERE CONVERT(VARCHAR(10), grade_date, 120) = ?
          AND player_name = ?
          AND market_key IN ('player_threes', 'player_threes_alternate')
        ORDER BY market_key, outcome_name, line_value
    """, sample_date, sample_player2)
    cols = [d[0] for d in cursor.description]
    p("  " + " | ".join(cols))
    for row in cursor.fetchall():
        p("  " + " | ".join(str(v) for v in row))

p()
p("=== breakdown: whole number vs decimal alternate lines across all markets ===")
cursor.execute("""
    SELECT
        market_key,
        SUM(CASE WHEN line_value = FLOOR(line_value) THEN 1 ELSE 0 END) AS whole_number,
        SUM(CASE WHEN line_value != FLOOR(line_value) THEN 1 ELSE 0 END) AS decimal_half,
        COUNT(*) AS total
    FROM common.daily_grades
    WHERE CONVERT(VARCHAR(10), grade_date, 120) = ?
      AND market_key LIKE '%_alternate'
      AND outcome_name = 'Over'
    GROUP BY market_key
    ORDER BY market_key
""", sample_date)
cols = [d[0] for d in cursor.description]
p("  " + "  ".join(f"{c:>45}" if i == 0 else f"{c:>15}" for i, c in enumerate(cols)))
for row in cursor.fetchall():
    p("  " + "  ".join(f"{str(v):>45}" if i == 0 else f"{str(v):>15}" for i, v in enumerate(row)))

p()
p("=== what line values exist for player_points_alternate (Over only, with price) ===")
cursor.execute("""
    SELECT DISTINCT line_value,
           CASE WHEN line_value = FLOOR(line_value) THEN 'whole' ELSE 'decimal' END AS num_type,
           over_price
    FROM common.daily_grades
    WHERE CONVERT(VARCHAR(10), grade_date, 120) = ?
      AND player_name = ?
      AND market_key = 'player_points_alternate'
      AND outcome_name = 'Over'
      AND over_price IS NOT NULL
    ORDER BY line_value
""", sample_date, sample_player)
cols = [d[0] for d in cursor.description]
p("  " + " | ".join(cols))
for row in cursor.fetchall():
    p("  " + " | ".join(str(v) for v in row))

conn.close()
p()
p("Done.")

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
