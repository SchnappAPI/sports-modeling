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

out("=== common.daily_grades columns ===")
cur.execute("""
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades'
ORDER BY ORDINAL_POSITION;
""")
for r in cur.fetchall():
    out(f"  {r[0]:<40} {r[1]:<20} {r[2]}")

out("")
out("=== candidate date columns: min / max ===")
cur.execute("""
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades'
  AND (DATA_TYPE IN ('date','datetime','datetime2','smalldatetime')
       OR COLUMN_NAME LIKE '%date%' OR COLUMN_NAME LIKE '%day%');
""")
for r in cur.fetchall():
    col = r[0]
    cur2 = conn.cursor()
    cur2.execute(f"SELECT MIN([{col}]), MAX([{col}]), COUNT(DISTINCT [{col}]) FROM common.daily_grades;")
    mn, mx, nd = cur2.fetchone()
    out(f"  {col}: min={mn} max={mx} distinct={nd}")

conn.close()

with open("/tmp/schema_probe_output.txt", "w") as f:
    f.write("\n".join(lines) + "\n")
