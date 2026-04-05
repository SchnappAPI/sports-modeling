"""
db_inventory.py — temporary: list all columns in mlb.batting_stats and mlb.pitching_stats
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

conn   = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

for table in ("batting_stats", "pitching_stats"):
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, table)
    rows = cursor.fetchall()
    print(f"\n=== mlb.{table} ({len(rows)} columns) ===")
    for r in rows:
        print(f"  {r[0]}  ({r[1]})")

conn.close()
