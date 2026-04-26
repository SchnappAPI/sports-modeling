"""Pilot script for mac-runner: prove the Mac can query local SQL Server.

Read-only. Mirrors the shape of etl/db_inventory.py but uses generic
SQL_* env vars so it can target localhost on the Mac runner without
disturbing the Azure SQL credentials used by production workflows.
"""
import getpass
import os
import socket

import pyodbc

print(f"Host: {socket.gethostname()}")
print(f"User: {getpass.getuser()}")

server = os.environ.get("SQL_SERVER", "localhost,1433")
database = os.environ.get("SQL_DATABASE", "sports-modeling")
username = os.environ.get("SQL_USERNAME", "sa")
password = os.environ["SQL_PASSWORD"]
trust = os.environ.get("SQL_TRUST_CERT", "yes")

conn = pyodbc.connect(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};DATABASE={database};"
    f"UID={username};PWD={password};"
    f"Encrypt=yes;TrustServerCertificate={trust};Connection Timeout=15;"
)
cur = conn.cursor()

cur.execute("SELECT @@VERSION")
print(f"\nServer: {cur.fetchone()[0].splitlines()[0]}")

cur.execute(
    """
    SELECT s.name AS schema_name, COUNT(*) AS table_count
    FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name IN ('common','nba','mlb','nfl','odds')
    GROUP BY s.name ORDER BY s.name
    """
)
print("\nTables by schema:")
for schema_name, table_count in cur.fetchall():
    print(f"  {schema_name:8} {table_count:>3} tables")

cur.execute(
    """
    SELECT 'common.daily_grades', COUNT_BIG(*) FROM common.daily_grades
    UNION ALL
    SELECT 'odds.player_props', COUNT_BIG(*) FROM odds.player_props
    UNION ALL
    SELECT 'mlb.play_by_play', COUNT_BIG(*) FROM mlb.play_by_play
    """
)
print("\nRow counts on key tables:")
for label, count in cur.fetchall():
    print(f"  {label:30} {count:>14,}")

conn.close()
print("\nOK")
