"""
db_inventory.py — diagnostic script placeholder.
Replace this content with whatever query you need to run, then trigger
db_inventory.yml via workflow_dispatch.
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

cursor.execute("SELECT 'DB connection OK' AS status")
print(cursor.fetchone()[0])

conn.close()
