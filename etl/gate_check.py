"""
gate_check.py

Prints 'true' if there is at least one non-final NBA game starting within
the next 3 hours (UTC), 'false' otherwise.

Used by pregame-refresh.yml to skip odds ETL and grading on days with no
upcoming games, avoiding unnecessary Odds API quota burn.

Exit code is always 0. Output is a single line: 'true' or 'false'.
"""

import os
import sys
import time
from datetime import datetime, timezone, timedelta

import pyodbc

DRIVER   = "ODBC Driver 18 for SQL Server"
SERVER   = os.environ["AZURE_SQL_SERVER"]
DATABASE = os.environ["AZURE_SQL_DATABASE"]
USERNAME = os.environ["AZURE_SQL_USERNAME"]
PASSWORD = os.environ["AZURE_SQL_PASSWORD"]
TRUST    = os.environ.get("AZURE_SQL_TRUST_CERT", "no")

CONN_STR = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    f"TrustServerCertificate={TRUST};"
    "Connection Timeout=90;"
)

QUERY = """
SELECT COUNT(1)
FROM nba.schedule
WHERE game_status IN (1, 2)
  AND game_date = CAST(GETUTCDATE() AS DATE)
"""

def check_gate() -> bool:
    for attempt in range(1, 4):
        try:
            conn = pyodbc.connect(CONN_STR, timeout=90)
            cursor = conn.cursor()
            cursor.execute(QUERY)
            count = cursor.fetchone()[0]
            conn.close()
            return count > 0
        except Exception as exc:
            print(f"DB attempt {attempt}/3 failed: {exc}", file=sys.stderr)
            if attempt < 3:
                time.sleep(45)
    print("Gate check failed after 3 attempts, defaulting to false.", file=sys.stderr)
    return False

if __name__ == "__main__":
    result = check_gate()
    print("true" if result else "false")
