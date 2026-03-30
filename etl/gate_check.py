"""Game window gate check for the pre-game refresh workflow.

Prints 'true' if any NBA game today is non-final and starts within 3 hours
of now (UTC). Prints 'false' otherwise. Exit code is always 0 so the calling
workflow can capture stdout cleanly.

Usage:
    python etl/gate_check.py
"""
import os
import re
import sys
import time
from datetime import date, datetime, timezone, timedelta

from sqlalchemy import create_engine, text

server   = os.environ["AZURE_SQL_SERVER"]
database = os.environ["AZURE_SQL_DATABASE"]
username = os.environ["AZURE_SQL_USERNAME"]
password = os.environ["AZURE_SQL_PASSWORD"]
conn_str = (
    f"mssql+pyodbc://{username}:{password}"
    f"@{server}/{database}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes&TrustServerCertificate=no"
    "&Connection+Timeout=90"
)
engine = create_engine(conn_str, fast_executemany=True)

for attempt in range(1, 4):
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        break
    except Exception:
        if attempt < 3:
            time.sleep(60)
        else:
            print("false")
            sys.exit(0)

today   = date.today()
now_utc = datetime.now(timezone.utc)
cutoff  = now_utc + timedelta(hours=3)

time_re = re.compile(r"(\d{1,2}):(\d{2})\s*(am|pm)", re.IGNORECASE)

with engine.connect() as conn:
    rows = [
        dict(r._mapping)
        for r in conn.execute(
            text(
                "SELECT game_status, game_status_text FROM nba.schedule "
                "WHERE game_date = :today AND (game_status IS NULL OR game_status != 3)"
            ),
            {"today": today},
        )
    ]

for row in rows:
    gst = row.get("game_status_text") or ""
    m   = time_re.search(gst)
    if not m:
        print("true")
        sys.exit(0)
    hour   = int(m.group(1))
    minute = int(m.group(2))
    ampm   = m.group(3).lower()
    if ampm == "pm" and hour != 12:
        hour += 12
    elif ampm == "am" and hour == 12:
        hour = 0
    et_tz     = timezone(timedelta(hours=-4))
    start_et  = datetime(today.year, today.month, today.day, hour, minute, tzinfo=et_tz)
    start_utc = start_et.astimezone(timezone.utc)
    if start_utc <= cutoff:
        print("true")
        sys.exit(0)

print("false")
