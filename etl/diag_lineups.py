import os
from sqlalchemy import text
from db import get_engine

engine = get_engine()

with engine.connect() as conn:
    cols = conn.execute(text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'nba' AND TABLE_NAME = 'daily_lineups'
        ORDER BY ORDINAL_POSITION
    """)).fetchall()
    print("nba.daily_lineups columns:")
    for c in cols:
        print(dict(c._mapping))

    sample = conn.execute(text("""
        SELECT TOP 3 * FROM nba.daily_lineups
    """)).fetchall()
    print("\nSample rows:")
    for r in sample:
        print(dict(r._mapping))
