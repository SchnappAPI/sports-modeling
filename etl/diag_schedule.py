import os
from sqlalchemy import text
from db import get_engine

engine = get_engine()

with engine.connect() as conn:
    # Row count
    count = conn.execute(text("SELECT COUNT(*) FROM nba.schedule")).scalar()
    print(f"nba.schedule row count: {count}")

    # Sample of recent dates and their raw values
    rows = conn.execute(text("""
        SELECT TOP 10
            game_id,
            game_date,
            CONVERT(VARCHAR(30), game_date, 120) AS game_date_str,
            game_status,
            game_status_text
        FROM nba.schedule
        ORDER BY game_date DESC
    """)).fetchall()

    print("\nMost recent rows:")
    for r in rows:
        print(dict(r._mapping))

    # Check if anything exists near 2025-03-28
    nearby = conn.execute(text("""
        SELECT TOP 5
            game_id,
            CONVERT(VARCHAR(10), game_date, 120) AS game_date_str,
            game_status
        FROM nba.schedule
        WHERE game_date >= '2025-03-25'
        ORDER BY game_date
    """)).fetchall()

    print("\nGames on or after 2025-03-25:")
    for r in nearby:
        print(dict(r._mapping))
