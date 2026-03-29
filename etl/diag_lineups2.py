from sqlalchemy import text
from db import get_engine

engine = get_engine()

with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM nba.daily_lineups")).scalar()
    print(f"Total rows: {count}")

    recent = conn.execute(text("""
        SELECT TOP 5 game_id, game_date, COUNT(*) AS players
        FROM nba.daily_lineups
        GROUP BY game_id, game_date
        ORDER BY game_date DESC
    """)).fetchall()
    print("\nMost recent games in daily_lineups:")
    for r in recent:
        print(dict(r._mapping))

    # Check if today's game IDs exist at all
    today_games = conn.execute(text("""
        SELECT game_id FROM nba.schedule
        WHERE CONVERT(VARCHAR(10), game_date, 120) = '2026-03-29'
    """)).fetchall()
    print("\nToday's game IDs from schedule:")
    for r in today_games:
        print(r[0])
