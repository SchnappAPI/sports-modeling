from sqlalchemy import text
from db import get_engine

engine = get_engine()

with engine.connect() as conn:
    # What periods exist for Garland at all
    periods = conn.execute(text("""
        SELECT DISTINCT period, COUNT(*) AS cnt
        FROM nba.player_box_score_stats
        WHERE player_id = 1629636
        GROUP BY period
        ORDER BY period
    """)).fetchall()
    print("Periods for Garland (1629636):")
    for r in periods:
        print(dict(r._mapping))

    # What distinct periods exist across the whole table
    all_periods = conn.execute(text("""
        SELECT DISTINCT period, COUNT(*) AS cnt
        FROM nba.player_box_score_stats
        GROUP BY period
        ORDER BY period
    """)).fetchall()
    print("\nAll distinct period values in table:")
    for r in all_periods:
        print(dict(r._mapping))
