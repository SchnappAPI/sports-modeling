from sqlalchemy import text
from db import get_engine

engine = get_engine()

with engine.connect() as conn:
    cols = conn.execute(text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'nba' AND TABLE_NAME = 'player_box_score_stats'
        ORDER BY ORDINAL_POSITION
    """)).fetchall()
    print("nba.player_box_score_stats columns:")
    for c in cols:
        print(dict(c._mapping))

    # Check grades coverage for recent dates
    grades = conn.execute(text("""
        SELECT TOP 5
            CONVERT(VARCHAR(10), grade_date, 120) AS grade_date,
            COUNT(*) AS rows
        FROM common.daily_grades
        GROUP BY grade_date
        ORDER BY grade_date DESC
    """)).fetchall()
    print("\nMost recent grade dates:")
    for r in grades:
        print(dict(r._mapping))

    # Check if event_game_map has entries for recent game dates
    egm = conn.execute(text("""
        SELECT TOP 5
            egm.game_id,
            egm.event_id,
            CONVERT(VARCHAR(10), egm.game_date, 120) AS game_date
        FROM odds.event_game_map egm
        ORDER BY egm.game_date DESC
    """)).fetchall()
    print("\nMost recent event_game_map entries:")
    for r in egm:
        print(dict(r._mapping))
