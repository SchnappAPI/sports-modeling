from sqlalchemy import text
from db import get_engine

engine = get_engine()

with engine.connect() as conn:
    # Check nba.games coverage vs player_box_score_stats
    games_count = conn.execute(text("SELECT COUNT(*) FROM nba.games")).scalar()
    pbs_games = conn.execute(text("SELECT COUNT(DISTINCT game_id) FROM nba.player_box_score_stats")).scalar()
    print(f"nba.games rows: {games_count}")
    print(f"nba.player_box_score_stats distinct game_ids: {pbs_games}")

    # Most recent games in nba.games
    recent_games = conn.execute(text("""
        SELECT TOP 3 game_id, CONVERT(VARCHAR(10), game_date, 120) AS game_date
        FROM nba.games ORDER BY game_date DESC
    """)).fetchall()
    print("\nMost recent nba.games:")
    for r in recent_games:
        print(dict(r._mapping))

    # Check if Garland's box score rows exist and what game_ids they use
    garland = conn.execute(text("""
        SELECT TOP 5
            game_id,
            CONVERT(VARCHAR(10), game_date, 120) AS game_date,
            period, pts, reb, ast
        FROM nba.player_box_score_stats
        WHERE player_id = 1629636 AND period = 'FullGame'
        ORDER BY game_date DESC
    """)).fetchall()
    print("\nGarland recent FullGame rows in player_box_score_stats:")
    for r in garland:
        print(dict(r._mapping))

    # Check if those game_ids are in nba.games
    if garland:
        gids = tuple(r[0] for r in garland)
        placeholders = ','.join(f"'{g}'" for g in gids)
        matched = conn.execute(text(f"""
            SELECT game_id FROM nba.games WHERE game_id IN ({placeholders})
        """)).fetchall()
        print("\nOf those game_ids, which exist in nba.games:")
        for r in matched:
            print(r[0])
