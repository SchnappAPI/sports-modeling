# etl/nba_etl.py
from etl.db import get_engine
from etl.nba.ddl import create_all_tables
from etl.nba import (
    teams,
    players,
    games,
    rebound_chances,
    potential_ast,
    box_scores,
)

BATCH_SIZE = 10

def get_loaded_dates(engine):
    rows = engine.execute(
        "SELECT DISTINCT game_date FROM nba.rebound_chances"
    ).fetchall()
    return {str(r[0]) for r in rows}

def main():
    engine = get_engine()

    # DDL
    create_all_tables(engine)

    # Reference + schedule
    teams.load(engine)
    players.load(engine)
    games.load(engine)

    # Incremental facts
    all_dates = set(
        r[0].isoformat()
        for r in engine.execute(
            "SELECT DISTINCT game_date FROM nba.games"
        )
    )

    loaded_dates = get_loaded_dates(engine)
    work_dates = sorted(all_dates - loaded_dates)[:BATCH_SIZE]

    for d in work_dates:
        rebound_chances.load_date(engine, d)
        potential_ast.load_date(engine, d)
        box_scores.load_date(engine, d)

if __name__ == "__main__":
    main()
