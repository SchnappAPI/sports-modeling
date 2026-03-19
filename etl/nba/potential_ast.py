# etl/nba/potential_ast.py
import pandas as pd
from etl.db import upsert
from etl.clean import clean_dataframe
from etl.nba.common import get_json
from etl.nba.game_dates import get_game_dates

def run(engine, batch_size=10):
    desired = set(get_game_dates())
    existing = set(
        pd.read_sql("SELECT DISTINCT game_date FROM nba.potential_ast", engine)["game_date"].astype(str)
    )

    for d in sorted(desired - existing)[:batch_size]:
        url = "https://stats.nba.com/stats/leaguedashptstats"
        params = {
            "Season": "2025-26",
            "SeasonType": "Regular Season",
            "PlayerOrTeam": "Player",
            "PtMeasureType": "Passing",
            "PerMode": "Totals",
            "DateFrom": d,
            "DateTo": d
        }
        rs = get_json(url, params)["resultSets"][0]
        df = pd.DataFrame(rs["rowSet"], columns=rs["headers"])
        if df.empty:
            continue
        df["game_date"] = d
        df = clean_dataframe(df)
        upsert(engine, df, "nba", "potential_ast", ["game_date", "player_id"])
