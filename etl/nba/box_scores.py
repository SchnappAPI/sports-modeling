# etl/nba/box_scores.py
import pandas as pd
from etl.db import upsert
from etl.clean import clean_dataframe
from etl.nba.common import get_json

PERIODS = [
    ("1", "1Q"),
    ("2", "2Q"),
    ("3", "3Q"),
    ("4", "4Q"),
    ("",  "OT"),
]

def fetch(period_value, period_label, game_date):
    url = "https://stats.nba.com/stats/playergamelogs"
    params = {
        "Season": "2025-26",
        "SeasonType": "Regular Season",
        "PlayerOrTeam": "P",
        "MeasureType": "Base",
        "Period": period_value,
        "DateFrom": game_date,
        "DateTo": ""
    }

    rs = get_json(url, params)["resultSets"][0]
    df = pd.DataFrame(rs["rowSet"], columns=rs["headers"])
    if df.empty:
        return df

    df["period"] = period_label
    return df

def load_date(engine, game_date):
    frames = [
        fetch(p, label, game_date)
        for p, label in PERIODS
    ]

    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        return

    df["game_date"] = game_date
    df = clean_dataframe(df)

    upsert(
        engine,
        df,
        schema="nba",
        table="box_scores",
        keys=["game_id", "player_id", "period"]
    )
