# etl/nba/rebound_chances.py
import pandas as pd
from etl.db import upsert
from etl.clean import clean_dataframe
from etl.nba.common import get_json
from etl.nba.game_dates import get_game_dates

TABLE = "rebound_chances"
SCHEMA = "nba"

def get_existing(engine):
    df = pd.read_sql(f"SELECT DISTINCT game_date FROM nba.{TABLE}", engine)
    return set(df["game_date"].astype(str))

def fetch_date(d):
    url = "https://stats.nba.com/stats/leaguedashptstats"
    params = {
        "Season": "2025-26",
        "SeasonType": "Regular Season",
        "PlayerOrTeam": "Player",
        "PtMeasureType": "Rebounding",
        "PerMode": "Totals",
        "DateFrom": d,
        "DateTo": d
    }
    rs = get_json(url, params)["resultSets"][0]
    df = pd.DataFrame(rs["rowSet"], columns=rs["headers"])
    if df.empty:
        return df
    df["game_date"] = d
    return df

def run(engine, batch_size=10):
    desired = set(get_game_dates())
    existing = get_existing(engine)
    work = sorted(desired - existing)[:batch_size]

    for d in work:
        df = fetch_date(d)
        if df.empty:
            continue
        df = clean_dataframe(df)
        upsert(engine, df, SCHEMA, TABLE, ["game_date", "player_id"])
