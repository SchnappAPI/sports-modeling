# etl/nba/players.py
import pandas as pd
from sqlalchemy import text
from etl.nba.common import get_json

URL = "https://stats.nba.com/stats/commonallplayers"

def load(engine):
    data = get_json(URL, params={
        "IsOnlyCurrentSeason": "1",
        "LeagueID": "00",
        "Season": "2025-26"
    })

    rs = data["resultSets"][0]
    df = pd.DataFrame(rs["rowSet"], columns=rs["headers"])
    df = df[df["ROSTERSTATUS"] == 1]

    df = df.rename(columns={
        "PERSON_ID": "player_id",
        "DISPLAY_FIRST_LAST": "player_name",
        "TEAM_ID": "team_id"
    })

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM nba.players"))

    df[["player_id", "player_name", "team_id"]].to_sql(
        "players",
        engine,
        schema="nba",
        if_exists="append",
        index=False
    )
