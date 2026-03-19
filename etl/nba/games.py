# etl/nba/games.py
import pandas as pd
from etl.db import upsert
from etl.clean import clean_dataframe
from etl.nba.common import get_json

SCHEDULE_URL = "https://stats.nba.com/stats/scheduleleaguev2"

def fetch_schedule(season="2025-26"):
    data = get_json(
        SCHEDULE_URL,
        params={"Season": season, "LeagueID": "00"}
    )

    rows = []

    for d in data["leagueSchedule"]["gameDates"]:
        game_date = d["gameDate"][:10]

        for g in d["games"]:
            rows.append({
                "game_id": g["gameId"],
                "game_code": g["gameCode"],
                "game_date": game_date,
                "game_status": g["gameStatus"],
                "game_status_text": g["gameStatusText"],
                "game_datetime_est": g["gameDateTimeEst"],
                "game_datetime_utc": g["gameDateTimeUTC"],
                "home_team_id": g["homeTeam"]["teamId"],
                "home_team_tricode": g["homeTeam"]["teamTricode"],
                "away_team_id": g["awayTeam"]["teamId"],
                "away_team_tricode": g["awayTeam"]["teamTricode"],
                "arena_name": g["arenaName"],
                "arena_city": g["arenaCity"],
                "arena_state": g["arenaState"],
            })

    return pd.DataFrame(rows)

def load(engine):
    df = fetch_schedule()

    if df.empty:
        print("nba.games: no schedule data returned")
        return

    df = clean_dataframe(df)

    upsert(
        engine,
        df,
        schema="nba",
        table="games",
        keys=["game_id"]
    )

    print(f"nba.games: loaded {len(df)} rows")
