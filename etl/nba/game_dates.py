# etl/nba/game_dates.py
from datetime import date
from etl.nba.common import get_json

def get_game_dates(season="2025-26"):
    url = "https://stats.nba.com/stats/scheduleleaguev2"
    data = get_json(url, params={"Season": season, "LeagueID": "00"})

    dates = [
        g["gameDate"][:10]
        for g in data["leagueSchedule"]["gameDates"]
    ]

    today = date.today().isoformat()
    return sorted(d for d in dates if d < today)
