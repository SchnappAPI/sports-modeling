# etl/nba/teams.py
import pandas as pd
from sqlalchemy import text

DATA = [
    (1610612737, "Atlanta", "Hawks", "ATL", "East", "Southeast"),
    (1610612738, "Boston", "Celtics", "BOS", "East", "Atlantic"),
    (1610612751, "Brooklyn", "Nets", "BKN", "East", "Atlantic"),
    (1610612766, "Charlotte", "Hornets", "CHA", "East", "Southeast"),
    (1610612741, "Chicago", "Bulls", "CHI", "East", "Central"),
    (1610612739, "Cleveland", "Cavaliers", "CLE", "East", "Central"),
    (1610612742, "Dallas", "Mavericks", "DAL", "West", "Southwest"),
    (1610612743, "Denver", "Nuggets", "DEN", "West", "Northwest"),
    (1610612765, "Detroit", "Pistons", "DET", "East", "Central"),
    (1610612744, "Golden State", "Warriors", "GSW", "West", "Pacific"),
    (1610612745, "Houston", "Rockets", "HOU", "West", "Southwest"),
    (1610612754, "Indiana", "Pacers", "IND", "East", "Central"),
    (1610612746, "LA", "Clippers", "LAC", "West", "Pacific"),
    (1610612747, "Los Angeles", "Lakers", "LAL", "West", "Pacific"),
    (1610612763, "Memphis", "Grizzlies", "MEM", "West", "Southwest"),
    (1610612748, "Miami", "Heat", "MIA", "East", "Southeast"),
    (1610612749, "Milwaukee", "Bucks", "MIL", "East", "Central"),
    (1610612750, "Minnesota", "Timberwolves", "MIN", "West", "Northwest"),
    (1610612740, "New Orleans", "Pelicans", "NOP", "West", "Southwest"),
    (1610612752, "New York", "Knicks", "NYK", "East", "Atlantic"),
    (1610612760, "Oklahoma City", "Thunder", "OKC", "West", "Northwest"),
    (1610612753, "Orlando", "Magic", "ORL", "East", "Southeast"),
    (1610612755, "Philadelphia", "76ers", "PHI", "East", "Atlantic"),
    (1610612756, "Phoenix", "Suns", "PHX", "West", "Pacific"),
    (1610612757, "Portland", "Trail Blazers", "POR", "West", "Northwest"),
    (1610612758, "Sacramento", "Kings", "SAC", "West", "Pacific"),
    (1610612759, "San Antonio", "Spurs", "SAS", "West", "Southwest"),
    (1610612761, "Toronto", "Raptors", "TOR", "East", "Atlantic"),
    (1610612762, "Utah", "Jazz", "UTA", "West", "Northwest"),
    (1610612764, "Washington", "Wizards", "WAS", "East", "Southeast"),
]

def load(engine):
    df = pd.DataFrame(DATA, columns=[
        "team_id", "team_city", "team_name",
        "team_abbreviation", "conference", "division"
    ])

    with engine.begin() as conn:
        conn.execute(text("DELETE FROM nba.teams"))

    df.to_sql("teams", engine, schema="nba", if_exists="append", index=False)
