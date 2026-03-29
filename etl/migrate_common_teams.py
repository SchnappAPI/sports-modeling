"""
migrate_common_teams.py

Migrates common.teams from its current shape to the full target schema.

What this script does (in order):
  1. Backs up common.teams -> common.teams_backup (drops backup if it exists)
  2. Drops common.teams
  3. Creates common.teams with the new schema
  4. Re-inserts all 92 rows (MLB, NBA, NFL) with correct values for every column
  5. Fixes nfl.games where home_team or away_team = 'LA' -> 'LAR' (Rams)
  6. Prints a verification count per sport

Design decisions:
  - team_id is INT IDENTITY -- new surrogate PK, no dependencies exist yet
  - source_team_id VARCHAR(20) -- original source ID (NBA/MLB int as string, NFL tricode)
  - conference and division are stored as separate columns
  - NFL bridge columns (pff_team_id, pff_team_abbr, nflreadpy_abbr, alt_abbr) are
    populated from the known static bridge table data; NULL for NBA/MLB
  - Color columns are populated for NFL from PFF schedule API data captured
    in the VM script; NULL for NBA/MLB (can be added later)
  - participant_id (Odds API) is preserved from the existing rows
  - team_index is dropped -- it was an arbitrary sort order with no downstream use
  - sport_key keeps the Odds API convention: basketball_nba, baseball_mlb,
    americanfootball_nfl

Safe to re-run: backs up and rebuilds from scratch each time.

Run via GitHub Actions (migrate-common-teams.yml), manual dispatch only.
"""

import os
import time
import logging
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def get_engine():
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Connected to Azure SQL.")
            return engine
        except Exception as exc:
            log.warning("Connection attempt %d/3 failed: %s", attempt, exc)
            if attempt < 3:
                time.sleep(45)
    raise RuntimeError("Could not connect after 3 attempts.")


# ---------------------------------------------------------------------------
# Static team data
# ---------------------------------------------------------------------------

# Each tuple:
# (source_team_id, team_name, city, nickname, tricode, conference, division,
#  sport_key, league, participant_id,
#  pff_team_id, pff_team_abbr, nflreadpy_abbr, alt_abbr,
#  primary_color, secondary_color, tertiary_color,
#  dark_color_ref, light_color_ref, background_color_ref, foreground_color)
#
# NBA/MLB rows have None for all NFL-specific and color columns.
# Participant IDs copied exactly from existing common.teams.

NBA_TEAMS = [
    # source_team_id, team_name, city, nickname, tricode, conference, division, participant_id
    ("1610612737", "Atlanta Hawks",             "Atlanta",       "Hawks",           "ATL", "Eastern", "Southeast", "par_01hqmkq6fceknv7cwebesgrx03"),
    ("1610612738", "Boston Celtics",            "Boston",        "Celtics",         "BOS", "Eastern", "Atlantic",  "par_01hqmkq6fdf1pvq7jgdd7hdmpf"),
    ("1610612739", "Cleveland Cavaliers",       "Cleveland",     "Cavaliers",       "CLE", "Eastern", "Central",   "par_01hqmkq6fhec0t7bezwtzqv0fq"),
    ("1610612740", "New Orleans Pelicans",      "New Orleans",   "Pelicans",        "NOP", "Western", "Southwest", "par_01hqmkq6fyed09scyb5yv7xt2s"),
    ("1610612741", "Chicago Bulls",             "Chicago",       "Bulls",           "CHI", "Eastern", "Central",   "par_01hqmkq6fgf7krk5evvjfy9mr1"),
    ("1610612742", "Dallas Mavericks",          "Dallas",        "Mavericks",       "DAL", "Western", "Southwest", "par_01hqmkq6fje5rrsbnbx97seg30"),
    ("1610612743", "Denver Nuggets",            "Denver",        "Nuggets",         "DEN", "Western", "Northwest", "par_01hqmkq6fkf9r8wh7303b8hy40"),
    ("1610612744", "Golden State Warriors",     "Golden State",  "Warriors",        "GSW", "Western", "Pacific",   "par_01hqmkq6fne7nsfvf365y98r0h"),
    ("1610612745", "Houston Rockets",           "Houston",       "Rockets",         "HOU", "Western", "Southwest", "par_01hqmkq6fpetbrrcfrsgh982ed"),
    ("1610612746", "Los Angeles Clippers",      "Los Angeles",   "Clippers",        "LAC", "Western", "Pacific",   "par_01hqmkq6frex7v4gfdda5g204q"),
    ("1610612747", "Los Angeles Lakers",        "Los Angeles",   "Lakers",          "LAL", "Western", "Pacific",   "par_01hqmkq6fser5vcxm0fprbrcjz"),
    ("1610612748", "Miami Heat",                "Miami",         "Heat",            "MIA", "Eastern", "Southeast", "par_01hqmkq6fvemwsm7z5cv5d24q1"),
    ("1610612749", "Milwaukee Bucks",           "Milwaukee",     "Bucks",           "MIL", "Eastern", "Central",   "par_01hqmkq6fwfsdt20kfva62r4t7"),
    ("1610612750", "Minnesota Timberwolves",    "Minnesota",     "Timberwolves",    "MIN", "Western", "Northwest", "par_01hqmkq6fxfsca5pyfcqjjtaw9"),
    ("1610612751", "Brooklyn Nets",             "Brooklyn",      "Nets",            "BKN", "Eastern", "Atlantic",  "par_01hqmkq6fefp3r8597cv3wj3cr"),
    ("1610612752", "New York Knicks",           "New York",      "Knicks",          "NYK", "Eastern", "Atlantic",  "par_01hqmkq6fzfvyvrsb30jj85ade"),
    ("1610612753", "Orlando Magic",             "Orlando",       "Magic",           "ORL", "Eastern", "Southeast", "par_01hqmkq6g1edytehr6s6fzpfzn"),
    ("1610612754", "Indiana Pacers",            "Indiana",       "Pacers",          "IND", "Eastern", "Central",   "par_01hqmkq6fqfcgtzyhmce1m5g86"),
    ("1610612755", "Philadelphia 76ers",        "Philadelphia",  "76ers",           "PHI", "Eastern", "Atlantic",  "par_01hqmkq6g2egdt6asd59yp0vxp"),
    ("1610612756", "Phoenix Suns",              "Phoenix",       "Suns",            "PHX", "Western", "Pacific",   "par_01hqmkq6g3fnvve924dqwzxh90"),
    ("1610612757", "Portland Trail Blazers",    "Portland",      "Trail Blazers",   "POR", "Western", "Northwest", "par_01hqmkq6g4fz3tkx3w1w30mxb8"),
    ("1610612758", "Sacramento Kings",          "Sacramento",    "Kings",           "SAC", "Western", "Pacific",   "par_01hqmkq6g5f22am45k13jkjtn2"),
    ("1610612759", "San Antonio Spurs",         "San Antonio",   "Spurs",           "SAS", "Western", "Southwest", "par_01hqmkq6g6fzr95vwm0a66qmh5"),
    ("1610612760", "Oklahoma City Thunder",     "Oklahoma City", "Thunder",         "OKC", "Western", "Northwest", "par_01hqmkq6g0f44vajb32zbpwnbr"),
    ("1610612761", "Toronto Raptors",           "Toronto",       "Raptors",         "TOR", "Eastern", "Atlantic",  "par_01hqmkq6g7ffc92tna8axq3t4s"),
    ("1610612762", "Utah Jazz",                 "Utah",          "Jazz",            "UTA", "Western", "Northwest", "par_01hqmkq6g8e11sj1g4zv9z9snc"),
    ("1610612763", "Memphis Grizzlies",         "Memphis",       "Grizzlies",       "MEM", "Western", "Southwest", "par_01hqmkq6ftf24avayqxfd8840c"),
    ("1610612764", "Washington Wizards",        "Washington",    "Wizards",         "WAS", "Eastern", "Southeast", "par_01hqmkq6g9f0d9n781t7z01mn2"),
    ("1610612765", "Detroit Pistons",           "Detroit",       "Pistons",         "DET", "Eastern", "Central",   "par_01hqmkq6fmfyjsnjtexnh7vdwm"),
    ("1610612766", "Charlotte Hornets",         "Charlotte",     "Hornets",         "CHA", "Eastern", "Southeast", "par_01hqmkq6fffqq9gze9hqf1fwn6"),
]

MLB_TEAMS = [
    # source_team_id, team_name, city, nickname, tricode, conference(league), division, participant_id
    ("108",  "Los Angeles Angels",      "Los Angeles",   "Angels",      "LAA", "AL", "AL West",    "par_01hqmkrdwce34s2bm9drh78xqe"),
    ("109",  "Arizona Diamondbacks",    "Arizona",       "Diamondbacks","AZ",  "NL", "NL West",    "par_01hqmkrdw0fn28675jydfxxfwn"),
    ("110",  "Baltimore Orioles",       "Baltimore",     "Orioles",     "BAL", "AL", "AL East",    "par_01hqmkrdw2ey5sm4v3924da7ec"),
    ("111",  "Boston Red Sox",          "Boston",        "Red Sox",     "BOS", "AL", "AL East",    "par_01hqmkrdw3e9fr76tt7m3bns4m"),
    ("112",  "Chicago Cubs",            "Chicago",       "Cubs",        "CHC", "NL", "NL Central", "par_01hqmkrdw4frbt04kwb6z2exvr"),
    ("113",  "Cincinnati Reds",         "Cincinnati",    "Reds",        "CIN", "NL", "NL Central", "par_01hqmkrdw6fekahgy3j7ndevq4"),
    ("114",  "Cleveland Guardians",     "Cleveland",     "Guardians",   "CLE", "AL", "AL Central", "par_01hqmkrdw7e92rp1xj0h53tgtg"),
    ("115",  "Colorado Rockies",        "Colorado",      "Rockies",     "COL", "NL", "NL West",    "par_01hqmkrdw8end82x3xb2fhehv3"),
    ("116",  "Detroit Tigers",          "Detroit",       "Tigers",      "DET", "AL", "AL Central", "par_01hqmkrdw9fspanamf83tq59ax"),
    ("117",  "Houston Astros",          "Houston",       "Astros",      "HOU", "AL", "AL West",    "par_01hqmkrdwaebprg6k94kc3f7z5"),
    ("118",  "Kansas City Royals",      "Kansas City",   "Royals",      "KC",  "AL", "AL Central", "par_01hqmkrdwbfyhbre77gx6mgyfc"),
    ("119",  "Los Angeles Dodgers",     "Los Angeles",   "Dodgers",     "LAD", "NL", "NL West",    "par_01hqmkrdwdevbrzmkvf1gfsn5g"),
    ("120",  "Washington Nationals",    "Washington",    "Nationals",   "WSH", "NL", "NL East",    "par_01hqmkrdwyfdftwzgjyp2sevr4"),
    ("121",  "New York Mets",           "New York",      "Mets",        "NYM", "NL", "NL East",    "par_01hqmkrdwjf8jrcxq2twptkdy1"),
    ("133",  "Athletics",              "Oakland",       "Athletics",   "ATH", "AL", "AL West",    "par_01hqmkrdwmeba8n6jxwetryhk7"),
    ("134",  "Pittsburgh Pirates",      "Pittsburgh",    "Pirates",     "PIT", "NL", "NL Central", "par_01hqmkrdwpfmnbebtxvpfv1a7z"),
    ("135",  "San Diego Padres",        "San Diego",     "Padres",      "SD",  "NL", "NL West",    "par_01hqmkrdwqew5vtmpr3tbxa8gd"),
    ("136",  "Seattle Mariners",        "Seattle",       "Mariners",    "SEA", "AL", "AL West",    "par_01hqmkrdwse0qawha9hvcjkjf4"),
    ("137",  "San Francisco Giants",    "San Francisco", "Giants",      "SF",  "NL", "NL West",    "par_01hqmkrdwrex5b7e8x18zv15pg"),
    ("138",  "St. Louis Cardinals",     "St. Louis",     "Cardinals",   "STL", "NL", "NL Central", "par_01hqmkrdwtev1bxvy5y0yhvyyj"),
    ("139",  "Tampa Bay Rays",          "Tampa Bay",     "Rays",        "TB",  "AL", "AL East",    "par_01hqmkrdwvfed94w1vd601r5wt"),
    ("140",  "Texas Rangers",           "Texas",         "Rangers",     "TEX", "AL", "AL West",    "par_01hqmkrdwwfvssvhjx9f6rw2r8"),
    ("141",  "Toronto Blue Jays",       "Toronto",       "Blue Jays",   "TOR", "AL", "AL East",    "par_01hqmkrdwxfhabkjbbbk4by03r"),
    ("142",  "Minnesota Twins",         "Minnesota",     "Twins",       "MIN", "AL", "AL Central", "par_01hqmkrdwgfpvb3reb59xtp0sa"),
    ("143",  "Philadelphia Phillies",   "Philadelphia",  "Phillies",    "PHI", "NL", "NL East",    "par_01hqmkrdwnedzacd3r40hyrnxg"),
    ("144",  "Atlanta Braves",          "Atlanta",       "Braves",      "ATL", "NL", "NL East",    "par_01hqmkrdw1f3ma6f606jcfz6ax"),
    ("145",  "Chicago White Sox",       "Chicago",       "White Sox",   "CWS", "AL", "AL Central", "par_01hqmkrdw5ezqtm9np3kavr1py"),
    ("146",  "Miami Marlins",           "Miami",         "Marlins",     "MIA", "NL", "NL East",    "par_01hqmkrdweewjaqv7tj573a2fj"),
    ("147",  "New York Yankees",        "New York",      "Yankees",     "NYY", "AL", "AL East",    "par_01hqmkrdwke59s2wa1r1bpwvj5"),
    ("158",  "Milwaukee Brewers",       "Milwaukee",     "Brewers",     "MIL", "NL", "NL Central", "par_01hqmkrdwfecjscs9zecbc5xhd"),
]

# NFL: all bridge data from your existing table.
# Columns: source_team_id (=tricode=nflreadpy_abbr for most), team_name, city, nickname,
#          tricode, conference, division, participant_id,
#          pff_team_id, pff_team_abbr, nflreadpy_abbr, alt_abbr,
#          primary, secondary, tertiary, dark, light, background, foreground
#
# nflreadpy_abbr is the value that appears in nfl.games.home_team / away_team.
# Note: Rams use 'LA' in nfl.games (pyTeam), not 'LAR'.
# We store that in nflreadpy_abbr and fix nfl.games in step 5.
NFL_TEAMS = [
    #  src    team_name                    city             nickname       tricode  conf   division     participant_id                     pff_id  pff_abbr  py_abbr   alt
    ("ARI", "Arizona Cardinals",          "Arizona",       "Cardinals",   "ARI",  "NFC", "NFC West",  "par_01hqmkr1xsfxmrj5pdq0f23asx",  1,  "ARI", "ARI", "ARZ", "#97233f", "#ffffff", "#000000", "#97233f", "#ffffff", "#97233f", "#ffffff"),
    ("ATL", "Atlanta Falcons",            "Atlanta",       "Falcons",     "ATL",  "NFC", "NFC South", "par_01hqmkr1xtexkbhkq7ct921rne",  2,  "ATL", "ATL", "ATL", "#a71930", "#000000", "#ffffff", "#000000", "#a71930", "#a71930", "#ffffff"),
    ("BAL", "Baltimore Ravens",           "Baltimore",     "Ravens",      "BAL",  "AFC", "AFC North", "par_01hqmkr1xvev9rf557fy09k2cx",  3,  "BAL", "BAL", "BLT", "#1a195f", "#000000", "#9e7c0c", "#000000", "#1a195f", "#1a195f", "#ffffff"),
    ("BUF", "Buffalo Bills",              "Buffalo",       "Bills",       "BUF",  "AFC", "AFC East",  "par_01hqmkr1xwe6prjwr3j4gpqwx8",  4,  "BUF", "BUF", "BUF", "#00338d", "#c60c30", "#ffffff", "#00338d", "#ffffff", "#00338d", "#ffffff"),
    ("CAR", "Carolina Panthers",          "Carolina",      "Panthers",    "CAR",  "NFC", "NFC South", "par_01hqmkr1xxf2ebbqzb95qzxxxm",  5,  "CAR", "CAR", "CAR", "#0085ca", "#bfc0bf", "#000000", "#000000", "#0085ca", "#0085ca", "#ffffff"),
    ("CHI", "Chicago Bears",              "Chicago",       "Bears",       "CHI",  "NFC", "NFC North", "par_01hqmkr1xye20ahvp8fr2bvt74",  6,  "CHI", "CHI", "CHI", "#0b162a", "#c83803", "#ffffff", "#0b162a", "#c83803", "#0b162a", "#ffffff"),
    ("CIN", "Cincinnati Bengals",         "Cincinnati",    "Bengals",     "CIN",  "AFC", "AFC North", "par_01hqmkr1xze7xbceshy9tka512",  7,  "CIN", "CIN", "CIN", "#fb4f14", "#000000", "#ffffff", "#000000", "#fb4f14", "#fb4f14", "#ffffff"),
    ("CLE", "Cleveland Browns",           "Cleveland",     "Browns",      "CLE",  "AFC", "AFC North", "par_01hqmkr1y0ez5bem3gdncd8a0d",  8,  "CLE", "CLE", "CLV", "#311d00", "#ff3c00", "#ffffff", "#311d00", "#ff3c00", "#311d00", "#ffffff"),
    ("DAL", "Dallas Cowboys",             "Dallas",        "Cowboys",     "DAL",  "NFC", "NFC East",  "par_01hqmkr1y1esas88pmaxe87by4",  9,  "DAL", "DAL", "DAL", "#002244", "#ffffff", "#869397", "#002244", "#ffffff", "#002244", "#ffffff"),
    ("DEN", "Denver Broncos",             "Denver",        "Broncos",     "DEN",  "AFC", "AFC West",  "par_01hqmkr1y2e15tjsz9afcsj7da", 10,  "DEN", "DEN", "DEN", "#fb4f14", "#002244", "#ffffff", "#002244", "#fb4f14", "#fb4f14", "#ffffff"),
    ("DET", "Detroit Lions",              "Detroit",       "Lions",       "DET",  "NFC", "NFC North", "par_01hqmkr1y3fex9sq94dgg1107y", 11,  "DET", "DET", "DET", "#0076b6", "#b0b7bc", "#000000", "#0076b6", "#b0b7bc", "#0076b6", "#ffffff"),
    ("GB",  "Green Bay Packers",          "Green Bay",     "Packers",     "GB",   "NFC", "NFC North", "par_01hqmkr1y4ez38hyananses4hq", 12,  "GB",  "GB",  "GB",  "#183028", "#ffb81c", "#ffffff", "#183028", "#ffb81c", "#183028", "#ffffff"),
    ("HOU", "Houston Texans",             "Houston",       "Texans",      "HOU",  "AFC", "AFC South", "par_01hqmkr1y5f63reha26n71p2jx", 13,  "HOU", "HOU", "HST", "#03202f", "#a71930", "#ffffff", "#03202f", "#a71930", "#03202f", "#ffffff"),
    ("IND", "Indianapolis Colts",         "Indianapolis",  "Colts",       "IND",  "AFC", "AFC South", "par_01hqmkr1y6f10rxbf8y2y2xthh", 14,  "IND", "IND", "IND", "#002c5f", "#ffffff", "#a2aaad", "#002c5f", "#ffffff", "#002c5f", "#ffffff"),
    ("JAX", "Jacksonville Jaguars",       "Jacksonville",  "Jaguars",     "JAX",  "AFC", "AFC South", "par_01hqmkr1y7e2r9kcn2qe0dt1d5", 15,  "JAX", "JAX", "JAX", "#006778", "#d7a22a", "#000000", "#006778", "#d7a22a", "#006778", "#ffffff"),
    ("KC",  "Kansas City Chiefs",         "Kansas City",   "Chiefs",      "KC",   "AFC", "AFC West",  "par_01hqmkr1y8e9gt2q2rhmv196pv", 16,  "KC",  "KC",  "KC",  "#e31837", "#ffb81c", "#ffffff", "#e31837", "#ffb81c", "#e31837", "#ffffff"),
    ("LAC", "Los Angeles Chargers",       "Los Angeles",   "Chargers",    "LAC",  "AFC", "AFC West",  "par_01hqmkr1yafvas6wtv3jfs9f7a", 27,  "LAC", "LAC", "LAC", "#002a5e", "#ffc20e", "#0080c6", "#002a5e", "#ffc20e", "#002a5e", "#ffffff"),
    ("LAR", "Los Angeles Rams",           "Los Angeles",   "Rams",        "LAR",  "NFC", "NFC West",  "par_01hqmkr1ybfmfb8mhz10drfe21", 26,  "LAR", "LA",  "LA",  "#003594", "#ffd100", "#0c2340", "#003594", "#ffd100", "#003594", "#ffd100"),
    ("LV",  "Las Vegas Raiders",          "Las Vegas",     "Raiders",     "LV",   "AFC", "AFC West",  "par_01hqmkr1y9fkaaeekn9w035jft", 23,  "LV",  "LV",  "LV",  "#000000", "#a5acaf", "#ffffff", "#000000", "#a5acaf", "#000000", "#ffffff"),
    ("MIA", "Miami Dolphins",             "Miami",         "Dolphins",    "MIA",  "AFC", "AFC East",  "par_01hqmkr1ycf7dsbr1997gz03y9", 17,  "MIA", "MIA", "MIA", "#008e97", "#fc4c02", "#ffffff", "#008e97", "#fc4c02", "#008e97", "#ffffff"),
    ("MIN", "Minnesota Vikings",          "Minnesota",     "Vikings",     "MIN",  "NFC", "NFC North", "par_01hqmkr1ydf6vrfmd5f07caj88", 18,  "MIN", "MIN", "MIN", "#4f2683", "#ffc62f", "#ffffff", "#4f2683", "#ffc62f", "#4f2683", "#ffffff"),
    ("NE",  "New England Patriots",       "New England",   "Patriots",    "NE",   "AFC", "AFC East",  "par_01hqmkr1yeffz9y9spwv8bx3na", 19,  "NE",  "NE",  "NE",  "#002244", "#c60c30", "#b0b7bc", "#002244", "#c60c30", "#002244", "#ffffff"),
    ("NO",  "New Orleans Saints",         "New Orleans",   "Saints",      "NO",   "NFC", "NFC South", "par_01hqmkr1yfe62tp0rvy8bn2jyc", 20,  "NO",  "NO",  "NO",  "#d3bc8d", "#000000", "#ffffff", "#000000", "#d3bc8d", "#d3bc8d", "#000000"),
    ("NYG", "New York Giants",            "New York",      "Giants",      "NYG",  "NFC", "NFC East",  "par_01hqmkr1ygfzrv5sqe2v97c43e", 21,  "NYG", "NYG", "NYG", "#012352", "#a30d2d", "#9ba1a2", "#012352", "#9ba1a2", "#012352", "#ffffff"),
    ("NYJ", "New York Jets",              "New York",      "Jets",        "NYJ",  "AFC", "AFC East",  "par_01hqmkr1yhe4sb3y0wfzga67tf", 22,  "NYJ", "NYJ", "NYJ", "#125740", "#ffffff", "#000000", "#125740", "#ffffff", "#125740", "#ffffff"),
    ("PHI", "Philadelphia Eagles",        "Philadelphia",  "Eagles",      "PHI",  "NFC", "NFC East",  "par_01hqmkr1yjedgakx37g743855e", 24,  "PHI", "PHI", "PHI", "#004c54", "#a5acaf", "#000000", "#004c54", "#a5acaf", "#004c54", "#ffffff"),
    ("PIT", "Pittsburgh Steelers",        "Pittsburgh",    "Steelers",    "PIT",  "AFC", "AFC North", "par_01hqmkr1yker5bwcznt0b1jpj1", 25,  "PIT", "PIT", "PIT", "#000000", "#ffb612", "#ffffff", "#000000", "#ffb612", "#000000", "#ffffff"),
    ("SEA", "Seattle Seahawks",           "Seattle",       "Seahawks",    "SEA",  "NFC", "NFC West",  "par_01hqmkr1ynfwaa91y9zvagkavd", 29,  "SEA", "SEA", "SEA", "#002244", "#69be28", "#a5acaf", "#002244", "#69be28", "#002244", "#ffffff"),
    ("SF",  "San Francisco 49ers",        "San Francisco", "49ers",       "SF",   "NFC", "NFC West",  "par_01hqmkr1ymfv0a8kfg96ha10ag", 28,  "SF",  "SF",  "SF",  "#aa0000", "#ad995d", "#ffffff", "#aa0000", "#ad995d", "#aa0000", "#ffffff"),
    ("TB",  "Tampa Bay Buccaneers",       "Tampa Bay",     "Buccaneers",  "TB",   "NFC", "NFC South", "par_01hqmkr1ypeszan8sq8dh7rqbg", 30,  "TB",  "TB",  "TB",  "#d50a0a", "#34302b", "#ff7900", "#34302b", "#d50a0a", "#d50a0a", "#ffffff"),
    ("TEN", "Tennessee Titans",           "Tennessee",     "Titans",      "TEN",  "AFC", "AFC South", "par_01hqmkr1yqexebpc06vyfwxqqm", 31,  "TEN", "TEN", "TEN", "#0c2340", "#418fde", "#c8102e", "#0c2340", "#418fde", "#0c2340", "#ffffff"),
    ("WAS", "Washington Commanders",      "Washington",    "Commanders",  "WAS",  "NFC", "NFC East",  "par_01hqmkr1yrfsvbjjasn01a7xz4", 32,  "WAS", "WAS", "WAS", "#773141", "#ffb612", "#ffffff", "#773141", "#ffb612", "#773141", "#ffffff"),
]


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_TABLE = """
CREATE TABLE common.teams (
    team_id              INT           NOT NULL IDENTITY(1,1),
    sport_key            VARCHAR(30)   NOT NULL,
    league               VARCHAR(10)   NOT NULL,
    source_team_id       VARCHAR(20)   NOT NULL,
    team_name            VARCHAR(60)   NOT NULL,
    city                 VARCHAR(60)   NULL,
    nickname             VARCHAR(40)   NULL,
    tricode              VARCHAR(5)    NOT NULL,
    conference           VARCHAR(20)   NULL,
    division             VARCHAR(30)   NULL,
    participant_id       VARCHAR(50)   NULL,
    pff_team_id          INT           NULL,
    pff_team_abbr        VARCHAR(5)    NULL,
    nflreadpy_abbr       VARCHAR(5)    NULL,
    alt_abbr             VARCHAR(5)    NULL,
    primary_color        CHAR(7)       NULL,
    secondary_color      CHAR(7)       NULL,
    tertiary_color       CHAR(7)       NULL,
    dark_color_ref       CHAR(7)       NULL,
    light_color_ref      CHAR(7)       NULL,
    background_color_ref CHAR(7)       NULL,
    foreground_color     CHAR(7)       NULL,
    created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_common_teams         PRIMARY KEY (team_id),
    CONSTRAINT uq_common_teams_src     UNIQUE (sport_key, source_team_id)
)
"""

INSERT_SQL = """
INSERT INTO common.teams (
    sport_key, league, source_team_id, team_name, city, nickname, tricode,
    conference, division, participant_id,
    pff_team_id, pff_team_abbr, nflreadpy_abbr, alt_abbr,
    primary_color, secondary_color, tertiary_color,
    dark_color_ref, light_color_ref, background_color_ref, foreground_color
) VALUES (
    :sport_key, :league, :source_team_id, :team_name, :city, :nickname, :tricode,
    :conference, :division, :participant_id,
    :pff_team_id, :pff_team_abbr, :nflreadpy_abbr, :alt_abbr,
    :primary_color, :secondary_color, :tertiary_color,
    :dark_color_ref, :light_color_ref, :background_color_ref, :foreground_color
)
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    engine = get_engine()

    with engine.begin() as conn:

        # Step 1: backup
        log.info("Step 1: backing up common.teams -> common.teams_backup")
        conn.execute(text(
            "IF OBJECT_ID('common.teams_backup', 'U') IS NOT NULL "
            "DROP TABLE common.teams_backup"
        ))
        conn.execute(text(
            "SELECT * INTO common.teams_backup FROM common.teams"
        ))
        backup_count = conn.execute(
            text("SELECT COUNT(1) FROM common.teams_backup")
        ).scalar()
        log.info("  Backed up %d rows.", backup_count)

        # Step 2: drop existing table
        log.info("Step 2: dropping common.teams")
        conn.execute(text("DROP TABLE common.teams"))

        # Step 3: create new table
        log.info("Step 3: creating common.teams with new schema")
        conn.execute(text(CREATE_TABLE))

        # Step 4a: insert NBA
        log.info("Step 4a: inserting %d NBA teams", len(NBA_TEAMS))
        for row in NBA_TEAMS:
            (source_team_id, team_name, city, nickname, tricode,
             conference, division, participant_id) = row
            conn.execute(text(INSERT_SQL), {
                "sport_key": "basketball_nba", "league": "NBA",
                "source_team_id": source_team_id, "team_name": team_name,
                "city": city, "nickname": nickname, "tricode": tricode,
                "conference": conference, "division": division,
                "participant_id": participant_id,
                "pff_team_id": None, "pff_team_abbr": None,
                "nflreadpy_abbr": None, "alt_abbr": None,
                "primary_color": None, "secondary_color": None,
                "tertiary_color": None, "dark_color_ref": None,
                "light_color_ref": None, "background_color_ref": None,
                "foreground_color": None,
            })

        # Step 4b: insert MLB
        log.info("Step 4b: inserting %d MLB teams", len(MLB_TEAMS))
        for row in MLB_TEAMS:
            (source_team_id, team_name, city, nickname, tricode,
             conference, division, participant_id) = row
            conn.execute(text(INSERT_SQL), {
                "sport_key": "baseball_mlb", "league": "MLB",
                "source_team_id": source_team_id, "team_name": team_name,
                "city": city, "nickname": nickname, "tricode": tricode,
                "conference": conference, "division": division,
                "participant_id": participant_id,
                "pff_team_id": None, "pff_team_abbr": None,
                "nflreadpy_abbr": None, "alt_abbr": None,
                "primary_color": None, "secondary_color": None,
                "tertiary_color": None, "dark_color_ref": None,
                "light_color_ref": None, "background_color_ref": None,
                "foreground_color": None,
            })

        # Step 4c: insert NFL
        log.info("Step 4c: inserting %d NFL teams", len(NFL_TEAMS))
        for row in NFL_TEAMS:
            (source_team_id, team_name, city, nickname, tricode,
             conference, division, participant_id,
             pff_team_id, pff_team_abbr, nflreadpy_abbr, alt_abbr,
             primary_color, secondary_color, tertiary_color,
             dark_color_ref, light_color_ref, background_color_ref,
             foreground_color) = row
            conn.execute(text(INSERT_SQL), {
                "sport_key": "americanfootball_nfl", "league": "NFL",
                "source_team_id": source_team_id, "team_name": team_name,
                "city": city, "nickname": nickname, "tricode": tricode,
                "conference": conference, "division": division,
                "participant_id": participant_id,
                "pff_team_id": pff_team_id, "pff_team_abbr": pff_team_abbr,
                "nflreadpy_abbr": nflreadpy_abbr, "alt_abbr": alt_abbr,
                "primary_color": primary_color,
                "secondary_color": secondary_color,
                "tertiary_color": tertiary_color,
                "dark_color_ref": dark_color_ref,
                "light_color_ref": light_color_ref,
                "background_color_ref": background_color_ref,
                "foreground_color": foreground_color,
            })

        # Step 5: fix nfl.games LA -> LAR
        log.info("Step 5: fixing nfl.games home_team/away_team 'LA' -> 'LAR' (Rams)")
        r1 = conn.execute(text(
            "UPDATE nfl.games SET home_team = 'LAR' WHERE home_team = 'LA'"
        ))
        r2 = conn.execute(text(
            "UPDATE nfl.games SET away_team = 'LAR' WHERE away_team = 'LA'"
        ))
        log.info("  Updated %d home_team rows, %d away_team rows.",
                 r1.rowcount, r2.rowcount)

        # Step 6: verify
        log.info("Step 6: verification")
        counts = conn.execute(text(
            "SELECT league, COUNT(1) FROM common.teams GROUP BY league ORDER BY league"
        )).fetchall()
        total = 0
        for league, cnt in counts:
            log.info("  %s: %d rows", league, cnt)
            total += cnt
        log.info("  Total: %d rows", total)

        # Spot-check: Rams nflreadpy_abbr
        rams = conn.execute(text(
            "SELECT tricode, nflreadpy_abbr, pff_team_id "
            "FROM common.teams WHERE tricode = 'LAR'"
        )).fetchone()
        log.info("  Rams spot-check: tricode=%s nflreadpy_abbr=%s pff_team_id=%s",
                 rams[0], rams[1], rams[2])

        # Spot-check: nfl.games LA count should be 0
        la_remaining = conn.execute(text(
            "SELECT COUNT(1) FROM nfl.games "
            "WHERE home_team = 'LA' OR away_team = 'LA'"
        )).scalar()
        log.info("  nfl.games rows still using 'LA': %d (should be 0)", la_remaining)

    log.info("Migration complete.")


if __name__ == "__main__":
    main()
