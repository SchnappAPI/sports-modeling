"""
db_inventory.py — check schema of odds tables for link column,
and test what includeLinks=true returns from the Odds API
"""
import os
import pyodbc
import urllib.request
import json

DRIVER   = "ODBC Driver 18 for SQL Server"
SERVER   = os.environ["AZURE_SQL_SERVER"]
DATABASE = os.environ["AZURE_SQL_DATABASE"]
USERNAME = os.environ["AZURE_SQL_USERNAME"]
PASSWORD = os.environ["AZURE_SQL_PASSWORD"]
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

CONN_STR = (
    f"DRIVER={{{DRIVER}}};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=60;"
)

out = []
def p(line=""): print(line); out.append(line)

conn   = pyodbc.connect(CONN_STR)
cursor = conn.cursor()

p("=== COLUMNS IN odds.upcoming_player_props ===")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'upcoming_player_props'
    ORDER BY ORDINAL_POSITION
""")
for row in cursor.fetchall():
    p(f"  {row[0]:<40} {row[1]:<20} {str(row[2]) if row[2] else ''}")

p()
p("=== COLUMNS IN odds.player_props ===")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'odds' AND TABLE_NAME = 'player_props'
    ORDER BY ORDINAL_POSITION
""")
for row in cursor.fetchall():
    p(f"  {row[0]:<40} {row[1]:<20} {str(row[2]) if row[2] else ''}")

p()
p("=== CALLING ODDS API WITH includeLinks=true (single market, single event) ===")
if ODDS_API_KEY:
    # First get today's events to find one event ID
    url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={ODDS_API_KEY}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            events = json.loads(resp.read())
        if events:
            event_id = events[0]["id"]
            p(f"  Using event_id: {event_id}")
            p(f"  home: {events[0].get('home_team')} vs away: {events[0].get('away_team')}")
            # Now fetch one prop market with links
            prop_url = (
                f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event_id}/odds"
                f"?apiKey={ODDS_API_KEY}&regions=us&markets=player_points,player_points_alternate"
                f"&bookmakers=fanduel&oddsFormat=american&includeLinks=true"
            )
            with urllib.request.urlopen(prop_url, timeout=15) as resp:
                prop_data = json.loads(resp.read())
            p(f"  API response keys: {list(prop_data.keys())}")
            bookmakers = prop_data.get("bookmakers", [])
            if bookmakers:
                bk = bookmakers[0]
                p(f"  Bookmaker: {bk.get('key')}")
                for market in bk.get("markets", [])[:2]:
                    p(f"  Market: {market.get('key')}")
                    for outcome in market.get("outcomes", [])[:5]:
                        p(f"    {outcome}")
            else:
                p("  No bookmakers returned (game may not have props yet)")
        else:
            p("  No events found for today")
    except Exception as e:
        p(f"  Error: {e}")
else:
    p("  ODDS_API_KEY not set")

conn.close()
p()
p("Done.")

with open("/tmp/db_inventory_output.txt", "w") as f:
    f.write("\n".join(out))
