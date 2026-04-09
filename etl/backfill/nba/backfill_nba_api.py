"""
backfill_nba_api.py
Pulls NBA game logs, players, and teams from stats.nba.com via nba_api.
Requires NBA_PROXY_URL (Webshare rotating residential proxy).
Stores:
  nba/game_logs/season=YYYY/game_logs_{rg|po}.parquet
  nba/players/players.parquet
  nba/teams/teams.parquet
Container: nba-backfill
"""
import logging, os, sys, time
import pandas as pd
from nba_api.stats.endpoints import leaguegamelog, commonallplayers
from nba_api.stats.static import teams as nba_teams_static

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from storage_pandas import upload_parquet, checkpoint_exists, mark_checkpoint, log_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

PROXY_URL  = os.environ.get("NBA_PROXY_URL")
HEADERS    = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.nba.com/", "Origin": "https://www.nba.com",
    "Accept": "application/json",
    "x-nba-stats-origin": "stats", "x-nba-stats-token": "true",
}
START_YEAR = 2015
END_YEAR   = 2025

def season_str(year): return f"{year}-{str(year+1)[-2:]}"

def main():
    if not PROXY_URL:
        logging.error("NBA_PROXY_URL not set — aborting.")
        sys.exit(1)
    logging.info(f"Starting NBA API backfill: {START_YEAR}-{END_YEAR}")

    # Teams (static, no proxy needed)
    key = "nba_teams_reference"
    if not checkpoint_exists(key):
        try:
            upload_parquet(pd.DataFrame(nba_teams_static.get_teams()), "nba/teams/teams.parquet")
            mark_checkpoint(key)
        except Exception as e:
            log_error("nba_api", key, e)

    # Players
    key = "nba_players_reference"
    if not checkpoint_exists(key):
        try:
            df = commonallplayers.CommonAllPlayers(
                is_only_current_season=0, league_id="00", season="2024-25",
                proxy=PROXY_URL, headers=HEADERS, timeout=30
            ).get_data_frames()[0]
            upload_parquet(df, "nba/players/players.parquet")
            mark_checkpoint(key)
        except Exception as e:
            log_error("nba_api", key, e)

    # Game logs per season
    for year in range(START_YEAR, END_YEAR + 1):
        for season_type, short in [("Regular Season", "rg"), ("Playoffs", "po")]:
            key = f"nba_gamelogs_{year}_{short}"
            if checkpoint_exists(key):
                continue
            try:
                df = leaguegamelog.LeagueGameLog(
                    season=season_str(year), season_type_all_star=season_type,
                    player_or_team_abbreviation="P", league_id="00",
                    proxy=PROXY_URL, headers=HEADERS, timeout=60
                ).get_data_frames()[0]
                if df is not None and len(df) > 0:
                    upload_parquet(df, f"nba/game_logs/season={year}/game_logs_{short}.parquet")
                mark_checkpoint(key)
                logging.info(f"  {key}: {len(df)} rows")
                time.sleep(2)
            except Exception as e:
                log_error("nba_api", key, e)
                time.sleep(10)

    logging.info("NBA API backfill complete.")

if __name__ == "__main__":
    main()
