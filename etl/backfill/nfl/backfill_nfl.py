"""
backfill_nfl.py
Pulls NFL data from nflreadpy (nflverse pre-built Parquet files on GitHub).
No rate limiting, no proxy required. Returns polars DataFrames.
Stores:
  nfl/play_by_play/season=YYYY/pbp.parquet
  nfl/player_stats/season=YYYY/player_stats.parquet
  nfl/rosters/season=YYYY/rosters.parquet
  nfl/schedules/season=YYYY/schedules.parquet
  nfl/players/players.parquet
  nfl/teams/teams.parquet
Container: nfl-backfill
Note: AZURE_STORAGE_CONTAINER must be set to 'nfl-backfill' before running.
"""
import logging, os, sys, time, warnings
warnings.filterwarnings("ignore")
import nflreadpy as nfl

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from storage_polars import upload_parquet, checkpoint_exists, mark_checkpoint, log_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

START_YEAR   = 2015
CURRENT_YEAR = nfl.get_current_season()

def pull(key, fn, blob_path, *args):
    if checkpoint_exists(key):
        logging.info(f"SKIP (done): {key}")
        return
    try:
        df = fn(*args) if args else fn()
        if df is not None and len(df) > 0:
            upload_parquet(df, blob_path)
        mark_checkpoint(key)
        logging.info(f"{key}: {len(df) if df is not None else 0} rows")
        time.sleep(1)
    except Exception as e:
        log_error("nfl", key, e)
        time.sleep(5)

def main():
    logging.info(f"Starting NFL backfill: {START_YEAR} to {CURRENT_YEAR}")
    pull("nfl_players", nfl.load_players, "nfl/players/players.parquet")
    pull("nfl_teams",   nfl.load_teams,   "nfl/teams/teams.parquet")
    for year in range(START_YEAR, CURRENT_YEAR + 1):
        logging.info(f"=== Season {year} ===")
        pull(f"nfl_pbp_{year}",          nfl.load_pbp,          f"nfl/play_by_play/season={year}/pbp.parquet",          [year])
        pull(f"nfl_player_stats_{year}", nfl.load_player_stats, f"nfl/player_stats/season={year}/player_stats.parquet", [year])
        pull(f"nfl_rosters_{year}",      nfl.load_rosters,      f"nfl/rosters/season={year}/rosters.parquet",           [year])
        pull(f"nfl_schedules_{year}",    nfl.load_schedules,    f"nfl/schedules/season={year}/schedules.parquet",       [year])
    logging.info("NFL backfill complete.")

if __name__ == "__main__":
    main()
