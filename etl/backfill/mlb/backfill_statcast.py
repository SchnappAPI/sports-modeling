"""
backfill_statcast.py
Pulls Statcast pitch-by-pitch data from Baseball Savant via pybaseball.
Stores one Parquet file per week per season under:
  mlb/statcast_pitches/season=YYYY/week_YYYY-MM-DD.parquet
Container: mlb-backfill
Credentials: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY, AZURE_STORAGE_CONTAINER=mlb-backfill
"""
import logging, os, sys, time
from datetime import date, timedelta
import pandas as pd
from pybaseball import statcast
from pybaseball import cache as pb_cache

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from storage_pandas import upload_parquet, checkpoint_exists, mark_checkpoint, log_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")
pb_cache.enable()

START_YEAR = 2015
END_YEAR   = date.today().year

def season_weeks(year: int):
    start = date(year, 3, 20)
    end   = min(date(year, 11, 15), date.today())
    cursor = start
    while cursor < end:
        week_end = min(cursor + timedelta(days=6), end)
        yield cursor, week_end
        cursor = week_end + timedelta(days=1)

def main():
    logging.info(f"Starting Statcast backfill: {START_YEAR} to {END_YEAR}")
    for year in range(START_YEAR, END_YEAR + 1):
        logging.info(f"=== Season {year} ===")
        for start, end in season_weeks(year):
            key = f"statcast_{start.strftime('%Y-%m-%d')}"
            if checkpoint_exists(key):
                continue
            try:
                df = statcast(start_dt=start.strftime("%Y-%m-%d"), end_dt=end.strftime("%Y-%m-%d"))
                if df is None or len(df) == 0:
                    mark_checkpoint(key)
                    continue
                upload_parquet(df.reset_index(drop=True),
                               f"mlb/statcast_pitches/season={year}/week_{start.strftime('%Y-%m-%d')}.parquet")
                mark_checkpoint(key)
                time.sleep(3)
            except Exception as e:
                log_error("statcast", key, e)
                time.sleep(10)
    logging.info("Statcast backfill complete.")

if __name__ == "__main__":
    main()
