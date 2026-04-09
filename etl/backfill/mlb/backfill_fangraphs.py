"""
backfill_fangraphs.py
Pulls FanGraphs season-level batting and pitching stats via pybaseball.
Stores:
  mlb/batting_stats_season/season=YYYY/batting_stats.parquet
  mlb/pitching_stats_season/season=YYYY/pitching_stats.parquet
Container: mlb-backfill
"""
import logging, os, sys, time
from datetime import date
import pandas as pd
from pybaseball import batting_stats, pitching_stats

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from storage_pandas import upload_parquet, checkpoint_exists, mark_checkpoint, log_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

START_YEAR = 2015
END_YEAR   = date.today().year

def main():
    logging.info(f"Starting FanGraphs backfill: {START_YEAR} to {END_YEAR}")
    for year in range(START_YEAR, END_YEAR + 1):
        for stat_type, fn, folder in [
            ("batting",  batting_stats,  "batting_stats_season"),
            ("pitching", pitching_stats, "pitching_stats_season"),
        ]:
            key = f"fangraphs_{stat_type}_{year}"
            if checkpoint_exists(key):
                logging.info(f"SKIP (done): {stat_type} {year}")
                continue
            try:
                df = fn(year, year, qual=0)
                if df is not None and len(df) > 0:
                    upload_parquet(df.reset_index(drop=True),
                                   f"mlb/{folder}/season={year}/{stat_type}_stats.parquet")
                    mark_checkpoint(key)
                    logging.info(f"  {stat_type} {year}: {len(df)} rows, {len(df.columns)} cols")
                time.sleep(3)
            except Exception as e:
                log_error("fangraphs", key, e)
                time.sleep(10)
    logging.info("FanGraphs backfill complete.")

if __name__ == "__main__":
    main()
