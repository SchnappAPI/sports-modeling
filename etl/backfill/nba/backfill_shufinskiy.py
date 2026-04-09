"""
backfill_shufinskiy.py
Downloads pre-built NBA PBP files from shufinskiy/nba_data on GitHub.
No rate limiting. Files are .tar.xz in the datasets/ subfolder.
Stores:
  nba/play_by_play_nbastats/season=YYYY/{data_type}_{rg|po}.parquet
  nba/play_by_play_pbpstats/season=YYYY/
  nba/shot_detail/season=YYYY/
Container: nba-backfill
"""
import io, logging, os, sys, tarfile, time
import requests
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
from storage_pandas import upload_parquet, checkpoint_exists, mark_checkpoint, log_error

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s")

BASE_URL     = "https://github.com/shufinskiy/nba_data/raw/main/datasets/{filename}"
START_YEAR   = 2015
END_YEAR     = 2024
DATA_TYPES   = ["nbastats", "pbpstats", "shotdetail"]
SEASON_TYPES = ["rg", "po"]
TABLE_MAP    = {
    "nbastats":   "nba/play_by_play_nbastats",
    "pbpstats":   "nba/play_by_play_pbpstats",
    "shotdetail": "nba/shot_detail",
}

def main():
    logging.info(f"Starting shufinskiy NBA backfill: {START_YEAR}-{END_YEAR}")
    for year in range(START_YEAR, END_YEAR + 1):
        for data_type in DATA_TYPES:
            for season_type in SEASON_TYPES:
                key = f"shufinskiy_{data_type}_{season_type}_{year}"
                if checkpoint_exists(key):
                    continue
                fname = f"{data_type}_{year}.tar.xz" if season_type == "rg" else f"{data_type}_po_{year}.tar.xz"
                url   = BASE_URL.format(filename=fname)
                try:
                    resp = requests.get(url, timeout=120)
                    resp.raise_for_status()
                    buf = io.BytesIO(resp.content)
                    with tarfile.open(fileobj=buf, mode="r:xz") as tar:
                        df = pd.read_csv(tar.extractfile(tar.getmembers()[0]), low_memory=False)
                    upload_parquet(df, f"{TABLE_MAP[data_type]}/season={year}/{data_type}_{season_type}.parquet")
                    mark_checkpoint(key)
                    logging.info(f"  {key}: {len(df)} rows")
                    time.sleep(1)
                except Exception as e:
                    if "404" in str(e):
                        mark_checkpoint(key)
                    else:
                        log_error("shufinskiy", key, e)
                        time.sleep(5)
    logging.info("Shufinskiy NBA backfill complete.")

if __name__ == "__main__":
    main()
