"""
nba_clear.py

Clears all data from NBA tables in FK-safe order.
Leaves the schema and table structures intact so the ETL can reload cleanly.

Usage
  python etl/nba_clear.py
  python etl/nba_clear.py --confirm   # skip the interactive prompt

Secrets required (same as nba_etl.py)
  AZURE_SQL_SERVER, AZURE_SQL_DATABASE, AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD

Delete order (children before parents)
  1. nba.player_box_score_stats   -- references games and players
  2. nba.player_passing_stats     -- references players
  3. nba.player_rebound_chances   -- references players
  4. nba.daily_lineups            -- no FK, cleared early for cleanliness
  5. nba.team_box_score_stats     -- references games and teams (if exists)
  6. nba.games                    -- references teams
  7. nba.schedule                 -- no FK
  8. nba.players                  -- references teams
  9. nba.teams                    -- root reference table
"""

import argparse
import os
import sys
import time
import logging

from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Tables in FK-safe delete order.
# Each entry: (schema, table)
DELETE_ORDER = [
    ("nba", "player_box_score_stats"),
    ("nba", "player_passing_stats"),
    ("nba", "player_rebound_chances"),
    ("nba", "daily_lineups"),
    ("nba", "team_box_score_stats"),   # may not exist; handled gracefully
    ("nba", "games"),
    ("nba", "schedule"),
    ("nba", "players"),
    ("nba", "teams"),
]


def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        f"&Encrypt=yes&TrustServerCertificate={os.environ.get('AZURE_SQL_TRUST_CERT', 'no')}"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(f"DB connection attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                log.info("Waiting 60s for Azure SQL to resume...")
                time.sleep(60)
    raise RuntimeError("Could not connect to Azure SQL after 3 attempts.")


def table_exists(engine, schema, table):
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t"
            ),
            {"s": schema, "t": table},
        )
        return result.fetchone() is not None


def clear_tables(engine):
    for schema, table in DELETE_ORDER:
        if not table_exists(engine, schema, table):
            log.info(f"  {schema}.{table}: does not exist, skipping")
            continue
        with engine.begin() as conn:
            result = conn.execute(text(f"DELETE FROM {schema}.{table}"))
            log.info(f"  {schema}.{table}: {result.rowcount} rows deleted")
    log.info("All NBA tables cleared.")


def main():
    parser = argparse.ArgumentParser(description="Clear all NBA table data")
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip the interactive confirmation prompt",
    )
    args = parser.parse_args()

    if not args.confirm:
        print("This will DELETE all rows from every NBA table.")
        print("Table structures will be preserved. This cannot be undone.")
        response = input("Type YES to continue: ").strip()
        if response != "YES":
            print("Aborted.")
            sys.exit(0)

    engine = get_engine()
    clear_tables(engine)


if __name__ == "__main__":
    main()
