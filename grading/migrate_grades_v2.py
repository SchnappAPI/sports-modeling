"""
migrate_grades_v2.py

One-time migration: adds component and composite grade columns to
common.daily_grades. Safe to run multiple times — each ALTER is guarded
by an IF NOT EXISTS check against sys.columns.

Columns added
-------------
  trend_grade      FLOAT NULL   -- last-10 vs last-30 hit rate momentum
  momentum_grade   FLOAT NULL   -- uncapped consecutive hit/miss streak signal
  pattern_grade    FLOAT NULL   -- recurrence spacing and clustering signal
  matchup_grade    FLOAT NULL   -- defense rank for position+stat vs opponent
  regression_grade FLOAT NULL   -- z-score reversion signal vs season baseline
  composite_grade  FLOAT NULL   -- equal-weighted average of all non-NULL components

Run via: grades-migrate.yml (workflow_dispatch)
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

NEW_COLUMNS = [
    ("trend_grade",      "FLOAT"),
    ("momentum_grade",   "FLOAT"),
    ("pattern_grade",    "FLOAT"),
    ("matchup_grade",    "FLOAT"),
    ("regression_grade", "FLOAT"),
    ("composite_grade",  "FLOAT"),
]


def get_engine(max_retries=3, retry_wait=60):
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=False)
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(f"DB connection attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                log.info(f"Waiting {retry_wait}s for Azure SQL to resume...")
                time.sleep(retry_wait)
    raise RuntimeError("Could not connect to Azure SQL after retries.")


def run_migration(engine):
    with engine.begin() as conn:
        for col_name, col_type in NEW_COLUMNS:
            exists = conn.execute(text("""
                SELECT 1
                FROM sys.columns c
                JOIN sys.tables t  ON t.object_id  = c.object_id
                JOIN sys.schemas s ON s.schema_id  = t.schema_id
                WHERE s.name = 'common'
                  AND t.name = 'daily_grades'
                  AND c.name = :col
            """), {"col": col_name}).fetchone()

            if exists:
                log.info(f"  Column already exists, skipping: {col_name}")
            else:
                conn.execute(text(
                    f"ALTER TABLE common.daily_grades ADD {col_name} {col_type} NULL"
                ))
                log.info(f"  Added column: {col_name} {col_type} NULL")

    log.info("Migration complete.")


def main():
    engine = get_engine()
    run_migration(engine)


if __name__ == "__main__":
    main()
