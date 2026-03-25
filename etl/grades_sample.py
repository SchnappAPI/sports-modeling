import os
import pandas as pd
from sqlalchemy import create_engine, text
import time

def get_engine(max_retries=3, retry_wait=45):
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:{os.environ['AZURE_SQL_PASSWORD']}"
        f"@{os.environ['AZURE_SQL_SERVER']}/{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            return engine
        except Exception as e:
            if attempt < max_retries - 1:
                print(f'Connection attempt {attempt+1} failed. Retrying in {retry_wait}s...')
                time.sleep(retry_wait)
            else:
                raise

def run():
    engine = get_engine()
    print('Connected.\n')

    # Date range and total rows
    print('--- Date range ---')
    summary = pd.read_sql(
        "SELECT MIN(grade_date) AS earliest, MAX(grade_date) AS latest, COUNT(*) AS total_rows "
        "FROM common.daily_grades",
        engine
    )
    print(summary.to_string(index=False))

    # Row counts by market_key
    print('\n--- Rows by market_key ---')
    by_market = pd.read_sql(
        "SELECT market_key, COUNT(*) AS rows, "
        "MIN(grade_date) AS earliest, MAX(grade_date) AS latest "
        "FROM common.daily_grades "
        "GROUP BY market_key ORDER BY rows DESC",
        engine
    )
    print(by_market.to_string(index=False))

    # Row counts by bookmaker
    print('\n--- Rows by bookmaker ---')
    by_book = pd.read_sql(
        "SELECT bookmaker_key, COUNT(*) AS rows "
        "FROM common.daily_grades "
        "GROUP BY bookmaker_key ORDER BY rows DESC",
        engine
    )
    print(by_book.to_string(index=False))

    # Sample 20 rows from the most recent grade date, highest grade first
    print('\n--- Sample rows (most recent grade date, top 20 by grade desc) ---')
    sample = pd.read_sql(
        "SELECT TOP 20 grade_date, player_name, market_key, line_value, "
        "hit_rate_60, hit_rate_20, weighted_hit_rate, grade "
        "FROM common.daily_grades "
        "WHERE grade_date = (SELECT MAX(grade_date) FROM common.daily_grades) "
        "ORDER BY grade DESC",
        engine
    )
    print(sample.to_string(index=False))

if __name__ == '__main__':
    run()
