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

    # What columns exist in daily_grades
    print('--- Columns in common.daily_grades ---')
    cols = pd.read_sql(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'common' AND TABLE_NAME = 'daily_grades' "
        "ORDER BY ORDINAL_POSITION",
        engine
    )
    for _, r in cols.iterrows():
        print(f'  {r["COLUMN_NAME"]}  {r["DATA_TYPE"]}')

    # Date range and total rows
    print('\n--- Date range ---')
    summary = pd.read_sql(
        "SELECT MIN(grade_date) AS earliest, MAX(grade_date) AS latest, COUNT(*) AS total_rows "
        "FROM common.daily_grades",
        engine
    )
    print(summary.to_string(index=False))

    # Row counts by stat_code
    print('\n--- Rows by stat_code ---')
    by_stat = pd.read_sql(
        "SELECT stat_code, COUNT(*) AS rows, "
        "MIN(grade_date) AS earliest, MAX(grade_date) AS latest "
        "FROM common.daily_grades "
        "GROUP BY stat_code ORDER BY stat_code",
        engine
    )
    print(by_stat.to_string(index=False))

    # Sample 20 rows from the most recent grade date
    print('\n--- Sample rows (most recent grade date, top 20 by grade desc) ---')
    sample = pd.read_sql(
        "SELECT TOP 20 * FROM common.daily_grades "
        "WHERE grade_date = (SELECT MAX(grade_date) FROM common.daily_grades) "
        "ORDER BY grade DESC",
        engine
    )
    print(sample.to_string(index=False))

if __name__ == '__main__':
    run()
