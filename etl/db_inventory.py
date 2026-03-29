"""
Diagnostic: inspect alternate prop market keys and sample line values
from both odds.upcoming_player_props and odds.player_props.
"""
import os, time
import pandas as pd
from sqlalchemy import create_engine, text

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
                print(f'Attempt {attempt+1} failed, retrying in {retry_wait}s...')
                time.sleep(retry_wait)
            else:
                raise

def run():
    engine = get_engine()
    print('Connected.\n')

    # 1. All distinct market_keys containing 'alternate' in upcoming props
    print('=== upcoming_player_props: alternate market_keys ===')
    df = pd.read_sql(text("""
        SELECT DISTINCT market_key, bookmaker_key, outcome_name
        FROM odds.upcoming_player_props
        WHERE market_key LIKE '%alternate%'
        ORDER BY market_key, bookmaker_key
    """), engine)
    print(df.to_string(index=False))

    # 2. Sample line values per alternate market from upcoming props
    print('\n=== upcoming_player_props: sample outcome_point per alt market (fanduel, Over) ===')
    df2 = pd.read_sql(text("""
        SELECT
            market_key,
            STRING_AGG(CAST(CAST(outcome_point AS DECIMAL(6,1)) AS VARCHAR), ', ')
                WITHIN GROUP (ORDER BY outcome_point) AS line_values
        FROM (
            SELECT DISTINCT market_key, outcome_point
            FROM odds.upcoming_player_props
            WHERE market_key   LIKE '%alternate%'
              AND bookmaker_key = 'fanduel'
              AND outcome_name  = 'Over'
              AND outcome_point IS NOT NULL
        ) x
        GROUP BY market_key
        ORDER BY market_key
    """), engine)
    print(df2.to_string(index=False))

    # 3. Same check against historical player_props for broader coverage
    print('\n=== player_props: distinct alternate market_keys (fanduel) ===')
    df3 = pd.read_sql(text("""
        SELECT DISTINCT market_key
        FROM odds.player_props
        WHERE market_key   LIKE '%alternate%'
          AND bookmaker_key = 'fanduel'
        ORDER BY market_key
    """), engine)
    print(df3.to_string(index=False))

    # 4. Sample line values per alt market from historical props (last 30 days)
    print('\n=== player_props: outcome_point per alt market (fanduel, Over, last 30 days) ===')
    df4 = pd.read_sql(text("""
        SELECT
            market_key,
            STRING_AGG(CAST(CAST(outcome_point AS DECIMAL(6,1)) AS VARCHAR), ', ')
                WITHIN GROUP (ORDER BY outcome_point) AS line_values
        FROM (
            SELECT DISTINCT market_key, outcome_point
            FROM odds.player_props
            WHERE market_key    LIKE '%alternate%'
              AND bookmaker_key  = 'fanduel'
              AND outcome_name   = 'Over'
              AND outcome_point  IS NOT NULL
              AND commence_time >= DATEADD(day, -30, GETUTCDATE())
        ) x
        GROUP BY market_key
        ORDER BY market_key
    """), engine)
    print(df4.to_string(index=False))

if __name__ == '__main__':
    run()
