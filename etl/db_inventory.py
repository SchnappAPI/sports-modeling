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
    print('Connected to database.\n')

    tables_sql = """
        SELECT
            t.TABLE_SCHEMA   AS schema_name,
            t.TABLE_NAME     AS table_name
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
    """
    tables = pd.read_sql(tables_sql, engine)

    if tables.empty:
        print('No tables found.')
        return

    current_schema = None
    for _, row in tables.iterrows():
        schema = row['schema_name']
        table = row['table_name']

        if schema != current_schema:
            print(f'--- Schema: {schema} ---')
            current_schema = schema

        try:
            with engine.connect() as conn:
                result = conn.execute(text(f'SELECT COUNT(*) FROM [{schema}].[{table}]'))
                count = result.scalar()
            print(f'  {table}: {count:,} rows')
        except Exception as e:
            print(f'  {table}: ERROR - {e}')

    print('\n--- Column inventory ---')
    cols_sql = """
        SELECT
            c.TABLE_SCHEMA,
            c.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS c
        INNER JOIN INFORMATION_SCHEMA.TABLES t
            ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
            AND c.TABLE_NAME  = t.TABLE_NAME
            AND t.TABLE_TYPE  = 'BASE TABLE'
        ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
    """
    cols = pd.read_sql(cols_sql, engine)

    current_table = None
    for _, row in cols.iterrows():
        key = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}"
        if key != current_table:
            print(f'\n  [{row["TABLE_SCHEMA"]}].[{row["TABLE_NAME"]}]')
            current_table = key
        nullable = '(nullable)' if row['IS_NULLABLE'] == 'YES' else ''
        print(f'    {row["COLUMN_NAME"]}  {row["DATA_TYPE"]}  {nullable}')

if __name__ == '__main__':
    run()
