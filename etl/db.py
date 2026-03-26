# etl/db.py
import os
import time
from sqlalchemy import create_engine, text


def get_engine(max_retries=3, retry_wait=45):
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )

    engine = create_engine(conn_str, fast_executemany=True)

    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_wait)


def upsert(engine, df, schema, table, keys, dtype=None):
    """
    Upsert a DataFrame into a permanent table using a SQL Server MERGE statement.

    dtype (optional): dict mapping column name -> SQLAlchemy type, passed directly
    to to_sql. Use this to override pandas' inferred column types on the staging
    table when the inferred size would be too narrow for the actual data.
    Example: {"result_description": sqlalchemy.types.VARCHAR(1000)}

    Staging pattern:
      1. Explicitly drop the temp table if it exists from a previous call in
         this session. This avoids SQLAlchemy's reflection path, which cannot
         see SQL Server temp tables and raises InvalidRequestError when
         if_exists='replace' is used.
      2. Create the temp table fresh via to_sql with if_exists='append'.
         The table does not exist at this point, so 'append' behaves identically
         to 'replace' but skips the reflect-and-drop step entirely.
      3. MERGE from the staging table into the destination table.
    """
    staging = f"#stage_{table}"

    with engine.begin() as conn:
        conn.execute(text(f"IF OBJECT_ID('tempdb..{staging}') IS NOT NULL DROP TABLE {staging}"))

    df.to_sql(staging, engine, index=False, if_exists="append", chunksize=200, dtype=dtype)

    set_clause  = ", ".join(f"t.{c} = s.{c}" for c in df.columns if c not in keys)
    key_clause  = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    insert_cols = ", ".join(df.columns)
    insert_vals = ", ".join(f"s.{c}" for c in df.columns)

    sql = f"""
    MERGE {schema}.{table} AS t
    USING {staging} AS s
    ON ({key_clause})
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
    """

    with engine.begin() as conn:
        conn.execute(text(sql))
