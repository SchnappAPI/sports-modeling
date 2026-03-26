# etl/db.py
import os
import time
from sqlalchemy import create_engine, text


def _build_conn_str():
    return (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )


def get_engine(max_retries=3, retry_wait=45):
    """
    Returns a SQLAlchemy engine with fast_executemany=True.
    Use for all normal upserts where column widths are numeric or
    short fixed-width strings that pandas infers correctly.
    """
    engine = create_engine(_build_conn_str(), fast_executemany=True)
    for i in range(max_retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception:
            if i == max_retries - 1:
                raise
            time.sleep(retry_wait)


def get_engine_slow(max_retries=3, retry_wait=45):
    """
    Returns a SQLAlchemy engine with fast_executemany=False.

    Use when inserting into staging tables that contain long VARCHAR columns
    (e.g. mlb.play_by_play description fields). fast_executemany=True causes
    pyodbc to pre-calculate buffer sizes from the first row in each batch and
    ignores SQLAlchemy dtype overrides, producing right-truncation errors when
    a later row in the same batch contains a longer string.

    Also required for NVARCHAR(MAX) columns (see grading engine notes).
    """
    engine = create_engine(_build_conn_str(), fast_executemany=False)
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

    dtype (optional): dict mapping column name -> SQLAlchemy type, passed to
    to_sql for staging table creation. Only effective when engine was created
    with fast_executemany=False (use get_engine_slow for wide VARCHAR tables).

    Staging pattern:
      1. Drop temp table if it exists from a previous call in this session.
      2. Create fresh via to_sql with if_exists='append'.
      3. MERGE from staging into destination.
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
