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

def upsert(engine, df, schema, table, keys):
    staging = f"#stage_{table}"
    df.to_sql(staging, engine, index=False, if_exists="replace", chunksize=200)

    set_clause = ", ".join(
        f"t.{c} = s.{c}" for c in df.columns if c not in keys
    )
    key_clause = " AND ".join(f"t.{k} = s.{k}" for k in keys)
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
