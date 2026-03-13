import os
from sqlalchemy import create_engine

def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    driver   = "ODBC+Driver+18+for+SQL+Server"
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}?driver={driver}"
    )
    return create_engine(conn_str)
