"""
seed_user_codes.py

Idempotent seed script for common.user_codes.
Run via seed-user-codes.yml (workflow_dispatch).

The canonical code list is defined in CODES below. Running this script:
  - Creates common.user_codes if it does not exist (full schema).
  - Inserts missing codes.
  - Does NOT modify existing codes (name, mode, max_activations are
    set only on insert). Update those directly in the DB if needed.
"""

import os
import pyodbc

CODES = [
    # (code,              name,          mode,   max_activations)
    ("DRAMA-LLAMA",      "Unassigned",  "demo",  5),
    ("LLAMA-DRAMA",      "Unassigned",  "live",  5),
    ("SWAMP-PUPPY",      "Unassigned",  "demo",  5),
    ("DIRTY-DINGO",      "Unassigned",  "demo",  5),
    ("ERECT-EAGLE",      "Unassigned",  "demo",  5),
    ("FERAL-FINCH",      "Unassigned",  "demo",  5),
    ("GOOFY-GOOSE",      "Unassigned",  "demo",  5),
    ("MOODY-MOOSE",      "Unassigned",  "demo",  5),
    ("ROSSI-DEMO",       "Unassigned",  "demo", 10),
    ("ROSSI-LIVE",       "Unassigned",  "live", 10),
]

def get_conn():
    server   = os.environ['AZURE_SQL_SERVER']
    database = os.environ['AZURE_SQL_DATABASE']
    username = os.environ['AZURE_SQL_USERNAME']
    password = os.environ['AZURE_SQL_PASSWORD']
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={username};PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )
    return pyodbc.connect(conn_str)

def main():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'common' AND table_name = 'user_codes'
        )
        CREATE TABLE common.user_codes (
            code             NVARCHAR(50)  NOT NULL PRIMARY KEY,
            name             NVARCHAR(100) NOT NULL,
            active           BIT           NOT NULL DEFAULT 1,
            activated        BIT           NOT NULL DEFAULT 0,
            activated_at     DATETIME2     NULL,
            last_seen_at     DATETIME2     NULL,
            created_at       DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
            mode             VARCHAR(10)   NOT NULL DEFAULT 'live',
            max_activations  INT           NOT NULL DEFAULT 5
        )
    """)

    inserted = 0
    skipped  = 0
    for code, name, mode, max_act in CODES:
        cur.execute("SELECT 1 FROM common.user_codes WHERE code = ?", code)
        if cur.fetchone():
            skipped += 1
            continue
        cur.execute(
            """
            INSERT INTO common.user_codes
                (code, name, active, activated, mode, max_activations)
            VALUES (?, ?, 1, 0, ?, ?)
            """,
            code, name, mode, max_act
        )
        inserted += 1

    conn.commit()
    conn.close()
    print(f"Done. Inserted {inserted}, skipped {skipped} existing.")

if __name__ == '__main__':
    main()
