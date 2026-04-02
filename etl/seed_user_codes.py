"""
Seeds common.user_codes with the initial set of funny access codes.
Run via seed-user-codes.yml (workflow_dispatch).

All codes are created active=1, activated=0.
This script is idempotent: existing codes are skipped.

Also runs the DDL migration to add is_demo and demo_date_nba columns
if they do not already exist. Safe to re-run on an existing table.
"""

import os
import pyodbc

CODES = [
    ("SPICY-WALRUS-429",    "Unassigned"),
    ("CHAOS-NOODLE-817",    "Unassigned"),
    ("DRUNK-PENGUIN-203",   "Unassigned"),
    ("SOGGY-PROPHET-651",   "Unassigned"),
    ("FERAL-ACCOUNTANT-94", "Unassigned"),
    ("HAUNTED-SPATULA-382", "Unassigned"),
    ("CURSED-GOBLIN-715",   "Unassigned"),
    ("DISCO-VAMPIRE-540",   "Unassigned"),
    ("ROGUE-BISCUIT-267",   "Unassigned"),
    ("MELTED-WIZARD-893",   "Unassigned"),
    ("CRISPY-ORACLE-148",   "Unassigned"),
    ("SWAMP-SENATOR-976",   "Unassigned"),
    ("FLOPPY-WARLOCK-331",  "Unassigned"),
    ("NACHO-GHOST-584",     "Unassigned"),
    ("GREASY-MYSTIC-720",   "Unassigned"),
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

    # Create table if it doesn't exist (full schema including demo columns).
    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'common' AND table_name = 'user_codes'
        )
        CREATE TABLE common.user_codes (
            code          NVARCHAR(50)  NOT NULL PRIMARY KEY,
            name          NVARCHAR(100) NOT NULL,
            active        BIT           NOT NULL DEFAULT 1,
            activated     BIT           NOT NULL DEFAULT 0,
            activated_at  DATETIME2     NULL,
            last_seen_at  DATETIME2     NULL,
            created_at    DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
            is_demo       BIT           NOT NULL DEFAULT 0,
            demo_date_nba DATE          NULL
        )
    """)

    # Idempotent migrations for tables created before demo columns existed.
    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'common' AND table_name = 'user_codes'
              AND column_name = 'is_demo'
        )
        ALTER TABLE common.user_codes ADD is_demo BIT NOT NULL DEFAULT 0
    """)
    cur.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'common' AND table_name = 'user_codes'
              AND column_name = 'demo_date_nba'
        )
        ALTER TABLE common.user_codes ADD demo_date_nba DATE NULL
    """)

    inserted = 0
    skipped  = 0
    for code, name in CODES:
        cur.execute("SELECT 1 FROM common.user_codes WHERE code = ?", code)
        if cur.fetchone():
            skipped += 1
            continue
        cur.execute(
            "INSERT INTO common.user_codes (code, name, active, activated) VALUES (?, ?, 1, 0)",
            code, name
        )
        inserted += 1

    conn.commit()
    conn.close()
    print(f"Done. Inserted {inserted}, skipped {skipped} existing.")
    print("Columns is_demo and demo_date_nba verified.")

if __name__ == '__main__':
    main()
