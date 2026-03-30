"""
Seeds common.user_codes with the initial set of funny access codes.
Run once via db_inventory.yml (swap db_inventory.py temporarily) or
add a one-off GitHub Actions workflow.

All codes are created active=1, activated=0.
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

    # Create table if it doesn't exist
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
            created_at    DATETIME2     NOT NULL DEFAULT GETUTCDATE()
        )
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

if __name__ == '__main__':
    main()
