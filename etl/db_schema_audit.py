"""
db_schema_audit.py

Prints a full inventory of every schema, table, column, constraint, index,
and row count in the sports-modeling Azure SQL database.

Run via GitHub Actions (db-schema-audit.yml) and paste the output back
into the conversation for schema review.
"""

import os
import time
from sqlalchemy import create_engine, text

def get_engine():
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    )
    engine = create_engine(conn_str, fast_executemany=True)
    for attempt in range(1, 4):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return engine
        except Exception as exc:
            print(f"Connection attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                time.sleep(45)
    raise RuntimeError("Could not connect after 3 attempts.")


def run(conn, sql, params=None):
    return conn.execute(text(sql), params or {}).fetchall()


def main():
    engine = get_engine()
    print("Connected.\n")

    with engine.connect() as conn:

        # ── Schemas ──────────────────────────────────────────────────────────
        schemas = [r[0] for r in run(conn, """
            SELECT name FROM sys.schemas
            WHERE name NOT IN (
                'sys','INFORMATION_SCHEMA','guest','db_owner',
                'db_accessadmin','db_securityadmin','db_ddladmin',
                'db_backupoperator','db_datareader','db_datawriter',
                'db_denydatareader','db_denydatawriter'
            )
            ORDER BY name
        """)]
        print("=== SCHEMAS ===")
        for s in schemas:
            print(f"  {s}")
        print()

        # ── Tables with row counts ────────────────────────────────────────────
        print("=== TABLES & ROW COUNTS ===")
        tables = run(conn, """
            SELECT
                s.name        AS schema_name,
                t.name        AS table_name,
                SUM(p.rows)   AS row_count
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
            WHERE s.name NOT IN (
                'sys','INFORMATION_SCHEMA','guest','db_owner',
                'db_accessadmin','db_securityadmin','db_ddladmin',
                'db_backupoperator','db_datareader','db_datawriter',
                'db_denydatareader','db_denydatawriter'
            )
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name
        """)
        for schema, table, rows in tables:
            print(f"  {schema}.{table}  ({rows:,} rows)")
        print()

        # ── Columns ──────────────────────────────────────────────────────────
        print("=== COLUMNS ===")
        columns = run(conn, """
            SELECT
                s.name                          AS schema_name,
                t.name                          AS table_name,
                c.column_id,
                c.name                          AS column_name,
                tp.name                         AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                dc.definition                   AS default_value
            FROM sys.columns c
            JOIN sys.tables t   ON c.object_id = t.object_id
            JOIN sys.schemas s  ON t.schema_id = s.schema_id
            JOIN sys.types tp   ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.default_constraints dc
                ON dc.parent_object_id = c.object_id
                AND dc.parent_column_id = c.column_id
            WHERE s.name NOT IN (
                'sys','INFORMATION_SCHEMA','guest','db_owner',
                'db_accessadmin','db_securityadmin','db_ddladmin',
                'db_backupoperator','db_datareader','db_datawriter',
                'db_denydatareader','db_denydatawriter'
            )
            ORDER BY s.name, t.name, c.column_id
        """)

        current = None
        for schema, table, col_id, col_name, dtype, maxlen, prec, scale, nullable, identity, default in columns:
            key = f"{schema}.{table}"
            if key != current:
                print(f"\n  [{key}]")
                current = key
            null_str     = "NULL" if nullable else "NOT NULL"
            identity_str = " IDENTITY" if identity else ""
            default_str  = f" DEFAULT {default}" if default else ""
            if dtype in ("nvarchar", "varchar", "char", "nchar"):
                size = "MAX" if maxlen == -1 else str(maxlen if dtype.startswith("n") else maxlen)
                type_str = f"{dtype}({size})"
            elif dtype in ("decimal", "numeric"):
                type_str = f"{dtype}({prec},{scale})"
            else:
                type_str = dtype
            print(f"    {col_id:>3}. {col_name:<40} {type_str:<25} {null_str}{identity_str}{default_str}")
        print()

        # ── Primary Keys ─────────────────────────────────────────────────────
        print("=== PRIMARY KEYS ===")
        pks = run(conn, """
            SELECT
                s.name  AS schema_name,
                t.name  AS table_name,
                kc.name AS constraint_name,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
            FROM sys.key_constraints kc
            JOIN sys.tables t          ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s         ON t.schema_id = s.schema_id
            JOIN sys.index_columns ic  ON ic.object_id = t.object_id
                                      AND ic.index_id = kc.unique_index_id
            JOIN sys.columns c         ON c.object_id = t.object_id
                                      AND c.column_id = ic.column_id
            WHERE kc.type = 'PK'
            GROUP BY s.name, t.name, kc.name
            ORDER BY s.name, t.name
        """)
        for schema, table, name, cols in pks:
            print(f"  {schema}.{table}  PK ({cols})  [{name}]")
        print()

        # ── Foreign Keys ─────────────────────────────────────────────────────
        print("=== FOREIGN KEYS ===")
        fks = run(conn, """
            SELECT
                s1.name AS from_schema,
                tp.name AS from_table,
                STRING_AGG(fc.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS from_cols,
                s2.name AS to_schema,
                tr.name AS to_table,
                STRING_AGG(rc.name, ', ') WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS to_cols,
                fk.name AS constraint_name
            FROM sys.foreign_keys fk
            JOIN sys.tables tp      ON fk.parent_object_id = tp.object_id
            JOIN sys.schemas s1     ON tp.schema_id = s1.schema_id
            JOIN sys.tables tr      ON fk.referenced_object_id = tr.object_id
            JOIN sys.schemas s2     ON tr.schema_id = s2.schema_id
            JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns fc     ON fkc.parent_object_id = fc.object_id
                                    AND fkc.parent_column_id = fc.column_id
            JOIN sys.columns rc     ON fkc.referenced_object_id = rc.object_id
                                    AND fkc.referenced_column_id = rc.column_id
            GROUP BY s1.name, tp.name, s2.name, tr.name, fk.name
            ORDER BY s1.name, tp.name
        """)
        for from_s, from_t, from_c, to_s, to_t, to_c, name in fks:
            print(f"  {from_s}.{from_t}({from_c}) -> {to_s}.{to_t}({to_c})  [{name}]")
        print()

        # ── Indexes ───────────────────────────────────────────────────────────
        print("=== INDEXES (non-PK) ===")
        indexes = run(conn, """
            SELECT
                s.name  AS schema_name,
                t.name  AS table_name,
                i.name  AS index_name,
                i.type_desc,
                i.is_unique,
                STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_cols
            FROM sys.indexes i
            JOIN sys.tables t         ON i.object_id = t.object_id
            JOIN sys.schemas s        ON t.schema_id = s.schema_id
            JOIN sys.index_columns ic ON i.object_id = ic.object_id
                                     AND i.index_id = ic.index_id
                                     AND ic.is_included_column = 0
            JOIN sys.columns c        ON ic.object_id = c.object_id
                                     AND ic.column_id = c.column_id
            WHERE i.is_primary_key = 0
              AND i.type > 0
            GROUP BY s.name, t.name, i.name, i.type_desc, i.is_unique
            ORDER BY s.name, t.name, i.name
        """)
        for schema, table, idx, itype, unique, cols in indexes:
            u = "UNIQUE " if unique else ""
            print(f"  {schema}.{table}  {u}{itype} ({cols})  [{idx}]")
        print()

        # ── Sample data (first 3 rows per table) ─────────────────────────────
        print("=== SAMPLE DATA (3 rows per table) ===")
        for schema, table, _ in tables:
            try:
                rows = run(conn, f"SELECT TOP 3 * FROM [{schema}].[{table}]")
                col_names = [col_name for col_name, *_ in columns if f"{schema}.{table}" in f"{schema}.{table}"]
                print(f"\n  [{schema}.{table}]")
                if rows:
                    # Get column names from a fresh query
                    result = conn.execute(text(f"SELECT TOP 3 * FROM [{schema}].[{table}]"))
                    col_headers = list(result.keys())
                    print("    " + " | ".join(f"{h[:20]:<20}" for h in col_headers))
                    print("    " + "-" * min(len(col_headers) * 23, 120))
                    for row in result.fetchall():
                        vals = [str(v)[:20] if v is not None else "NULL" for v in row]
                        print("    " + " | ".join(f"{v:<20}" for v in vals))
                else:
                    print("    (empty)")
            except Exception as e:
                print(f"  [{schema}.{table}]  ERROR: {e}")

    print("\n=== AUDIT COMPLETE ===")


if __name__ == "__main__":
    main()
