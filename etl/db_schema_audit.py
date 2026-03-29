"""
db_schema_audit.py

Writes a full inventory of every schema, table, column, constraint, index,
and row count in the sports-modeling Azure SQL database to CSV files.

Outputs (written to ./audit_output/):
  schemas.csv
  tables.csv          -- includes row count
  columns.csv
  primary_keys.csv
  foreign_keys.csv
  indexes.csv
  sample_data.csv     -- first 3 rows per table

Run via GitHub Actions (db-schema-audit.yml). Download the artifact zip,
unzip, and share the CSVs for schema review.
"""

import os
import csv
import time
from pathlib import Path
from sqlalchemy import create_engine, text

OUT = Path("audit_output")
OUT.mkdir(exist_ok=True)


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
            print("Connected.")
            return engine
        except Exception as exc:
            print(f"Connection attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                time.sleep(45)
    raise RuntimeError("Could not connect after 3 attempts.")


def run(conn, sql):
    return conn.execute(text(sql)).fetchall()


def write_csv(filename, headers, rows):
    path = OUT / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"  Wrote {len(rows)} rows -> {path}")


USER_SCHEMAS_EXCLUDE = (
    "'sys','INFORMATION_SCHEMA','guest','db_owner',"
    "'db_accessadmin','db_securityadmin','db_ddladmin',"
    "'db_backupoperator','db_datareader','db_datawriter',"
    "'db_denydatareader','db_denydatawriter'"
)


def main():
    engine = get_engine()

    with engine.connect() as conn:

        # Schemas
        rows = run(conn, f"""
            SELECT name FROM sys.schemas
            WHERE name NOT IN ({USER_SCHEMAS_EXCLUDE})
            ORDER BY name
        """)
        write_csv("schemas.csv", ["schema_name"], rows)

        # Tables + row counts
        tables = run(conn, f"""
            SELECT
                s.name        AS schema_name,
                t.name        AS table_name,
                SUM(p.rows)   AS row_count
            FROM sys.tables t
            JOIN sys.schemas s    ON t.schema_id = s.schema_id
            JOIN sys.partitions p ON t.object_id = p.object_id
                                 AND p.index_id IN (0,1)
            WHERE s.name NOT IN ({USER_SCHEMAS_EXCLUDE})
            GROUP BY s.name, t.name
            ORDER BY s.name, t.name
        """)
        write_csv("tables.csv", ["schema_name", "table_name", "row_count"], tables)

        # Columns
        columns = run(conn, f"""
            SELECT
                s.name          AS schema_name,
                t.name          AS table_name,
                c.column_id,
                c.name          AS column_name,
                tp.name         AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                dc.definition   AS default_value
            FROM sys.columns c
            JOIN sys.tables t   ON c.object_id = t.object_id
            JOIN sys.schemas s  ON t.schema_id = s.schema_id
            JOIN sys.types tp   ON c.user_type_id = tp.user_type_id
            LEFT JOIN sys.default_constraints dc
                ON dc.parent_object_id = c.object_id
               AND dc.parent_column_id = c.column_id
            WHERE s.name NOT IN ({USER_SCHEMAS_EXCLUDE})
            ORDER BY s.name, t.name, c.column_id
        """)
        write_csv("columns.csv",
            ["schema_name","table_name","column_id","column_name",
             "data_type","max_length","precision","scale",
             "is_nullable","is_identity","default_value"],
            columns)

        # Primary keys
        pks = run(conn, f"""
            SELECT
                s.name  AS schema_name,
                t.name  AS table_name,
                kc.name AS constraint_name,
                STRING_AGG(c.name, ', ')
                    WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_columns
            FROM sys.key_constraints kc
            JOIN sys.tables t         ON kc.parent_object_id = t.object_id
            JOIN sys.schemas s        ON t.schema_id = s.schema_id
            JOIN sys.index_columns ic ON ic.object_id = t.object_id
                                     AND ic.index_id = kc.unique_index_id
            JOIN sys.columns c        ON c.object_id = t.object_id
                                     AND c.column_id = ic.column_id
            WHERE kc.type = 'PK'
              AND s.name NOT IN ({USER_SCHEMAS_EXCLUDE})
            GROUP BY s.name, t.name, kc.name
            ORDER BY s.name, t.name
        """)
        write_csv("primary_keys.csv",
            ["schema_name","table_name","constraint_name","key_columns"],
            pks)

        # Foreign keys
        fks = run(conn, f"""
            SELECT
                s1.name AS from_schema,
                tp.name AS from_table,
                STRING_AGG(fc.name, ', ')
                    WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS from_cols,
                s2.name AS to_schema,
                tr.name AS to_table,
                STRING_AGG(rc.name, ', ')
                    WITHIN GROUP (ORDER BY fkc.constraint_column_id) AS to_cols,
                fk.name AS constraint_name
            FROM sys.foreign_keys fk
            JOIN sys.tables tp   ON fk.parent_object_id = tp.object_id
            JOIN sys.schemas s1  ON tp.schema_id = s1.schema_id
            JOIN sys.tables tr   ON fk.referenced_object_id = tr.object_id
            JOIN sys.schemas s2  ON tr.schema_id = s2.schema_id
            JOIN sys.foreign_key_columns fkc
                ON fk.object_id = fkc.constraint_object_id
            JOIN sys.columns fc  ON fkc.parent_object_id = fc.object_id
                                AND fkc.parent_column_id = fc.column_id
            JOIN sys.columns rc  ON fkc.referenced_object_id = rc.object_id
                                AND fkc.referenced_column_id = rc.column_id
            WHERE s1.name NOT IN ({USER_SCHEMAS_EXCLUDE})
            GROUP BY s1.name, tp.name, s2.name, tr.name, fk.name
            ORDER BY s1.name, tp.name
        """)
        write_csv("foreign_keys.csv",
            ["from_schema","from_table","from_cols",
             "to_schema","to_table","to_cols","constraint_name"],
            fks)

        # Indexes (non-PK)
        indexes = run(conn, f"""
            SELECT
                s.name  AS schema_name,
                t.name  AS table_name,
                i.name  AS index_name,
                i.type_desc,
                i.is_unique,
                STRING_AGG(c.name, ', ')
                    WITHIN GROUP (ORDER BY ic.key_ordinal) AS key_cols
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
              AND s.name NOT IN ({USER_SCHEMAS_EXCLUDE})
            GROUP BY s.name, t.name, i.name, i.type_desc, i.is_unique
            ORDER BY s.name, t.name, i.name
        """)
        write_csv("indexes.csv",
            ["schema_name","table_name","index_name","type_desc","is_unique","key_cols"],
            indexes)

        # Sample data (3 rows per table)
        sample_rows = []
        for schema, table, _ in tables:
            try:
                result = conn.execute(
                    text(f"SELECT TOP 3 * FROM [{schema}].[{table}]")
                )
                col_headers = list(result.keys())
                for row in result.fetchall():
                    sample_rows.append(
                        [schema, table]
                        + [str(v) if v is not None else "" for v in row]
                    )
                # Write a header row per table as a sentinel
                # (handled by adding schema+table as first two cols)
            except Exception as e:
                sample_rows.append([schema, table, f"ERROR: {e}"])

        # Sample data goes into a single flat CSV with schema+table prefix
        path = OUT / "sample_data.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["schema_name", "table_name", "data"])
            for r in sample_rows:
                w.writerow(r)
        print(f"  Wrote {len(sample_rows)} sample rows -> {path}")

    print("\n=== AUDIT COMPLETE ===")
    print(f"Output files in: {OUT.resolve()}")


if __name__ == "__main__":
    main()
