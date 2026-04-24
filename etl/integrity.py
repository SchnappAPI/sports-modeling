# etl/integrity.py
"""
Data integrity and completeness framework.

See /docs/DECISIONS.md ADR-20260424-2 for the full design. This module
implements Layer 1 (invariants enforced at write time), the state tables
for Layer 2 (mapping resolver) and Layer 3 (daily retry), and helpers
consumed by the daily health report workflow.

Three layers, each addressing a distinct failure mode:

1. Layer 1 — per-row column-level invariants. Rows violating CRITICAL_FIELDS
   rules are written to `common.ingest_quarantine` instead of the destination
   table. Opt-in: tables not listed in CRITICAL_FIELDS pass through unchanged.

2. Layer 2 — entity mapping gaps tracked in `common.unmapped_entities`.
   Populated by ETL scripts when a source-feed entity (e.g., an odds-feed
   player name) cannot be resolved to a canonical id. Nightly resolver
   workflow attempts deterministic matches; escalates to a GitHub Issue
   after 3 attempts.

3. Layer 3 — daily retry state in `common.data_completeness_log`. Populated
   by the retroactive scan and by quarantine entries. The retry workflow
   reattempts the missing data once per day, max 3 attempts, then escalates.

Usage in an ETL script:

    from etl.integrity import validate_and_filter

    valid_rows = validate_and_filter(rows, 'nba.schedule', engine,
                                     source_workflow='nba-etl.yml')
    upsert(engine, pd.DataFrame(valid_rows), 'nba', 'schedule', keys=['game_id'])

HEALTH.md generation and retroactive scans live in separate functions
consumed by `daily-health-report.yml`.

Invariants documented in ADR-20260424-2:
- Stat-zero is NOT stat-null. PTS=0 is a valid value; PTS=NULL is a violation.
- "Did this player play?" is answered by MIN > 0 or starter_status != 'Inactive',
  never by stat-zero inference.
- Retroactive scan flags existing violations but does NOT move production rows.
- Successful Layer-1 validation clears prior quarantine + log entries for the
  same (table, row_key), regardless of which resolution path fixed the row.
"""

import json
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import bindparam, text


# =====================================================================
# CRITICAL_FIELDS catalog
# =====================================================================
# Populated during the cataloging pass (Phase 2 of Initiative A).
# Structure per table:
#
#   'schema.table': {
#       'row_key': [pk_col, ...],           # columns that form the row identifier
#       'always_required': [col, ...],      # must never be NULL
#       'required_when': {
#           col: {
#               'py_predicate': callable(row) -> bool,   # write-time enforcement
#               'sql_predicate': 'SQL fragment using row columns',  # scan-time
#               'description': 'Human-readable reason',
#           },
#           ...
#       },
#   }
#
# Keep empty until Phase 2 so no existing ETL is affected by the module
# landing. Opt-in enforcement means tables not listed here pass through.
CRITICAL_FIELDS: Dict[str, Dict[str, Any]] = {}


# =====================================================================
# RELATIONAL_CHECKS catalog
# =====================================================================
# Cross-table row-count alignment queries, scanned daily by the health
# report workflow. Distinct from Layer 1 because these are row-count
# invariants across tables, not column-level invariants within a row.
#
# Structure:
#   'check_name': {
#       'description': 'Human-readable',
#       'query': 'SELECT ... returning violating rows (COUNT used in report)',
#       'severity': 'warn' | 'error',
#   }
#
# Example (populated in Phase 2):
#   'nba_lineup_coverage': {
#       'description': 'Every started game should have >= 20 daily_lineups rows.',
#       'query': '''
#           SELECT s.game_id, COUNT(dl.player_id) AS lineup_rows
#           FROM nba.schedule s
#           LEFT JOIN nba.daily_lineups dl ON dl.game_id = s.game_id
#           WHERE s.game_status >= 1 AND s.game_date >= DATEADD(day, -30, GETUTCDATE())
#           GROUP BY s.game_id
#           HAVING COUNT(dl.player_id) < 20
#       ''',
#       'severity': 'warn',
#   }
RELATIONAL_CHECKS: Dict[str, Dict[str, Any]] = {}


# =====================================================================
# DDL for the three framework tables
# =====================================================================
# Single idempotent script. Safe to run repeatedly; CREATE IF NOT EXISTS
# semantics via sys.objects checks. Indexes are filtered on the
# resolved_at IS NULL partition because that is the hot path for every
# workflow that reads these tables.

DDL_STATEMENTS: List[str] = [
    # ----- common.ingest_quarantine -----
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID('common.ingest_quarantine') AND type = 'U')
    BEGIN
        CREATE TABLE common.ingest_quarantine (
            quarantine_id      BIGINT IDENTITY PRIMARY KEY,
            table_name         VARCHAR(100)    NOT NULL,
            row_key            NVARCHAR(500)   NOT NULL,
            row_payload        NVARCHAR(MAX)   NOT NULL,
            failed_invariant   VARCHAR(200)    NOT NULL,
            source_workflow    VARCHAR(100)    NULL,
            first_seen_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            last_retry_at      DATETIME2       NULL,
            retry_count        INT             NOT NULL DEFAULT 0,
            resolved_at        DATETIME2       NULL,
            resolution_notes   NVARCHAR(MAX)   NULL
        );
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_quarantine_table_key_open'
                     AND object_id = OBJECT_ID('common.ingest_quarantine'))
    BEGIN
        CREATE INDEX IX_quarantine_table_key_open
            ON common.ingest_quarantine(table_name, row_key)
            WHERE resolved_at IS NULL;
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_quarantine_retry_ready'
                     AND object_id = OBJECT_ID('common.ingest_quarantine'))
    BEGIN
        CREATE INDEX IX_quarantine_retry_ready
            ON common.ingest_quarantine(last_retry_at, retry_count)
            WHERE resolved_at IS NULL;
    END
    """,
    # ----- common.unmapped_entities -----
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID('common.unmapped_entities') AND type = 'U')
    BEGIN
        CREATE TABLE common.unmapped_entities (
            unmapped_id           BIGINT IDENTITY PRIMARY KEY,
            source_feed           VARCHAR(100)    NOT NULL,
            entity_type           VARCHAR(50)     NOT NULL,
            source_key            NVARCHAR(500)   NOT NULL,
            source_context        NVARCHAR(MAX)   NULL,
            first_seen_at         DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            last_seen_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            seen_count            INT             NOT NULL DEFAULT 1,
            candidate_match       NVARCHAR(500)   NULL,
            candidate_method      VARCHAR(50)     NULL,
            candidate_confidence  FLOAT           NULL,
            resolved_mapping      NVARCHAR(500)   NULL,
            resolved_at           DATETIME2       NULL,
            resolution_notes      NVARCHAR(MAX)   NULL,
            retry_count           INT             NOT NULL DEFAULT 0,
            CONSTRAINT UQ_unmapped_entities UNIQUE (source_feed, entity_type, source_key)
        );
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_unmapped_unresolved'
                     AND object_id = OBJECT_ID('common.unmapped_entities'))
    BEGIN
        CREATE INDEX IX_unmapped_unresolved
            ON common.unmapped_entities(retry_count, last_seen_at)
            WHERE resolved_at IS NULL;
    END
    """,
    # ----- common.data_completeness_log -----
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID('common.data_completeness_log') AND type = 'U')
    BEGIN
        CREATE TABLE common.data_completeness_log (
            log_id                  BIGINT IDENTITY PRIMARY KEY,
            table_name              VARCHAR(100)    NOT NULL,
            row_key                 NVARCHAR(500)   NOT NULL,
            column_name             VARCHAR(100)    NOT NULL,
            first_detected_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            last_attempt_at         DATETIME2       NULL,
            attempt_count           INT             NOT NULL DEFAULT 0,
            resolved_at             DATETIME2       NULL,
            detected_retroactively  BIT             NOT NULL DEFAULT 0,
            notes                   NVARCHAR(MAX)   NULL,
            CONSTRAINT UQ_completeness_log UNIQUE (table_name, row_key, column_name)
        );
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_completeness_retry_ready'
                     AND object_id = OBJECT_ID('common.data_completeness_log'))
    BEGIN
        CREATE INDEX IX_completeness_retry_ready
            ON common.data_completeness_log(last_attempt_at, attempt_count)
            WHERE resolved_at IS NULL;
    END
    """,
]


def ensure_tables(engine) -> None:
    """Create the three framework tables and their indexes if missing. Idempotent."""
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))


# =====================================================================
# Write-time validation (Layer 1)
# =====================================================================

def _row_key_str(row: Dict[str, Any], key_cols: List[str]) -> str:
    """Build a stable string representation of a row key for logging/indexing."""
    return "|".join(f"{c}={row.get(c)}" for c in key_cols)


def validate_row(table_name: str, row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Check a row against CRITICAL_FIELDS for its table.

    Returns (is_valid, failed_invariant). failed_invariant is None if valid.
    Tables not listed in CRITICAL_FIELDS always pass (opt-in enforcement).
    """
    rules = CRITICAL_FIELDS.get(table_name)
    if not rules:
        return True, None

    for col in rules.get("always_required", []):
        if row.get(col) is None:
            return False, f"always_required:{col}"

    for col, spec in rules.get("required_when", {}).items():
        pred = spec.get("py_predicate")
        if not callable(pred):
            continue
        try:
            should_be_present = bool(pred(row))
        except Exception:
            should_be_present = False
        if should_be_present and row.get(col) is None:
            return False, f"required_when:{col}:{spec.get('description', 'predicate_true')}"

    return True, None


def write_quarantine(
    engine,
    table_name: str,
    row: Dict[str, Any],
    failed_invariant: str,
    source_workflow: Optional[str] = None,
) -> None:
    """Record a single failed row in common.ingest_quarantine."""
    rules = CRITICAL_FIELDS.get(table_name, {})
    key_cols = rules.get("row_key", [])
    row_key = _row_key_str(row, key_cols) if key_cols else (
        json.dumps({k: str(v) for k, v in row.items() if v is not None}, default=str)[:500]
    )
    payload = json.dumps(row, default=str)
    sql = text("""
        INSERT INTO common.ingest_quarantine
            (table_name, row_key, row_payload, failed_invariant, source_workflow)
        VALUES (:t, :k, :p, :i, :w)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"t": table_name, "k": row_key, "p": payload,
                           "i": failed_invariant, "w": source_workflow})


def validate_and_filter(
    rows: Iterable[Dict[str, Any]],
    table_name: str,
    engine,
    source_workflow: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Main ETL integration point. Pass rows through invariant checks before upsert.

    Valid rows are returned. Invalid rows are quarantined and NOT returned.
    Any previously-open quarantine or completeness-log entries for a
    newly-valid row_key are cleared and Issues matching the row_key close
    (workflow-side responsibility to watch the cleared state).

    Tables not in CRITICAL_FIELDS pass through unchanged (opt-in). For
    those tables, no DB round trip is incurred.
    """
    rules = CRITICAL_FIELDS.get(table_name)
    if not rules:
        return list(rows)

    key_cols = rules.get("row_key", [])
    rows_list = list(rows)

    # Pre-fetch open row_keys for this table so per-row "was this previously
    # quarantined?" checks become O(1) set membership rather than DB round trips.
    open_keys: set = set()
    if key_cols:
        with engine.connect() as conn:
            q_result = conn.execute(
                text("SELECT DISTINCT row_key FROM common.ingest_quarantine "
                     "WHERE table_name = :t AND resolved_at IS NULL"),
                {"t": table_name},
            )
            open_keys.update(r[0] for r in q_result)
            l_result = conn.execute(
                text("SELECT DISTINCT row_key FROM common.data_completeness_log "
                     "WHERE table_name = :t AND resolved_at IS NULL"),
                {"t": table_name},
            )
            open_keys.update(r[0] for r in l_result)

    valid: List[Dict[str, Any]] = []
    keys_to_clear: List[str] = []
    for row in rows_list:
        ok, failed = validate_row(table_name, row)
        if ok:
            if key_cols:
                rk = _row_key_str(row, key_cols)
                if rk in open_keys:
                    keys_to_clear.append(rk)
            valid.append(row)
        else:
            write_quarantine(engine, table_name, row, failed, source_workflow)

    # Batch-clear open entries for newly-valid rows. One UPDATE per table.
    if keys_to_clear:
        clear_sql_q = text("""
            UPDATE common.ingest_quarantine
            SET resolved_at = GETUTCDATE(),
                resolution_notes = COALESCE(resolution_notes, '')
                                   + ' | cleared by successful re-validation'
            WHERE table_name = :t AND row_key IN :keys AND resolved_at IS NULL
        """).bindparams(bindparam("keys", expanding=True))
        clear_sql_l = text("""
            UPDATE common.data_completeness_log
            SET resolved_at = GETUTCDATE(),
                notes = COALESCE(notes, '')
                        + ' | cleared by successful re-validation'
            WHERE table_name = :t AND row_key IN :keys AND resolved_at IS NULL
        """).bindparams(bindparam("keys", expanding=True))
        with engine.begin() as conn:
            conn.execute(clear_sql_q, {"t": table_name, "keys": list(set(keys_to_clear))})
            conn.execute(clear_sql_l, {"t": table_name, "keys": list(set(keys_to_clear))})

    return valid


# =====================================================================
# Retroactive scan (Layer 3 seed)
# =====================================================================

def retroactive_scan(engine) -> Dict[str, int]:
    """
    For each table in CRITICAL_FIELDS, detect existing production rows that
    violate always_required invariants. Write one row per (table, row_key,
    column) violation to common.data_completeness_log with
    detected_retroactively = 1.

    Does NOT move existing production rows out to quarantine — retroactive
    movement is unsafe. The scan is visibility-only.

    Currently scans only always_required rules. required_when scan-time
    support is added per-table during the cataloging phase via sql_predicate
    expressions (see CRITICAL_FIELDS structure comment).

    Returns dict of {table_name: total_violations_detected}.
    """
    results: Dict[str, int] = {}
    with engine.begin() as conn:
        for table_name, rules in CRITICAL_FIELDS.items():
            always_required = rules.get("always_required", [])
            key_cols = rules.get("row_key", [])
            if not always_required or not key_cols:
                results[table_name] = 0
                continue

            table_count = 0
            key_expr = " + '|' + ".join(
                f"'{k}=' + CAST(t.[{k}] AS NVARCHAR(100))" for k in key_cols
            )
            for col in always_required:
                insert_sql = text(f"""
                    INSERT INTO common.data_completeness_log
                        (table_name, row_key, column_name, detected_retroactively, notes)
                    SELECT :tbl, {key_expr}, :col, 1, 'Retroactive scan'
                    FROM {table_name} t
                    WHERE t.[{col}] IS NULL
                    AND NOT EXISTS (
                        SELECT 1 FROM common.data_completeness_log dcl
                        WHERE dcl.table_name = :tbl
                          AND dcl.row_key = {key_expr}
                          AND dcl.column_name = :col
                    )
                """)
                result = conn.execute(insert_sql, {"tbl": table_name, "col": col})
                table_count += result.rowcount or 0
            results[table_name] = table_count
    return results


# =====================================================================
# Entity-mapping helpers (Layer 2)
# =====================================================================

def record_unmapped_entity(
    engine,
    source_feed: str,
    entity_type: str,
    source_key: str,
    source_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Record a source-feed entity that could not be resolved to a canonical id.

    Idempotent via UQ_unmapped_entities: repeat calls bump seen_count and
    refresh last_seen_at. Separate resolver workflow handles match attempts
    and issue escalation.
    """
    context_json = json.dumps(source_context, default=str) if source_context else None
    sql = text("""
        MERGE common.unmapped_entities AS t
        USING (SELECT :feed AS source_feed, :etype AS entity_type, :key AS source_key) AS s
        ON t.source_feed = s.source_feed
           AND t.entity_type = s.entity_type
           AND t.source_key = s.source_key
        WHEN MATCHED AND t.resolved_at IS NULL THEN
            UPDATE SET last_seen_at = GETUTCDATE(),
                       seen_count = t.seen_count + 1,
                       source_context = COALESCE(:ctx, t.source_context)
        WHEN NOT MATCHED THEN
            INSERT (source_feed, entity_type, source_key, source_context)
            VALUES (:feed, :etype, :key, :ctx);
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"feed": source_feed, "etype": entity_type,
                           "key": source_key, "ctx": context_json})
