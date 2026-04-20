# Shared ETL Patterns

**STATUS:** in development. Patterns are currently extracted from NBA; generalization happens as MLB and NFL come online.

## Purpose

Document reusable ETL patterns that apply across sports: incremental ingestion, Azure SQL retry and cold-start handling, Odds API client behavior, grading pipeline scaffolding.

## Files

No dedicated `_shared` code module exists yet. Patterns are currently duplicated across per-sport scripts (`nba_etl.py`, `mlb_etl.py`). Extraction into a shared module is a future refactor.

## Key Concepts

- **Incremental ingestion**: query the destination table for existing keys, compute the delta in Python, call the API only for missing partitions. No separate state store.
- **Azure SQL cold start**: first connection after auto-pause can take 20 to 60 seconds. Connect logic retries 3 times with 45-second waits.
- `fast_executemany=True` is correct for bulk ETL inserts of uniform rows. It breaks the grading engine which writes variable-length JSON. The grading engine uses its own engine instance without this flag.

## Invariants

- Destination table is the state table. No parallel state store.
- SQL Server `bit` columns require `CAST(... AS INT)` before `SUM()`.
- `MERGE` source rows must be deduplicated before the MERGE to avoid error 8672.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[shared][etl]`.

## Open Questions

When to extract shared patterns into an `etl/_shared.py` module vs. continuing with per-sport duplication.
