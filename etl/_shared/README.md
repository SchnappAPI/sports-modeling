# Shared ETL Patterns

**STATUS:** in development. Patterns are currently extracted from NBA; generalization happens as MLB and NFL come online.

## Purpose

Document reusable ETL patterns that apply across sports: incremental ingestion, Azure SQL retry and cold-start handling, Odds API client behavior, grading pipeline scaffolding, SQL Server quirks that have bitten us more than once.

## Files

No dedicated `_shared` code module exists yet. Patterns are currently duplicated across per-sport scripts (`nba_etl.py`, `mlb_etl.py`). Extraction into a shared module is a future refactor.

`etl/db.py` is the one existing shared helper; it provides the `upsert()` function that every ETL uses.

## Key Concepts

### Incremental ingestion

Track state against the destination table, not a separate state store.

Pattern: list desired keys, `SELECT DISTINCT` the existing keys from the destination, compute the missing set in Python, process the missing partitions oldest-first, upsert. Idempotent across runs.

When a single run loads multiple related tables, check existing keys against the most granular table only. Treat the related tables as all-complete or all-incomplete for that partition.

### Upsert

`etl/db.py:upsert()` stages rows to `#stage_{table}` and runs SQL `MERGE`. Never raw `INSERT` into a destination table.

`MERGE` source rows must be deduplicated before the merge or SQL Server returns error 8672 ("The MERGE statement attempted to UPDATE or DELETE the same row more than once").

### Azure SQL cold start and retry

Tier is Serverless and auto-pauses. First connection after a pause can take 20-60 seconds, sometimes longer. ETL connect logic retries 3 times with 45-second waits. Uptime Robot hitting `/api/ping` every 30 minutes keeps the DB warm during active hours.

### `fast_executemany` caveat

Set `fast_executemany=True` on the SQLAlchemy engine for bulk ETL inserts of uniform rows. It is the correct setting for box scores, odds, lineups, and the like.

It breaks the grading engine. `grade_props.py` writes variable-length JSON-bearing rows, and `fast_executemany=True` truncates NVARCHAR(MAX) fields. The grading engine uses its own engine instance with `fast_executemany=False`. Do not unify them.

### SQL Server specifics to remember

- `minutes` not `min`. `min` is reserved
- `DELETE` not `TRUNCATE`. FK constraints block TRUNCATE
- `BIT` columns require `CAST(col AS INT)` before `SUM()`
- `MERGE` statements end with a semicolon. Omitting it is a parse error
- Every fact table has `created_at DATETIME2 DEFAULT GETUTCDATE()`

### One-off DB queries via MCP

Preferred pattern for ad-hoc Python-driven DB work from the Schnapp Ops MCP:

1. Write a Python script to `/tmp/script.py`
2. Execute with `/home/schnapp-admin/venv/bin/python /tmp/script.py`
3. Use pyodbc with the ODBC Driver 18 connection string
4. Open a separate `with engine.connect()` block for each query. A single block containing multiple queries can hit "connection busy" errors against Azure SQL Serverless

Output writes to `/tmp/<something>.txt` and is read back with `shell_exec cat`.

### Odds API client (shared across sports)

- FanDuel only, globally. `bookmakers=fanduel` on every call
- Bulk endpoint for discovery and probe; per-event endpoint when link data is needed (`includeLinks=true` is per-event only)
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Dispatches on the `sport` argument: `basketball_nba`, `baseball_mlb`, etc.
- Missing feed cells are a coverage reality of the Odds API relative to FanDuel's native app, not a bug

## Invariants

- Destination table is the state table. No parallel state store
- `fast_executemany=True` for bulk ETL, `False` for the grading engine. Never unified
- `BIT` columns cast to `INT` before `SUM()`
- `MERGE` source deduplicated before the merge; trailing semicolon required
- Bookmaker is FanDuel only across all sports
- Azure SQL connect retries: 3 attempts, 45-second wait

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[shared][etl]`.

## Open Questions

- When to extract shared patterns into an `etl/_shared.py` module vs. continuing with per-sport duplication. Likely once MLB web goes live and a second grading caller exists
- Whether to generalize the grading pipeline's engine-selection logic into the shared module ahead of MLB grading
