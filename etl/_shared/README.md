# Shared ETL Patterns

**STATUS:** in development. Patterns are currently extracted from NBA; generalization happens as MLB and NFL come online.

## Purpose

Document reusable ETL patterns that apply across sports: incremental ingestion, Azure SQL retry and cold-start handling, Odds API client behavior, grading pipeline scaffolding, SQL Server quirks that have bitten us more than once.

## Files

No dedicated `_shared` code module exists yet. Patterns are currently duplicated across per-sport scripts (`nba_etl.py`, `mlb_etl.py`). Extraction into a shared module is a future refactor.

`etl/db.py` is the one existing shared helper. It provides:

- `get_engine(max_retries=3, retry_wait=45)` - SQLAlchemy engine with `fast_executemany=True`. Default for normal upserts
- `get_engine_slow(max_retries=3, retry_wait=45)` - same with `fast_executemany=False`. Used when staging tables contain long VARCHAR columns (for example MLB PBP description fields) where the fast-path pre-sizes the buffer from the first row and truncates later rows
- `upsert(engine, df, schema, table, keys, dtype=None)` - stages a DataFrame to `#stage_{table}` via `df.to_sql`, then runs `MERGE` from staging to destination

Grading has its own engine in `grading/grade_props.py:get_engine(max_retries=3, retry_wait=60)` with `fast_executemany=False`. It is separate from `etl/db.py` by design; see below.

## Key Concepts

### Incremental ingestion

Track state against the destination table, not a separate state store.

Pattern: list desired keys, `SELECT DISTINCT` the existing keys from the destination, compute the missing set in Python, process the missing partitions oldest-first, upsert. Idempotent across runs.

When a single run loads multiple related tables, check existing keys against the most granular table only. Treat the related tables as all-complete or all-incomplete for that partition.

### Upsert

`etl/db.py:upsert()` stages rows to `#stage_{table}` via `df.to_sql` and runs SQL `MERGE`. Never raw `INSERT` into a destination table.

`MERGE` source rows must be deduplicated before the merge or SQL Server returns error 8672 ("The MERGE statement attempted to UPDATE or DELETE the same row more than once").

### Three engine variants

The project uses three distinct engine configurations. They are not interchangeable.

| Caller | `fast_executemany` | Retry wait | Reason |
|--------|--------------------|-----------|--------|
| `etl/db.py:get_engine` | `True` | 45s | Default for bulk upserts of uniform-width rows |
| `etl/db.py:get_engine_slow` | `False` | 45s | Required for long VARCHAR staging tables. `fast_executemany=True` pre-sizes the buffer from row 1 and right-truncates later rows with longer strings |
| `grading/grade_props.py:get_engine` | `False` | 60s | NVARCHAR(MAX) safety for grading output and 60s wait tuned for serverless cold start from grading's call pattern |

Do not unify these. Each has been observed to fail in ways that its specific configuration fixes.

### Azure SQL cold start

Tier is Serverless and auto-pauses after 60 minutes of idle. First connection after a pause can take 20-60 seconds, sometimes longer. All three `get_engine` variants above retry 3 times. Uptime Robot previously pinged `/api/ping` every 30 minutes to keep the DB warm, but was paused 2026-04-23 to allow auto-pause billing savings; ETL runs now tolerate cold start via the retry logic. Web routes that query the DB must degrade gracefully on cold start — a cold start exceeds the 15-second SWA function timeout.

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
- `fast_executemany=True` for most bulk ETL, `False` for long-VARCHAR staging and for grading. The three engine variants in `etl/db.py` and `grading/grade_props.py` stay separate
- `BIT` columns cast to `INT` before `SUM()`
- `MERGE` source deduplicated before the merge; trailing semicolon required
- Bookmaker is FanDuel only across all sports
- Azure SQL connect retries: 3 attempts, 45-60 second wait depending on caller

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[shared][etl]`.

## Open Questions

- When to extract shared patterns into an `etl/_shared.py` module vs. continuing with per-sport duplication. Likely once MLB web goes live and a second grading caller exists
- Whether to generalize the grading pipeline's engine-selection logic into the shared module ahead of MLB grading
