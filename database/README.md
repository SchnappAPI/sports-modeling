# Database

Area router for the Azure SQL schema. Database: `sports-modeling` on `sports-modeling-server.database.windows.net` (General Purpose Serverless, auto-pauses). Connection details in `/docs/CONNECTIONS.md`.

## Schemas

- `nba` - NBA tables. STATUS: live. See `/database/nba/README.md`.
- `mlb` - MLB tables. STATUS: design phase. See `/database/mlb/README.md`.
- `nfl` - NFL tables. STATUS: planning. See `/database/nfl/README.md`.
- `odds` - cross-sport odds tables. See `/database/_shared/README.md`.
- `common` - cross-sport utility tables (user codes, demo config, teams, patterns). See `/database/_shared/README.md`.

## Files

DDL currently lives inside Python ETL migration scripts under `/etl/` (for example, table-create logic inside `nba_etl.py` and `mlb_etl.py`, plus `db_inventory.py` which lists schemas and tables). Dedicated `.sql` files per schema may be added during Step 4 of the documentation restructure; until then the Python ETL is the source of truth for DDL.

## Key Concepts

Azure SQL Serverless auto-pauses after inactivity. First connection can take 20 to 60 seconds. Uptime Robot pings `/api/ping` every 30 minutes to keep the database warm. ETL scripts retry 3 times with 45-second waits to absorb cold-start latency.

Naming: schemas are lowercase (`nba`, `mlb`). Table and column names are snake_case. Primary keys are usually surrogate integers with a unique constraint on business keys.

## Invariants

- One database, five schemas.
- Schemas match sport names. Cross-sport data lives in `common` or `odds`.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[database]`.

## Open Questions

Whether to introduce dedicated `.sql` DDL files per schema alongside the Python ETL, or continue with DDL-in-Python.
