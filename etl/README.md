# ETL

Area router for `/etl/`. Python ETL pipelines run in GitHub Actions on the self-hosted runner and write to Azure SQL.

## Per-sport docs

- `/etl/nba/README.md` - STATUS: live
- `/etl/mlb/README.md` - STATUS: in development
- `/etl/nfl/README.md` - STATUS: in development (pipeline runs on schedule; no active work; no product consumer)
- `/etl/_shared/README.md` - cross-sport patterns

## Files

Code files live flat in `/etl/` per `/docs/DECISIONS.md` ADR-0002. Examples: `etl/nba_etl.py`, `etl/mlb_etl.py`, `etl/nfl_etl.py`, `etl/odds_etl.py`, `etl/mlb_play_by_play.py`, `etl/runner.py`. Grading lives separately under `/grading/` (`grade_props.py`). As of 2026-04-24, 30 workflows exist under `.github/workflows/`, all referencing these flat paths.

## Key Concepts

Power Query in Excel was used during design to prototype and validate API behavior; production execution is always Python. Incremental ingestion tracks state against the destination table rather than a separate state store.

## Invariants

- Production ETL runs in Python. Power Query is design/prototype only.
- Code files stay flat in `/etl/`. Doc subfolders are additive only (ADR-0002).
- ETL workflows declare `runs-on: [self-hosted, schnapp-runner]`.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[etl]`.

## Open Questions

None at area level. Sport-specific questions live in the per-sport READMEs.
