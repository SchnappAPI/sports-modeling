# Shared Database Schemas

**STATUS:** live.

## Purpose

Documents the `common` and `odds` schemas, which serve cross-sport tables.

## Files

DDL is defined inside the ETL scripts that create or alter each table. No standalone SQL files yet.

## Key Concepts

### `common.*` tables

- `common.user_codes` - passcode gate (live and demo codes)
- `common.demo_config` - demo mode configuration (fixed historical date per sport)
- `common.user_activations` - activation count per code
- `common.teams` - cross-sport team reference
- `common.player_line_patterns` - NBA lag-1 transition probabilities (per player, per line). Currently NBA-specific despite the generic name.
- `common.daily_grades` - daily prop grades (reads `outcome_name`, `over_price`, composite grade, component grades). Cross-sport schema.
- `common.player_tier_lines` - NBA KDE tier lines (Safe/Value/HighRisk/Lotto) per player-market-game. Added 2026-04-23, ADR-20260423-1. Currently NBA-specific despite the generic name. Written by `grade_props.py:upsert_tier_lines` in lockstep with `common.daily_grades`.

Data-integrity framework tables (added 2026-04-24, ADR-20260424-2). All three are meta tables that track data quality; they do not store product data themselves:

- `common.ingest_quarantine` - Rows that violated Layer-1 invariants at write time. Full row payload as JSON, plus the failed invariant. Partial index on `(table_name, row_key) WHERE resolved_at IS NULL` covers the open-row hot path.
- `common.unmapped_entities` - Source-feed entities that could not be resolved to a canonical id (Layer 2). UQ on `(source_feed, entity_type, source_key)` for idempotent writes. Nightly resolver workflow (pending) auto-resolves via exact match + last-name/first-initial + normalized string distance; escalates to GitHub Issue after 3 attempts.
- `common.data_completeness_log` - Per-column NULL violations detected by the retroactive scan or by the daily retry workflow. UQ on `(table_name, row_key, column_name)`. `detected_retroactively = 1` for scan-sourced rows (historical, never moved out of production); `= 0` for rows quarantined during normal writes.

Schema evolution for the three meta tables is owned by `etl/integrity.py` (the `DDL_STATEMENTS` module constant + `ensure_tables()`). Do not hand-write DDL for them.

### `odds.*` tables

- `odds.upcoming_events` - event_id, home_tricode, away_tricode, commence_time, game_id
- `odds.upcoming_player_props` - event_id, market_key, outcome_point, player_name, bookmaker_key, snap_ts, outcome_price, outcome_name, link

## Invariants

- `common.teams` is cross-sport. Per-sport team attributes live in `<sport>.teams` if needed.
- `odds.upcoming_player_props.link` is populated only from the Odds API per-event endpoint (not the bulk endpoint).
- `common.daily_grades.outcome_name` is part of the UNIQUE key. Grading writes both Over and Under rows.
- `common.player_tier_lines` UNIQUE key is `(grade_date, game_id, player_id, market_key)`. Over-side only. Written in lockstep with `common.daily_grades` inside `grade_props.py` — never skip one and write the other.
- Data-integrity framework tables (`common.ingest_quarantine`, `common.unmapped_entities`, `common.data_completeness_log`) are owned by `etl/integrity.py`. Do not write directly to them from ETL scripts — always go through `validate_and_filter()`, `record_unmapped_entity()`, or `retroactive_scan()`. The framework tables are the state for Layers 1-3 of the integrity design (ADR-20260424-2); direct writes bypass the invariants.
- `common.*` tables opted into CRITICAL_FIELDS enforcement as of 2026-04-24: `teams`, `user_codes`, `demo_config`, `player_line_patterns`, `daily_grades`, `player_tier_lines`. Catalog lives in `etl/integrity.py`. `common.user_activations` is not yet in the catalog (add if and when a write path touches it from the ETL).

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[shared][database]`.

## Open Questions

- Should `common.player_line_patterns` and `common.player_tier_lines` be renamed `nba.player_line_patterns` / `nba.player_tier_lines` before MLB introduces equivalent tables, to avoid confusion?
- Should demo mode be sport-scoped rather than global?
