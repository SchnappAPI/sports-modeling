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

### `odds.*` tables

- `odds.upcoming_events` - event_id, home_tricode, away_tricode, commence_time, game_id
- `odds.upcoming_player_props` - event_id, market_key, outcome_point, player_name, bookmaker_key, snap_ts, outcome_price, outcome_name, link

## Invariants

- `common.teams` is cross-sport. Per-sport team attributes live in `<sport>.teams` if needed.
- `odds.upcoming_player_props.link` is populated only from the Odds API per-event endpoint (not the bulk endpoint).
- `common.daily_grades.outcome_name` is part of the UNIQUE key. Grading writes both Over and Under rows.
- `common.player_tier_lines` UNIQUE key is `(grade_date, game_id, player_id, market_key)`. Over-side only. Written in lockstep with `common.daily_grades` inside `grade_props.py` — never skip one and write the other.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[shared][database]`.

## Open Questions

- Should `common.player_line_patterns` and `common.player_tier_lines` be renamed `nba.player_line_patterns` / `nba.player_tier_lines` before MLB introduces equivalent tables, to avoid confusion?
- Should demo mode be sport-scoped rather than global?
