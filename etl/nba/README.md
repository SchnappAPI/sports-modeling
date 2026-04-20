# NBA ETL

**STATUS:** live.

## Purpose

Ingests NBA data from `stats.nba.com`, `cdn.nba.com`, and The Odds API. Produces box scores, rosters, game logs, odds lines, lineups, and grades.

## Files

Code files live flat in `/etl/` per ADR-0002:

- `etl/nba_etl.py` - main ETL entry point
- `etl/nba_grading.py`, `etl/grade_props.py` - grading pipeline
- `etl/odds_etl.py` - odds ingestion (shared with MLB, dispatches by sport)
- `etl/lineup_poll.py` - two-stage lineup polling
- `etl/nba_live.py` - live box score via CDN
- `etl/runner.py` - Flask live-data service on the VM

Workflows under `.github/workflows/` orchestrate these scripts on schedules (nba-game-day.yml, refresh-lines.yml, compute-patterns.yml, etc.).

## Key Concepts

Detailed content migrating from legacy `/PROJECT_REFERENCE.md` in Step 4 of the documentation restructure. Until then, the legacy file remains authoritative for:

- Grading schema v3 (Over and Under rows, `over_price`, `outcome_name`)
- STREAK and DUE signal redesign
- `common.player_line_patterns` table and lag-1 transition probabilities
- Odds API mechanics (FanDuel only, per-event endpoint required for betslip links)
- Two-stage lineup poll (official JSON plus `boxscorepreviewv3`)
- Live scoreboard and box score via Cloudflare tunnel

## Invariants

Migrating in Step 4. Do not revert these without a superseding ADR:

- `common.daily_grades` has `outcome_name` (Over/Under) and `over_price`. The UNIQUE key includes `outcome_name`.
- `_common_grade_data` returns a 6-tuple (the last element is patterns). Never revert to the 5-tuple form.
- Position grouping uses `posToGroup()` (PG and SG map to G; SF and PF to F; C to C). Never use `position[0]`.
- `includeLinks=true` is valid only on the Odds API per-event endpoint, not bulk.
- Compact stats columns in the web layer: MIN, PTS, 3PM, REB, AST, PRA, PR, PA, RA. All-stats adds FG, 3PA, FT, STL, BLK, TOV.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][etl]`. Historical entries before the restructure are in the legacy root `/CHANGELOG.md`.

## Open Questions

Content migration from PROJECT_REFERENCE.md (Step 4 of the restructure).
