# NBA Database

**STATUS:** live.

## Purpose

The `nba` schema holds player, team, game, box score, lineup, and grade-input data. Grading outputs land in `common.daily_grades`.

## Files

DDL is defined in the NBA ETL scripts. A current table inventory can be produced via `etl/db_inventory.py` (triggered by the `db_inventory.yml` workflow, output lands in `/tmp/db_inventory_output.txt` on the VM).

## Key Concepts

Detailed table list and column-level invariants migrate from legacy `/PROJECT_REFERENCE.md` in Step 4 of the documentation restructure. Known tables include `nba.player_box_score_stats`, `nba.player_game_logs`, `nba.rosters`, `nba.lineups`, `nba.games`, plus `nba.odds_mappings` for the odds-to-player reconciliation.

## Invariants

Migrating from the legacy file. Until then, critical points not to violate:

- `nba.player_box_score_stats.period` is `VARCHAR(2)`. Values over 2 characters will truncate or fail. Valid values: `'1Q'`, `'2Q'`, `'3Q'`, `'4Q'`, `'OT'`.
- Grading writes to `common.daily_grades` with `outcome_name` (Over/Under) and `over_price` (INT). UNIQUE key includes `outcome_name`.
- Web `getGrades` reads `dg.outcome_name` and `dg.over_price` directly from `common.daily_grades`.
- `common.player_line_patterns` is populated nightly at 07:30 UTC by `compute-patterns.yml`.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][database]`.

## Open Questions

Full column-level invariants migrate from PROJECT_REFERENCE.md in Step 4.
