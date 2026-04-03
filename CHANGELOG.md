# Changelog

> **For Claude — how to use this file:**
> - Read this BEFORE making any code change. If an area was previously changed, understand why before modifying it.
> - Append new entries at the TOP (most recent first) at the end of every session.
> - Never edit old entries. Append only.
> - Format: `## YYYY-MM-DD` date header, then `### Category | file` entries with bullet points.
> - Categories: `UI`, `API`, `Schema`, `Grading`, `ETL`, `Infra`, `Docs`

---

## 2026-04-03 (session close)

### Docs | sports-session-close SKILL.md
- Created `sports-session-close` skill file. Installed at `/mnt/skills/user/sports-session-close/SKILL.md`.
- Trigger phrases: "update everything", "close out the session", "I'm starting a new chat", "wrap this up", "update the docs".
- Runs 5 steps: audit session, append CHANGELOG, update PROJECT_REFERENCE Current State, update memory, generate handoff primer.

### Infra | deploy retry
- Triggered redeploy to recover from Azure SWA transient deployment cancellation. No code changes.

### UI | StatsTable.tsx + PlayerPageInner.tsx — separator change
- Changed separator in all made-attempted ratio display from `/` to `-`.
- StatsTable: `fmtRatio()` now returns `7.1-14.8` instead of `7.1/14.8`.
- PlayerPageInner: `fmtS()` now returns `7-14` instead of `7/14`. `fmtPT()` now returns `5-9` instead of `5/9`.
- Applies to FG, 3PT, FT columns in game log, and FG, 3PT columns in stats table.
- Do not revert to slash separator.

### Docs | PROJECT_REFERENCE.md + CHANGELOG.md
- Restructured PROJECT_REFERENCE.md: current state at top, trimmed stable lookup sections, added canonical UI layout tables, added two-file session protocol.
- Created CHANGELOG.md as append-only record of intentional changes. Check before modifying any file.
- Updated memory to reflect two-file session protocol.

### UI | StatsTable.tsx
- FG column: changed from `FG%` (percentage) to `FG` showing `avgFgm-avgFga` ratio (e.g. `7.1-14.8`).
- 3PT column: changed from `3P%` (percentage) to `3PT` showing `avg3pm-avg3pa` ratio (e.g. `2.1-5.6`).
- Uses `fmtRatio()` helper. `fmtPct()` is NOT used for these columns. Headers are `FG` and `3PT`.
- Do not revert to percentage display.

### API | queries.ts — getGrades
- Removed `best_price` CTE join to `odds.upcoming_player_props` and `odds.player_props`.
- Now reads `dg.outcome_name` and `dg.over_price` directly from `common.daily_grades`.
- Reason: the CTE join filtered `outcome_name = 'Over'` which attached Over prices to Under rows, making Under rows pass the `overPrice != null` filter and appear in the Over tab.
- Added `outcomeName` field to `GradeRow` interface.
- Do not reintroduce the best_price CTE join.

### API | queries.ts — getPlayerProps
- Now reads `dg.over_price` directly and filters `outcome_name = 'Over'` to exclude Under rows from player page prop cards.

### API | team-averages/route.ts
- Added `fg3a` to `game_totals` CTE aggregation.
- Added `avg3pa` (AVG fg3a) and `avgFgm`, `avgFga` to SELECT. These are required by StatsTable FG/3PT ratio columns.

---

## 2026-04-02

### Grading | grade_props.py — schema v3 migration
- Added `outcome_name` (VARCHAR(5), 'Over'/'Under') and `over_price` (INT) columns to `common.daily_grades`.
- UNIQUE constraint updated to include `outcome_name` (was missing, preventing Over+Under rows for same line).
- `over_price` stores direction-appropriate price: Over price for Over rows, Under price for Under rows.
- Both Over and Under rows now written for standard markets. Alternates are Over-only.

### Grading | grade_props.py — Under grades
- Added `fetch_under_prices()` to pull Under prices from `odds.upcoming_player_props`.
- Added `build_under_props()` to construct Under prop rows (standard markets, posted line only, no bracket).
- Added `direction` parameter to `grade_props_for_date()` and `compute_all_hit_rates()`.
- Under hit rate: `stat < line` instead of `stat > line`.
- All components inverted for Under rows via `_invert()` helper (100 - value, centered at 50).

### Grading | grade_props.py — performance
- `precompute_line_grades`: restructured outer loop to iterate by `(player_id, market_key)` pair.
- Loads player stat sequence once per pair, fans across all line values in inner loop.
- Reduces outer iterations from ~6200 to ~560 in upcoming mode (~10x improvement).

### Grading | grade_props.py — drop_bracket_lines_covered_by_alts
- Added function to suppress standard bracket lines that duplicate an alternate market line for the same player/stat/value.
- Applied in `run_upcoming` before building the over props list.
- WARNING: this function was previously added and reverted. Current implementation matches on `(player_id, stat_col, line_value)` across standard and alternate market keys. Do not remove it again without understanding why.

### UI | PlayerPageInner.tsx — column layout overhaul
- Removed `Str` column permanently. Starter status moved to MIN column.
- MIN column: `*21:49` for starters, `21:49` for bench, `DNP` for did-not-play rows.
- Column order: Date, Opp, MIN, PTS, FG, 3PT, REB, AST, STL, BLK, TOV, FT.
- FG column: shows `fgm-fga` per game (e.g. `7-14`). NOT a percentage.
- 3PT column: renamed from `3PM`, shows `fg3m-fg3a` per game (e.g. `3-8`). NOT a percentage.
- `fg3a` added to `PlayerGameRow` interface and `getPlayerGames` SQL (`pbs.fg3a`).

### UI | StatsTable.tsx — starters/bench layout
- Starters render at top with no section header row.
- Bench players collapsed behind tappable `Bench (N)` row with arrow indicator.
- `benchOpen` state per `TeamStatsTable` instance, defaults to collapsed.
- `hasLineup` gate: only applies grouping when `starterStatus` is non-null for at least one player.

### Schema | common.daily_grades
- `ensure_tables()` uses ADD COLUMN IF NOT EXISTS pattern to add new columns without dropping the table.
- UNIQUE constraint name changed to `uq_daily_grades_v3` to allow recreation with the new column set.
