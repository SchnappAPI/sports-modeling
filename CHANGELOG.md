# Changelog

> **For Claude — how to use this file:**
> - Read this BEFORE making any code change. If an area was previously changed, understand why before modifying it.
> - Append new entries at the TOP (most recent first) at the end of every session.
> - Never edit old entries. Append only.
> - Format: `## YYYY-MM-DD` date header, then `### Category | file` entries with bullet points.
> - Categories: `UI`, `API`, `Schema`, `Grading`, `ETL`, `Infra`, `Docs`

---

## 2026-04-04

### Grading | grading/grade_props.py — grade_date UTC midnight mismatch fix
- Root cause: `run_upcoming` and `run_intraday` used `date.today()` which returns UTC date on GitHub Actions runners. When the pregame-refresh workflow fires after midnight UTC (still same NBA game night in ET), yesterday's late games received today's `grade_date`, causing At a Glance to show those games in tomorrow's dropdown.
- Fix: added `today_et()` helper using `datetime.now(timezone.utc)` offset by -4h (EDT). Both `run_upcoming` and `run_intraday` now call `today_et()` instead of `str(date.today())`. Import updated to `from datetime import date, datetime, timezone, timedelta`.
- Also manually deleted 2,647 misattributed rows from `common.daily_grades` for 2026-04-04 that belonged to 7 April 3rd games (game IDs: 0022501121, 0022501122, 0022501124, 0022501125, 0022501126, 0022501127, 0022501128).
- At a Glance now shows only the 3 correct games for April 4th.
- Do not revert to `date.today()` — runners are UTC and NBA season runs EDT (UTC-4).

---

## 2026-04-03 (session close 4)

### UI | web/app/nba/player/[playerId]/PlayerPageInner.tsx — Today's Props horizontal strip + dot plot + alt line layout
- Replaced collapsible `MarketSection`/`LinePairRow` design with: horizontal scrollable strip of market cells (one per market: label, posted line, composite grade), tappable to expand a panel below.
- Strip uses `flex w-full divide-x` with `flex-1 min-w-[52px]` on each cell — spreads evenly when space allows, scrolls when cramped. No `min-w-max` on the outer wrapper (that caused bunching). No `border-t` on the strip container (that caused a stray horizontal line between the header and the strip).
- Added `StatDotPlot` SVG component: `preserveAspectRatio="none"` on a 600-wide viewBox so it fills the full container width. Oldest game left, most recent right. Green dots above the prop line, red dots below. L10/L30/L50/All window selector in the header row.
- `MarketPanel` shows the dot plot then alt lines with full two-row detail per entry (row 1: line value, O odds, U odds, grade; row 2: L20% and L60% hit rates). Standard line rows were removed — the strip cell already shows the posted line and grade.
- `TodayPropsSection` now takes `summaries: GameSummary[]` prop (passed from parent where it is already in scope). Auto-selects first market on load.
- The section header row and the strip share the same `border-b` container so there is no extra line between them.
- Do not revert the `preserveAspectRatio="none"` dot plot — the old fixed 280px viewBox caused all dots to bunch to the left.

### UI | web/components/StatsTable.tsx — 3PT column split
- Compact view: `3PM` column shows `avg3pm` as a plain average (e.g. `1.5`). Was previously showing `avg3pm-avg3pa` ratio.
- All Stats view: `3PM` and `3PA` are now two separate columns, each showing a plain average.
- `colSpanTotal` updated: compact = 11, all-stats = 17.
- Do not revert — the previous made-att ratio in compact was not useful; the separate columns are more readable.

### UI | web/lib/queries.ts + web/components/RosterTable.tsx — roster badge logic + inactive section
- `getRoster` query: returns `starterStatus` (raw string: 'Starter'/'Bench'/'Inactive') instead of the old boolean `isStarter`. SQL ORDER BY now sorts Inactive after Bench.
- `RosterRow` interface updated: `starterStatus: string | null` replaces `isStarter: boolean`.
- `RosterTable.tsx`: `teamBadge()` now shows Confirmed (green) only when at least one player has `lineupStatus === 'Confirmed'`, Projected (yellow) when any player has `lineupStatus === 'Projected'`, and Expected (gray) when all `lineupStatus` values are null. Previously it showed Confirmed whenever the lineup was not Projected, including when no lineup data was confirmed yet.
- Added `Inactive` section below Bench with an "Out / Inactive" label row. Inactive rows render dimmed (`opacity-40`).
- Do not revert the badge logic — the old version showed every pre-game lineup as Confirmed.

### UI | web/app/nba/grades/GradesPageInner.tsx — default min odds filter
- `ODDS_DEFAULT` changed from -1000 to -600. Page loads with the odds floor at -600, hiding lines with odds worse than -600 (e.g. -800, -1000, -5000).
- Slider still reaches -1000 so the user can drag left to reveal worse lines.
- Reset button now resets to `ODDS_MIN` (-1000) instead of `ODDS_DEFAULT`, so clicking Reset shows everything.
- `oddsFilterActive` now reflects `minOdds > ODDS_MIN` (not `> ODDS_DEFAULT`), so the active indicator correctly lights up at -600.
- Do not change `ODDS_DEFAULT` back to -1000 — the purpose is to filter junk lines by default.

---

## 2026-04-03 (session close 3)

### UI | web/app/nba/player/[playerId]/PlayerPageInner.tsx — compact/all-stats toggle + props redesign
- Added `showAllStats` boolean state shared between the splits strip and game log.
- Compact view (default): Player, MIN, PTS, 3PT (as 3PM-3PA), REB, AST, PRA, PR, PA, RA.
- All Stats view: adds FG (FGM-FGA), FT (FTM-FTA), STL, BLK, TOV. Toggle button lives in splits header row, far right.
- PRA/PR/PA/RA are computed combo columns: PRA=pts+reb+ast, PR=pts+reb, PA=pts+ast, RA=reb+ast. Combo prop coloring added via `getComboLineCls`.
- Companion values (REB-RebChances, AST-PotAst) shown only in player game log, only when full game (no period filter). Not added to team views.
- PF dropped — not in schema. Note as future addition.
- Today's Props section redesigned: `TodayGradeRow` now includes `outcomeName` field. Over/Under rows paired into `LinePair` structs keyed by `(baseMarket, lineValue)`. Each market is a collapsible section (expanded by default). Standard lines show Over price, Under price, composite grade, hit rates on one row. Alt lines rendered horizontally, collapsed by default under each market. Do not revert to the old card layout.

### UI | web/components/StatsTable.tsx — compact/all-stats toggle + combo columns
- `showAllStats` prop added to `TeamStatsTable`, controlled from parent `StatsTable`.
- Compact: Player, GP, MIN, PTS, 3PT, REB, AST, PRA, PR, PA, RA.
- All Stats: adds FG, FT, STL, BLK, TOV. Toggle button in filter bar.
- `avgFtm` and `avgFta` added to `PlayerAvg` interface (were missing, required for FT column).
- `colSpanTotal` updated to handle variable column count for collapsible section headers.
- Do not revert to the old FG%/3P% percentage columns.

### UI | web/components/BoxScoreTable.tsx — compact/all-stats toggle + combo columns
- `showAllStats` state in `BoxScoreTable`, passed to `TeamBox`.
- Same compact/all-stats column sets as StatsTable and PlayerPageInner.
- `COMBO_MARKETS` constant added for PRA/PR/PA/RA prop coloring in box score rows.
- `getComboLine` helper added alongside existing `getLine`.
- `colSpanTotal` updated for DNP rows and section headers.
- Toggle button right-aligned in period filter bar.

### API | web/app/api/team-averages/route.ts — avgFtm, avgFta
- Added `AVG(CAST(r.ftm AS FLOAT)) AS avgFtm` and `AVG(CAST(r.fta AS FLOAT)) AS avgFta` to SELECT.
- Required by StatsTable FT column in all-stats mode.

### UI | web/components/MatchupDefense.tsx — column order
- Reordered `STAT_LABELS` to match game log column order: PTS, 3PM, REB, AST, STL, BLK, TOV.
- Was: PTS, REB, AST, STL, BLK, 3PM, TOV.
- PRA/PR/PA/RA not added — requires extending `/api/contextual` query to compute combo defense averages. Deferred.

---

## 2026-04-03 (session close 2)

### ETL | nba_etl.py — today's games in nba.games
- Changed `game_date < today` to `game_date <= today` in `load_schedule` when populating `nba.games`.
- Root cause: today's final games were excluded from `nba.games`, blocking the FK on `nba.player_box_score_stats`, so box score rows could never be written for same-day games.
- Box score tab now shows data for today's completed games after the nightly ETL runs.

### ETL | nba_etl.py — inactive player detection
- Added `INACTIVE_LINEUP_KEYWORDS` constant: `("out", "inactive", "not with team", "gtd")`.
- `fetch_lineups_for_game_date` now checks `lineupStatus` before assigning `starter_status`. Players whose `lineupStatus` contains any inactive keyword get `'Inactive'` regardless of `rosterStatus`.
- Root cause: active-roster players listed as Out (e.g. Wembanyama) had `rosterStatus='Active'` with no position, so they were assigned `'Bench'` and appeared mixed in with available bench players.
- Do not revert the keyword check — the old logic of `"Bench" if roster == "Active"` was incorrect for injured/inactive players.

### ETL | etl/gate_check.py — recreated
- Recreated `etl/gate_check.py` after it was deleted earlier. The file had been missing, causing `pregame-refresh.yml` to fail on every run with "No such file or directory".
- Queries `nba.schedule` for any game today with `game_status IN (1, 2)`. Prints `true` or `false`. Exit code always 0.
- Uses pyodbc directly (not SQLAlchemy) with 3-attempt retry and 45s wait.

### UI | web/components/StatsTable.tsx — inactive player section
- Added separate collapsible `Inactive (N)` section below Bench for players with `starterStatus === 'Inactive'`.
- Inactive rows rendered with `opacity-40`. Section defaults to collapsed.
- Added `inactiveOpen` state alongside existing `benchOpen`.
- `bench` filter now strictly checks `starterStatus === 'Bench'` instead of `!== 'Starter'`, so inactive players no longer fall through to bench.
- Do not revert — previously inactive players like Wembanyama appeared as bench players.

### API | web/lib/queries.ts — fg3a in getBoxscore
- Added `fg3a` to `BoxscoreRow` interface and to the `getBoxscore` SQL (`pbs.fg3a`).
- Root cause: `fg3a` was missing from the interface and query, so `BoxScoreTable.tsx` had no 3PA value to use.

### UI | web/components/BoxScoreTable.tsx — 3P column fix + fetch decoupling
- Added `fg3a` to `BoxRow`, `PlayerTotals`, `ZERO_TOTALS`, `buildTotals`.
- Fixed 3P render cell: `fmtShoot(t.fg3m, t.fg3a)` — was incorrectly using `t.fga` (field goal attempts) as the denominator, showing e.g. `5/15` instead of `5/8`.
- Changed separator in `fmtShoot` from `/` to `-` to match canonical dash separator.
- Decoupled `/api/boxscore` and `/api/game-grades` fetches — grading failure is now non-fatal and does not prevent the box score from rendering.
- Do not revert the `fg3a` fix or the fetch decoupling.

### ETL | etl/lineup_fix_fragment.py — deleted
- Accidentally created during session; replaced with a comment stub. Safe to delete entirely.

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
