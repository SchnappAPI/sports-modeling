# NBA Web

**STATUS:** live.

## Purpose

NBA pages, components, and UI invariants. Canonical UI lives here (stats column sets, At-a-Glance defaults, props strip layout, refresh patterns).

## Files

Detailed component list migrates from legacy `/PROJECT_REFERENCE.md` in Step 4. Known components include:

- `StatsTable`, `RosterTable`, `MatchupGrid`, `LiveBoxScore`, `TodayPropsSection`, `MatchupsTab`
- `RefreshDataButton` (admin-only, requires `ADMIN_REFRESH_CODE`)
- Page components under `app/nba/`

## Key Concepts

Migrating from the legacy file. Until then, critical canonical UI:

- **Compact stats columns**: MIN, PTS, 3PM, REB, AST, PRA, PR, PA, RA
- **All-stats adds**: FG, 3PA (separate column from 3PM), FT, STL, BLK, TOV
- **StatsTable colSpanTotal**: compact = 11, all-stats = 17
- **At a Glance defaults**: `minOdds = -600`, `ODDS_MIN = -1000`, reset returns to -1000
- **Props strip**: horizontal `flex-1` cells, tap to expand into dot plot and alt panel
- **RosterTable**: "Confirmed" badge appears only when `lineupStatus = Confirmed`
- **Odds cells** link to FanDuel betslip when `row.link` is present and the game is open
- **Refresh polling**: scoreboard and box score every 30s; live odds every 60s
- **MIN format**: `mm:ss` (e.g., `21:49`); prefix `*` indicates the player started

## Invariants

These are all live and load-bearing. Do not revert without an ADR:

- Compact stats column set (above)
- All-stats column set and `colSpanTotal` values
- `getGrades` reads `dg.outcome_name` and `dg.over_price` directly from `common.daily_grades`
- Position grouping via `posToGroup()`, not `position[0]`
- At-a-Glance default odds floors

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][web]`. Historical entries are in the legacy root `/CHANGELOG.md`.

## Open Questions

Full component-by-component migration from PROJECT_REFERENCE.md (Step 4).
