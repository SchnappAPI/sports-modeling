# NBA Web

**STATUS:** live.

## Purpose

NBA pages, components, and canonical UI invariants. The source of truth for stats column sets, At-a-Glance behavior, props strip layout, refresh mechanics, and the live box-score view.

## Files

Components live in `/web/components/`. Pages under `/web/app/nba/`. Signal logic lives in `/web/lib/signals.ts`.

Components (standalone `.tsx` files):

- `StatsTable.tsx` - team and aggregate stat tables with compact/all-stats toggle
- `RosterTable.tsx` - per-game roster view including starter/bench split and Confirmed badge
- `BoxScoreTable.tsx` - per-game player box score with period filter
- `MatchupGrid.tsx` - two defense panels side by side on the game Matchups tab
- `TrendsGrid.tsx` - per-game tier-line grid on the Trends tab (see ADR-20260423-1 and `/api/tier-grid`)
- `MatchupDefense.tsx` - VS Defense panel on the player page
- `LiveBoxScore.tsx` - live score header + starter/bench split, refreshes every 30s
- `GameStrip.tsx` - game list with live/final scores or spread-and-total for upcoming
- `GameTabs.tsx` - tab container for the game page (includes Matchups via `MatchupGrid`, Trends via `TrendsGrid`)
- `PropMatrix.tsx` - At-a-Glance matrix view
- `HelpPanel.tsx` - `?` button content in the At-a-Glance header
- `RefreshDataButton.tsx` - admin-only four-step refresh trigger. Requires `ADMIN_REFRESH_CODE`
- `PasscodeGate.tsx` - passcode auth for the whole app

Not standalone components (live inside other files):

- `TodayPropsSection` - horizontal props strip + tap-to-expand panel. Part of `PlayerPageInner.tsx` under `app/nba/player/[playerId]/`
- `MatchupsTab` - tab wrapper for `MatchupGrid`. Part of `GameTabs.tsx`

Pages:

- `app/nba/page.tsx` - game strip + tabs
- `app/nba/player/[playerId]/page.tsx` - player page
- `app/nba/grades/page.tsx` - At a Glance
- `app/admin/page.tsx` - admin tools

## Key Concepts

### Stat column sets

**Compact (default)** columns for all stat tables: Player/Date/Opp, MIN, PTS, 3PT, REB, AST, PRA, PR, PA, RA.

- In `StatsTable`: 3PT column is 3PM as a plain average
- In the player game log: 3PT column is `3PM-3PA` with a dash separator

**All-stats toggle** adds: FG (`FGM-FGA`), 3PA (plain average, separate column from 3PM), FT (`FTM-FTA`), STL, BLK, TOV.

FG and 3PT use a dash separator (not a slash) and display made-attempted, not percentage. More useful for prop research.

**`colSpanTotal`** in `StatsTable`: compact = 11, all-stats = 17. Do not change without matching the actual rendered column count.

Companion values on the player game log only: `REB-RebChances` and `AST-PotAst`, full-game rows only. Team views don't have per-player PT stat data in the team-averages API.

### Player game log columns

| Col | Value | Notes |
|-----|-------|-------|
| Date | `gameDate.slice(5)` | MM-DD |
| Opp | `@abbr` or `abbr` | |
| MIN | `*21:49`, `21:49`, or `DNP` | `*` prefix = starter |
| PTS | integer | prop colored |
| FG | `fgm-fga` | all-stats only, dash separator |
| 3PT | `fg3m-fg3a` | dash separator |
| REB | `reb-rebChances` | companion value full-game only |
| AST | `ast-potentialAst` | companion value full-game only |
| PRA, PR, PA, RA | sums | prop colored |
| STL, BLK | integer | all-stats only, prop colored |
| TOV | integer | all-stats only |
| FT | `ftm-fta` | all-stats only, dash separator |

### Today's Props strip

Horizontal strip, one cell per market (label, posted line, composite grade). Wrapper is `flex w-full divide-x`; each cell is `flex-1 min-w-[52px]`. No `min-w-max` on the wrapper, no `border-t` on the strip div.

Tapping a cell expands a panel below containing: a full-width SVG dot plot (`preserveAspectRatio="none"`, 600-wide viewBox, oldest-left newest-right, green = hit, red = miss), then the alt-lines table with two-row detail.

Standard line detail rows are intentionally absent from the panel; the strip cell already shows the posted line + grade.

`TodayPropsSection` takes `summaries: GameSummary[]`. `getGrades` reads `dg.outcome_name` + `dg.over_price` directly from `common.daily_grades`. No join to odds tables. The old `best_price` CTE was removed because it attached Over prices to Under rows; do not reintroduce it.

### Signals

`web/lib/signals.ts` defines all chip logic. Two families plus a cell value family:

- **Player-level** (same across every line for this player): `HOT` if `trend_grade > 72`, `COLD` if `trend_grade < 28`, `DUE` if `regression_grade > 72`, `FADE` if `regression_grade < 28`. HOT suppresses FADE. DUE suppresses COLD.
- **Line-level** (per posted line): `STREAK` when `momentum_grade > 70`. `SLUMP` (displayed as the green DUE chip) when `momentum_grade > 65` and `hit_rate_60 >= 0.35` and `STREAK` did not fire.
- **Cell value**: `LONGSHOT` when `over_price > 250` and `hit_rate_20 > 0` and `hit_rate_60 >= 0.20`.

Note: player-level `DUE` and line-level `SLUMP` both display with the "DUE" label but are different signals with different inputs.

### At a Glance

- Default `minOdds = -600`. Slider range `-1000` to `+200`. Reset button goes to `-1000` (shows everything).
- `ODDS_MIN = -1000`. `oddsFilterActive = minOdds > ODDS_MIN`. Do not change this condition.
- Over/Under toggle filters on `r.outcomeName`.
- Refresh Data button requires admin passcode (`ADMIN_REFRESH_CODE` set in Azure SWA app settings).

### Live tab

`LiveBoxScore.tsx`. Score header: AWY abbr + score (left), pulsing Live dot + period/clock (center), HME abbr + score (right). Leading score displays brighter (home wins ties for brightness).

Player rows show a green dot when `oncourt`. The `starter` boolean comes from the CDN box score, not from `nba.daily_lineups`. Starters and Bench sections are split on that CDN field. Refreshes every 30s.

### Game strip

`GameStrip.tsx`. Live and Final games show `awayScore awayAbbr @ homeAbbr homeScore`. Leading score `text-gray-100`, trailing `text-gray-500`. Upcoming games show the matchup row plus a spread/total row. The `Game` interface carries `homeScore`, `awayScore`, `period`, `gameClock` (all nullable). Do not revert live/final games to spread-only display.

### Matchups tab

`MatchupGrid.tsx` + `/api/matchup-grid`. Two defense panels side by side: away defense on the left, home defense on the right.

- Rows: G, F, C position groups
- Columns: PTS, REB, AST, 3PM, STL, BLK, TOV
- Each cell shows season average allowed plus rank out of 30. Rank 1-10 green (exploitable), 21-30 red (tough)
- Tapping a row expands to show today's active players at that position facing that defense; player name links to the player page
- Panel labels: "vs AWY Defense" shows home-team players (they attack AWY). "vs HME Defense" shows away-team players
- Lineup position source: starters use `dl.position` (game-specific, precise). Bench uses `COALESCE(p.position, dl.position)`

`posToGroup()` uses exact `IN()` matches for PG/SG/SF/PF/C and then `LEFT(1)` for compound position strings (G-F → G, F-G → F, C-F → C). Never `position[0]` and never `LEFT(position, 2)`.

### Odds display

Opening vs live price comparison: opening price shown with strike-through gray, directional arrow, live price in green (moved favorably) or yellow (moved against). Odds polling interval 60s. Scoreboard and live box score polling 30s.

Line and Odds cells in the At-a-Glance table become tappable FanDuel betslip deep links when `row.link` is present and the game is still open. `row.link` comes from `odds.upcoming_player_props.link` populated by the per-event Odds API call.

### Refresh buttons

- `RefreshDataButton` is on the NBA page header and on At a Glance. Hits `/api/refresh-data`. Requires `ADMIN_REFRESH_CODE`. Runs all four steps (live box, odds, grading, lineup poll).
- A separate unauthenticated refresh hits `/api/refresh-lines` and runs odds + grading only. Used from older on-page controls.

### Flask live-data integration

All three routes that call Flask on the VM (`/api/games`, `/api/scoreboard`, `/api/live-boxscore`) now use the Cloudflare-proxied subdomain `https://live.schnapp.bet` as the base URL. Never hardcode VM IPs in web routes. DNS resolves via Cloudflare to the VM's current public IP (`172.173.126.81`). The `X-Runner-Key` header is still required on `/scoreboard` and `/boxscore`; `/ping` is open.

### API routes

Timezone note: `/api/games` uses `todayCT()` (Central Time) when `?date` is omitted, to match the ETL which normalizes game dates to Central. Other routes that look up "today" may differ (e.g. `grade_props.py` uses `today_et()`).

- `/api/ping` - anonymous. `SELECT 1` via `ping()`. Originally used by Uptime Robot for DB keep-alive; Uptime Robot was paused 2026-04-23. Route retained for future re-enablement and for ad-hoc health checks
- `/api/games?date=&sport=nba` - reads the game list from `nba.schedule` via `getGames()` for any date. For today only, overlays CDN scoreboard data (status + scores) onto games already present in the DB via `https://live.schnapp.bet/scoreboard`. CDN never drives the game list. Falls back to DB-only if Flask is unreachable
- `/api/scoreboard` - thin passthrough to Flask `/scoreboard` via `https://live.schnapp.bet`
- `/api/grades?date=&gameId=` - reads `dg.outcome_name`, `dg.over_price`, and `dg.outcome` directly from `common.daily_grades`. Also joins `odds.upcoming_player_props` for `link` when that column exists (FanDuel betslip deep link; NULL for historical rows)
- `/api/game-grades?gameId=` - grades filtered to a single game
- `/api/player-grades` - grades filtered to a player
- `/api/player-props` - historical prop rows for a single player. Over-only, FanDuel-only (see `getPlayerProps` in `web/lib/queries.ts`)
- `/api/team-averages?homeTeamId=&awayTeamId=&context=&periods=&opp=&gameId=` - both teams' lineup-aware player averages. Returns `avgPts/Reb/Ast/Stl/Blk/Tov/Min` plus `avg3pm/3pa`, `avgFgm/Fga`, `avgFtm/Fta`. `context` accepts a number (last N games), `all`, or `opp`
- `/api/player-averages?gameId=&lastN=` - lineup-anchored averages for a single game
- `/api/contextual?oppTeamId=&position=` - defense ranks. Rank 1 = most allowed
- `/api/matchup-grid?gameId=` - both teams' defense plus today's lineup players
- `/api/live-boxscore?gameId=` - calls Flask `/boxscore` via `https://live.schnapp.bet`
- `/api/boxscore?gameId=` - persisted per-quarter box score from `nba.player_box_score_stats`
- `/api/roster?gameId=` - daily lineup rows for a game
- `/api/player?id=` - player header + last-N game log
- `/api/refresh-lines` POST - dispatches `refresh-lines.yml` via GitHub Actions REST API using `GITHUB_PAT`. No passcode. Returns `{ runId }` for polling
- `/api/refresh-data` POST - validates `ADMIN_REFRESH_CODE` (uppercased compare), then dispatches `refresh-data.yml` via `GITHUB_PAT`. Returns `{ runId }` for polling
- `/api/refresh-status?runId=` - polls workflow run status
- `/api/admin/*` - admin-only routes (user code management, etc.)
- `/api/auth/*` - passcode gate endpoints
- `/api/live`, `/api/live-props`, `/api/mlb-*` - live-odds and MLB routes

Route cache headers (in `next.config.mjs`):

- `/api/games`, `/api/roster`, `/api/player`: `s-maxage=60, stale-while-revalidate=120`
- `/api/grades`, `/api/game-grades`: `s-maxage=90, stale-while-revalidate=180`
- `/api/contextual`: `s-maxage=600, stale-while-revalidate=1200`
- `/api/boxscore`, `/api/ping`: `no-cache, no-store`

## Invariants

Do not revert without an ADR.

- Compact stats columns: MIN, PTS, 3PM, REB, AST, PRA, PR, PA, RA
- All-stats additions: FG, 3PA (separate column from 3PM), FT, STL, BLK, TOV
- `StatsTable.colSpanTotal`: 11 compact / 17 all-stats
- `minOdds` default -600; `ODDS_MIN = -1000`; reset returns to -1000
- `oddsFilterActive = minOdds > ODDS_MIN`
- `getGrades` reads `dg.outcome_name` and `dg.over_price` directly; no join to odds tables for prices. The only odds-table join is to `odds.upcoming_player_props` for the FanDuel betslip `link` column
- Props strip uses `flex w-full divide-x` with `flex-1 min-w-[52px]` cells; no `min-w-max`
- `RosterTable` "Confirmed" badge appears only when `lineupStatus = 'Confirmed'`
- Live and Final games on `GameStrip` show scores, not spread/total
- FG and 3PT in game log and stats table use a dash separator showing made-attempted, not percent
- MIN format is `mm:ss`, with `*` prefix for starters; DNP otherwise
- Polling intervals: scoreboard and live box score 30s, live odds 60s
- Position grouping via `posToGroup()` (PG/SG → G, SF/PF → F, C → C, compound by `LEFT(1)`)
- FanDuel betslip link appears when `row.link` is present and the game is open
- `RefreshDataButton` requires `ADMIN_REFRESH_CODE`; `/api/refresh-lines` is unauthenticated
- Signal logic lives in `web/lib/signals.ts`. Player-level `DUE` (regression-based) and line-level `SLUMP` displayed as `DUE` (momentum-based) are distinct signals with different inputs
- `/api/games` treats the DB as the game-list source of truth. CDN only overlays live score/status onto games already in the DB; CDN-only games are ignored
- All web routes that call Flask on the VM use `https://live.schnapp.bet` (Cloudflare subdomain), never a hardcoded IP

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][web]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether to extend `/api/contextual` with combo-stat (PRA/PR/PA/RA) defense averages for the VS Defense panel. Non-trivial query change; deferred.
- Whether PWA start URL should remain `/nba` (clean) vs. date-specific.
