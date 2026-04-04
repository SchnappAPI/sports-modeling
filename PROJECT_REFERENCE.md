# Sports Modeling — Project Reference

> **For Claude — session protocol:**
> 1. Read this file.
> 2. Read `CHANGELOG.md` — check recent entries before touching any file.
> 3. Do the work.
> 4. Append new entries to `CHANGELOG.md` for everything that changed.
> 5. Update the Current State section below if build status, known issues, or next steps changed.
>
> Never rewrite this whole file. Use `str_replace` to update only what changed.

---

## Current State (updated 2026-04-04 session 4)

**What is working:**
- NBA data pipeline fully active. Box scores, live updates, odds, lineup poll, grading all running.
- Grading: all components live, Over + Under grades written daily. `grade_props.py` restored and verified OK (py_compile clean, 939 lines).
- Web: all views live at schnapp.bet. Player page, stats tab, At a Glance, matchup defense, grades all functional.
- PWA active. Install via Safari Share → Add to Home Screen.
- `sports-session-close` skill installed — use at end of every session to update docs and generate handoff primer.
- Compact/All Stats toggle live across player page (splits + game log), StatsTable, and BoxScoreTable.
- Today's Props: horizontal strip of market cells, tappable to expand dot plot + alt lines panel.
- Roster tab: badge logic fixed. Inactive players shown in separate dimmed section.
- Self-hosted runner live: `schnapp-runner` Azure VM (West US 2, B2s_v2). All active workflows use it. ETL ~25 seconds.
- Uptime Robot active: pings schnapp.bet/api/ping every 30 minutes.
- **Refresh Data button live** on both NBA page header and At a Glance. Admin passcode required (`ADMIN_REFRESH_CODE` in SWA app settings). Runs all four steps: live box score, odds, grading, lineup poll.
- `refresh-data.yml` workflow wired. `/api/refresh-data` route validates passcode, dispatches workflow. `RefreshDataButton.tsx` component polls for completion.

**Known issues:**
- `etl/lineup_fix_fragment.py` is a stub file left from an accidental create — safe to delete.
- PasscodeGate `BYPASS = true` — gate disabled for dev. Re-enable before sharing with users (`PasscodeGate.tsx`).
- Odds/grading backfill gap — pre-April 2026 dates still being backfilled by nightly chain.
- NFL workflow missing — `nfl_etl.py` exists, `nfl-etl.yml` does not.
- NFL and MLB `run_mappings` not implemented in odds_etl.py — only NBA branch exists.
- Auto-pause delay locked — free database offer prevents changing it. Uptime Robot mitigates.
- PNG icons not generated — SVG covers all modern browsers; generate via `web/scripts/generate-icons.mjs` if needed.
- VS Defense does not show combo market columns (PRA/PR/PA/RA) — requires extending `/api/contextual` query. Deferred.
- Game log prop coloring may use wrong line if an alt line appears in `gradeMap` before the standard line.
- `dev` branch has a stale merge conflict on `PlayerPageInner.tsx`. Close PR #29 without merging.
- VM resize pending — B2s_v2 on free trial. Downsize to B1s_v2 after trial credits expire.

**Next up:**
- Verify nba-game-day.yml test run dispatched at end of session 4 completed successfully (grading step should now pass with restored grade_props.py).
- PlayerPageInner.tsx: (1) Move All Stats toggle to period filter bar. (2) Add vs Opp filter button. (3) Fix game log prop coloring — gradeMap should prefer standard line over alt. (4) Link date/opp cells to box score. (5) Fix Today's Props strip header when standardLines empty. (6) Alt lines panel: side-by-side chips in scrollable row.
- New page: VS Defense dashboard at `/nba/defense`.
- Step 15: MLB ETL and web views.
- Step 16: NFL ETL automation and web views.
- Re-enable PasscodeGate (`BYPASS = false`) before sharing with users.
- Downsize schnapp-runner VM to B1s_v2 after free trial credits expire.

---

## Infrastructure

### Azure SQL
- Server: `sports-modeling-server.database.windows.net` / DB: `sports-modeling` / Login: `sqladmin`
- Tier: GP_S_Gen5_2 Serverless — auto-pauses; first connection 20–60s cold start
- Free offer applied — auto-pause delay cannot be changed
- Firewall: `0.0.0.0–255.255.255.255` (required for GitHub Actions). Allow Azure Services ON.
- Connection: SQLAlchemy + pyodbc, ODBC Driver 18. `fast_executemany=True` except grading engine uses `False` (prevents NVARCHAR(MAX) truncation).
- Retry: 3 attempts, 45s wait
- MSSQL MCP (`mssql-mcp:ExecuteSql`): available on VM only. ThreatLocker blocks it on corporate machine.
- Keep-alive: Uptime Robot pings `https://schnapp.bet/api/ping` every 30 min. `keepalive.yml` is now dispatch-only.

### GitHub Actions / Runner
- Repo: `SchnappAPI/sports-modeling` (private)
- **Self-hosted runner:** `schnapp-runner` Azure VM (West US 2, Standard B2s_v2, Ubuntu 24.04). IP: `20.109.181.21`. Admin: `schnapp-admin` / `Sports#2026VM`.
- Runner service: systemd, starts on boot, always online. Python venv at `~/venv` with pinned deps pre-installed. ODBC Driver 18 pre-installed.
- All active workflows use `runs-on: [self-hosted, schnapp-runner]`. No ODBC or pip install steps in any workflow.
- After trial: downsize VM to B1s_v2 (~$15-20/month). Resize via Azure Portal, no data loss.
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10`, `GITHUB_PAT`
- Always fetch file SHA before `create_or_update_file`. Use `push_files` for multi-file atomic commits — BUT NEVER for Python files (corrupts newlines). Use `create_or_update_file` for all Python files.

### Active Workflows
| Workflow | Trigger | Purpose |
|----------|---------|--------|
| `nba-game-day.yml` | Every 5 min UTC 16-23 + 0-6 | Live scores, odds refresh, grading, lineup poll |
| `nba-etl.yml` | Daily UTC 09:00 | Box scores, PT stats, schedule, rosters |
| `odds-etl.yml` | Daily UTC 10:00 | Today's FanDuel lines |
| `grading.yml` | After odds-etl succeeds (workflow_run) | Grade today's props |
| `nba-backfill.yml` | Dispatched by nba-game-day when game goes Final | Odds + grade backfill for completed games |
| `refresh-lines.yml` | POST /api/refresh-lines from web app | Manual odds + grade refresh (no auth) |
| `refresh-data.yml` | POST /api/refresh-data from web app (admin passcode) | Full refresh: live box + odds + grading + lineups |

### Retired Workflows (dispatch-only)
`pregame-refresh.yml`, `nba-live.yml`, `lineup-poll.yml` — kept for manual one-off runs only. Do not re-add schedules.

### Azure Static Web Apps
- Resource: `sports-modeling-web` / URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
- Custom domains: `schnapp.bet`, `www.schnapp.bet` — SSL active, Cloudflare DNS-only (not proxied)
- Deploy: push to `main` → auto-deploys in ~90s
- Next.js 15.2.8, React 19
- Auth: passcode gate (`PasscodeGate.tsx`). `BYPASS = true` currently. Set to `false` to re-enable.
- App settings: `AZURE_SQL_CONNECTION_STRING`, `GITHUB_PAT` (workflow scope), `ADMIN_REFRESH_CODE` (admin refresh button passcode)

### Local Dev
- Laptop: Node.js v24.12.0. `npm run dev` blocked by ThreatLocker. Test by pushing to `main`.
- Repo: `C:\Users\1stLake\sports-modeling`. Git push works.
- VM: MSSQL MCP available. For git operations — write files via Windows-MCP:FileSystem, give Austin PowerShell commands to run himself.

### PWA
- Manifest: `web/public/manifest.json` — name "Schnapp", start `/nba`, standalone
- Service worker: `web/public/sw.js` — network-first HTML, cache-first static, never caches API
- Icon: `web/public/icon.svg` — `sizes: "any"` covers all modern browsers

### Route Cache Headers (next.config.mjs)
- `/api/games`, `/api/roster`, `/api/player`: `s-maxage=60, stale-while-revalidate=120`
- `/api/grades`, `/api/game-grades`: `s-maxage=90, stale-while-revalidate=180`
- `/api/contextual`: `s-maxage=600, stale-while-revalidate=1200`
- `/api/boxscore`, `/api/ping`: `no-cache, no-store`

---

## Database Schema

### Schemas
`nba`, `mlb`, `nfl`, `odds`, `common`

### Key Rules
- Snake case everywhere. `created_at DATETIME2 DEFAULT GETUTCDATE()` on every fact table.
- `DELETE` not `TRUNCATE` — FK constraints block TRUNCATE.
- `minutes` not `min` — `min` is reserved in SQL Server.
- No `FullGame` period — always SUM quarters for game totals.

### NBA Tables
- `nba.schedule` — USE THIS for game queries. ALL games regardless of status. home/away scores updated live.
- `nba.games` — completed games only (box score ETL source). Populated for `game_date <= today` and `game_status = 3`.
- `nba.teams` — hardcoded static dict in ETL
- `nba.players` — `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active), `position`
- `nba.daily_lineups` — keyed by `player_name` + `team_tricode`. No `player_id` or `team_id`. `starter_status` = 'Starter'/'Bench'/'Inactive'.
- `nba.player_box_score_stats` — PK: `(game_id, player_id, period)`. Periods: '1Q','2Q','3Q','4Q','OT' only. Columns include `fg3a`. `minutes` is DECIMAL.
- `nba.player_passing_stats` — `(player_id, game_date)`. `potential_ast`.
- `nba.player_rebound_chances` — `(player_id, game_date)`. `reb_chances`.

### Odds Tables
- `odds.event_game_map` — event_id → game_id + game_date. Written by odds_etl upcoming mode.
- `odds.upcoming_player_props` / `odds.player_props` — FanDuel lines, today and historical
- `odds.upcoming_game_lines` / `odds.upcoming_events` — today's game lines
- `odds.player_map` — maps odds player names to `nba.players.player_id`

### common.daily_grades — Schema v3 (migrated 2026-04-02)
Full columns: `grade_id`, `grade_date`, `event_id`, `game_id`, `player_id`, `player_name`, `market_key`, `bookmaker_key`, `line_value`, `outcome_name` (VARCHAR(5): 'Over'/'Under'), `over_price` (INT — direction-appropriate price), `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`, `trend_grade`, `momentum_grade`, `pattern_grade`, `matchup_grade`, `regression_grade`, `composite_grade`, `hit_rate_opp`, `sample_size_opp`, `created_at`

UNIQUE: `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value, outcome_name)`

**Critical:** `getGrades` reads `dg.outcome_name` and `dg.over_price` DIRECTLY from this table. There is NO join to odds tables for prices. The old `best_price` CTE join was removed because it attached Over prices to Under rows. Do not reintroduce it.

---

## ETL Patterns

### Core Pattern
Desired keys → existing keys (SELECT DISTINCT) → missing set → process oldest N → upsert. Idempotent.

### Upsert
`etl/db.py:upsert()` — stages to `#stage_{table}`, SQL MERGE. Never raw INSERT.

### NBA ETL
- Box scores: `playergamelogs`, 5 calls per run (one per period). Returns `fg3a`.
- OT: `Period=""` + `GameSegment=Overtime`
- All stats.nba.com calls via Webshare rotating residential proxy (`NBA_PROXY_URL`). Required from GitHub Actions IPs.
- PT stats (`leaguedashptstats`): no proxy
- Teams: hardcoded STATIC_TEAMS dict (eliminated HTTP dependency)
- Players: `playerindex` via proxy

### NBA Live ETL
- `etl/nba_live.py` — two-phase: `update_schedule()` always runs (ScoreboardV3), `update_box_scores()` gates on status=2.
- Workflow: `nba-game-day.yml` — every 5 min UTC 16:00–06:00

### Odds ETL
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly: `upcoming/nba/days-ahead=1`. Upcoming mode writes to `odds.event_game_map`.

### Lineup Poll
- `etl/lineup_poll.py` — standalone, does NOT import nba_etl.py (top-level argparse would trigger)
- Runs inside `nba-game-day.yml` odds+grading step every ~15 min when games are active
- `starter_status` values: 'Starter' (has position field), 'Inactive' (lineupStatus contains out/inactive/not with team/gtd), 'Bench' (Active roster, no position, not inactive)

### Refresh Lines
- `refresh-lines.yml` — triggered by POST to `/api/refresh-lines` via GITHUB_PAT in SWA app settings. No passcode.
- `refresh-data.yml` — triggered by POST to `/api/refresh-data`. Requires `ADMIN_REFRESH_CODE` passcode. Runs all four steps.

---

## Grading Model

### Components (all live)
- `weighted_hit_rate`: 60% × L20 hit rate + 40% × L60 hit rate. Falls back to L60 if L20 sample < 5.
- `trend_grade`: L10 mean vs L30 mean, centered at 50.
- `momentum_grade`: consecutive hit/miss streak, log-scaled.
- `pattern_grade`: historical reversal rate after runs of current streak length.
- `matchup_grade`: defense rank for player position vs today's opponent. Rank 1 = most allowed.
- `regression_grade`: z-score of recent L10 mean vs full season.
- `composite_grade`: equal-weighted average of all non-NULL components.

All components inverted for Under rows (100 - value). Rising trend is bad for an under.

### Under Grades
- Standard markets only, posted line only (no bracket expansion).
- `outcome_name = 'Under'`, `over_price` stores the Under price.
- Alternate lines are Over-only.

### Performance
- `precompute_line_grades`: iterates by `(player_id, market_key)` pair, loads stat sequence once, fans across line values. ~560 outer iterations vs ~6200 previously.

---

## Web Application

### CANONICAL UI LAYOUTS — do not revert without checking CHANGELOG.md

**All stat tables — compact vs all-stats toggle:**
- Compact (default): Player/Date/Opp, MIN, PTS, 3PT (3PM only as plain avg in StatsTable; 3PM-3PA in game log), REB, AST, PRA, PR, PA, RA
- All Stats: adds FG (FGM-FGA), 3PA (separate column), FT (FTM-FTA), STL, BLK, TOV
- Toggle button: period filter bar (player page + boxscore), filter bar (stats)
- Companion values (REB-RebChances, AST-PotAst): player game log only, full game only

**Player game log (`PlayerPageInner.tsx`):**
| Col | Value | Notes |
|-----|-------|-------|
| Date | `gameDate.slice(5)` | MM-DD |
| Opp | `@abbr` or `abbr` | |
| MIN | `*21:49` / `21:49` / `DNP` | `*` = starter |
| PTS | integer | prop colored |
| FG | `fgm-fga` | all-stats only — NOT % — DASH separator |
| 3PT | `fg3m-fg3a` | e.g. `3-8` — NOT % — DASH separator |
| REB | `reb-rebChances` | second value when full game only |
| AST | `ast-potentialAst` | second value when full game only |
| PRA | `pts+reb+ast` | prop colored |
| PR | `pts+reb` | prop colored |
| PA | `pts+ast` | prop colored |
| RA | `reb+ast` | prop colored |
| STL | integer | all-stats only, prop colored |
| BLK | integer | all-stats only, prop colored |
| TOV | integer | all-stats only |
| FT | `ftm-fta` | all-stats only — DASH separator |

**Stats table (`StatsTable.tsx`):**
Compact: Player, GP, MIN, PTS, 3PM (plain avg), REB, AST, PRA, PR, PA, RA. All Stats adds FG (avgFgm-avgFga), 3PA (plain avg, separate col), FT (avgFtm-avgFta), STL, BLK, TOV. colSpanTotal: compact=11, all-stats=17.

**VS Defense (`MatchupDefense.tsx`):**
Column order: PTS, 3PM, REB, AST, STL, BLK, TOV. Matches game log order.

**Today's Props (`PlayerPageInner.tsx` — TodayPropsSection):**
- Horizontal strip: one cell per market (label, posted line, composite grade). `flex w-full divide-x`, each cell `flex-1 min-w-[52px]`. No `min-w-max` on wrapper, no `border-t` on the strip div.
- Tapping a cell expands a panel below containing: full-width SVG dot plot (`preserveAspectRatio="none"`, 600-wide viewBox, oldest-left newest-right, green=hit red=miss), then alt lines with two-row detail.
- Standard line detail rows removed from panel — the strip cell already shows posted line + grade.
- `TodayPropsSection` accepts `summaries: GameSummary[]` prop.
- `getGrades` reads `dg.outcome_name` + `dg.over_price` directly — no join to odds tables.

**At a Glance (`GradesPageInner.tsx`):**
- Default min odds: -600. Slider range: -1000 to +200. Reset button goes to -1000 (shows everything).
- `oddsFilterActive` = `minOdds > ODDS_MIN` (-1000). Do not change this condition.
- Over/Under toggle filters on `r.outcomeName`.
- Refresh Data button (RefreshDataButton component) requires admin passcode.

### API Routes
- `/api/ping` — public (anonymous). SELECT 1. Used by Uptime Robot for DB keep-alive.
- `/api/grades?date=&gameId=` — reads `dg.outcome_name` + `dg.over_price` directly
- `/api/team-averages` — returns `avgFgm`, `avgFga`, `avg3pm`, `avg3pa`, `avgFtm`, `avgFta` plus standard stats
- `/api/contextual?oppTeamId=&position=` — defense ranks. Rank 1 = most allowed.
- `/api/refresh-lines` POST — triggers `refresh-lines.yml` via GITHUB_PAT (no passcode)
- `/api/refresh-data` POST — validates `ADMIN_REFRESH_CODE`, triggers `refresh-data.yml`
- `/api/refresh-status?runId=` — polls workflow run status
- `/api/live-boxscore?gameId=` — proxies BoxScoreTraditionalV3 from stats.nba.com server-side

### Navigation
- `/nba` — game strip + tabs
- `/nba?gameId=&tab=` — active game (Live tab when gameStatus=2)
- `/nba/player/[playerId]?gameId=&tab=&opp=&date=` — player page
- `/nba/grades?date=` — At a Glance

---

## Decision Log

| Decision | Rationale |
|----------|-----------|
| Next.js API routes, not Azure Functions | SWA promotes them automatically. One repo, one deploy. |
| Azure SWA | Free tier, native GHA integration. |
| mssql driver, not ORM | Full SQL control, all queries in one file. |
| Passcode gate, not GitHub auth | Small known group; passcode simpler than OAuth. |
| PWA, not native app | No App Store, no distribution friction. |
| SVG icon `sizes: "any"` | Covers all modern browsers without PNG generation. |
| Service worker never caches API routes | Live data must never be stale. |
| Stats tab uses `nba.players` not `nba.daily_lineups` | Lineup table empty until lineup-poll runs; players always populated. |
| Full boxscore fetched once, filtered client-side | Period filter must be instant. |
| `fast_executemany=False` in grading | `True` truncates NVARCHAR(MAX). |
| `mssql` in `serverExternalPackages` | Prevents Next.js bundling native bindings. |
| `minutes` not `min` | `min` is reserved in SQL Server. |
| DELETE not TRUNCATE | FK constraints block TRUNCATE. |
| FanDuel only | Most complete prop line coverage. |
| Teams dict hardcoded | Eliminated HTTP dependency after proxy failures. |
| `lineup_poll.py` standalone | `nba_etl.py` top-level argparse triggers on import. |
| Live box score via DB, not direct browser call | Browser never hits stats.nba.com. |
| GITHUB_PAT in SWA app settings | SWA API routes cannot use build-time secrets. |
| `getGrades` reads `dg.over_price` directly | Old `best_price` CTE join attached Over prices to Under rows, showing them in Over tab. |
| `outcome_name` in daily_grades UNIQUE key (v3) | Allows Over + Under rows for same player/market/line. |
| Str column removed from game log | Only showed DNP; starter status now in MIN column with `*` prefix. |
| FG/3PT as made/att ratios with dash separator, not % | More useful for prop research. Dash chosen over slash. Applies to both game log and stats table. |
| `precompute_line_grades` iterates by player-market pair | Eliminates ~10x redundant DataFrame reads. |
| Under component grades inverted | Rising trend/momentum/good matchup is bad for an under bet. |
| Cloudflare DNS-only, not proxied | Azure SWA requires direct DNS resolution for SSL issuance. |
| `nba.games` uses `game_date <= today` | Changed from `< today` — today's finals must enter `nba.games` so FK allows same-day box score writes. |
| Inactive detection uses `lineupStatus` keywords | `rosterStatus='Active'` alone cannot distinguish injured/inactive players from available bench players. |
| Compact/all-stats toggle instead of always showing all columns | Reduces visual noise on mobile; PRA/PR/PA/RA are the most useful default prop research columns. |
| Companion values (rebChances, potentialAst) on player page only | Team views don't have per-player PT stat data in team-averages API; not worth the join complexity. |
| VS Defense combo columns deferred | Requires extending /api/contextual to compute sum-stat defense averages — non-trivial query change. |
| At a Glance default min odds -600 | Filters out extreme chalk lines (-800, -1000, -5000) that have no betting value. Slider reaches -1000. |
| RosterTable badge: Confirmed only when lineupStatus=Confirmed | Old logic showed Confirmed whenever lineup wasn't Projected, including null (not-yet-confirmed). |
| StatsTable 3PM/3PA split into separate columns | Made-att ratio in compact was not useful; plain averages in separate columns are more readable. |
| Self-hosted runner on Azure VM | Eliminates 25-40s ODBC install overhead per run. ETL drops from 2-4 min to ~25 seconds. No local machine dependency. Low-latency to Azure SQL. |
| Uptime Robot replaces keepalive.yml | Free, no runner minutes consumed, simpler than a workflow for an HTTP ping. |
| DB-timestamp gate replaces run_number % 3 | Drift-proof: checks actual time since last grade instead of a counter that shifts with manual dispatches. |
| grading.yml uses workflow_run trigger | Grading starts immediately after odds finishes, not after a fixed 30-min buffer that can be wrong in both directions. |
| Backfill isolated to nba-backfill.yml | Prevents expensive backfill competing with live score bandwidth in the same 12-min job window. |
| requirements.txt pinned to exact versions | Prevents silent breakage from upstream package releases. |
| VM B2s_v2 initially, downsize to B1s_v2 after trial | Free trial credits cover B2s_v2; B1s_v2 (~$15-20/month) is sufficient for I/O-bound ETL workloads. |
| RefreshDataButton uses separate ADMIN_REFRESH_CODE | Keeps admin refresh distinct from user passcodes; allows sharing app without exposing the refresh trigger. |
| NEVER use push_files for Python files | push_files serializes content with literal \n strings instead of real newlines, producing a single-line file that fails py_compile. Always use create_or_update_file for .py files. |
| refresh-data.yml separate from refresh-lines.yml | refresh-lines is unauthenticated (odds+grading only, used by old button); refresh-data is admin-passcode-gated and runs all four steps. |
