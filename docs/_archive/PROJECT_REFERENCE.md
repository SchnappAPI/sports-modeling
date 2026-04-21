> **ARCHIVED 2026-04-20.** This file is preserved for historical reference only and is no longer authoritative.
>
> The active documentation now lives at:
> - `/docs/README.md` — router to everything
> - `/docs/SESSION_PROTOCOL.md` — session protocol that replaces the one at the top of this file
> - `/docs/DECISIONS.md` — ADR log (ADR-0001 captures the rationale for this restructure)
> - Component READMEs under `/etl/<sport>/`, `/database/<sport>/`, `/web/<sport>/`, `/infrastructure/`
>
> Do not update this file. For ongoing work, update the component README or append to `/docs/CHANGELOG.md`. See ADR-0016 for the archive decision.

---

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

## Current State (updated 2026-04-10)

**What is working:**
- NBA data pipeline fully active. Box scores, live updates, odds, lineup poll, grading all running.
- Lineup poll: two-stage architecture. Stage 1 = official JSON starters (5/team). Stage 2 = boxscorepreviewv3 full roster. PREVIEW_TIMEOUT=20s, no retry.
- Live box scores: nba_live.py uses todaysScoreboard CDN. LiveBoxScore.tsx refreshes every 30s. Flask /scoreboard route on runner.
- At a Glance: list view + matrix view (PropMatrix.tsx). Help panel (HelpPanel.tsx) with ? button in header.
- Player page: full game log, splits, VS Defense, Today's Props strip with dot plot, signal chips.
- Grading v3: daily_grades has outcome_name + over_price. Over and Under rows written. UNIQUE key v3 includes outcome_name.
- Signal system: shared signals.ts. Signals split into player-level (HOT/COLD/DUE/FADE) and line-level (STREAK/DUE). STREAK fires when momentum_grade > 70. DUE (formerly SLUMP) fires when momentum_grade > 65 AND hr60 >= 0.35 for miss streak bounce-back.
- Personal pattern table: common.player_line_patterns populated with 27,765 rows (lag-1 transition probabilities per player-line combo). Updated nightly by compute-patterns.yml.
- Grading uses personal patterns: precompute_line_grades() looks up p_hit_after_hit / p_hit_after_miss from common.player_line_patterns. Fallback to season hit rate when no pattern exists.
- Refresh Data button live (At a Glance + player page header).
- Auth: common.user_codes, common.user_activations, common.demo_config. PasscodeGate.tsx. /admin page.
- MLB web layer: game strip + box score tables. MLB ETL backfilled 2023-2026.
- Schnapp Ops MCP: FastMCP on port 8000, Cloudflare tunnel at https://mcp.schnapp.bet/mcp. 8 tools including shell_exec and read_file.
- Cron gap fixed: nba-game-day.yml covers 22:00-23:59 UTC with */15 cron entry.
- NBA odds backfill confirmed complete (Mar 24-Apr 3). Mappings ran — 135 remain unmapped, all inactive, not blocking grading.

**Known issues / pending:**
- compute_patterns.py first run took 17 min (looped batched inserts). Fixed to single executemany call — verify next nightly run completes under 2 min.
- Signal backtest pending re-run in 2-3 weeks once personal pattern grades have accumulated more resolved outcomes.
- After MCP reconnects to new VM, verify Schnapp Ops tools (flask_status, live_scoreboard) work end-to-end.

**Build status:** Green. Last successful deploy: commit 1b622f7.

**Grading pipeline dependency order:**
1. odds-etl.yml — fetches FanDuel lines
2. grading.yml (triggered by workflow_run after odds) — runs grade_props.py run_upcoming
3. compute-grade-outcomes.yml — resolves Won/Lost after games finish
4. compute-patterns.yml — nightly at 07:30 UTC, updates player_line_patterns from resolved outcomes


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
- **Self-hosted runner:** `schnapp-runner` Azure VM (Central US, Standard B1s, Ubuntu 24.04). IP: `172.173.126.81`. Admin: `schnapp-admin` / `Sports#2026VM`.
- Runner service: systemd, starts on boot, always online. Python venv at `~/venv` with pinned deps pre-installed. ODBC Driver 18 pre-installed.
- All active workflows use `runs-on: [self-hosted, schnapp-runner]`. No ODBC or pip install steps in any workflow.
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10`, `GITHUB_PAT`, `MCP_AUTH_TOKEN`, `GH_PAT`
- Always fetch file SHA before `create_or_update_file`. Use `push_files` for multi-file atomic commits — BUT NEVER for Python files OR TSX files with non-ASCII Unicode characters. Both cause corruption. Use `create_or_update_file` for all Python files and any TSX with Unicode symbols.

### Flask Runner on VM
- `etl/runner.py` — lightweight Flask service on VM, port 5000. Systemd service: `schnapp-flask.service`.
- Serves `/ping` (health), `/scoreboard` (today's game statuses from CDN), `/boxscore?gameId=` (live player stats + score).
- Auth: `X-Runner-Key: runner-Lake4971` header required on all endpoints.
- CDN endpoints (both public, no proxy): scoreboard `todaysScoreboard_00.json`, boxscore `boxscore_{game_id}.json`.
- `/scoreboard` response per game: `gameId`, `gameStatus`, `gameStatusText`, `period`, `gameClock`, `homeTeamId`, `homeTeamAbbr`, `homeScore`, `awayTeamId`, `awayTeamAbbr`, `awayScore`.
- `/boxscore` response includes: `gameStatusText`, `homeScore`, `awayScore`, `homeTeamAbbr`, `awayTeamAbbr`, `players[]`. Each player has `starter` (bool), `oncourt` (bool).
- `NBA_PROXY_URL` remains in systemd env but is unused by runner.py.

### MCP Server on VM
- `mcp/server.py` — FastMCP server, port 8000, bound to 127.0.0.1. Systemd service: `schnapp-mcp.service`.
- Tools: `flask_status`, `flask_restart`, `live_scoreboard`, `live_boxscore`, `workflow_trigger`, `workflow_status`.
- Exposed via Cloudflare Tunnel: `https://mcp.schnapp.bet/mcp`. Connected as "Schnapp Ops" in claude.ai.
- MCP venv: `~/mcp-venv`. Re-run `install-mcp.yml` after any change to `mcp/server.py`.
- WorkingDirectory for schnapp-mcp.service: `/home/schnapp-admin/sports-modeling` (direct clone, not actions-runner work dir).

### Active Workflows
| Workflow | Trigger | Purpose |
|----------|---------|--------|
| `nba-game-day.yml` | 09:30 UTC daily + every 15 min 00:00-06:00 + every 15 min 22:00-23:59 UTC | Live scores, odds refresh, grading, lineup poll |
| `nba-etl.yml` | Daily UTC 09:00 | Box scores, PT stats, schedule, rosters |
| `odds-etl.yml` | Daily UTC 10:00 | Today's FanDuel lines |
| `grading.yml` | After odds-etl succeeds (workflow_run) | Grade today's props |
| `nba-backfill.yml` | Dispatched by nba-game-day when game goes Final | Odds + grade backfill for completed games |
| `refresh-lines.yml` | POST /api/refresh-lines from web app | Manual odds + grade refresh (no auth) |
| `refresh-data.yml` | POST /api/refresh-data from web app (admin passcode) | Full refresh: live box + odds + grading + lineups |
| `restart-flask.yml` | workflow_dispatch | Restart schnapp-flask.service on VM, smoke test /ping |
| `install-mcp.yml` | workflow_dispatch | Install/update MCP server on VM |
| `compute-patterns.yml` | Nightly 07:30 UTC + workflow_dispatch | Update player_line_patterns from resolved outcomes |

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
- `nba.schedule` — USE THIS for game queries. ALL games regardless of status. home/away scores updated live by nba_live.py.
- `nba.games` — completed games only (box score ETL source). Populated for `game_date <= today` and `game_status = 3`.
- `nba.teams` — hardcoded static dict in ETL
- `nba.players` — `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active), `position` (may be compound: G-F, F-G, C-F, F-C)
- `nba.daily_lineups` — keyed by `player_name` + `team_tricode`. No `player_id` or `team_id`. `starter_status` = 'Starter'/'Bench'/'Inactive'. Position values are full strings: PG, SG, SF, PF, C (from official lineup JSON for starters). Historical data preserved — DELETE only runs for games in current poll cycle.
- `nba.player_box_score_stats` — PK: `(game_id, player_id, period)`. Periods: '1Q','2Q','3Q','4Q','OT' only. Columns include `fg3a`. `minutes` is DECIMAL. Period column is VARCHAR(2) — do not insert values longer than 2 chars.
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

### common.player_line_patterns
PK: `(player_id, market_key, line_value)`. Columns: `n`, `hr_overall`, `p_hit_after_hit`, `p_hit_after_miss`, `hit_momentum`, `miss_momentum`, `pattern_strength`, `is_momentum_player` (BIT), `is_reversion_player` (BIT), `is_bouncy_player` (BIT), `last_updated`.
- MIN_GAMES=10 to create a row. MIN_TRANSITION_OBS=3 per state for p_hit_after_hit/p_hit_after_miss.
- BIT columns: use CAST(col AS INT) in any SUM() — SQL Server does not allow SUM on BIT directly.
- Updated nightly by compute-patterns.yml.

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
- `etl/nba_live.py` — two-phase: `update_schedule()` always runs (CDN scoreboard, no proxy), `verify_live_box_scores()` logs CDN availability for in-progress games (no DB write).
- Schedule source: `https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json` — public, no proxy, no auth. Replaced ScoreboardV3 via proxy.
- Live box scores served directly from NBA CDN by Flask runner, not written to DB.
- Workflow: `nba-game-day.yml`

### Games API — today vs historical
- `/api/games?date=today` — calls Flask `/scoreboard` (CDN, live, no DB). Falls back to DB in 5s if Flask unreachable.
- `/api/games?date=other` — queries `nba.schedule` via `getGames()`. Returns `homeScore`/`awayScore` from DB.
- `todayET()` in route computes today's NBA calendar date in ET to avoid UTC midnight boundary issues.

### Odds ETL
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly: `upcoming/nba/days-ahead=1`. Upcoming mode writes to `odds.event_game_map`.

### Lineup Poll
- `etl/lineup_poll.py` — two-stage: Stage 1 fetches official JSON (starters only, 5 per team, lineup_status Confirmed/Projected). Stage 2 always fetches boxscorepreviewv3 for full roster (bench + inactive). Stage 1 starter designations override Stage 2 for overlapping players.
- PREVIEW_TIMEOUT=20s, BETWEEN_GAMES_DELAY=0.5s. No retry on preview path (prevents timeout).
- Position values in daily_lineups are full strings (PG, SG, SF, PF, C). Use posToGroup() mapping — NOT position[0] — when grouping by G/F/C. PG/SG→G, SF/PF→F, C→C.
- Runs inside `nba-game-day.yml` every cycle. Also runs in `refresh-data.yml`.

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

**Live tab (`LiveBoxScore.tsx`):**
- Score header at top: AWY abbr + score (left), pulsing Live dot + period/clock (center), HME abbr + score (right). Leading score brighter.
- Player rows: green dot on `oncourt` players. `starter` boolean from CDN (not from DB lineup).
- Starters/Bench sections based on CDN `starter` field. Refreshes every 30s.

**Game strip (`GameStrip.tsx`):**
- Live/Final with scores: shows `awayScore awayAbbr @ homeAbbr homeScore`. Leading score `text-gray-100`, trailing `text-gray-500`. Home wins ties for brightness.
- Upcoming: shows `AWY @ HME` matchup row + spread/total row below.
- `Game` interface includes `homeScore`, `awayScore`, `period`, `gameClock` (all nullable).
- Do not revert to spread-only display for live/final games.

**Matchups tab (`MatchupGrid.tsx` + `/api/matchup-grid`):**
- Two defense panels side by side (away defense left, home defense right).
- Rows: G / F / C position groups. Columns: PTS, REB, AST, 3PM, STL, BLK, TOV.
- Each cell: season avg allowed + rank out of 30. Green rank 1-10 (soft/exploitable), red rank 21-30 (tough).
- Tap row to expand — shows today's active players at that position facing that defense. Player links to player page.
- `posToGroup()` uses exact IN() matches for PG/SG/SF/PF/C, then LEFT(1) for compound values (G-F→G, F-G→F, C-F→C). DO NOT use position[0] or LEFT(position,2) for G/F detection.
- Panel labels: "vs AWY Defense" shows home team players (they attack AWY). "vs HME Defense" shows away team players.
- Lineup: starters use `dl.position` (game-specific PG/SG/SF/PF/C), bench uses `COALESCE(p.position, dl.position)`.

### API Routes
- `/api/ping` — public (anonymous). SELECT 1. Used by Uptime Robot for DB keep-alive.
- `/api/games?date=` — today: Flask CDN path (live, no DB). Other dates: DB path with scores.
- `/api/grades?date=&gameId=` — reads `dg.outcome_name` + `dg.over_price` directly
- `/api/team-averages` — returns `avgFgm`, `avgFga`, `avg3pm`, `avg3pa`, `avgFtm`, `avgFta` plus standard stats
- `/api/contextual?oppTeamId=&position=` — defense ranks. Rank 1 = most allowed.
- `/api/matchup-grid?gameId=` — both teams' defense by position group + today's lineup players.
- `/api/refresh-lines` POST — triggers `refresh-lines.yml` via GITHUB_PAT (no passcode)
- `/api/refresh-data` POST — validates `ADMIN_REFRESH_CODE`, triggers `refresh-data.yml`
- `/api/refresh-status?runId=` — polls workflow run status
- `/api/live-boxscore?gameId=` — calls VM Flask at `http://172.173.126.81:5000/boxscore?gameId=`. Flask fetches CDN directly.

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
| Live box score via Flask CDN proxy, not DB | DB path had VARCHAR column constraints blocking Unicode player names; CDN is faster and always fresh. |
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
| VM migrated to B1s Central US (schnapp-runner-2) | Old B2s_v2 West US 2 replaced. B1s is sufficient for I/O-bound ETL. Central US chosen for new VM creation. |
| RefreshDataButton uses separate ADMIN_REFRESH_CODE | Keeps admin refresh distinct from user passcodes; allows sharing app without exposing the refresh trigger. |
| NEVER use push_files for Python files | push_files serializes content with literal \n strings instead of real newlines, producing a single-line file that fails py_compile. Always use create_or_update_file for .py files. |
| NEVER use push_files for TSX with non-ASCII Unicode | push_files also corrupts non-ASCII characters (arrows ▲▼, em dash —) in TSX, causing client-side JavaScript crash on load. Use create_or_update_file for any TSX with Unicode symbols. |
| refresh-data.yml separate from refresh-lines.yml | refresh-lines is unauthenticated (odds+grading only, used by old button); refresh-data is admin-passcode-gated and runs all four steps. |
| posToGroup() uses exact IN() then LEFT(1) for compound positions | position[0] gives P/S/C — none match G or F. LEFT(2) misses G-F, F-G compound values. LEFT(1) correctly maps first character of compound position to primary group. |
| Lineup poll Stage 2 always runs | Official JSON only has 5 starters per team. Skipping Stage 2 when Stage 1 has data left bench and inactive unwritten. |
| Lineup poll PREVIEW_TIMEOUT=20s, no retry | 60s timeout × 3 retries × 3 games exceeded 5-minute refresh-data timeout. Single 20s attempt is sufficient — 404 on live games is expected and handled. |
| CDN boxscore endpoint instead of BoxScoreTraditionalV3 | stats.nba.com V3 returned homeTeam=null for in-progress games from VM IPs even with proxy. CDN is public, no proxy, returns full cumulative player stats reliably. |
| nba_live.py does not write live rows to DB | DB write required period='G' (4 chars, exceeded VARCHAR(2)) and player_name with Unicode caused truncation. Flask CDN path is strictly better — always fresh, no storage needed. |
| period column is VARCHAR(2) | Valid values: '1Q','2Q','3Q','4Q','OT'. Do not insert longer strings. |
| CDN scoreboard replaces ScoreboardV3 in nba_live.py | todaysScoreboard_00.json is public CDN, no proxy. ScoreboardV3 required Webshare proxy — any proxy hiccup broke game status tracking. Same data, better reliability. |
| /api/games uses Flask CDN for today, DB for historical | Flask /scoreboard returns live CDN data with no DB round trip — eliminates cron gap problem for game strip. Historical dates still need DB (CDN only has today). 5s timeout with DB fallback. |
| homeScore/awayScore added to GameRow and getGames | nba.schedule already stored scores from nba_live.py writes. They were never returned to the app. Historical game strip now shows final scores. |
| GameStrip shows scores not odds for live/final games | Spread and total are null for today (Flask path has no odds data). Scores are the useful signal during and after games. |
| MCP auth via Cloudflare tunnel, not bearer token | claude.ai connector UI only supports OAuth fields, not arbitrary bearer tokens. Cloudflare tunnel credential already secures the endpoint — adding a bearer token layer is redundant. |
| GH_PAT not GITHUB_PAT as secret name | GitHub reserves the GITHUB_ prefix for built-in secrets. Workflow inputs are also not masked in logs — always use repo secrets for tokens. |
| Player headshot from CDN, client-side only | No ETL needed. URL pattern is `cdn.nba.com/headshots/nba/latest/260x190/{player_id}.png`. CDN returns silhouette placeholder for missing players — no error handling required. |
| gameLineupPosition preferred over nba.players.position for matchup defense | Starters get precise PG/SG/SF/PF/C from official NBA lineup JSON. nba.players.position may be compound (G-F) which is less precise for defense bucket matching. |
| nba.daily_lineups preserves historical data | DELETE only runs for games in the current poll cycle (non-final games). Final game rows persist. Starter/bench split for defense analysis is possible from this table. |
| SUM(BIT) not allowed in SQL Server | Must CAST BIT columns to INT before aggregating: SUM(CAST(is_momentum_player AS INT)). Applies to all BIT columns in common.player_line_patterns. |
| schnapp-mcp.service WorkingDirectory is ~/sports-modeling, not actions-runner work dir | On initial VM setup the actions-runner work dir doesn't exist until the first job runs. Direct clone is always present. |
