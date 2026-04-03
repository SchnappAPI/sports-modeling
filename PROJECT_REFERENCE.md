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

## Current State (updated 2026-04-03)

**What is working:**
- NBA data pipeline fully active. Box scores, live updates, odds, lineup poll, pre-game refresh, Refresh Lines all running.
- Grading: all components live, Over + Under grades written daily.
- Web: all views live at schnapp.bet. Player page, stats tab, At a Glance, matchup defense, grades all functional.
- PWA active. Install via Safari Share → Add to Home Screen.

**Known issues:**
- Box score tab and live stats tab not loading — runtime error not yet diagnosed. Need browser error text to fix.
- PasscodeGate `BYPASS = true` — gate disabled for dev. Re-enable before sharing with users (`PasscodeGate.tsx`).
- Odds/grading backfill gap — pre-April 2026 dates still being backfilled by nightly chain.
- NFL workflow missing — `nfl_etl.py` exists, `nfl-etl.yml` does not.
- NFL and MLB `run_mappings` not implemented in odds_etl.py — only NBA branch exists.
- Auto-pause delay locked — free database offer prevents changing it. Keep-alive workflow mitigates.
- PNG icons not generated — SVG covers all modern browsers; generate via `web/scripts/generate-icons.mjs` if needed.

**Next up:**
- Step 14: Mobile-first UI redesign — bottom nav, card-based grades list, swipeable game view.
- Step 15: MLB ETL and web views.
- Step 16: NFL ETL automation and web views.
- Diagnose box score / live stats breakage (need browser error text first).

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
- Keep-alive: `keepalive.yml` pings `/api/ping` every 45 min UTC 10:00–05:00

### GitHub Actions
- Repo: `SchnappAPI/sports-modeling` (private)
- All automated Python runs here. Cannot run locally (ThreatLocker) or scheduled from VM (auto-logout).
- Runners: ephemeral ubuntu-latest. ODBC Driver 18 + pip deps installed fresh each run.
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10`
- Always fetch file SHA before `create_or_update_file`. Use `push_files` for multi-file atomic commits.

### Azure Static Web Apps
- Resource: `sports-modeling-web` / URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
- Custom domains: `schnapp.bet`, `www.schnapp.bet` — SSL active, Cloudflare DNS-only (not proxied)
- Deploy: push to `main` → auto-deploys in ~90s
- Next.js 15.2.8, React 19
- Auth: passcode gate (`PasscodeGate.tsx`). `BYPASS = true` currently. Set to `false` to re-enable.
- App settings: `AZURE_SQL_CONNECTION_STRING`, `GITHUB_PAT` (workflow scope, for Refresh Lines)

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
- `nba.games` — completed games only (box score ETL source)
- `nba.teams` — hardcoded static dict in ETL
- `nba.players` — `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active), `position`
- `nba.daily_lineups` — keyed by `player_name` + `team_tricode`. No `player_id` or `team_id`. `starter_status` = 'Starter'/'Bench'.
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
- `etl/nba_live.py` — gate: `game_status = 2`. ScoreboardV3 → scores, BoxScoreTraditionalV3 → stats.
- Workflow: `nba-live.yml` — every 5 min UTC 17:00–06:00

### Odds ETL
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly: `upcoming/nba/days-ahead=1`. Upcoming mode writes to `odds.event_game_map`.

### Lineup Poll
- `etl/lineup_poll.py` — standalone, does NOT import nba_etl.py (top-level argparse would trigger)
- Workflow: `lineup-poll.yml` — every 15 min UTC 16:00–03:59

### Pre-Game Refresh
- `pregame-refresh.yml` — every 30 min UTC 14:00–03:30
- Gate: non-final game starting within 3 hours. When passes: odds_etl upcoming → grade_props upcoming.

### Refresh Lines
- `refresh-lines.yml` — triggered by POST to `/api/refresh-lines` via GITHUB_PAT in SWA app settings.

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

**Player game log (`PlayerPageInner.tsx`):**
| Col | Value | Notes |
|-----|-------|-------|
| Date | `gameDate.slice(5)` | MM-DD |
| Opp | `@abbr` or `abbr` | |
| MIN | `*21:49` / `21:49` / `DNP` | `*` = starter |
| PTS | integer | prop colored |
| FG | `fgm/fga` | e.g. `7/14` — NOT % |
| 3PT | `fg3m/fg3a` | e.g. `3/8` — NOT % |
| REB | `reb/rebChances` | second value when available |
| AST | `ast/potentialAst` | second value when available |
| STL | integer | prop colored |
| BLK | integer | prop colored |
| TOV | integer | |
| FT | `ftm/fta` | |

No Str column. `fg3a` flows: `nba.player_box_score_stats.fg3a` → `getPlayerGames` (includes `pbs.fg3a`) → `PlayerGameRow.fg3a` → `buildGameSummaries` accumulates `fg3a` → rendered via `fmtS(g.fg3m, g.fg3a)`.

**Stats table (`StatsTable.tsx`):**
| Col | Value | Notes |
|-----|-------|-------|
| Player | link | |
| GP | integer | |
| MIN | `avgMin` | 1 decimal |
| PTS | `avgPts` | 1 decimal |
| FG | `avgFgm/avgFga` | e.g. `7.1/14.8` — NOT % |
| 3PT | `avg3pm/avg3pa` | e.g. `2.1/5.6` — NOT % |
| REB | `avgReb` | |
| AST | `avgAst` | |
| STL | `avgStl` | |
| BLK | `avgBlk` | |
| TOV | `avgTov` | |

`fmtRatio()` helper used for FG and 3PT. `fmtPct()` is NOT used for these columns. API returns `avgFgm`, `avgFga`, `avg3pm`, `avg3pa`. Starters first, bench collapsed (`benchOpen` state, tappable row).

**At a Glance (`GradesPageInner.tsx`):**
- `r.overPrice != null` gates all display
- Over/Under toggle filters on `r.outcomeName` ('Over'/'Under')
- Prices and direction come from `dg.outcome_name` + `dg.over_price` directly — no odds table join

### API Routes
- `/api/ping` — public (anonymous). SELECT 1. Used by keep-alive.
- `/api/grades?date=&gameId=` — reads `dg.outcome_name` + `dg.over_price` directly
- `/api/team-averages` — returns `avgFgm`, `avgFga`, `avg3pm`, `avg3pa` in addition to standard stats
- `/api/contextual?oppTeamId=&position=` — defense ranks. Rank 1 = most allowed.
- `/api/refresh-lines` POST — triggers `refresh-lines.yml` via GITHUB_PAT
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
| FG/3PT as made/att ratios, not % | More useful for prop research than shooting percentage. Applies to both game log and stats table. |
| `precompute_line_grades` iterates by player-market pair | Eliminates ~10x redundant DataFrame reads. |
| Under component grades inverted | Rising trend/momentum/good matchup is bad for an under bet. |
| Cloudflare DNS-only, not proxied | Azure SWA requires direct DNS resolution for SSL issuance. |
