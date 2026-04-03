# Sports Modeling — Project Reference

> **For Claude:** Read this at the start of every session. Update it immediately whenever infrastructure, scripts, schema, or build status changes. This is the single source of truth. All prior documents are retired.
>
> Written for how you actually use it: dense, factual, no padding. Every section answers a specific question you would otherwise have to re-derive. The decision log captures non-obvious choices that caused pain before — do not re-ask those questions or suggest approaches already ruled out.

---

## 1. What This Is

Personal sports intelligence app for NBA, NFL, and MLB prop betting research. Primary use case is mobile — small group of users install it as a PWA from Safari. Desktop also supported. Loads each morning, shows today's games, lets you drill into any player's stats and historical averages in one tap, surfaces the highest-probability prop bets. No manual lookups.

Three layers:
- **Data:** Azure SQL fed by nightly GitHub Actions ETL pipelines
- **Intelligence:** Grading model scoring player props against historical patterns
- **Presentation:** React/Next.js web app on Azure Static Web Apps — replaces Power BI as the primary consumption surface

---

## 2. Infrastructure

### Azure SQL
- Server: `sports-modeling-server.database.windows.net` / Database: `sports-modeling` / Login: `sqladmin`
- Tier: General Purpose Serverless (GP_S_Gen5_2) — auto-pauses; first connection of the day takes 20–60s
- Free database offer is Applied — auto-pause delay cannot be changed while free offer is active
- Firewall: `0.0.0.0–255.255.255.255` under Selected Networks (required for GitHub Actions)
- Allow Azure Services: must remain ON
- Connection: SQLAlchemy + pyodbc, ODBC Driver 18, `fast_executemany=True` (grading uses `False` to prevent NVARCHAR(MAX) truncation)
- Retry logic: 3 attempts, 45s wait — handles auto-pause resume
- MSSQL MCP (mssql-mcp:ExecuteSql) available on VM only; ThreatLocker blocks it on corporate machine.
- Cold start mitigation: `/api/ping` is public (anonymous role). Keep-alive workflow (`keepalive.yml`) pings it every 45 min UTC 10:00–05:00.

### GitHub Actions
- Repo: `SchnappAPI/sports-modeling` (private)
- All automated Python runs here. Python cannot run locally (ThreatLocker) or on a schedule from the Windows VM (auto-logout).
- Runners: ephemeral ubuntu-latest; every run installs ODBC Driver 18 and pip deps fresh
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10`
- Always retrieve file SHA before `create_or_update_file`

### Azure Static Web Apps
- Resource name: `sports-modeling-web`
- URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
- Custom domains: `schnapp.bet` and `www.schnapp.bet` — both validated, SSL active
- DNS: Cloudflare. CNAME records with DNS only (not proxied). TXT `_dnsauth` record present.
- Resource group: `sports-modeling` / Region: Global / SKU: Free
- Deploy workflow: `.github/workflows/azure-static-web-apps-red-smoke-0bbe1fb10.yml`
- Triggers on push to `main`; `app_location: /web`
- Next.js 15.2.8, React 19
- Auth: Passcode gate (`PasscodeGate.tsx`). `BYPASS = true` currently set — gate disabled for dev. Set to `false` to re-enable.
- DB connection string in SWA application settings — env var: `AZURE_SQL_CONNECTION_STRING`
- **GITHUB_PAT** (workflow scope) added to SWA app settings — required for Refresh Lines button.
- Status: **Steps 1–13 complete. PWA configured.**

### Local Dev
- Node.js v24.12.0 installed on laptop. `npm run dev` blocked by ThreatLocker (next.cmd blocked).
- Testing: push to `main`, check live SWA URL. ~90 seconds per deploy cycle.
- Local repo at `C:\Users\1stLake\sports-modeling`. Git push works.

### PWA
- Manifest: `web/public/manifest.json` — name "Schnapp", starts at `/nba`, standalone display
- Service worker: `web/public/sw.js` — network-first HTML, cache-first static, never caches API routes
- Icon: `web/public/icon.svg` — dark background with white "S", `sizes: "any"` in manifest
- Install: open site in Safari on iPhone, tap Share, Add to Home Screen

### Route caching (next.config.mjs)
- `/api/games`, `/api/roster`, `/api/player`: `s-maxage=60, stale-while-revalidate=120`
- `/api/grades`, `/api/game-grades`: `s-maxage=90, stale-while-revalidate=180`
- `/api/contextual`: `s-maxage=600, stale-while-revalidate=1200`
- `/api/boxscore`, `/api/ping`: `no-cache, no-store`

---

## 3. Repository Structure

```
.github/workflows/
  nba-etl.yml          # Nightly cron + manual dispatch. ACTIVE
  nba-live.yml         # Every 5 min UTC 17:00-06:00. Live box score. ACTIVE
  mlb-etl.yml          # Manual dispatch only. INCOMPLETE
  odds-etl.yml         # Nightly cron UTC 10:00 + manual. ACTIVE
  grading.yml          # Nightly cron UTC 10:30 + manual. ACTIVE
  keepalive.yml        # Every 45 min UTC 10:00-05:00. Pings /api/ping. ACTIVE
  lineup-poll.yml      # Every 15 min UTC 16:00-03:59. ACTIVE
  pregame-refresh.yml  # Every 30 min UTC 14:00-03:30. Chains odds+grading. ACTIVE
  refresh-lines.yml    # Manual dispatch triggered by /api/refresh-lines POST. ACTIVE
  nba-clear.yml        # Utility: clears NBA tables. Manual only.
  db_inventory.yml     # Utility: DB inventory. Manual only.
  azure-static-web-apps-red-smoke-0bbe1fb10.yml  # SWA CI/CD. ACTIVE.

etl/
  db.py / nba_etl.py / mlb_etl.py / nfl_etl.py / odds_etl.py
  nba_live.py       # Intra-day live box score updater. ACTIVE.
  lineup_poll.py    # Standalone lineup poller. ACTIVE.
  requirements.txt

grading/
  grade_props.py    # NBA prop grading. FULLY FUNCTIONAL with all components + Under grades.

web/
  app/
    page.tsx / layout.tsx / globals.css
    nba/
      page.tsx / NbaPageInner.tsx
      grades/
        page.tsx / GradesPageInner.tsx   # At a Glance — LIVE
      player/[playerId]/
        page.tsx / PlayerPageInner.tsx   # Player page — LIVE
    api/
      ping/          grades/          game-grades/     player-grades/
      games/         roster/          player/          player-averages/
      team-averages/ boxscore/        contextual/      team-players/
      refresh-lines/ refresh-status/  live-boxscore/   live/
  components/
    PasscodeGate.tsx   GameStrip.tsx    GameTabs.tsx     LiveBoxScore.tsx
    MatchupDefense.tsx RosterTable.tsx  StatsTable.tsx   BoxScoreTable.tsx
  lib/
    db.ts / queries.ts
  public/
    manifest.json / sw.js / icon.svg
  staticwebapp.config.json
  next.config.mjs / package.json
```

---

## 4. Database Schema

### Schemas
`nba`, `mlb`, `nfl`, `odds`, `common`

### NBA Tables
- `nba.games` — completed games only
- `nba.schedule` — ALL games regardless of status. USE THIS for game queries.
- `nba.teams` — hardcoded static dict in ETL
- `nba.players` — `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active), `position`
- `nba.daily_lineups` — keyed by `player_name` + `team_tricode`. `starter_status` = 'Starter'/'Bench'.
- `nba.player_box_score_stats` — quarters only: '1Q','2Q','3Q','4Q','OT'. Columns include `fg3a`. Minutes column = `minutes` (DECIMAL). Sum quarters for game totals.
- `nba.player_passing_stats` / `nba.player_rebound_chances` — game-level PT stats

### Odds Tables
- `odds.event_game_map` — event_id → game_id + game_date
- `odds.upcoming_player_props` — today's FanDuel lines
- `odds.player_props` — historical FanDuel lines
- `odds.upcoming_game_lines` / `odds.upcoming_events` — today's game lines

### Common Tables
- `common.daily_grades` — grading output. **Schema v3 (migrated 2026-04-02).**
  - Full column list: `grade_id`, `grade_date`, `event_id`, `game_id`, `player_id`, `player_name`, `market_key`, `bookmaker_key`, `line_value`, `outcome_name` (VARCHAR(5), 'Over'/'Under'), `over_price` (INT — stores Over price for Over rows, Under price for Under rows), `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`, `trend_grade`, `momentum_grade`, `pattern_grade`, `matchup_grade`, `regression_grade`, `composite_grade`, `hit_rate_opp`, `sample_size_opp`, `created_at`
  - UNIQUE constraint: `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value, outcome_name)` — v3 key includes outcome_name
  - Both Over and Under rows are written for standard markets. Alternate lines are Over-only.
  - `getGrades` query reads `dg.outcome_name` and `dg.over_price` DIRECTLY from the table — does NOT join odds tables for prices. This is critical. Do not revert to the old best_price CTE join.
- `common.dim_date` — calendar 2015–2035

### Key Rules
- Snake case. `created_at DATETIME2 DEFAULT GETUTCDATE()` on every fact table.
- `DELETE` not `TRUNCATE` — FK constraints block TRUNCATE.
- `minutes` not `min` — `min` is reserved in SQL Server.
- No 'FullGame' period — always SUM quarters.

---

## 5. ETL Patterns

### Incremental Ingestion
Desired keys → existing keys (SELECT DISTINCT) → missing set → process oldest N → upsert. Idempotent.

### Upsert
`etl/db.py:upsert()` — stage to `#stage_{table}`, load via `to_sql`, SQL MERGE. Never raw INSERT.

### NBA ETL Key Facts
- Box scores: `playergamelogs` with period filter params. Returns `fg3a`.
- OT: `Period=""` + `GameSegment=Overtime`
- Teams: hardcoded STATIC_TEAMS dict
- Players: `playerindex` via proxy
- PT stats: direct HTTP to `leaguedashptstats` with no proxy
- Proxy: Webshare rotating residential. Secret: `NBA_PROXY_URL`.

### NBA Live ETL
- Script: `etl/nba_live.py`
- Gate: queries `nba.schedule` for `game_status = 2` today. Exits if none.
- Workflow: `nba-live.yml` — every 5 min UTC 17:00–06:00.

### Odds ETL
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly cron UTC 10:00: `upcoming/nba/days-ahead=1`
- Upcoming mode writes to `odds.event_game_map`.

### Lineup Poll
- Script: `etl/lineup_poll.py` — standalone, does not import from nba_etl.py
- Workflow: `lineup-poll.yml` — every 15 min UTC 16:00–03:59.

### Pre-Game Refresh
- Workflow: `pregame-refresh.yml` — every 30 min UTC 14:00–03:30.
- Gate: checks `nba.schedule` for any non-final game starting within 3 hours.
- When gate passes: runs odds_etl upcoming then grade_props upcoming.

### Refresh Lines
- Workflow: `refresh-lines.yml` — triggered by POST to `/api/refresh-lines` via GITHUB_PAT.

---

## 6. Grading Model

**Components (all live as of 2026-04-02):**
- `weighted_hit_rate`: blended 20/60-day hit rate. Primary signal.
- `trend_grade`: last-10 mean vs last-30. Centered at 50. Inverted for unders.
- `momentum_grade`: consecutive hit/miss streak, log-scaled. Inverted for unders.
- `pattern_grade`: historical reversal rate after runs of current streak length. Inverted for unders.
- `matchup_grade`: defense rank for position vs today's opponent. Inverted for unders.
- `regression_grade`: z-score of recent 10-game mean vs season. Inverted for unders.
- `composite_grade`: equal-weighted average of all non-NULL components.

**Under grades:** Standard markets only, posted line only (no bracket). Components inverted. `outcome_name = 'Under'`, `over_price` stores the Under price.

**`precompute_line_grades` optimization (2026-04-02):** Outer loop iterates by `(player_id, market_key)` pair, loads stat sequence once, fans out across all line values in inner loop. Reduces outer iterations from ~6200 to ~560 in upcoming mode.

**Writes to:** `common.daily_grades` — one row per (grade_date, event_id, player_id, market_key, bookmaker_key, line_value, outcome_name).

---

## 7. Web Application

### UI Column Layouts (CANONICAL — do not revert)

**Player game log (PlayerPageInner.tsx):**
- Columns: Date, Opp, MIN, PTS, FG, 3PT, REB, AST, STL, BLK, TOV, FT
- No Str column. Str column was removed permanently.
- MIN: shows `*21:49` for starters (asterisk prefix), `21:49` for bench, `DNP` for did-not-play
- FG: shows `fgm/fga` per game (e.g. `7/14`), not percentage
- 3PT: shows `fg3m/fg3a` per game (e.g. `3/8`), not percentage. Header is `3PT` not `3PM`.
- `fg3a` flows from `nba.player_box_score_stats` through `getPlayerGames` query (includes `pbs.fg3a`) through `PlayerGameRow` interface through `buildGameSummaries` accumulator.

**Stats table (StatsTable.tsx):**
- Columns: Player, GP, MIN, PTS, FG, 3PT, REB, AST, STL, BLK, TOV
- FG: shows `avgFgm/avgFga` (e.g. `7.1/14.8`) — averages of made/attempted, NOT percentage
- 3PT: shows `avg3pm/avg3pa` (e.g. `2.1/5.6`) — averages of made/attempted, NOT percentage
- Headers are `FG` and `3PT` (not FG% or 3P%)
- `fmtRatio()` helper formats these. `fmtPct()` helper is NOT used for these columns.
- API (`team-averages/route.ts`) returns `avgFgm`, `avgFga`, `avg3pm`, `avg3pa` from aggregating `fg3a` in `game_totals` CTE.
- Starters shown first. Bench collapsed behind tappable `Bench (N)` row. `benchOpen` state per TeamStatsTable instance.

**At a Glance (GradesPageInner.tsx):**
- Filter: `r.overPrice != null` gates display
- Direction filter: Over/Under toggle using `r.outcomeName` field
- `outcomeName` and `overPrice` come directly from `dg.outcome_name` and `dg.over_price` in the DB — NOT from a join to odds tables
- The old `best_price` CTE join was removed. Do not reintroduce it. It caused Under rows to display in the Over tab by attaching Over prices to Under rows.

### Navigation
- `/nba` — game strip + tabs
- `/nba?gameId=&tab=` — active game
- `/nba/player/[playerId]?gameId=&tab=&opp=` — player page
- `/nba/grades?date=` — At a Glance grades

### Contextual Defense
- `/api/contextual?oppTeamId=&position=` — per-stat averages + ranks for position vs opponent
- Rank 1 = most allowed = best matchup for overs
- Season window computed dynamically in SQL (current season start = Oct 1 of current or prior year)

### Live Data Flow
1. `nba-live.yml` fires every 5 min UTC 17:00–06:00
2. Gate: `game_status = 2`. Exit if none.
3. ScoreboardV3 → update scores; BoxScoreTraditionalV3 → upsert stats
4. Front end polls `/api/boxscore` every 30s via `LiveBoxScore` component

---

## 8. Sport Status

### NBA
- Data: ACTIVE. Box scores current through late March 2026. Live updates active.
- Odds: nightly chain running; backfill ongoing for pre-April 2026 dates.
- Grading: FULLY FUNCTIONAL. All components live. Over + Under grades written.
- Web: ALL VIEWS LIVE.
- PWA: ACTIVE.

### MLB / NFL
- ETL partially built. Not wired to web or grading. After NBA.

---

## 9. Build Sequence

1–13. DONE — NBA data pipeline, odds ETL, SWA setup, all NBA UI views, keep-alive, lineup poll, pre-game refresh, live data layer, contextual matchup defense, Refresh Lines button, grades UI, all grading components, Under grades, UI column layout overhaul.
14. **Step 14: Mobile-first UI redesign** — bottom nav, card-based grades list, swipeable game view.
15. **Step 15: MLB ETL and web views**
16. **Step 16: NFL ETL automation and web views**

---

## 10. Known Issues

| Issue | Status |
|-------|--------|
| PasscodeGate BYPASS = true | Gate disabled for dev convenience. Re-enable before sharing with users. |
| NFL workflow file missing | `nfl_etl.py` exists, `nfl-etl.yml` does not. |
| Odds/grading backfill gap | Nightly chain running. Pre-April 2026 dates being backfilled. |
| PNG icons not generated | SVG icon works on all modern browsers. Generate via generate-icons.mjs if needed. |
| UI is desktop-first | Mobile-first redesign planned as Step 14. |
| Auto-pause delay locked | Free database offer prevents changing auto-pause delay. Keep-alive mitigates. |
| Box score and live stats not working | Runtime error not yet diagnosed. Error text from browser needed to fix. |
| grading.yml backfill lacks time-aware auto re-dispatch | Should match odds-backfill.yml pattern. Not yet implemented. |
| NFL and MLB run_mappings not implemented | Only NBA mappings branch exists in odds_etl.py. |

---

## 11. Decision Log

| Decision | Rationale |
|----------|-----------|
| Next.js over plain React | File-based routing, built-in API routes, server components, first-class SWA support. |
| Next.js API routes over separate Azure Functions | SWA promotes them automatically. One repo, one deployment. |
| Azure SWA over App Service or Vercel | Free tier, built-in auth, native GHA integration. |
| mssql driver over ORM | Direct parameterized queries, full control, all SQL in one file. |
| Web app over Power BI mobile | Power BI mobile is a report viewer. Required interaction model cannot be built there. |
| Passcode gate over GitHub auth | Small known user group; passcode simpler than OAuth for this audience. |
| PWA over native app | No App Store approval, no distribution friction, zero incremental cost. |
| SVG icon over PNG for PWA | `sizes: "any"` in manifest covers all modern browsers. |
| Service worker network-first for HTML | Users always get fresh HTML when online. |
| Service worker never caches API routes | Live data must never be stale. |
| Route cache headers via next.config.mjs | SWA CDN edge caches responses. Reduces DB hits. |
| Persistent game strip as React state | No browser history accumulation, no full page reload on game switch. |
| Tab memory via URL query parameter | Preserves tab across game switches. Linkable, survives refresh. |
| Stats tab uses nba.players not nba.daily_lineups | Lineup table empty for today until lineup-poll runs. |
| Full boxscore fetched once, filtered client-side | Period filter must feel instant with no round trips. |
| /api/ping made public (anonymous) | Required for keep-alive workflow. |
| `fast_executemany=False` in grading engine | True causes NVARCHAR(MAX) truncation. |
| `mssql` as `serverExternalPackages` | Prevents Next.js bundling mssql native bindings. |
| `minutes` not `min` | `min` is reserved in SQL Server. |
| No 'FullGame' period — always SUM quarters | ETL stores quarters only. |
| DELETE not TRUNCATE | FK constraints block TRUNCATE. |
| FanDuel as sole grading bookmaker | Most complete prop line coverage. |
| Teams dict hardcoded in NBA ETL | Eliminated HTTP dependency after persistent proxy failures. |
| lineup_poll.py standalone, not imported from nba_etl.py | nba_etl.py top-level argparse triggers on import. |
| Live box score via DB poll not direct browser API call | Browser never hits stats.nba.com. |
| 30-second front-end poll interval | Fast enough for live; slow enough to not hammer DB. |
| Refresh Lines triggers via GITHUB_PAT not webhook | SWA API routes cannot use build-time secrets. |
| Implied probability (Imp%) stored without vig removal | Raw implied prob sufficient for display. |
| `getGrades` reads over_price from dg directly, not odds join | Old best_price CTE join caused Under rows to receive Over prices and appear in Over tab. Direct read of dg.outcome_name and dg.over_price is correct and must not be reverted. |
| outcome_name in daily_grades UNIQUE key (v3 migration) | Allows Over and Under rows for same player/market/line. Prior unique key lacked this column. |
| Str column removed from player game log | Column only showed DNP anyway; starter status now encoded in MIN column with asterisk prefix. |
| FG and 3PT columns show made/attempted ratios not percentages | Both in stats table (averages) and game log (per-game actuals). Per-game actuals use integers; averages use one decimal. fmtRatio() helper used in StatsTable; fmtS() inline in PlayerPageInner. |
| precompute_line_grades iterates by player-market pair not by line value | Eliminates ~10x redundant DataFrame re-reads. Outer loop ~560 iterations vs ~6200 previously. |
| Under grades inverted components | Rising trend/momentum/good matchup is bad for under. All components flipped around 50. |
| Cloudflare DNS only (not proxied) for custom domain | Azure SWA requires direct DNS resolution for SSL issuance. |
