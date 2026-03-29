# Sports Modeling — Project Reference

> **For Claude:** Read this at the start of every session. Update it immediately whenever infrastructure, scripts, schema, or build status changes. This is the single source of truth. All prior documents are retired.
>
> Written for how you actually use it: dense, factual, no padding. Every section answers a specific question you would otherwise have to re-derive. The decision log captures non-obvious choices that caused pain before — do not re-ask those questions or suggest approaches already ruled out.

---

## 1. What This Is

Personal sports intelligence app for NBA, NFL, and MLB prop betting research. Loads each morning, shows today's games, lets you drill into any player's stats and historical averages in one tap, surfaces the highest-probability prop bets. No manual lookups.

Three layers:
- **Data:** Azure SQL fed by nightly GitHub Actions ETL pipelines
- **Intelligence:** Grading model scoring player props against historical patterns
- **Presentation:** React/Next.js web app on Azure Static Web Apps — replaces Power BI as the primary consumption surface

**Why not Power BI:** Power BI mobile is a report viewer. The required interaction model — persistent game context, inline player peek, cross-game player tracking, sub-30-second task completion — cannot be built reliably there. Next.js with Azure Functions API gives full control over layout, navigation, caching, and load states. Power BI Desktop is retained for ad hoc analysis only.

---

## 2. Infrastructure

### Azure SQL
- Server: `sports-modeling-server.database.windows.net` / Database: `sports-modeling` / Login: `sqladmin`
- Tier: General Purpose Serverless — auto-pauses; first connection of the day takes 20–60s
- Firewall: `0.0.0.0–255.255.255.255` under Selected Networks (required for GitHub Actions)
- Allow Azure Services: must remain ON
- Connection: SQLAlchemy + pyodbc, ODBC Driver 18, `fast_executemany=True` (grading uses `False` to prevent NVARCHAR(MAX) truncation)
- Retry logic: 3 attempts, 45s wait — handles auto-pause resume
- No MSSQL MCP available. DB queries run via Python in GitHub Actions or via ETL scripts through the GitHub MCP.
- Cold start mitigation: web app calls `/api/ping` (SELECT 1) on load to wake DB before data requests. Keep-alive cron (every 45 min during active hours) will eliminate cold starts entirely — not yet built.

### GitHub Actions
- Repo: `SchnappAPI/sports-modeling` (private)
- All automated Python runs here. Python cannot run locally (ThreatLocker) or on a schedule from the Windows VM (auto-logout).
- Runners: ephemeral ubuntu-latest; every run installs ODBC Driver 18 and pip deps fresh
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10` (SWA deploy token, auto-added by Azure)
- Always retrieve file SHA before `create_or_update_file`

### Azure Static Web Apps
- Resource name: `sports-modeling-web`
- URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
- Resource group: `sports-modeling` / Region: Global / SKU: Free
- Deploy workflow: `.github/workflows/azure-static-web-apps-red-smoke-0bbe1fb10.yml`
- Deploy secret: `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10` (auto-provisioned by Azure)
- Triggers on push to `main`; `app_location: /web`, api and output locations blank
- Next.js 15.2.8, React 19
- Built-in auth: GitHub identity provider, configured in `staticwebapp.config.json` — all routes require `authenticated` role, 401 redirects to GitHub login
- DB connection string in SWA application settings (not in code/repo) — env var name: `AZURE_SQL_CONNECTION_STRING`
- Next.js API routes auto-deploy as managed Azure Functions — no separate Functions app needed
- Status: Step 8 NBA UI views complete through player detail. At a Glance grades view is next.

### Windows VM
- Manual tasks only. PFF Selenium script runs here when PFF grade data is needed.
- Not reliable for scheduled or long-running processes.

### Power BI
- File: `sports-model.pbix` (OneDrive, My Files\_RESOURCES). Retained for ad hoc analysis only.
- MCP connection: run `ListLocalInstances` → `Connect` using `connectionString` and `initialCatalog` from instance where `parentWindowTitle` = `sports-model`. Port is dynamic; re-run each session.

### Local Dev (ThreatLocker)
- Node.js v24.12.0 installed and working. `npm run dev` is blocked by ThreatLocker (next.cmd blocked).
- Testing via push to `main` and checking live SWA URL. ~90 seconds per deploy cycle.
- Azure Functions Core Tools not required; Next.js handles API routes natively.

---

## 3. Repository Structure

```
.github/workflows/
  nba-etl.yml                                         # Nightly cron + manual dispatch. ACTIVE
  mlb-etl.yml                                         # Manual dispatch only. INCOMPLETE
  odds-etl.yml                                        # Nightly cron UTC 10:00 (upcoming/nba) + manual. ACTIVE
  grading.yml                                         # Nightly cron UTC 10:30 + manual. ACTIVE
  nba-clear.yml                                       # Utility: clears NBA tables. Manual only.
  db_inventory.yml                                    # Utility: DB inventory. Manual only.
  grades_sample.yml                                   # Prototype. Not production.
  proxy-test.yml                                      # Proxy diagnostics. Not production.
  azure-static-web-apps-red-smoke-0bbe1fb10.yml       # SWA CI/CD. Auto-generated by Azure. ACTIVE.
  pregame-refresh.yml                                 # Every 30 min pre-game window. Chains odds ETL + grades. TO BUILD
  lineup-poll.yml                                     # Every 15 min pre-game window. Polls lineups for unconfirmed games. TO BUILD
  diag-lineups.yml                                    # Utility: runs diagnostic scripts. Manual only.
  diag-schedule.yml                                   # Utility: schedule diagnostic. Manual only.
  # NOTE: No nfl-etl.yml exists despite nfl_etl.py being present

etl/
  db.py                # Shared: get_engine(), upsert() via SQL MERGE
  nba_etl.py           # ACTIVE. 2025-26 season backfill in progress.
  mlb_etl.py           # INCOMPLETE
  nfl_etl.py           # BUILT, NOT AUTOMATED. No workflow file.
  odds_etl.py          # ACTIVE. Nightly cron for upcoming mode.
  nba_clear.py         # Utility: truncates NBA tables
  nba_add_indexes.sql  # Index DDL for NBA tables
  db_inventory.py      # Inventory script
  grades_sample.py     # Prototype
  diag_schedule.py     # Diagnostic: schedule table coverage
  diag_lineups.py      # Diagnostic: lineups table schema/coverage
  diag_lineups2.py     # Diagnostic: lineup date coverage vs schedule
  diag_boxscore.py     # Diagnostic: box score column names + grades/egm coverage
  diag_player.py       # Diagnostic: player detail period values
  requirements.txt     # nflreadpy, nfl-data-py, sqlalchemy, pyodbc, pandas, pyarrow, requests, mlb-statsapi, nba_api>=1.11.4
  lineup_poll.py       # Lineup fetch and upsert for pre-game polling. TO BUILD

grading/
  grade_props.py       # NBA prop grading model. FUNCTIONAL. See Section 6.

web/
  app/
    page.tsx           # Redirects to /nba
    layout.tsx         # Root layout: imports globals.css, dark base bg
    globals.css        # Tailwind directives
    nba/
      page.tsx         # Suspense wrapper shell — LIVE
      NbaPageInner.tsx # Game strip + tab nav — LIVE
      grades/page.tsx  # At a Glance — TO BUILD (step 8f)
      player/[playerId]/
        page.tsx           # Suspense wrapper shell — LIVE
        PlayerPageInner.tsx # Game log view — LIVE
    api/
      ping/route.ts             # LIVE. SELECT 1.
      games/route.ts            # LIVE. Returns games from nba.schedule + odds lines. Response: {sport, date, games:[]}
      roster/route.ts           # LIVE. Returns players from nba.daily_lineups. Response: {gameId, roster:[]}. Empty until lineup poll built.
      player-averages/route.ts  # LIVE. Returns averages anchored to lineups. Response: {gameId, lastN, players:[]}. Empty until lineup poll built.
      team-averages/route.ts    # LIVE. Returns averages for all roster_status=1 players by team ID. Response: {players:[]}. No lineup dependency.
      boxscore/route.ts         # LIVE. Returns per-quarter stats. Response: {gameId, rows:[]}
      player/route.ts           # LIVE. Returns game log last N games summed across quarters. Response: {playerId, lastN, sport, log:[]}
      grades/route.ts           # LIVE. Returns grades from common.daily_grades. Response: {grades:[]}. Empty until odds backfill catches up.
      live/route.ts             # Stub — TO BUILD (step 11)
      contextual/route.ts       # Stub — TO BUILD (step 12)
  components/
    GameStrip.tsx       # LIVE. Horizontal scrollable game cards. Exports Game interface (includes homeTeamId, awayTeamId).
    GameTabs.tsx        # LIVE. Roster/Stats/Box Score tab nav. Props: gameId, homeTeamId, awayTeamId.
    RosterTable.tsx     # LIVE. Two-team roster from nba.daily_lineups. Shows empty state gracefully.
    StatsTable.tsx      # LIVE. Two-team averages from /api/team-averages. Player names are clickable links to player detail.
    BoxScoreTable.tsx   # LIVE. Per-quarter box score with period filter buttons (All/1Q/2Q/3Q/4Q/OT). Client-side aggregation.
  lib/
    db.ts              # mssql singleton pool reading AZURE_SQL_CONNECTION_STRING
    queries.ts         # All parameterized SQL functions — VALIDATED
  staticwebapp.config.json  # Auth gate: authenticated role required on all routes; 401 redirects to GitHub login
  tailwind.config.ts
  postcss.config.mjs
  next.config.mjs      # serverExternalPackages: ['mssql']
  package.json         # next 15.2.8, react 19, mssql ^11, tailwindcss ^3
  tsconfig.json
```

---

## 4. Database Schema

### Schemas
| Schema | Purpose |
|--------|--------|
| `nba` | NBA tables |
| `mlb` | MLB tables |
| `nfl` | NFL tables |
| `odds` | Odds API: events, lines, props, mappings |
| `common` | Grading output, date dimension |

### NBA Tables
- `nba.games` — game metadata (completed games only; populated by box score ETL)
- `nba.schedule` — all games regardless of status; USE THIS for game queries, not nba.games
- `nba.teams` — team reference (hardcoded static dict in ETL)
- `nba.players` — player reference. Key columns: `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active). Stats tab queries this directly by team_id — no lineup dependency.
- `nba.daily_lineups` — game-day lineups. Key schema facts:
  - NO player_id or team_id columns
  - Identified by `player_name` (VARCHAR) and `team_tricode` (CHAR)
  - Starter flag is `starter_status` VARCHAR with values 'Starter' / 'Bench'
  - Also has: `position`, `lineup_status`, `roster_status`, `home_away`, `game_date`
  - Join to `nba.players` on `player_name` to get `player_id` — use LEFT JOIN (player may not exist yet)
  - Coverage: current through prior day; today's games empty until nightly ETL runs
- `nba.player_box_score_stats` — per-quarter box scores only. Key schema facts:
  - Periods stored: '1Q', '2Q', '3Q', '4Q', 'OT' — NO 'FullGame' period exists
  - Minutes column is named `minutes` (DECIMAL), NOT `min`
  - To get game totals: GROUP BY player_id, game_id and SUM all stat columns
  - Also has: `game_date` (DATE) directly on row — does not require join to nba.games for date
  - Other columns: `season_year`, `team_tricode`, `matchup`, `minutes_sec`, `fg_pct`, `fg3a`, `fg3_pct`,
    `ft_pct`, `oreb`, `dreb`, `blka`, `pf`, `pfd`, `plus_minus`, `dd2`, `td3`, `available_flag`
- `nba.player_passing_stats` — passing/assist tracking stats (game level)
- `nba.player_rebound_chances` — rebound opportunity tracking stats (game level)

### Odds Tables
- `odds.events` — game events from The Odds API
- `odds.game_lines` — spreads, totals, moneylines; spread used by contextual comparison
- `odds.upcoming_game_lines` — today's game lines (JOIN to odds.upcoming_events for home_team)
- `odds.upcoming_events` — today's events
- `odds.player_props` — historical FanDuel lines
- `odds.upcoming_player_props` — today's FanDuel lines (upcoming mode)
- `odds.event_game_map` — resolves `event_id` → `game_id` + `game_date`. Coverage currently through 2026-03-23.
- `odds.player_map` — maps odds player names to internal `player_id`

### Common Tables
- `common.daily_grades` — grading output (see Section 6). Coverage currently through 2026-03-23.
- `common.dim_date` — calendar dimension 2015–2035

### Key Schema Rules
- Snake case everywhere
- Every fact table: `created_at DATETIME2 DEFAULT GETUTCDATE()`
- Key columns need non-clustered indexes for fast `SELECT DISTINCT`
- `DELETE` not `TRUNCATE` — FK constraints block TRUNCATE even on empty child tables
- FK teardown order: query `sys.foreign_keys` joined with `sys.tables` and `sys.schemas`
- MLB dropped FK constraints on child tables intentionally — allows independent reload

---

## 5. ETL Patterns

### Deterministic Incremental Ingestion
1. Build desired key set
2. Query destination for existing keys (`SELECT DISTINCT key_col`)
3. Compute missing = desired minus existing
4. Take oldest N (`--days` arg; default 3 nightly, higher for backfill)
5. Short-circuit if nothing to do
6. Fetch from API
7. Upsert

Idempotent: running twice produces the same state as once.

### Upsert
All writes via `etl/db.py:upsert()`. Stages to `#stage_{table}` temp table, loads via `to_sql`, executes SQL MERGE. Never use raw INSERT.

### DataFrame Rules
- `df.where(pd.notna(df), other=None)` before every upsert (NaN → None)
- Never use `method='multi'` in `to_sql` — hits SQL Server 2100-parameter cap
- `chunksize=200`, `index=False`

### NBA ETL Specifics
- Box scores: `playergamelogs` endpoint with period filter params, second-based ranges for genuine per-quarter splits
- OT: `Period=""` + `GameSegment=Overtime`, not `Period=5`
- Teams: hardcoded `STATIC_TEAMS` dict — no HTTP dependency
- Players: direct HTTP `playerindex` via proxy
- Game discovery: via `scheduleleaguev2`
- PT stats: direct `requests.get` to `leaguedashptstats` with browser headers; `proxies={"http": None, "https": None}` explicitly set
- Proxy: Webshare rotating residential. Secret: `NBA_PROXY_URL`.
- 15s delay between passing and rebounding PT stats calls (prevents HTTP 500)

### Odds ETL Specifics
- Source: The Odds API (`ODDS_API_KEY`)
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly cron: UTC 10:00, runs `upcoming` for NBA, 30 min before grading
- `github.event_name == 'schedule'` branch hard-wires `upcoming/nba/days-ahead=1`; manual dispatch retains full input control

---

## 6. Grading Model

### Current implementation (grade_props.py — FUNCTIONAL)

```
hit_rate_60       = hits / games, stat > line, prior 60 days
hit_rate_20       = hits / games, stat > line, prior 20 days
weighted_hit_rate = (0.60 × hit_rate_20) + (0.40 × hit_rate_60)
grade             = weighted_hit_rate × 100, rounded to 1 decimal
```
If `sample_size_20 < 5`, falls back to `hit_rate_60` only. Grade written regardless of sample size — thin-sample rows are visible, not silently omitted.

**What it reads:** `odds.upcoming_player_props` or `odds.player_props`, `odds.event_game_map`, `odds.player_map`, `nba.player_box_score_stats` (summed across all periods for game totals — no FullGame period exists)

**What it writes:** `common.daily_grades` — one row per `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value)`

**Current schema of `common.daily_grades`:**
`grade_id`, `grade_date`, `event_id`, `game_id`, `player_id`, `player_name`, `market_key`, `bookmaker_key`, `line_value`, `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`, `created_at`

**Bookmaker:** FanDuel only. `bookmaker_key` retained in schema for future extension.

**Modes:** `upcoming` (nightly production), `backfill` (`--batch N`, oldest ungraded date first)

### Grading expansion — planned (none implemented yet)

See prior session notes for component weights, flag definitions, and migration workflow. Schema additions (`trend_grade`, `matchup_grade`, `regression_grade`, `streak_grade`, `adaptive_grade`, `correlation_grade`, `composite_grade`, `flags`) not yet applied.

---

## 7. Web Application

### Navigation model

**Persistent game strip:** Horizontal scrollable strip. Clicking a card sets active game via React state + `router.replace`. URL: `?gameId=&tab=`. Strip never disappears.

**Game research view (`/nba?gameId=&tab=`):**
- Roster tab: both teams from `nba.daily_lineups`. Empty for today until nightly ETL runs.
- Stats tab: both teams from `nba.players` (roster_status=1) + last 20 game averages from box scores. No lineup dependency — always has data. Player names link to player detail.
- Box Score tab: per-quarter stats with period filter (All/1Q/2Q/3Q/4Q/OT). Client-side aggregation from single fetch. Empty until game completes and ETL runs.

**Player detail view (`/nba/player/[playerId]?gameId=&tab=`):** Last 20 game log. Back link preserves gameId and tab.

**At a Glance view (`/nba/grades`):** TO BUILD (step 8f). Ranked graded props by grade descending.

### API response shapes (critical — do not guess)
- `/api/games` → `{ sport, date, games: GameRow[] }`
- `/api/roster` → `{ gameId, roster: RosterRow[] }`
- `/api/team-averages` → `{ players: PlayerAvg[] }` — queries nba.players by team_id, no lineup dependency
- `/api/player-averages` → `{ gameId, lastN, players: PlayerAvg[] }` — lineup-anchored, empty for today
- `/api/boxscore` → `{ gameId, rows: BoxRow[] }`
- `/api/player` → `{ playerId, lastN, sport, log: GameLogRow[] }`
- `/api/grades` → `{ grades: GradeRow[] }`

### Game interface (GameStrip.tsx)
```typescript
interface Game {
  gameId: string; gameDate: string; gameStatus: number | null;
  gameStatusText: string | null; homeTeamId: number; awayTeamId: number;
  homeTeamAbbr: string; awayTeamAbbr: string;
  homeTeamName: string; awayTeamName: string;
  spread: number | null; total: number | null;
}
```

### Next.js 15 patterns
- `useSearchParams()` must be in a component wrapped by `<Suspense>`. Pattern: thin `page.tsx` exports Suspense wrapper, inner `*Inner.tsx` does the work.
- Dynamic route params typed as `Promise<{...}>` and awaited in server components.
- Client components need `'use client'` at top; cannot use hooks in server components.

### Authentication
SWA built-in auth. All routes require `authenticated` role. Configured in `web/staticwebapp.config.json`.

---

## 8. In-Game Contextual Comparison

Live-only feature. TO BUILD (step 12). Uses `nba.player_box_score_stats`, `nba.daily_lineups`, `odds.game_lines`.

---

## 9. Pre-Game Automation

Both TO BUILD (step 10). Pre-game refresh every 30 min, lineup polling every 15 min.

---

## 10. Sport Status

### NBA
- **Data:** ACTIVE. Box scores current through 2026-03-28.
- **Odds:** ACTIVE. event_game_map and daily_grades coverage through 2026-03-23; backfill in progress.
- **Grading:** FUNCTIONAL (hit rate only). Coverage through 2026-03-23.
- **Web app:** Game strip, Roster tab, Stats tab, Box Score tab, Player detail all LIVE. At a Glance grades view is next (step 8f).

### MLB
- Data loaded historically. ETL incomplete. Not wired to web or grading.

### NFL
- ETL built, not automated. No workflow file. Not wired to web or grading.

---

## 11. Build Sequence

1. ~~Stabilize NBA data pipeline~~ DONE
2. ~~Add upcoming cron to odds-etl.yml~~ DONE
3. ~~Set up Azure Static Web Apps~~ DONE
4. ~~Build Next.js scaffold~~ DONE
5. ~~Add AZURE_SQL_CONNECTION_STRING to SWA~~ DONE
6. ~~Build and validate all API routes~~ DONE
7. ~~Build NBA web app views~~ IN PROGRESS — game strip, stats, box score, player detail done. At a Glance next.
8. **Step 8f: At a Glance grades view** — `/nba/grades`, ranked props by grade.
9. **Step 9: Keep-alive workflow** — GHA cron every 45 min during active hours.
10. **Step 10: pregame-refresh.yml + lineup-poll.yml + lineup_poll.py**
11. **Step 11: Live data layer** — `/api/live`, Live View tab, front-end polling.
12. **Step 12: Contextual comparison**
13. **Step 13: Grading model expansion** — trend + matchup components first.
14. **Step 14: MLB ETL and web views**
15. **Step 15: NFL ETL automation and web views**

---

## 12. Known Issues

| Issue | Status |
|-------|--------|
| next.cmd blocked by ThreatLocker locally | Testing via push to main + live SWA URL. ~90s per cycle. |
| NFL workflow file missing | `nfl_etl.py` exists, `nfl-etl.yml` does not. |
| PFF DDL not finalized | Pending CSV column confirmation from VM Selenium run. |
| Azure SQL auto-pause cold start | Ping on app load masks it. Keep-alive cron (step 9) will eliminate it. |
| Grading: one component only | Hit rate only. Six additional components planned. |
| `flags` + component columns not in `common.daily_grades` | Migration required before any flag-producing component deploys. |
| odds/grading backfill gap | event_game_map and daily_grades only current to 2026-03-23. |
| `pregame-refresh.yml` not built | Pre-game chaining does not exist yet. |
| `lineup-poll.yml` + `lineup_poll.py` not built | Lineup polling does not exist yet. |
| `/api/live` + `/api/contextual` not built | Live data and contextual endpoints do not exist yet. |
| Roster tab empty for today | nba.daily_lineups only populated through prior day by nightly ETL. Resolved by lineup poll (step 10). |
| Box Score tab empty for today | Today's games not yet played. Populates overnight after ETL runs. |
| At a Glance grades view not built | Step 8f. |

---

## 13. Decision Log

| Decision | Rationale |
|----------|-----------|
| Next.js over plain React | File-based routing, built-in API routes, server components, first-class SWA support. |
| Next.js API routes over separate Azure Functions | SWA promotes them automatically. One repo, one deployment. |
| Azure SWA over App Service or Vercel | Free tier, built-in auth, native GHA integration, existing Azure footprint. |
| mssql driver over ORM | Direct parameterized queries, full control, all SQL in one file. |
| Web app over Power BI mobile | Power BI mobile is a report viewer. Required interaction model cannot be built there. |
| Power BI retained for ad hoc analysis | Faster for exploratory work and grade distribution review. |
| GitHub built-in auth | Zero code, zero user management, GitHub accounts are the access control primitive. |
| Persistent game strip as React state | Switching games must not accumulate browser history or trigger full page reload. |
| Tab memory via URL query parameter | Preserves tab across game switches. Linkable, shareable, survives refresh. |
| Stats tab uses nba.players not nba.daily_lineups | Lineup table empty for today; players table always has active roster. No lineup dependency means stats are always available. |
| Full boxscore payload fetched once, client-side period filter | Period filter changes must feel instant with no round trips. |
| useSearchParams requires Suspense boundary in Next.js 15 | Build fails without it. Pattern: thin page.tsx wraps Suspense, inner *Inner.tsx does work. |
| Game interface includes homeTeamId and awayTeamId | Required to pass team IDs to Stats tab for team-averages API call. |
| Grading components as nullable columns | No migration cascades, NULL excluded from composite, simpler to add incrementally. |
| Confidence flags as JSON array | Multiple flags per row, variable set over time, simpler than a flags table. |
| Lotto as flag not scored component | Binary pattern signal — forcing to 0–100 scale misrepresents its meaning. |
| Starter/bench as contextual anchor | Directly determines role, minutes, usage. Home/away has weaker per-player signal. |
| Pre-game refresh via polling + workflow chaining | GHA does not support dynamic scheduling. |
| Lineup polling every 15 min | Lineup releases are unpredictable. 15-min interval catches releases promptly. |
| Teams dict hardcoded in NBA ETL | Eliminated HTTP dependency after persistent proxy failures. |
| PT stats via direct HTTP | `nba_api` wrapper missing required headers for `leaguedashptstats`. |
| `proxies={"http": None, "https": None}` not `proxies=None` | Required to explicitly bypass proxy when env vars are set. |
| MLB dropped FK constraints on child tables | Allows independent truncate/reload of reference tables. |
| `DELETE` not `TRUNCATE` | TRUNCATE blocked by FK constraints even on empty child tables. |
| FanDuel as sole grading bookmaker | Most complete prop line coverage. `bookmaker_key` retained for future extension. |
| `fast_executemany=False` in grading engine | `True` causes NVARCHAR(MAX) truncation on wide rows. |
| `mssql` as `serverExternalPackages` in next.config.mjs | Prevents Next.js from bundling mssql, which has native bindings that fail in the SWA build environment. |
| Next.js dynamic route params typed as `Promise<{...}>` and awaited | Required in Next.js 15 — sync params type causes TypeScript build failure. |
| nba.player_box_score_stats has no FullGame period | ETL stores quarters only. All queries must SUM across periods to get game totals. |
| nba.daily_lineups has no player_id or team_id | Join to nba.players on player_name (LEFT JOIN). starter_status not is_starter. |
| minutes column named `minutes` not `min` | `min` is a SQL Server reserved word. Column is DECIMAL named `minutes`. |
