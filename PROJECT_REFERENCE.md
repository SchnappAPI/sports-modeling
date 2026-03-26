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
- Next.js 15.2.4, React 19
- Built-in auth: GitHub/Microsoft/Google as identity providers, zero code, configured via `staticwebapp.config.json`
- DB connection string in SWA application settings (not in code/repo) — env var name: `AZURE_SQL_CONNECTION_STRING`
- Next.js API routes auto-deploy as managed Azure Functions — no separate Functions app needed
- Status: PROVISIONED. Placeholder page live. Full scaffold TO BUILD (step 5).

### Windows VM
- Manual tasks only. PFF Selenium script runs here when PFF grade data is needed.
- Not reliable for scheduled or long-running processes.

### Power BI
- File: `sports-model.pbix` (OneDrive, My Files\_RESOURCES). Retained for ad hoc analysis only.
- MCP connection: run `ListLocalInstances` → `Connect` using `connectionString` and `initialCatalog` from instance where `parentWindowTitle` = `sports-model`. Port is dynamic; re-run each session.

### Local Dev (ThreatLocker)
- Only executables needed locally: Node.js and npm
- Install Node.js via nodejs.org installer during ThreatLocker learning mode session, then whitelist by hash
- All dev runs through `npm run dev` — covered by whitelisted Node.js executable
- Azure Functions Core Tools not required; Next.js handles API routes natively via `npm run dev`
- Alternative if learning mode unavailable: push to dev branch, deploy to staging SWA environment, test there

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
  requirements.txt     # nflreadpy, nfl-data-py, sqlalchemy, pyodbc, pandas, pyarrow, requests, mlb-statsapi, nba_api>=1.11.4
  lineup_poll.py       # Lineup fetch and upsert for pre-game polling. TO BUILD

grading/
  grade_props.py       # NBA prop grading model. FUNCTIONAL. See Section 6.

web/                   # Next.js 15 application. Placeholder live. Full scaffold TO BUILD.
  app/
    page.tsx           # Root placeholder (live). TO REPLACE with redirect to /nba
    layout.tsx         # Root layout (live). TO REPLACE with persistent game strip, nav, auth gate
    api/
      ping/route.ts    # SELECT 1 stub (live). TO WIRE to Azure SQL.
  next.config.mjs      # Next.js config
  package.json         # Next.js 15.2.4, React 19
  tsconfig.json
```

Full planned structure (TO BUILD):
```
web/app/
  [sport]/
    page.tsx              # Game selection view
    player/[playerId]/
      page.tsx            # Player detail
    grades/
      page.tsx            # At a Glance
  api/
    games/route.ts
    roster/route.ts
    player-averages/route.ts
    boxscore/route.ts
    player/route.ts
    grades/route.ts
    live/route.ts
    contextual/route.ts
web/components/
  GameStrip.tsx
  RosterTable.tsx
  PlayerRow.tsx
  FocusedStatTable.tsx
  GranularityFilter.tsx
  StatColumnPicker.tsx
  PlayerDetail.tsx
  SplitsTable.tsx
  GameLogTable.tsx
  GradesBadge.tsx
  FlagTags.tsx
  ContextualPanel.tsx
web/lib/
  db.ts              # Azure SQL singleton pool via mssql package
  queries.ts         # All parameterized SQL
web/staticwebapp.config.json
web/tailwind.config.ts
```

---

## 4. Database Schema

### Schemas
| Schema | Purpose |
|--------|---------|
| `nba` | NBA tables |
| `mlb` | MLB tables |
| `nfl` | NFL tables |
| `odds` | Odds API: events, lines, props, mappings |
| `common` | Grading output, date dimension |

### NBA Tables
- `nba.games` — game metadata
- `nba.schedule` — schedule
- `nba.teams` — team reference (hardcoded static dict in ETL)
- `nba.players` — player reference
- `nba.daily_lineups` — game-day lineups; starter flag used by contextual comparison
- `nba.player_box_score_stats` — quarter-level box scores (Q1/Q2/Q3/Q4/OT), core fact table
- `nba.player_passing_stats` — passing/assist tracking stats (game level)
- `nba.player_rebound_chances` — rebound opportunity tracking stats (game level)

### Odds Tables
- `odds.events` — game events from The Odds API
- `odds.game_lines` — spreads, totals, moneylines; spread used by contextual comparison
- `odds.player_props` — historical FanDuel lines
- `odds.upcoming_player_props` — today's FanDuel lines (upcoming mode)
- `odds.event_game_map` — resolves `event_id` → `game_id` + `game_date`
- `odds.player_map` — maps odds player names to internal `player_id`

### Common Tables
- `common.daily_grades` — grading output (see Section 6)
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
- Box scores: `BoxScoreTraditionalV3`, `range_type=2`, second-based `start_range`/`end_range` for genuine per-quarter splits
- OT: `Period=""` + `GameSegment=Overtime`, not `Period=5`
- Teams: hardcoded `STATIC_TEAMS` dict — no HTTP dependency
- Players: direct HTTP `commonteamroster` (no proxy)
- Game discovery: direct HTTP `leaguegamelog` (no proxy)
- PT stats: direct `requests.get` to `leaguedashptstats` with browser headers, bypassing `nba_api` wrapper; `proxies={"http": None, "https": None}` explicitly set
- Proxy: Webshare rotating residential, monkey-patched via `requests.Session.__init__` before importing `nba_api`. Secret: `NBA_PROXY_URL`.
- V3 endpoints: use typed dataset accessors (`.player_stats.get_data_frame()`); `get_normalized_dict()` returns empty for V3
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

**Performance design:** 3 DB round trips per grade date: (1) fetch props, (2) bulk fetch history, (3) upsert. Hit rates computed entirely in pandas via vectorized groupby. Zero per-row DB calls. Full-season backfill fits within GitHub Actions limits.

**What it reads:** `odds.upcoming_player_props` or `odds.player_props`, `odds.event_game_map`, `odds.player_map`, `nba.player_box_score_stats` (summed across all periods for game totals)

**What it writes:** `common.daily_grades` — one row per `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value)`

**Current schema of `common.daily_grades`:**
`grade_id`, `grade_date`, `event_id`, `game_id`, `player_id`, `player_name`, `market_key`, `bookmaker_key`, `line_value`, `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`, `created_at`

**Bookmaker:** FanDuel only. `bookmaker_key` retained in schema for future extension.

**Markets graded:** All player-level prop and alt-prop markets. Excludes team totals, h2h, spreads, totals, half/quarter game lines. `double_double`, `triple_double`, `first_basket` have no stat expression and are skipped for hit rate computation.

**Modes:** `upcoming` (nightly production), `backfill` (`--batch N`, oldest ungraded date first)

### Grading expansion — planned components (none implemented yet)

Each component is a standalone function returning a 0–100 score. The orchestrator calls all components, assembles results, and writes one upsert. No component calls another.

**Schema addition required before any new component deploys:** Add `flags` column (JSON array, nullable) plus one nullable FLOAT column per new component to `common.daily_grades` via a migration script run through GitHub Actions. NULL components are excluded from composite weight calculation — do not backfill unless historical scores are wanted.

**Planned schema additions:**
- `trend_grade FLOAT NULL`
- `matchup_grade FLOAT NULL`
- `regression_grade FLOAT NULL`
- `streak_grade FLOAT NULL`
- `adaptive_grade FLOAT NULL`
- `correlation_grade FLOAT NULL`
- `composite_grade FLOAT NULL`
- `flags NVARCHAR(500) NULL`

**Component addition workflow:**
1. Add nullable column(s) via migration script (GitHub Actions, manual dispatch)
2. Implement component function — signature: `(player_id, market_key, line_value, grade_date) → (score: float, flags: list[str])`
3. Add to orchestrator component list with target weight
4. Update composite weight calculation
5. Nightly workflow picks up automatically
6. Backfill via `grading.yml` backfill mode

**Composite weight targets:**

| Component | Weight |
|-----------|--------|
| Historical hit rate | 25% |
| Weighted recent trend | 20% |
| Matchup defense | 20% |
| Regression metric | 15% |
| Streak/cycle detection | 10% |
| Adaptive estimator | 7% |
| Correlation | 3% |

### Confidence flags

Stored as JSON array in `flags` column. Each component returns both a score and a list of flags. Orchestrator deduplicates and writes combined array.

| Flag | Condition |
|------|-----------|
| `CYCLE` | Repeating interval pattern detected |
| `MATCHUP_OUTLIER` | Strong opponent-specific pattern, sample >= 4 |
| `HOT_STREAK` | Recent trend >= 60% above season line, last 5 games |
| `COLD_STREAK` | Recent trend >= 40% below season line, last 5 games |
| `REGRESSION_DUE` | Recent 10-game avg > 1.5 SD above 60-game baseline |
| `CORRELATED_BOOST` | Positively correlated teammate line increased |
| `CORRELATED_SUPPRESS` | Negatively correlated teammate active |
| `LOTTO` | Line set significantly below recent averages at +300 or longer |

---

## 7. Web Application

### Stack rationale
- Next.js over plain React: file-based routing, built-in API routes, server components, first-class SWA support
- Next.js API routes over separate Azure Functions: SWA promotes them automatically — one repo, one deployment
- Azure SWA over App Service or Vercel: free tier covers use case, built-in auth, native GHA integration
- mssql driver over ORM: direct parameterized queries, all SQL in `queries.ts`

### Navigation model

**Persistent game strip:** Horizontal scrollable strip pinned at top. Tapping a card swaps content area via React state — no route change. URL updates via `router.replace`. Strip never disappears.

**Tab memory:** Active tab preserved in URL as `?tab=` query param. Defaults to `roster`.

**Game research view (`/[sport]?tab=roster&gameId=...`):** Both teams' rosters. Stat averages under active filters. Box Score View + Focused Stat View. Granularity filter. Live View tab when game is in progress.

**Player detail view (`/[sport]/player/[playerId]`):** Splits summary + game log. Shared granularity filter. Contextual comparison panel when game is live.

**At a Glance view (`/[sport]/grades`):** Ranked graded props by grade descending. Grade component columns + composite. Tags column with flag pills.

### API layer

All DB access through Next.js API routes. All queries parameterized in `queries.ts`.

| Endpoint | Purpose |
|----------|---------|
| `GET /api/ping` | SELECT 1 — cold start wake |
| `GET /api/games?sport=&date=` | Today's games + spread/total |
| `GET /api/roster?gameId=&sport=` | Active roster with starter flag |
| `GET /api/player-averages?gameId=&context=` | Stat averages for all game players |
| `GET /api/boxscore?gameId=&periods=` | Full per-period history for client filtering |
| `GET /api/player?playerId=&games=&sport=` | Player game log, last N games |
| `GET /api/grades?date=&sport=&gameId=` | All graded props for a date |
| `GET /api/live?gameId=&sport=` | In-progress stats, never written to SQL |
| `GET /api/contextual?playerId=&gameId=&quarter=&stat=&sport=` | Situational similarity match |

**DB connection:** singleton pool in `web/lib/db.ts` via `mssql`. Env var: `AZURE_SQL_CONNECTION_STRING`.

### Authentication

SWA built-in auth. All routes require `authenticated` role. Unauthenticated users redirect to GitHub login.

### Deployment

Auto-deploy workflow `azure-static-web-apps-red-smoke-0bbe1fb10.yml` triggers on push to `main`. `app_location: /web`.

---

## 8. In-Game Contextual Comparison

Live-only feature. Collapsible panel in Live View tab and player detail when game is active.

**Similarity dimensions (progressive relaxation):**
1. Starter/bench — anchor, never dropped
2. Pre-game spread bucket
3. Score margin bucket at quarter boundary
4. Player pace relative to average (hot/cold/on pace)

Drop order: 4 first, then 3, then 2. Multiple result sets shown simultaneously.

**Output per result set:** 10th/25th/50th/75th/90th percentile of final stat totals, proportion exceeding prop line, matched game IDs.

**Data:** No new tables. Uses `nba.player_box_score_stats`, `nba.daily_lineups`, `odds.game_lines`.

---

## 9. Pre-Game Automation

### Pre-game odds and grades refresh (TO BUILD)
Runs every 30 min UTC 14:00–03:00. Checks for games starting within 3 hours. Chains odds ETL then grading for that game.

### Lineup polling (TO BUILD)
Runs every 15 min UTC 16:00–03:00. Fetches lineups for unconfirmed games starting within 4 hours. Requires `etl/lineup_poll.py`.

---

## 10. Sport Status

### NBA
- **Data:** ACTIVE. Box scores, PT stats, lineups, players, teams, schedule loading. 2025-26 season backfill in progress.
- **Odds:** ACTIVE. Nightly cron UTC 10:00 upcoming mode.
- **Grading:** FUNCTIONAL (hit rate only). Nightly cron UTC 10:30.
- **Web app:** Placeholder live at SWA URL. Full scaffold not yet built.
- **Outstanding:** `nba.player_box_score_detail` (ESPN source) not populated. `common.dim_stat` seed (57 rows) incomplete. `flags` + component columns not yet added to `common.daily_grades`.

### MLB
- **Data:** Historical data loaded. `mlb_etl.py` INCOMPLETE. No automated schedule.
- **Odds/Grading/Web:** Not wired up. After NBA.

### NFL
- **Data:** `nfl_etl.py` built. No `nfl-etl.yml`. NOT AUTOMATED.
- **PFF grades:** Manual Selenium on VM. CSV columns unconfirmed — DDL not finalized.
- **Odds/Grading/Web:** Not wired up. After NBA and MLB.

---

## 11. Build Sequence

1. ~~**Stabilize NBA data pipeline**~~ — ACTIVE and running.
2. ~~**Add upcoming cron to odds-etl.yml**~~ — DONE.
3. ~~**Set up Azure Static Web Apps**~~ — DONE. URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`. Placeholder live.
4. **Install Node.js locally** — ThreatLocker learning mode session, whitelist by hash.
5. **Build Next.js scaffold** — full routing structure, `db.ts` + `queries.ts`, all API route stubs returning mock data. Add `staticwebapp.config.json` and Tailwind.
6. **Add `AZURE_SQL_CONNECTION_STRING` to SWA application settings** — required before any API route can query the DB.
7. **Build and validate all API routes** — wire each stub to real SQL against live Azure SQL. Validate responses.
8. **Build NBA web app views** — View 1 through 4. Persistent game strip first, then game research, then player detail, then At a Glance.
9. **Add keep-alive workflow** — GHA cron every 45 min during active hours.
10. **Build pregame-refresh.yml + lineup-poll.yml + lineup_poll.py.**
11. **Add live data layer** — `/api/live`, Live View tab, front-end polling.
12. **Build contextual comparison** — `/api/contextual`, `ContextualPanel.tsx`, percentile UI.
13. **Expand grading model** — migration script for new columns, implement trend + matchup components first.
14. **Complete MLB ETL and web views.**
15. **Automate NFL ETL and build NFL web views.**
16. **One app vs. three decision** — revisit once NBA views are polished.

---

## 12. Known Issues

| Issue | Status |
|-------|--------|
| Webshare proxy 502 errors for `nba_api` wrapper calls | Workaround active: proxy only for box score. |
| NFL workflow file missing | `nfl_etl.py` exists, `nfl-etl.yml` does not. |
| PFF DDL not finalized | Pending CSV column confirmation from VM Selenium run. |
| Azure SQL auto-pause cold start | Ping on app load masks it. Keep-alive cron (step 9) will eliminate it. |
| Grading: one component only | Hit rate only. Six additional components planned. |
| `flags` + component columns not in `common.daily_grades` | Migration required before any flag-producing component deploys. |
| `common.dim_stat` seed incomplete | 57 rows needed before `player_box_score_stats` FK can be enabled. |
| Node.js not installed locally | Install during ThreatLocker learning mode. |
| `AZURE_SQL_CONNECTION_STRING` not set in SWA | Must be added to SWA application settings before API routes can query DB. |
| Web app scaffold not built | Placeholder live. Full scaffold is step 5. |
| `pregame-refresh.yml` not built | Pre-game chaining does not exist yet. |
| `lineup-poll.yml` + `lineup_poll.py` not built | Lineup polling does not exist yet. |
| `/api/live` + `/api/contextual` not built | Live data and contextual endpoints do not exist yet. |

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
| Game research shows historical averages, not live box score | Purpose is prop research. Stat averages under active filters is the core workflow. |
| Full game payload fetched once, filtering client-side | Granularity filter changes must feel instant. |
| Granularity filter shared across both Player Detail panels | One control updates splits summary and game log simultaneously. |
| Grading components as nullable columns | No migration cascades, NULL excluded from composite, simpler to add incrementally. |
| Each grading component as isolated function | Independently testable and backfillable. Adding a component touches one function only. |
| Confidence flags as JSON array | Multiple flags per row, variable set over time, simpler than a flags table. |
| Lotto as flag not scored component | Binary pattern signal — forcing to 0–100 scale misrepresents its meaning. |
| Starter/bench as contextual anchor | Directly determines role, minutes, usage. Home/away has weaker per-player signal. |
| Probability distribution over point projection | Single number implies false precision. Distribution width is the confidence signal. |
| Multiple relaxation result sets shown simultaneously | Tight small sample and loose large sample tell different things. Both shown. |
| Minimum contextual sample threshold of 8 | Below 8, percentile distributions are too noisy. Configurable constant. |
| Pre-game refresh via polling + workflow chaining | GHA does not support dynamic scheduling. Polling is the only reliable approach. |
| Lineup polling every 15 min | Lineup releases are unpredictable. 15-min interval catches releases promptly. |
| Teams dict hardcoded in NBA ETL | Eliminated HTTP dependency after persistent proxy failures. |
| PT stats via direct HTTP | `nba_api` wrapper missing required headers for `leaguedashptstats`. |
| `proxies={"http": None, "https": None}` not `proxies=None` | Required to explicitly bypass proxy when env vars are set. |
| `range_type=2` with second-based ranges | `range_type=0` ignores period filters and returns full-game totals. |
| MLB dropped FK constraints on child tables | Allows independent truncate/reload of reference tables. |
| `DELETE` not `TRUNCATE` | TRUNCATE blocked by FK constraints even on empty child tables. |
| FanDuel as sole grading bookmaker | Most complete prop line coverage. `bookmaker_key` retained for future extension. |
| One `requirements.txt` for all sports | Simpler. Split only if a library conflict arises. |
| `fast_executemany=False` in grading engine | `True` causes NVARCHAR(MAX) truncation on wide rows. |
