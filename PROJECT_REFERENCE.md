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
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN` (web app deployment)
- Always retrieve file SHA before `create_or_update_file`

### Azure Static Web Apps
- Hosts Next.js front end + Azure Functions API backend under one deployment
- Free tier: 100GB bandwidth/month, custom domain, managed SSL
- Built-in auth: GitHub/Microsoft/Google as identity providers, zero code, configured via `staticwebapp.config.json`
- DB connection string in SWA application settings (not in code/repo)
- Next.js API routes auto-deploy as managed Azure Functions — no separate Functions app needed
- Deployment token: `AZURE_STATIC_WEB_APPS_API_TOKEN` secret

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
  nba-etl.yml          # Nightly cron + manual dispatch. ACTIVE
  mlb-etl.yml          # Manual dispatch only. INCOMPLETE
  odds-etl.yml         # Nightly cron UTC 10:00 (upcoming/nba) + manual. ACTIVE
  grading.yml          # Nightly cron UTC 10:30 + manual. ACTIVE
  nba-clear.yml        # Utility: clears NBA tables. Manual only.
  db_inventory.yml     # Utility: DB inventory. Manual only.
  grades_sample.yml    # Prototype. Not production.
  proxy-test.yml       # Proxy diagnostics. Not production.
  deploy-web.yml       # CI/CD for Next.js web app. Triggers on push to main under web/. TO BUILD
  pregame-refresh.yml  # Every 30 min pre-game window. Chains odds ETL + grades. TO BUILD
  lineup-poll.yml      # Every 15 min pre-game window. Polls lineups for unconfirmed games. TO BUILD
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

web/                   # Next.js application root. TO BUILD.
  app/
    page.tsx           # Root: redirects to /nba or sport landing
    layout.tsx         # Root layout: persistent game strip, nav, auth gate
    [sport]/
      page.tsx         # Game selection view
      game/[gameId]/
        page.tsx       # Game research view: active roster + stat averages, two sub-views
      player/[playerId]/
        page.tsx       # Player detail: splits summary + game log
      grades/
        page.tsx       # At a Glance: ranked props for today
    api/
      ping/route.ts              # SELECT 1 — cold start wake
      games/route.ts             # GET /api/games?sport=&date=
      roster/route.ts            # GET /api/roster?gameId=&sport=
      player-averages/route.ts   # GET /api/player-averages?gameId=&context=
      boxscore/route.ts          # GET /api/boxscore?gameId=&periods= — full per-period payload for client-side filtering
      player/route.ts            # GET /api/player?playerId=&games=&sport=
      grades/route.ts            # GET /api/grades?date=&sport=&gameId=
      live/route.ts              # GET /api/live?gameId=&sport= — in-progress stats, never touches SQL
      contextual/route.ts        # GET /api/contextual?playerId=&gameId=&quarter=&stat=&sport=
  components/
    GameStrip.tsx          # Persistent horizontal game selector (React state, not routing)
    RosterTable.tsx        # ESPN-style layout: players as rows, stat averages as columns, sticky name col
    PlayerRow.tsx          # Row with inline peek expansion (last 5 game lines)
    FocusedStatTable.tsx   # Single-stat view: players as rows, split contexts as columns
    GranularityFilter.tsx  # Multi-select period/play type/pitch filter; all updates client-side
    StatColumnPicker.tsx   # Pill row for active stat columns; persisted to localStorage by sport
    PlayerDetail.tsx       # Player view container: shared granularity filter + splits + game log
    SplitsTable.tsx        # Stats as columns, split contexts as rows, prop line ref row at top
    GameLogTable.tsx       # Reverse chronological, no month grouping, prop overlay row
    GradesBadge.tsx        # Grade pill component
    FlagTags.tsx           # Colored pill tags rendered next to player name or in At a Glance tags col
    ContextualPanel.tsx    # Collapsible live-game probability distribution panel
  lib/
    db.ts              # Azure SQL singleton pool via mssql package
    queries.ts         # All parameterized SQL — no inline SQL anywhere else
  staticwebapp.config.json
  tailwind.config.ts
  next.config.ts
  package.json
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
- `trend_grade FLOAT NULL` — weighted recent trend (last N games, decay-weighted)
- `matchup_grade FLOAT NULL` — opponent defensive rank vs. stat category
- `regression_grade FLOAT NULL` — recent avg vs. season baseline
- `streak_grade FLOAT NULL` — streak and cycle detection
- `adaptive_grade FLOAT NULL` — rolling forecast with correction factor
- `correlation_grade FLOAT NULL` — correlated player boost/suppression
- `composite_grade FLOAT NULL` — weighted average of all non-NULL active components, computed at write time
- `flags NVARCHAR(500) NULL` — JSON array of active signal flags, e.g. `["CYCLE","MATCHUP_OUTLIER"]`

**Component addition workflow:**
1. Add nullable column(s) via migration script (GitHub Actions, manual dispatch)
2. Implement component function in `grading/grade_props.py` — signature: `(player_id, market_key, line_value, grade_date) → (score: float, flags: list[str])`
3. Add to orchestrator component list with target weight
4. Update composite weight calculation
5. Nightly workflow picks up automatically
6. Backfill via `grading.yml` backfill mode — MERGE upsert updates existing rows

**Composite weight targets (starting point, subject to calibration):**

| Component | Weight | Notes |
|-----------|--------|-------|
| Historical hit rate (active) | 25% | Baseline frequency signal |
| Weighted recent trend | 20% | Recency signal |
| Matchup defense | 20% | Opponent context |
| Regression metric | 15% | Mean reversion |
| Streak/cycle detection | 10% | Phase modifier |
| Adaptive estimator | 7% | Rolling accuracy correction |
| Correlation | 3% | Boost/suppression from teammates |

### Confidence flags

Flags are discrete qualitative signals that bypass the composite score. A player with a weak composite but a `CYCLE` flag and a `MATCHUP_OUTLIER` flag is a different situation than a weak composite with no flags. Stored as JSON array in `flags` column. Each component function returns both a score and a list of any flags it sets. Orchestrator deduplicates and writes combined array.

**Defined flags:**

| Flag | Condition |
|------|-----------|
| `CYCLE` | Repeating interval pattern detected with sufficient occurrences — e.g. hit 3+ threes in every 5th game, 8 times this season, next game is the 5th |
| `MATCHUP_OUTLIER` | Strong opponent-specific pattern, sample >= 4 — e.g. averages 25pts vs this opponent, never scored below 20 |
| `HOT_STREAK` | Recent trend >= 60% above season line across last 5 games |
| `COLD_STREAK` | Recent trend >= 40% below season line across last 5 games |
| `REGRESSION_DUE` | Recent 10-game avg > 1.5 SD above 60-game baseline |
| `CORRELATED_BOOST` | Positively correlated teammate's line increased |
| `CORRELATED_SUPPRESS` | Negatively correlated teammate active (usage suppression) |
| `LOTTO` | Outlier odds pattern at +300 or longer — line set significantly below recent averages |

Flag thresholds are constants in each component function, subject to calibration.

UI treatment: pill tags next to player name in game research and player detail views. Dedicated tags column in At a Glance view. At a Glance can be filtered to show only flagged rows.

---

## 7. Web Application

### Stack rationale
- **Next.js over plain React:** file-based routing, built-in API routes, server components, first-class SWA support
- **Next.js API routes over separate Azure Functions:** SWA promotes them automatically — one repo, one deployment
- **Azure SWA over App Service or Vercel:** free tier covers the use case, built-in auth, native GHA integration, stays in existing Azure footprint
- **mssql driver over ORM:** direct parameterized queries, all SQL in `queries.ts`, no abstraction overhead

### Navigation model

**Persistent game strip:** Horizontal scrollable strip of today's game cards pinned at top of every sport view. Tapping a card swaps the content area below via React state (`selectedGameId` at sport layout level) — no route change, no history accumulation. URL updates via `router.replace`. Strip never disappears.

**Tab memory:** Active tab (Game Research, Player Detail, At a Glance) preserved in URL as `?tab=` query param. Switching games keeps the same tab. Defaults to `roster` if absent.

**Game research view (`/[sport]?tab=roster&gameId=...`):**
Shows both teams' projected or confirmed active rosters as a unified table. Values are historical stat averages under active filters — not a live box score. ESPN box score layout: sticky player name column, starters above bench, team separator. Two sub-views toggled by compact control:

- **Box Score View:** stat averages for all pinned stat columns under active granularity filter. Grade badge and prop line as additional columns when grades exist. Tapping a player row expands inline peek (last 5 game lines). Second tap on player name navigates to Player Detail.
- **Focused Stat View:** single stat selected; columns are split contexts side by side (season, last 10, vs opponent, vs similar total, vs similar spread, etc.) plus prop line, grade, safe line, lotto line.

Granularity filter (multi-select pill row above table): changes the period/play type/pitch context for all stat averages instantly. One API fetch on load; all filtering is client-side. Selections persist to `localStorage` by sport.

**Player detail view (`/[sport]/player/[playerId]`):**
Player-first entry point. No active game context required. Reachable from inline peek or directly by URL (shareable).

Granularity filter pinned at top — updates both panels simultaneously.

- **Panel A (Splits Summary):** stats as columns, split contexts as rows. Prop line as reference row at top of table so every average is readable against the line. All values update when granularity filter changes.
- **Panel B (Game Log):** reverse chronological, no month grouping, granularity filter applies per row (Q1+Q2 selected → each row shows first-half stats for that game). Prop line overlay row above most recent game when grade exists for today.

For MLB, additional panels: exit velocity log (date, opp, result, EV, LA, distance, xBA, PA, inning), pitcher vs. batter matchup stats, pitcher season-to-date stats.

**At a Glance view (`/[sport]/grades`):** Ranked list of today's graded props, sorted by grade descending. Filterable by game and stat. One column per active grade component plus composite. Tags column with confidence flag pills. Filterable to show only flagged rows.

**Splits to support (not exhaustive):**
NBA/NFL/MLB common: season, last N games, home, away, vs opponent, wins, losses, by day of week, by month.
NBA additions: by quarter, first half, second half, overtime, by score margin.
MLB additions: vs RHP, vs LHP, by batting order position, by inning, vs specific pitcher.
NFL additions: by play type (pass/run), by down and distance, home/away dome/outdoor.

**Granularity:**
- NBA: quarter-level stats stored, selectable (strip OT, Q1–Q3 only, etc.)
- MLB: pitch/plate appearance level (Statcast)
- NFL: play-by-play level

### API layer

All DB access through Next.js API routes. Front end never connects to Azure SQL directly. All queries parameterized — no string interpolation.

| Endpoint | Purpose |
|----------|---------|
| `GET /api/ping` | SELECT 1 — cold start wake. Returns `{status, latency_ms}` |
| `GET /api/games?sport=&date=` | Today's games. Joins games/schedule + odds.game_lines for spread/total. |
| `GET /api/roster?gameId=&sport=` | Projected/confirmed active roster. Sources `nba.daily_lineups`, falls back to full roster. Returns starter flag, team grouping. |
| `GET /api/player-averages?gameId=&context=` | Historical stat averages for all players in game, given split context. Full payload fetched once; client filters from there. |
| `GET /api/boxscore?gameId=&periods=` | Full per-period stat history for client-side granularity filtering. Joins `common.daily_grades` for grade data. |
| `GET /api/player?playerId=&games=&sport=` | Player game log, last N games (default 20, max 82). Joins box score, metadata, grades. |
| `GET /api/grades?date=&sport=&gameId=` | All graded props for a date. Joins `common.daily_grades` + `odds.upcoming_player_props` + `odds.player_map`. |
| `GET /api/live?gameId=&sport=` | In-progress player stats from NBA Stats API (`ScoreboardV3`). Server-side call, no proxy needed. Never written to SQL. Front end polls every 60s when game is `in_progress`. |
| `GET /api/contextual?playerId=&gameId=&quarter=&stat=&sport=` | Situational similarity match (see Section 8). |

**DB connection:** singleton pool in `web/lib/db.ts` via `mssql`. Connection string from `AZURE_SQL_CONNECTION_STRING` env var in SWA application settings.

### Authentication

SWA built-in auth via `staticwebapp.config.json`. All routes require `authenticated` role. Unauthenticated users redirect to GitHub login. To share with others: add them to SWA built-in roles via Azure portal.

### Live data layer

Completed game data is in Azure SQL, loaded by overnight ETL. Live in-game data is fetched at request time by the `/api/live` endpoint, merged with historical data in the API layer, returned as a unified payload. Never written to SQL. When a game ends, the live layer goes dark; the next morning's ETL loads the final result through the normal pipeline.

In the game research view, when a game is live, a third sub-view tab — **Live View** — appears alongside Box Score View and Focused Stat View. Shows each active player's current in-game stat line alongside their historical average for that stat, with a visual above/below indicator. Granularity filter applies to live stats the same as historical.

In the player detail view, when the player is in a live game, the top game log row is replaced with the live in-progress line (visually distinct from completed rows). Splits summary updates in real time as live stats change.

### Deployment

`deploy-web.yml` triggers on push to `main` for files under `web/`. Uses `Azure/static-web-apps-deploy@v1` with `AZURE_STATIC_WEB_APPS_API_TOKEN` secret. `app_location: /web`, `output_location: .next`, `api_location: ""` (SWA manages Next.js API routes).

### Cost
| Resource | Tier | Cost |
|----------|------|------|
| Azure Static Web Apps | Free | $0 |
| Azure Functions (via SWA) | Included | $0 |
| Azure SQL | Existing Serverless | No change |
| Bandwidth | Free tier 100GB/month | $0 |

---

## 8. In-Game Contextual Comparison

Live-only feature. Appears as a collapsible panel in the Live View tab (game research view) and in the player detail view when a live game is active. Not shown for completed or upcoming games.

### What it answers

Given where this player is right now in this game, what does their own history in comparable situations tell us about how the rest of the game is likely to go, and how confident should we be in that projection?

Output is a probability distribution over possible final stat totals — not a single projected number. Distribution width is the confidence signal: tight range = reliable pattern, wide range = high variance regardless of sample size.

### Similarity dimensions

Always anchor on **starter vs. bench** (dimension 1). Never compare a starting performance against a bench performance. Then attempt all four simultaneously and progressively relax if sample is below minimum threshold (default: 8 games).

| # | Dimension | Description |
|---|-----------|-------------|
| 1 | Starter / bench | Whether the player started or came off the bench this game. Anchor — always applied. |
| 2 | Pre-game spread bucket | Large fav (>6.5), small fav (3–6.5), pick (<3 either side), small dog (3–6.5), large dog (>6.5). From `odds.game_lines`. |
| 3 | Score margin bucket at quarter boundary | Winning big (>10), winning small (4–10), close (within 3), losing small (4–10), losing big (>10). |
| 4 | Player pace relative to average | Hot (≥1.5x per-quarter average at this point), cold (≤0.5x), on pace. |

Progressive relaxation order: drop dimension 4 first, then 3, then 2. Dimension 1 is never dropped.

Multiple result sets are shown simultaneously — each is labeled with its dimension set and sample size. A 4-dimension match with 9 games and a 2-dimension match with 35 games tell different things. Both are shown; user weighs them.

### Output

`GET /api/contextual?playerId=...&gameId=...&quarter=2&stat=points&sport=nba`

Per matched result set:
- Dimensions used + sample size
- 10th, 25th, 50th, 75th, 90th percentile of final stat totals
- Proportion of matched games where final exceeded today's prop line
- Proportion finishing above / at / below season average
- Matched game IDs (auditability)
- Plain-language summary generated in API layer

Multiple stats via comma-separated `stat` param in one call.

### Data requirements

No new ETL or SQL tables. All data exists:
- `nba.player_box_score_stats` (quarter-level) — reconstructs in-game state vectors
- `nba.daily_lineups` — starter/bench flag
- `odds.game_lines` — pre-game spread

API route assembles historical game state vectors at query time. NBA-first feature; MLB/NFL require inning-level or play-by-play data not yet loaded.

### UI treatment

Panel leads with plain-language summary for tightest match. Horizontal percentile band visualization (box plot style) with today's prop line overlaid as reference marker. Secondary rows for each relaxed dimension result set. Matched dimensions and sample size always visible.

---

## 9. Pre-Game Automation

Nightly 4:00 AM cron is the baseline. Two additional workflows handle time-sensitive updates before games start.

### Pre-game odds and grades refresh (`pregame-refresh.yml` — TO BUILD)

Runs every 30 min, UTC 14:00–03:00 (9:00 AM–10:00 PM CST). On each run:
1. Check whether any game that day starts within the next X hours (default: 3, configurable workflow input)
2. If yes and odds not yet refreshed: trigger `odds-etl.yml` in `upcoming` mode for that game
3. On odds ETL completion: dispatch `grading.yml` via GitHub API with `game_id` input to regrade only that game's props

Chaining: odds ETL job calls GitHub API at end of run to dispatch `grading.yml`. Grading workflow already accepts `--date` argument; needs `game_id` input added to restrict to single game. No changes to underlying scripts otherwise.

### Lineup polling (`lineup-poll.yml` — TO BUILD)

Runs every 15 min, UTC 16:00–03:00 (11:00 AM–10:00 PM CST). On each run:
1. Check which games that day have no confirmed lineup and start within 4 hours
2. Fetch lineups from NBA Stats API for those games
3. Upsert to `nba.daily_lineups`
4. Exit early if all games confirmed or started

Requires `etl/lineup_poll.py` (TO BUILD). Follows standard incremental pattern: check existing lineup records, fetch only for unconfirmed games, upsert. Starter/bench flag from this table feeds the contextual comparison anchor dimension.

---

## 10. Application Views

### Structure (all three sports)

**View 1 — Game Selection (`/[sport]`):**
Card-based layout. Each card: game time, team logos, spread/total from odds data, probable pitchers (MLB). Tapping sets `selectedGameId` in persistent strip, transitions content area to View 2.

**View 2 — Game Research (`/[sport]?tab=roster&gameId=...`):**
See Section 7 navigation model. Both teams' projected/confirmed active rosters. Stat averages under active filters. Box Score View + Focused Stat View. Granularity filter. Inline player peek. Live View tab when game is in progress.

**View 3 — Player Detail (`/[sport]/player/[playerId]`):**
See Section 7 navigation model. Splits summary + game log, shared granularity filter. Contextual comparison panel when game is live. MLB additional panels: exit velocity log, pitcher matchup, pitcher season stats.

**View 4 — At a Glance (`/[sport]/grades`):**
Ranked graded props, grade descending. One column per active grade component + composite. Tags column with confidence flag pills. Filterable by game, stat, or flag presence.

### One app or three?
Defer until NBA views are built. Three separate SWA instances is simpler to build and debug; one app with sport-switching at `/[sport]` is cleaner for daily use.

---

## 11. Sport Status

### NBA
- **Data:** ACTIVE. Box scores, PT stats, lineups, players, teams, schedule loading. 2025-26 season backfill in progress.
- **Odds:** ACTIVE. Nightly cron UTC 10:00 upcoming mode.
- **Grading:** FUNCTIONAL (hit rate only). Nightly cron UTC 10:30.
- **Web app:** NOT STARTED. Build here first.
- **Outstanding:** `nba.player_box_score_detail` (ESPN source) not populated. `common.dim_stat` seed (57 rows) incomplete — blocks `player_box_score_stats` FK if enabled. `flags` + component columns not yet added to `common.daily_grades`.

### MLB
- **Data:** Historical data loaded. `mlb_etl.py` exists but INCOMPLETE. No automated schedule.
- **Odds:** Manual only.
- **Grading:** Not wired up.
- **Web app:** NOT STARTED. After NBA.
- **Outstanding:** Complete ETL, add `mlb-etl.yml` with cron, backfill.

### NFL
- **Data:** `nfl_etl.py` built via `nflreadpy`. Schema created. No `nfl-etl.yml`. NOT AUTOMATED.
- **PFF grades:** Manual Selenium script on Windows VM. CSV column headers not confirmed — DDL cannot be finalized.
- **Odds:** Manual only.
- **Grading:** Not wired up.
- **Web app:** NOT STARTED. After NBA and MLB.
- **Outstanding:** Create `nfl-etl.yml`. Confirm PFF CSV columns before finalizing DDL.

---

## 12. Build Sequence

1. **Stabilize NBA data pipeline** — confirm backfill current, upcoming mode and grading cron running reliably.
2. ~~**Add upcoming cron to odds-etl.yml**~~ — DONE. UTC 10:00.
3. **Set up Azure Static Web Apps** — provision resource, connect repo, confirm deploy pipeline with placeholder Next.js app.
4. **Install Node.js locally** — ThreatLocker learning mode session, whitelist by hash.
5. **Build Next.js scaffold** — `web/` directory, routing structure, `db.ts` connection layer, all API route stubs returning mock data.
6. **Build and validate all API routes** — ping, games, roster, player-averages, boxscore, player, grades. Confirm queries return correct data against live Azure SQL.
7. **Build NBA web app views** — View 1 through 4 against live data. Persistent game strip first, then game research, then player detail, then at a glance.
8. **Add keep-alive workflow** — GHA cron every 45 min during active hours. Eliminates Azure SQL cold start.
9. **Build pregame-refresh.yml + lineup-poll.yml + lineup_poll.py** — pre-game odds/grades chaining and lineup polling.
10. **Add live data layer** — `/api/live` endpoint, Live View tab, polling on front end.
11. **Build contextual comparison** — `/api/contextual`, `ContextualPanel.tsx`, probability distribution UI.
12. **Expand grading model** — add `flags` column and component columns to `common.daily_grades`, implement trend + matchup components first, add composite_grade.
13. **Complete MLB ETL** — finish script, `mlb-etl.yml` with cron, backfill.
14. **Build MLB web app views** — reuse NBA structure, add MLB-specific panels.
15. **Automate NFL ETL** — create `nfl-etl.yml`, confirm PFF columns, finalize DDL.
16. **Build NFL web app views.**
17. **One app vs. three decision** — revisit once NBA views are polished.

---

## 13. Known Issues

| Issue | Status |
|-------|--------|
| Webshare proxy 502 errors for `nba_api` wrapper calls | Workaround active: proxy eliminated for teams, players, game discovery. Proxy only for box score via monkey-patch. |
| ~~Odds ETL no nightly cron~~ | RESOLVED. UTC 10:00. |
| NFL workflow file missing | `nfl_etl.py` exists, `nfl-etl.yml` does not. |
| PFF DDL not finalized | Pending CSV column confirmation from Selenium run on VM. |
| Azure SQL auto-pause cold start | Ping on app load masks it. Keep-alive cron (step 8) will eliminate it. |
| Grading: one component only | Hit rate only. Six additional components + flags column planned, none implemented. |
| `flags` + component columns not in `common.daily_grades` | Migration script required before any flag-producing component is deployed. |
| `common.dim_stat` seed incomplete | 57 rows needed before `player_box_score_stats` FK can be enabled. |
| Node.js not installed locally | Install during ThreatLocker learning mode. |
| Web app not built | SWA not yet provisioned. Build sequence steps 3–11 pending. |
| `pregame-refresh.yml` not built | Pre-game odds/grades chaining does not exist yet. |
| `lineup-poll.yml` + `lineup_poll.py` not built | Lineup polling does not exist yet. Until built, lineup data relies on nightly ETL only. |
| `/api/live` + `/api/contextual` not built | Live data and contextual comparison endpoints do not exist yet. |
| `proxy-test.yml` is not production | Diagnostic file, not used in production pipeline. |

---

## 14. Decision Log

| Decision | Rationale |
|----------|-----------|
| Next.js over plain React | File-based routing, built-in API routes, server components, first-class SWA support. No separate routing library or Functions app needed. |
| Next.js API routes over separate Azure Functions app | SWA promotes them automatically. One repo, one deployment, no separate Functions resource. |
| Azure SWA over App Service or Vercel | Free tier covers the use case. Built-in auth. Native GHA integration. Stays in existing Azure footprint. |
| mssql driver over ORM | Direct parameterized queries, full control, all SQL in one file, no abstraction overhead. |
| Web app over Power BI mobile | Power BI mobile is a report viewer. The required interaction model cannot be built there reliably. |
| Power BI retained for ad hoc analysis | Faster for exploratory work, pivot analysis, grade distribution review. Not for daily game-day workflow. |
| GitHub built-in auth for access control | Zero code, zero user management, GitHub accounts are already the access control primitive (private repo). |
| Persistent game strip as React state, not routing | Switching games must not accumulate browser history or trigger a full page reload. `router.replace` syncs URL without history push. |
| Tab memory via URL query parameter | Preserves tab across game switches. Linkable, shareable, survives refresh. |
| Game research view shows historical averages, not live box score | Purpose is prop research, not score tracking. Stat averages for the expected active roster under active filters is the core research workflow. |
| Full game data payload fetched once, filtering client-side | Granularity filter changes must feel instant. One fetch on load; all filtering in browser against that payload. |
| Granularity filter shared across both panels in Player Detail | One control updates both splits summary and game log simultaneously. Filter state lives at view level, passed as prop. |
| Grading components as nullable columns on daily_grades | No schema migration cascades, no JOIN needed to read all scores, NULL components excluded from composite (not zeroed). Simpler to add incrementally. |
| Each grading component as an isolated function | Independently testable, independently backfillable, no coupling. Adding a component touches one function and the orchestrator weight list only. |
| Confidence flags as JSON array in flags column, not separate columns or table | Multiple flags per row, variable set over time, rendered as tags in UI. JSON array in a single column is simpler than a flags table or a boolean column per flag. |
| Lotto line as a flag, not a scored component | Binary pattern signal, not a probability distribution. Forcing onto 0–100 scale would misrepresent its meaning. |
| Starter/bench as contextual comparison anchor dimension instead of home/away | Starter vs. bench directly determines role, minutes, and usage for that game. Home/away has weaker signal for individual player projections. Never relaxed — always the anchor. |
| Probability distribution over point projection for contextual comparison | Single projected number implies false precision. Percentile distribution communicates central tendency and spread. Distribution width is itself the confidence signal. |
| Multiple dimension relaxation result sets shown simultaneously | Tight match with small sample and loose match with large sample tell different things. Showing both lets user weigh specific vs. general pattern evidence. |
| Minimum sample threshold of 8 for contextual comparison | Below 8, percentile distributions are too noisy to be meaningful. Configurable constant; subject to calibration. |
| Pre-game refresh via 30-min polling + workflow chaining vs. computed trigger times | GitHub Actions does not support dynamic scheduling. Polling during the active window checks whether X-hours threshold is met; chaining dispatches grading only when odds ETL completes. More reliable than computed trigger times. |
| Lineup polling every 15 min with 4-hour lookahead | Lineup releases are not predictable (typically 30–90 min pre-game, sometimes later). Polling is the only reliable approach. 15-min interval catches releases promptly. Self-terminates when all games confirmed or started. |
| Teams dict hardcoded in NBA ETL | Eliminated HTTP dependency after persistent proxy failures with `TeamInfoCommon`. |
| PT stats via direct HTTP, not `nba_api` wrapper | `nba_api` doesn't send required headers for `leaguedashptstats`. Direct requests with browser headers works reliably. |
| `proxies={"http": None, "https": None}` not `proxies=None` | Required to explicitly bypass proxy when env vars set. `None` alone does not override env proxy. |
| `range_type=2` with second-based ranges for box scores | `range_type=0` ignores period filters and returns full-game totals, defeating quarter-level storage. |
| MLB dropped FK constraints on child tables | Allows independent truncate/reload of reference tables without cascade ordering. |
| `DELETE` not `TRUNCATE` | TRUNCATE blocked by FK constraints even on empty child tables in SQL Server. |
| FanDuel as sole grading bookmaker | Reference bookmaker, most complete prop line coverage. `bookmaker_key` column retained for future extension. |
| One `requirements.txt` for all sports | Simpler. Create sport-specific file only if a library conflict arises. |
| `fast_executemany=False` in grading engine | `True` causes NVARCHAR(MAX) truncation on wide rows. Grading uses `False`; ETL uses `True`. |
