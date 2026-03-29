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
- Cold start mitigation: `/api/ping` is public (anonymous role). Keep-alive workflow (`keepalive.yml`) pings it every 45 min UTC 10:00–05:00. Should eliminate morning cold starts.

### GitHub Actions
- Repo: `SchnappAPI/sports-modeling` (private)
- All automated Python runs here. Python cannot run locally (ThreatLocker) or on a schedule from the Windows VM (auto-logout).
- Runners: ephemeral ubuntu-latest; every run installs ODBC Driver 18 and pip deps fresh
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`, `AZURE_STATIC_WEB_APPS_API_TOKEN_RED_SMOKE_0BBE1FB10`
- Always retrieve file SHA before `create_or_update_file`

### Azure Static Web Apps
- Resource name: `sports-modeling-web`
- URL: `https://red-smoke-0bbe1fb10.2.azurestaticapps.net`
- Resource group: `sports-modeling` / Region: Global / SKU: Free
- Deploy workflow: `.github/workflows/azure-static-web-apps-red-smoke-0bbe1fb10.yml`
- Triggers on push to `main`; `app_location: /web`
- Next.js 15.2.8, React 19
- Auth: GitHub identity provider. All routes require `authenticated` role EXCEPT `/api/ping` which is `anonymous`. 401 redirects to GitHub login.
- DB connection string in SWA application settings — env var: `AZURE_SQL_CONNECTION_STRING`
- Status: **Step 8 fully complete. Step 9 (keep-alive) complete. Step 10 next.**

### Local Dev
- Node.js v24.12.0 installed. `npm run dev` blocked by ThreatLocker (next.cmd blocked).
- Testing: push to `main`, check live SWA URL. ~90 seconds per deploy cycle.

---

## 3. Repository Structure

```
.github/workflows/
  nba-etl.yml                                         # Nightly cron + manual dispatch. ACTIVE
  mlb-etl.yml                                         # Manual dispatch only. INCOMPLETE
  odds-etl.yml                                        # Nightly cron UTC 10:00 + manual. ACTIVE
  grading.yml                                         # Nightly cron UTC 10:30 + manual. ACTIVE
  keepalive.yml                                       # Every 45 min UTC 10:00-05:00. Pings /api/ping. ACTIVE
  nba-clear.yml                                       # Utility: clears NBA tables. Manual only.
  db_inventory.yml                                    # Utility: DB inventory. Manual only.
  azure-static-web-apps-red-smoke-0bbe1fb10.yml       # SWA CI/CD. ACTIVE.
  pregame-refresh.yml                                 # TO BUILD (step 10)
  lineup-poll.yml                                     # TO BUILD (step 10)
  diag-lineups.yml / diag-schedule.yml                # Diagnostic utilities. Manual only.

etl/
  db.py / nba_etl.py / mlb_etl.py / nfl_etl.py / odds_etl.py
  requirements.txt

grading/
  grade_props.py       # NBA prop grading. FUNCTIONAL (hit rate only).

web/
  app/
    page.tsx                          # Redirects to /nba
    layout.tsx / globals.css
    nba/
      page.tsx                        # Suspense wrapper — LIVE
      NbaPageInner.tsx                # Game strip + GameTabs + At a Glance link — LIVE
      grades/
        page.tsx                      # Suspense wrapper — LIVE
        GradesPageInner.tsx           # At a Glance ranked prop grades — LIVE
      player/[playerId]/
        page.tsx                      # Suspense wrapper — LIVE
        PlayerPageInner.tsx           # Splits strip + full season game log with DNP — LIVE
    api/
      ping/route.ts           # PUBLIC. SELECT 1.
      games/route.ts          # Auth. {sport, date, games:[]}
      roster/route.ts         # Auth. {gameId, roster:[]}
      player-averages/route.ts # Auth. {gameId, lastN, players:[]} — lineup-anchored
      team-averages/route.ts  # Auth. {players:[]} — team_id-anchored, no lineup dependency
      boxscore/route.ts       # Auth. {gameId, rows:[]}
      player/route.ts         # Auth. {playerId, lastN, sport, log:[]} — full season with DNP
      grades/route.ts         # Auth. {date, gameId, grades:[]}
      live/route.ts           # Stub — TO BUILD (step 11)
      contextual/route.ts     # Stub — TO BUILD (step 12)
  components/
    GameStrip.tsx      # Scrollable game cards. Game interface includes homeTeamId/awayTeamId/homeTeamAbbr/awayTeamAbbr.
    GameTabs.tsx       # Roster/Stats/Box Score tabs. Props: gameId, homeTeamId, awayTeamId, homeTeamAbbr, awayTeamAbbr.
    RosterTable.tsx    # From nba.daily_lineups. Empty for today until nightly ETL.
    StatsTable.tsx     # From /api/team-averages. Player links pass opp= param for vs-split.
    BoxScoreTable.tsx  # Period filter (All/1Q/2Q/3Q/4Q/OT). Client-side aggregation.
  lib/
    db.ts / queries.ts
  staticwebapp.config.json   # /api/ping anonymous; all others authenticated
  next.config.mjs            # serverExternalPackages: ['mssql']
  package.json               # next 15.2.8, react 19, mssql ^11, tailwindcss ^3
```

---

## 4. Database Schema

### Schemas
`nba`, `mlb`, `nfl`, `odds`, `common`

### NBA Tables
- `nba.games` — completed games only (box score ETL source)
- `nba.schedule` — ALL games regardless of status. USE THIS for game queries.
- `nba.teams` — hardcoded static dict in ETL
- `nba.players` — `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active). Stats tab queries by team_id directly.
- `nba.daily_lineups` — NO player_id/team_id. Keyed by `player_name` + `team_tricode`. `starter_status` = 'Starter'/'Bench'. Coverage: prior day and earlier.
- `nba.player_box_score_stats` — quarters only: '1Q','2Q','3Q','4Q','OT'. NO 'FullGame'. Minutes column = `minutes` (DECIMAL). Sum quarters for game totals.
- `nba.player_passing_stats` / `nba.player_rebound_chances` — game-level PT stats

### Odds Tables
- `odds.event_game_map` — event_id → game_id + game_date. Coverage through 2026-03-23.
- `odds.upcoming_player_props` — today's FanDuel lines
- `odds.player_props` — historical FanDuel lines
- `odds.upcoming_game_lines` / `odds.upcoming_events` — today's game lines

### Common Tables
- `common.daily_grades` — grading output. Coverage through 2026-03-23.
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
- Box scores: `playergamelogs` with period filter params
- OT: `Period=""` + `GameSegment=Overtime`
- Teams: hardcoded STATIC_TEAMS dict
- Players: `playerindex` via proxy
- PT stats: direct HTTP to `leaguedashptstats` with `proxies={"http": None, "https": None}`
- Proxy: Webshare rotating residential. Secret: `NBA_PROXY_URL`.

### Odds ETL
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly cron UTC 10:00: `upcoming/nba/days-ahead=1`

---

## 6. Grading Model

**Formula:**
```
hit_rate_60 = hits/games (stat > line, prior 60 days)
hit_rate_20 = hits/games (stat > line, prior 20 days)
weighted    = 0.60 × hit_rate_20 + 0.40 × hit_rate_60
grade       = weighted × 100 (rounded to 1 decimal)
```
Falls back to hit_rate_60 if sample_size_20 < 5.

**Writes to:** `common.daily_grades` — one row per (grade_date, event_id, player_id, market_key, bookmaker_key, line_value)

**Schema:** `grade_id`, `grade_date`, `event_id`, `game_id`, `player_id`, `player_name`, `market_key`, `bookmaker_key`, `line_value`, `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`, `created_at`

**Expansion planned** (none implemented): trend_grade, matchup_grade, regression_grade, streak_grade, adaptive_grade, correlation_grade, composite_grade, flags columns. Migration script required before any deploy.

---

## 7. Web Application

### Navigation
- `/nba` — game strip + tabs. Header has "At a Glance" link top-right.
- `/nba?gameId=&tab=` — active game with Roster/Stats/Box Score tabs
- `/nba/player/[playerId]?gameId=&tab=&opp=` — splits strip + full season game log
- `/nba/grades?gameId=` — ranked prop grades

### API Response Shapes
- `/api/games` → `{ sport, date, games: GameRow[] }`
- `/api/roster` → `{ gameId, roster: RosterRow[] }`
- `/api/team-averages` → `{ players: PlayerAvg[] }` — by team_id, no lineup dependency
- `/api/player-averages` → `{ gameId, lastN, players: PlayerAvg[] }` — lineup-anchored
- `/api/boxscore` → `{ gameId, rows: BoxRow[] }`
- `/api/player` → `{ playerId, lastN, sport, log: GameLogRow[] }` — full season, includes dnp:boolean
- `/api/grades` → `{ date, gameId, grades: GradeRow[] }`

### Game Interface
```typescript
interface Game {
  gameId: string; gameDate: string; gameStatus: number | null;
  gameStatusText: string | null; homeTeamId: number; awayTeamId: number;
  homeTeamAbbr: string; awayTeamAbbr: string;
  homeTeamName: string; awayTeamName: string;
  spread: number | null; total: number | null;
}
```

### Player Detail Page
- Splits strip at top: Season, Last 10, vs [opp] (when opp= param present)
- All three computed client-side from game log — no extra API call
- `opp=` passed from StatsTable player link (opponent team abbreviation)
- Full season game log below: every team game, DNP rows dimmed at opacity-40 with "DNP" spanning stat columns
- Header shows "X GP / Y team games"

### Next.js 15 Patterns
- `useSearchParams()` requires `<Suspense>` wrapper. Pattern: thin `page.tsx` → Suspense → `*Inner.tsx`
- Dynamic route params: `Promise<{...}>`, must be awaited in server components
- `'use client'` required for any component using hooks

---

## 8. Sport Status

### NBA
- Data: ACTIVE. Box scores current through 2026-03-28.
- Odds: event_game_map and daily_grades through 2026-03-23; backfill running.
- Grading: FUNCTIONAL (hit rate only). Through 2026-03-23.
- Web: ALL VIEWS LIVE. Game strip, Roster, Stats, Box Score, Player Detail (splits + game log), At a Glance.

### MLB / NFL
- ETL partially built. Not wired to web or grading. After NBA.

---

## 9. Build Sequence

1–8. ~~DONE~~ — NBA data pipeline, odds ETL, SWA setup, API routes, all NBA UI views, keep-alive workflow.
9. **Step 10: pregame-refresh.yml + lineup-poll.yml + lineup_poll.py**
10. **Step 11: Live data layer** — `/api/live`, Live View tab, front-end polling.
11. **Step 12: Contextual comparison**
12. **Step 13: Grading model expansion** — trend + matchup components first, then migration script.
13. **Step 14: MLB ETL and web views**
14. **Step 15: NFL ETL automation and web views**

---

## 10. Known Issues

| Issue | Status |
|-------|--------|
| next.cmd blocked by ThreatLocker | Testing via push to main + live SWA. ~90s/cycle. |
| NFL workflow file missing | `nfl_etl.py` exists, `nfl-etl.yml` does not. |
| PFF DDL not finalized | Pending CSV column confirmation. |
| Grading: one component only | Hit rate only. Migration + 6 more components planned. |
| `flags` + component columns not in `common.daily_grades` | Migration required before any flag-producing component. |
| odds/grading backfill gap | event_game_map and daily_grades only through 2026-03-23. |
| `pregame-refresh.yml` not built | Step 10. |
| `lineup-poll.yml` + `lineup_poll.py` not built | Step 10. |
| `/api/live` + `/api/contextual` not built | Steps 11–12. |
| Roster tab empty for today | nba.daily_lineups populated through prior day only. Fixed by lineup poll (step 10). |
| Box Score tab empty for today | Today's games not yet played. Populates overnight. |
| At a Glance empty for today | Grades through 2026-03-23. Populates as backfill + nightly ETL catches up. |

---

## 11. Decision Log

| Decision | Rationale |
|----------|-----------|
| Next.js over plain React | File-based routing, built-in API routes, server components, first-class SWA support. |
| Next.js API routes over separate Azure Functions | SWA promotes them automatically. One repo, one deployment. |
| Azure SWA over App Service or Vercel | Free tier, built-in auth, native GHA integration. |
| mssql driver over ORM | Direct parameterized queries, full control, all SQL in one file. |
| Web app over Power BI mobile | Power BI mobile is a report viewer. Required interaction model cannot be built there. |
| GitHub built-in auth | Zero code, zero user management. |
| Persistent game strip as React state | No browser history accumulation, no full page reload on game switch. |
| Tab memory via URL query parameter | Preserves tab across game switches. Linkable, survives refresh. |
| Stats tab uses nba.players not nba.daily_lineups | Lineup table empty for today; players table always has active roster. |
| Full boxscore fetched once, filtered client-side | Period filter must feel instant with no round trips. |
| Player game log anchored to team's game schedule | Full season view with DNP rows requires starting from team games, not player box scores. |
| Splits computed client-side from game log | No extra API call needed; data already loaded. opp= param passed from StatsTable link. |
| vs-opponent split this season only | No multi-season historical box score data available. |
| /api/ping made public (anonymous) | Required for keep-alive workflow to reach DB without auth headers. |
| useSearchParams requires Suspense in Next.js 15 | Build fails without it. |
| Game interface includes homeTeamId/awayTeamId/homeTeamAbbr/awayTeamAbbr | Required for team-averages call and opp= link construction. |
| Grading components as nullable columns | No migration cascades, NULL excluded from composite. |
| Confidence flags as JSON array | Multiple flags per row, variable set over time. |
| `fast_executemany=False` in grading engine | True causes NVARCHAR(MAX) truncation. |
| `mssql` as `serverExternalPackages` | Prevents Next.js bundling mssql native bindings. |
| Next.js dynamic route params as `Promise<{...}>` | Required in Next.js 15. |
| `minutes` not `min` | `min` is reserved in SQL Server. |
| No 'FullGame' period — always SUM quarters | ETL stores quarters only. |
| nba.daily_lineups has no player_id/team_id | Join to nba.players on player_name (LEFT JOIN). |
| DELETE not TRUNCATE | FK constraints block TRUNCATE even on empty child tables. |
| FanDuel as sole grading bookmaker | Most complete prop line coverage. |
| Teams dict hardcoded in NBA ETL | Eliminated HTTP dependency after persistent proxy failures. |
| PT stats via direct HTTP with explicit proxy bypass | nba_api wrapper missing required headers; env var proxy must be explicitly bypassed. |
