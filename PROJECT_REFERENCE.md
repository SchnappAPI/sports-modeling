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
- Resource group: `sports-modeling` / Region: Global / SKU: Free
- Deploy workflow: `.github/workflows/azure-static-web-apps-red-smoke-0bbe1fb10.yml`
- Triggers on push to `main`; `app_location: /web`
- Next.js 15.2.8, React 19
- Auth: GitHub identity provider. All routes require `authenticated` role EXCEPT `/api/ping` which is `anonymous`. 401 redirects to GitHub login.
- DB connection string in SWA application settings — env var: `AZURE_SQL_CONNECTION_STRING`
- Status: **Steps 1–11 complete. Step 12 next.**

### Local Dev
- Node.js v24.12.0 installed. `npm run dev` blocked by ThreatLocker (next.cmd blocked).
- Testing: push to `main`, check live SWA URL. ~90 seconds per deploy cycle.

---

## 3. Repository Structure

```
.github/workflows/
  nba-etl.yml                                         # Nightly cron + manual dispatch. ACTIVE
  nba-live.yml                                        # Every 5 min UTC 17:00-06:00. Live box score. ACTIVE (step 11)
  mlb-etl.yml                                         # Manual dispatch only. INCOMPLETE
  odds-etl.yml                                        # Nightly cron UTC 10:00 + manual. ACTIVE
  grading.yml                                         # Nightly cron UTC 10:30 + manual. ACTIVE
  keepalive.yml                                       # Every 45 min UTC 10:00-05:00. Pings /api/ping. ACTIVE
  lineup-poll.yml                                     # Every 15 min UTC 16:00-03:59. ACTIVE (step 10)
  pregame-refresh.yml                                 # Every 30 min UTC 14:00-03:30. Chains odds+grading. ACTIVE (step 10)
  nba-clear.yml                                       # Utility: clears NBA tables. Manual only.
  db_inventory.yml                                    # Utility: DB inventory. Manual only.
  azure-static-web-apps-red-smoke-0bbe1fb10.yml       # SWA CI/CD. ACTIVE.
  diag-lineups.yml / diag-schedule.yml                # Diagnostic utilities. Manual only.

etl/
  db.py / nba_etl.py / mlb_etl.py / nfl_etl.py / odds_etl.py
  nba_live.py       # Intra-day live box score updater (step 11). ACTIVE.
  lineup_poll.py    # Standalone lineup poller (step 10). ACTIVE.
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
      game-grades/route.ts    # Auth. {gameId, grades:[]} — per-game prop lines for box score coloring
      player-grades/route.ts  # Auth. {playerId, grades:[]} — all grades for a player
      team-players/route.ts   # Auth. {gameId, players:[]} — roster for player switcher
      contextual/route.ts     # Stub — TO BUILD (step 12)
  components/
    GameStrip.tsx      # Scrollable game cards. Pulsing red dot for live games (gameStatus=2).
    GameTabs.tsx       # Live/Roster/Stats/Box Score tabs. Live tab only shown when gameStatus=2.
    LiveBoxScore.tsx   # Polls /api/boxscore every 60s. Keys BoxScoreTable on tick to force remount.
    RosterTable.tsx    # From nba.daily_lineups. Populated pre-game by lineup-poll.yml.
    StatsTable.tsx     # From /api/team-averages. Player links pass opp= param for vs-split.
    BoxScoreTable.tsx  # Period filter (All/1Q/2Q/3Q/4Q/OT). Client-side aggregation. Player links.
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
- `nba.schedule` — ALL games regardless of status. USE THIS for game queries. home_score/away_score updated live by nba_live.py.
- `nba.teams` — hardcoded static dict in ETL
- `nba.players` — `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1=active). Stats tab queries by team_id directly.
- `nba.daily_lineups` — NO player_id/team_id. Keyed by `player_name` + `team_tricode`. `starter_status` = 'Starter'/'Bench'. Coverage: prior day and earlier. Now updated pre-game by lineup-poll.yml.
- `nba.player_box_score_stats` — quarters only: '1Q','2Q','3Q','4Q','OT'. NO 'FullGame'. Minutes column = `minutes` (DECIMAL). Sum quarters for game totals. Written live by nba_live.py every 5 min during games.
- `nba.player_passing_stats` / `nba.player_rebound_chances` — game-level PT stats

### Odds Tables
- `odds.event_game_map` — event_id → game_id + game_date. Coverage through 2026-03-23. Upcoming entries written by odds_etl upcoming mode.
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

### NBA Live ETL (step 11)
- Script: `etl/nba_live.py` — imports helpers from nba_etl.py (safe to import; nba_etl guards main() under `if __name__ == "__main__"`)
- Gate: queries `nba.schedule` for `game_status = 2` today. Exits if none.
- When gate passes: calls `ScoreboardV3` once to update all scores in `nba.schedule`, then `BoxScoreTraditionalV3` per in-progress game_id.
- Upserts into `nba.player_box_score_stats` on PK `(game_id, player_id, period)` — idempotent.
- Period mapping from V3 integers: 1→1Q, 2→2Q, 3→3Q, 4→4Q, 5+→OT.
- Minutes parsed from PT##M##.##S clock string format.
- Workflow: `nba-live.yml` — every 5 min UTC 17:00–06:00 (noon–1am ET). Two cron entries to span midnight.

### Odds ETL
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`
- Nightly cron UTC 10:00: `upcoming/nba/days-ahead=1`
- Upcoming mode now also writes to `odds.event_game_map` so grading engine can join on event_id.

### Lineup Poll (step 10)
- Script: `etl/lineup_poll.py` — standalone, does not import from nba_etl.py
- Workflow: `lineup-poll.yml` — every 15 min UTC 16:00–03:59 (noon–midnight ET).

### Pre-Game Refresh (step 10)
- Workflow: `pregame-refresh.yml` — every 30 min UTC 14:00–03:30 (10 AM–midnight ET).
- Gate: inline Python checks `nba.schedule` for any non-final game starting within 3 hours.
- When gate passes: runs `odds_etl.py --mode upcoming --sport nba --days-ahead 1`, then `grade_props.py --mode upcoming`.

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
- `/nba` — game strip + tabs. Header has "At a Glance" link (always all-games for selected date).
- `/nba?gameId=&tab=` — active game. Live tab appears automatically when gameStatus=2.
- `/nba/player/[playerId]?gameId=&tab=&opp=` — splits strip + full season game log
- `/nba/grades?date=` — ranked prop grades for all games on date (market filter dropdown)

### Live Data Flow
1. `nba-live.yml` fires every 5 min UTC 17:00–06:00
2. Gate: check `nba.schedule` for `game_status = 2`. Exit if none.
3. `ScoreboardV3` → update `nba.schedule` home_score/away_score/game_status_text
4. `BoxScoreTraditionalV3` per game → upsert `nba.player_box_score_stats`
5. Front end: `GameStrip` shows pulsing red dot + live score text for in-progress games
6. `GameTabs` auto-selects "Live" tab when `gameStatus === 2`
7. `LiveBoxScore` component polls `/api/boxscore` every 60s by keying `BoxScoreTable` on a tick counter

### API Response Shapes
- `/api/games` → `{ sport, date, games: GameRow[] }`
- `/api/roster` → `{ gameId, roster: RosterRow[] }`
- `/api/team-averages` → `{ players: PlayerAvg[] }` — by team_id, no lineup dependency
- `/api/player-averages` → `{ gameId, lastN, players: PlayerAvg[] }` — lineup-anchored
- `/api/boxscore` → `{ gameId, rows: BoxRow[] }`
- `/api/player` → `{ playerId, lastN, sport, log: GameLogRow[] }` — full season, includes dnp:boolean
- `/api/grades` → `{ date, gameId, grades: GradeRow[] }` — includes overPrice from FanDuel

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

### GameTabs Props
```typescript
{ gameId, homeTeamId, awayTeamId, homeTeamAbbr, awayTeamAbbr, selectedDate, gameStatus }
```
`gameStatus` required to conditionally show Live tab.

### Next.js 15 Patterns
- `useSearchParams()` requires `<Suspense>` wrapper. Pattern: thin `page.tsx` → Suspense → `*Inner.tsx`
- Dynamic route params: `Promise<{...}>`, must be awaited in server components
- `'use client'` required for any component using hooks

---

## 8. Sport Status

### NBA
- Data: ACTIVE. Box scores current through 2026-03-28. Live updates via nba-live.yml during games.
- Odds: event_game_map and daily_grades through 2026-03-23; nightly chain catching up.
- Grading: FUNCTIONAL (hit rate only).
- Web: ALL VIEWS LIVE including Live tab (step 11).
- Lineup polling: ACTIVE. Pre-game refresh: ACTIVE.

### MLB / NFL
- ETL partially built. Not wired to web or grading. After NBA.

---

## 9. Build Sequence

1–11. ~~DONE~~ — NBA data pipeline, odds ETL, SWA setup, API routes, all NBA UI views, keep-alive, lineup poll, pre-game refresh, live data layer.
12. **Step 12: Contextual comparison** — `/api/contextual`, matchup defense view.
13. **Step 13: Grading model expansion** — trend + matchup components first, then migration script.
14. **Step 14: MLB ETL and web views**
15. **Step 15: NFL ETL automation and web views**

---

## 10. Known Issues

| Issue | Status |
|-------|--------|
| next.cmd blocked by ThreatLocker | Testing via push to main + live SWA. ~90s/cycle. |
| NFL workflow file missing | `nfl_etl.py` exists, `nfl-etl.yml` does not. |
| PFF DDL not finalized | Pending CSV column confirmation. |
| Grading: one component only | Hit rate only. Migration + 6 more components planned. |
| `flags` + component columns not in `common.daily_grades` | Migration required before any flag-producing component. |
| odds/grading backfill gap | event_game_map and daily_grades only through 2026-03-23. Nightly chain running. |
| Box Score tab empty for today | Today's games not yet played. Live tab populates during games via nba-live.yml. |
| nba_live.py untested against actual live game | First test opportunity when next NBA game is in progress. |

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
| Stats tab uses nba.players not nba.daily_lineups | Lineup table empty for today until lineup-poll runs; players table always has active roster. |
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
| lineup_poll.py standalone, not imported from nba_etl.py | nba_etl.py has top-level argparse/main; importing it triggers argument parsing. Safer to duplicate small helpers. |
| nba_live.py imports from nba_etl.py | nba_etl.py guards main() under __name__ == "__main__" so imports are safe. Avoids duplicating all helpers. |
| Live box score via DB poll not direct browser API call | Browser never hits stats.nba.com. DB is always source of truth. Clean separation. |
| LiveBoxScore keys BoxScoreTable on tick | Forces full remount + re-fetch each interval without modifying BoxScoreTable. No new props needed. |
| 60-second front-end poll interval | Fast enough for live scores; slow enough to not hammer the DB. nba_live.py writes every 5 min anyway. |
| ScoreboardV3 called once per nba_live.py run | One call updates all games' scores/status; cheaper than per-game score fetches. |
| Lineup poll deletes before upsert per game_id | Ensures scratches are reflected on every refresh. |
| Pre-game refresh gate uses 3-hour window | Avoids burning Odds API quota on days with no upcoming games. |
| EDT (UTC-4) assumed for game time parsing | NBA season runs Oct-Jun, mostly EDT. 30-minute gate error is acceptable. |
| Two cron entries per workflow that spans midnight | GHA cron cannot span midnight in a single expression. |
| At a Glance link always navigates to all-games view | Per-game scoping was confusing; market filter dropdown handles narrowing. |
| odds_etl upcoming mode writes to event_game_map | Grading engine JOINs event_game_map on event_id with game_id IS NOT NULL. Without this, upcoming props are invisible to grading. |
