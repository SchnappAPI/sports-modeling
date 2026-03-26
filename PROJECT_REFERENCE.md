# Sports Modeling — Project Reference

> **For Claude:** This is the authoritative working reference. Read this at the start of any session. Update it immediately whenever infrastructure, scripts, schema, or build status changes. Never let it go stale. This file is the replacement for `sports_modeling_framework_v1_1_revised.docx`, which is retired.

---

## 1. What This System Is

A personal sports intelligence app for NBA, NFL, and MLB prop betting research. The end state is a mobile-friendly Power BI dashboard that loads each morning, shows today's games, lets you drill into any player's stats and game log in one tap, and surfaces the highest-probability prop bets automatically. No clicking through ESPN layers. No manual lookups. Everything computed and waiting.

The system has three layers:
- **Data layer:** Azure SQL database fed by nightly GitHub Actions ETL pipelines
- **Intelligence layer:** Grading model that scores player props against historical patterns
- **Presentation layer:** Power BI dashboard (sports-model.pbix) — currently a blank canvas with tables connected but no measures or pages built yet

---

## 2. Infrastructure

### Azure SQL
- Server: `sports-modeling-server.database.windows.net`
- Database: `sports-modeling`
- Login: `sqladmin` (credentials in GitHub Secrets)
- Tier: General Purpose Serverless — auto-pauses; first connection of the day takes 20-60s
- Firewall: `0.0.0.0–255.255.255.255` under Selected Networks (required for GitHub Actions)
- Allow Azure Services: must remain ON
- Connection: SQLAlchemy + pyodbc, ODBC Driver 18, `fast_executemany=True`
- Retry logic: 3 attempts, 45s wait between — handles auto-pause resume
- **No MSSQL MCP available.** Database queries run via Python in GitHub Actions or by reading/writing ETL scripts through the GitHub MCP.

### GitHub Actions
- Repo: `SchnappAPI/sports-modeling` (private)
- All automated Python runs here — Python cannot run on the local corporate machine (ThreatLocker) or on a schedule from the Windows VM (auto-logout)
- Each sport has its own isolated workflow file
- Runners are ephemeral ubuntu-latest; every workflow installs ODBC Driver 18 and pip dependencies fresh each run
- Secrets: `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE`, `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`, `NBA_PROXY_URL`, `ODDS_API_KEY`
- GitHub MCP tool used for all file reads/writes; always retrieve file SHA before `create_or_update_file`

### Windows VM
- Used only for short-lived manual tasks
- PFF Selenium script runs here manually when PFF grade data is needed
- Not reliable for scheduled or long-running processes

### Power BI
- File: `sports-model.pbix` (stored in OneDrive, My Files\_RESOURCES)
- Must be open in Power BI Desktop before MCP connection is possible
- Power BI MCP connection procedure: run `ListLocalInstances` → `Connect` using returned `connectionString` and `initialCatalog` from the instance whose `parentWindowTitle` is `sports-model`. Port is dynamic; re-run this procedure each session.
- Current state: all SQL tables imported as DirectQuery or Import sources, zero measures written, no report pages built

---

## 3. Repository Structure

```
.github/workflows/
  nba-etl.yml          # Scheduled nightly + manual dispatch. Status: ACTIVE
  mlb-etl.yml          # Manual dispatch only. Status: INCOMPLETE
  odds-etl.yml         # Manual dispatch only (modes: discover/probe/backfill/mappings/upcoming). Status: ACTIVE
  grading.yml          # Grading model. Status: EXISTS, needs scheduling review
  nba-clear.yml        # Utility: clears NBA tables. Manual only.
  db_inventory.yml     # Utility: DB inventory. Manual only.
  grades_sample.yml    # Prototype. Not production.
  # NOTE: No nfl-etl.yml exists despite nfl_etl.py being present

etl/
  db.py                # Shared: get_engine(), upsert() via SQL MERGE
  nba_etl.py           # NBA ETL. Status: ACTIVE, 2025-26 season backfill in progress
  mlb_etl.py           # MLB ETL. Status: INCOMPLETE
  nfl_etl.py           # NFL ETL script exists but no workflow file. Status: BUILT, NOT AUTOMATED
  odds_etl.py          # Odds ETL (The Odds API). Status: ACTIVE, manual-only
  nba_clear.py         # Utility: truncates NBA tables
  nba_add_indexes.sql  # Index DDL for NBA tables
  db_inventory.py      # Inventory script
  grades_sample.py     # Prototype
  requirements.txt     # Shared: nflreadpy, nfl-data-py, sqlalchemy, pyodbc, pandas, pyarrow, requests, mlb-statsapi, nba_api>=1.11.4

grading/
  grade_props.py       # NBA prop grading model. Status: FUNCTIONAL (see Section 6)
```

---

## 4. Database Schema

### Schemas
| Schema | Purpose |
|--------|----------------------------------------------------------|
| `nba` | NBA tables |
| `mlb` | MLB tables |
| `nfl` | NFL tables |
| `odds` | Odds API data: events, lines, props, mappings |
| `common` | Shared: grading output, date dimension |

### NBA Tables (loaded in Power BI model)
- `nba.games` — game metadata
- `nba.schedule` — schedule
- `nba.teams` — team reference (hardcoded static dict in ETL, no HTTP dependency)
- `nba.players` — player reference
- `nba.daily_lineups` — game-day lineups
- `nba.player_box_score_stats` — **quarter-level** box scores (Q1/Q2/Q3/Q4/OT), core fact table
- `nba.player_passing_stats` — passing/assist tracking stats (game level)
- `nba.player_rebound_chances` — rebound opportunity tracking stats (game level)

### Odds Tables (loaded in Power BI model)
- `odds.events` — game events from The Odds API
- `odds.game_lines` — spreads, totals, moneylines
- `odds.player_props` — historical FanDuel player prop lines
- `odds.upcoming_player_props` — today's FanDuel lines (populated by `upcoming` mode)
- `odds.event_game_map` — resolves `event_id` → `game_id` + `game_date`
- `odds.player_map` — maps odds player names to internal `player_id`

### Common Tables
- `common.daily_grades` — grading output (see Section 6)
- `common.dim_date` — calendar dimension 2015–2035

### Key Schema Rules
- Snake case everywhere
- Every fact table has `created_at DATETIME2 DEFAULT GETUTCDATE()`
- Key columns (typically `game_date`) must have non-clustered indexes for fast `SELECT DISTINCT`
- Use `DELETE` not `TRUNCATE` — FK constraints block TRUNCATE even on empty child tables
- FK constraint teardown order: query `sys.foreign_keys` joined with `sys.tables` to determine child-before-parent order
- MLB dropped FK constraints on child tables intentionally to allow independent reload

---

## 5. ETL Patterns

### Deterministic Incremental Ingestion (all scripts follow this)
1. Build desired key set (all game dates that should exist)
2. Query destination table for existing keys (`SELECT DISTINCT key_col`)
3. Compute missing = desired minus existing
4. Take oldest N (`--days` arg, default 3 nightly / higher for backfill)
5. Short-circuit if nothing to do
6. Fetch from API
7. Upsert to destination

Running twice produces the same state as running once. Backfill and nightly incremental are the same code path — only the `--days` argument changes.

### Upsert
All writes go through `etl/db.py:upsert()`. Creates a `#stage_{table}` temp table, loads via `to_sql`, executes SQL MERGE. Never use raw INSERT.

### DataFrame Rules
- `df.where(pd.notna(df), other=None)` before every upsert — converts NaN to None
- Omit `method` parameter from `to_sql` entirely (never `method='multi'` — hits SQL Server 2100-parameter limit)
- `chunksize=200`
- `index=False`

### NBA ETL Specifics
- Box scores: `BoxScoreTraditionalV3` with `range_type=2` and second-based `start_range`/`end_range` for genuine per-quarter splits
- OT: `Period=""` + `GameSegment=Overtime`, not `Period=5`
- Teams: hardcoded `STATIC_TEAMS` dict — HTTP eliminated after proxy failures
- Players: direct HTTP `commonteamroster` (no proxy)
- Game discovery: direct HTTP `leaguegamelog` (no proxy)
- PT stats: direct `requests.get` to `leaguedashptstats` with browser headers, bypassing `nba_api` wrapper; `proxies={"http": None, "https": None}` explicitly set
- Proxy: Webshare rotating residential, patched via `requests.Session.__init__` monkey-patch before importing `nba_api`. Stored as `NBA_PROXY_URL` secret.
- V3 endpoints use typed dataset accessors (`.player_stats.get_data_frame()`); `get_normalized_dict()` returns empty for V3
- 15s delay between passing and rebounding PT stats calls (prevents HTTP 500)

### Odds ETL Specifics
- Source: The Odds API (`ODDS_API_KEY` secret)
- Modes: `discover` (walk calendar), `probe` (fetch lines for known events), `backfill` (historical props), `mappings` (build player name map), `upcoming` (today's lines)
- No cron schedule — currently manual dispatch only
- `upcoming` mode must be run before each day's grading to populate `odds.upcoming_player_props`
- Needs a nightly cron added to run `upcoming` mode automatically each morning before grading

---

## 6. Grading Model — Current State

### What it does
Scores each player prop line with a weighted historical hit rate.

```
hit_rate_60  = games where stat > line / total games, prior 60 days
hit_rate_20  = same, prior 20 days
weighted     = (0.60 × hit_rate_20) + (0.40 × hit_rate_60)
grade        = weighted × 100  (0–100 scale)
```

If `sample_size_20 < 5`, falls back to `hit_rate_60` only.

### What it reads
- `odds.upcoming_player_props` (upcoming mode) or `odds.player_props` (backfill mode)
- `odds.event_game_map` for `event_id` → `game_id` + `game_date`
- `odds.player_map` for name → `player_id` resolution
- `nba.player_box_score_stats` for historical stat totals (summed across all periods per game)

### What it writes
- `common.daily_grades` — one row per (grade_date, event_id, player_id, market_key, bookmaker_key, line_value)
- Columns: `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`

### Bookmaker
FanDuel only. `bookmaker_key = 'fanduel'` hardcoded.

### Markets graded
All player-level prop and alt-prop markets. Excludes team totals, game lines, spreads, half/quarter game lines.

### Modes
- `upcoming` — grades today's lines. This is the nightly production mode.
- `backfill` — works through historical dates oldest-first, bounded by `--batch N`

### What it is NOT doing yet
This is one grade component (historical hit rate) of many planned. The following components from the design are not yet implemented:
- Matchup defense adjustment (opponent defensive rank vs. stat category)
- Weighted recent trend (last N games, decay-weighted)
- Regression metric (recent avg vs. season baseline)
- Streak and cycle detection (Hot/Cold/Transitional/Neutral phases)
- Adaptive self-correcting estimator (rolling forecast with correction factor)
- Correlation confirmation (positively/negatively correlated player boosts/suppression)
- Lotto line detection (outlier pattern signals at +300 or longer odds)

---

## 7. Dashboard Vision — Power BI

### Structure (applies to all three sports)
Four pages per sport. NBA, NFL, and MLB each have their own set. MLB has additional pages.

**Page 1 — Game Selection**
Card-based layout. Each card shows: game time, both teams with logos, key contextual info (probable pitchers for MLB, spread/total from odds data). Tapping a card navigates to Page 2 for that game. Reference: image7 (MLB card layout), image13/14 (NFL card layout).

**Page 2 — Game Analysis**
Both team rosters combined. Players as rows, stats as columns. Starters grouped above bench. Two views:
- Box score view: all stats, averaged over the selected split context. Reference: image1, image2 (NBA/ESPN style), image15/16 (NFL by position group).
- Focused stat view: one stat selected, columns become splits side by side (season avg, last 10, vs opponent, vs similar total, vs similar spread, etc.), plus prop line, hit rate, safe line, lotto line.

Granularity filter: strips data to selected periods/quarters/play types. NBA: Q1-Q4+OT selectable. NFL: play type filter. MLB: pitch type or at-bat result filter. Everything updates instantly.

Player selection from this page drills through to Page 3.

**Page 3 — Player Detail**
Same structure as Page 2 but player-centric. Stats/props as rows instead of players. Two panels:
- Splits summary: stat averages across all split contexts (reference: image5, image6 — full splits table with home/road, by month, by opponent, by score margin, by day of week, etc.)
- Game log: one row per game, all stats visible, no month grouping, most recent at top. Reference: image4 (preferred format), not image3 (month-grouped).

For MLB, additional panels: exit velocity log (reference: image11 — date, opp, result, EV, LA, distance, xBA, PA, inning), pitcher vs. batter matchup stats, pitcher season-to-date stats.

**Page 4 — At a Glance**
Ranked list of today's best prop opportunities, sorted by grade descending. Filterable by game and stat. Multiple grade columns (one per component, plus overall weighted composite). This is the "skip straight to the best bets" page.

### Splits to support (not exhaustive)
NBA/NFL/MLB common: season, last N games, home, away, vs opponent, wins, losses, by day of week, by month.
NBA additions: by quarter, first half, second half, overtime, by score margin.
MLB additions: vs RHP, vs LHP, by batting order position, by inning, vs specific pitcher.
NFL additions: by play type (pass/run), by down and distance, home/away dome/outdoor.

### Granularity
- NBA: quarter-level stats stored, selectable in dashboard (strip OT, filter to Q1-Q3, etc.)
- MLB: pitch/plate appearance level (Statcast). `nba.player_box_score_stats` equivalent for MLB is pitch-by-pitch.
- NFL: play-by-play level.

### One file or three?
Not yet decided. Three separate files (one per sport) is simpler to build and debug. One file with sport-switching is cleaner for daily use. Defer this decision until first sport's pages are built.

---

## 8. Sport Status

### NBA
- **Data:** Active. Box scores, PT stats, lineups, players, teams, schedule all loading. 2025-26 season backfill in progress.
- **Odds:** Active manually. `upcoming` mode needs nightly cron.
- **Grading:** Functional for hit rate component. Runs on grading.yml (needs scheduling review).
- **Dashboard:** Not started. Build here first.
- **Outstanding:** `nba.player_box_score_detail` (ESPN source) not yet populated. `common.dim_stat` seed (57 rows) not complete — blocks `player_box_score_stats` FK if enabled.

### MLB
- **Data:** Historical data loaded. ETL script exists (`mlb_etl.py`) but marked incomplete. No automated schedule.
- **Odds:** Same as NBA — manual only.
- **Grading:** Not yet wired up (grading script is NBA-only currently).
- **Dashboard:** Not started. Comes after NBA.
- **Outstanding:** ETL needs completion and a workflow file with cron.

### NFL
- **Data:** ETL script (`nfl_etl.py`) built using `nflreadpy`. Schema created. No workflow file — not automated.
- **PFF grades:** Require manual Selenium script on Windows VM. CSV column headers not yet confirmed — DDL cannot be finalized until confirmed.
- **Odds:** Same as NBA — manual only.
- **Grading:** Not yet wired up.
- **Dashboard:** Not started. Comes after NBA and MLB.
- **Outstanding:** Create `nfl-etl.yml`. Confirm PFF CSV columns before finalizing DDL.

---

## 9. Build Sequence (Recommended)

This is the order that minimizes rework and delivers usable value fastest.

1. **Stabilize NBA data pipeline** — confirm backfill is current, `upcoming` mode running reliably, grading workflow scheduled correctly.
2. **Add `upcoming` cron to odds-etl.yml** — needs to run each morning before grading so `odds.upcoming_player_props` is populated.
3. **Build NBA dashboard pages** — Page 1 through 4 in Power BI against live NBA + odds data.
4. **Expand grading model** — add matchup defense, recent trend, regression metric as additional grade columns in `common.daily_grades`. Add composite weighted grade.
5. **Complete MLB ETL** — finish script, add `mlb-etl.yml` with cron, backfill.
6. **Build MLB dashboard pages** — reuse NBA page structure, add MLB-specific panels.
7. **Automate NFL ETL** — create `nfl-etl.yml`, confirm PFF columns, finalize DDL.
8. **Build NFL dashboard pages.**
9. **One file vs. three decision** — revisit once first sport's pages are polished.

---

## 10. Known Issues and Constraints

| Issue | Status |
|-------|--------|
| Webshare proxy returns 502 for `nba_api` wrapper calls | Workaround in place: eliminated proxy dependency for teams, players, game discovery. Proxy only used for box score calls via monkey-patch. |
| Odds ETL has no nightly cron | Needs `upcoming` mode added to a schedule so props are ready each morning before grading runs |
| NFL workflow file missing | `nfl_etl.py` exists but `.github/workflows/nfl-etl.yml` does not |
| PFF DDL not finalized | Pending confirmation of actual CSV column headers from Selenium script run on VM |
| Power BI has zero measures | All pages and calculations still need to be built |
| Grading covers only one component | Hit rate only. Five additional components planned but not implemented. |
| `common.dim_stat` seed incomplete | 57 rows needed before `player_box_score_stats` FK dependency is satisfied |

---

## 11. Decision Log

| Decision | Rationale |
|----------|-----------|
| Teams dict hardcoded in NBA ETL | Eliminated HTTP dependency after persistent proxy failures with `TeamInfoCommon` |
| PT stats call direct HTTP, not `nba_api` wrapper | `nba_api` doesn't send required headers for `leaguedashptstats`; direct requests with browser headers works reliably |
| `proxies={"http": None, "https": None}` not `proxies=None` | Required to explicitly bypass proxy when env vars set; `None` alone does not override env proxy |
| `range_type=2` with second-based ranges for box scores | `range_type=0` ignores period filters and returns full-game totals, defeating quarter-level storage |
| MLB dropped FK constraints on child tables | Allows independent truncate/reload of reference tables without cascade ordering |
| DELETE not TRUNCATE in teardown scripts | TRUNCATE blocked by FK constraints even on empty child tables in SQL Server |
| FanDuel as sole grading bookmaker | Reference bookmaker; most complete prop line coverage. Schema retains `bookmaker_key` for future extension. |
| One `requirements.txt` for all sports | Simpler; create sport-specific file only if a library conflict arises |
