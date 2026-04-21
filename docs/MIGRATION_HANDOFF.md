# Documentation Migration Handoff

This file is a working handoff for sessions resuming the documentation restructure. It captures content that is not yet durably stored elsewhere in the repo. Read this file any time you are about to work on Step 4, Step 5, or later steps of the migration described in `/docs/DECISIONS.md` ADR-0001.

Delete this file after Step 7 (legacy file retirement) completes.

---

## Migration status snapshot

- **Step 1 complete**: `/docs/README.md`, `/docs/SESSION_PROTOCOL.md`, `/docs/CHANGELOG.md`, `/docs/DECISIONS.md` (ADRs 0001-0004). Commits: 980786c, cd95799, 7ce3a6b, 853a5f5.
- **Step 2 complete**: `/docs/PRODUCT_BLUEPRINT.md`, `/docs/CONNECTIONS.md`, `/docs/GLOSSARY.md`, `/docs/ROADMAP.md`. Commits: f0ecf25, ab28f59, f4a69cc, b19e8e1, 0bf6b0b (CHANGELOG entry).
- **Step 3 complete**: 16 component README skeletons across `/etl/`, `/database/`, `/web/`, `/infrastructure/` plus CHANGELOG entry 76f1976. NBA READMEs explicitly defer substantive content to Step 4 and point to legacy `/PROJECT_REFERENCE.md` as authoritative until then.

- **Step 4 pending**: NBA content migration. Largest single step.
- **Step 5 pending**: MLB content migration. Draws from the design session captured below in this file.
- **Step 6 pending**: NFL placeholder content (mostly done via Step 3 scaffolds; may need minor updates).
- **Step 7 pending**: Legacy file retirement. Delete `/PROJECT_REFERENCE.md` and root `/CHANGELOG.md` after verifying no workflow, code, or doc references them by path.

## Azure SWA deploy "failures" are benign

During the commit bursts of Steps 1-3, many Azure SWA deploys showed conclusion=failure. Verified via GitHub API: these are supersede cancellations, not real errors. When commit B arrives while commit A's SWA deploy is still in flight, A's deploy gets cancelled. A's run shows failure in the UI; B's run (or the next one that isn't superseded) shows success. The latest non-superseded commit is what the site actually serves. This is documented in user memory as "Deployment Canceled on older runs when superseded is expected/normal." No remediation needed.

Apply the same expectation to Step 4 and later commit bursts.

---

## Step 4: NBA content migration plan

### Source
Legacy `/PROJECT_REFERENCE.md` at the repo root. Roughly 30KB. Everything in it except the "On the horizon" section and any cross-sport product description is NBA content.

### Targets and what goes where

**`/etl/nba/README.md`** (STATUS: live; skeleton at commit 6440f6a)
- Ingestion pipelines: nba_etl.py scope, what it pulls from stats.nba.com vs cdn.nba.com
- Proxy handling for stats.nba.com (Webshare rotating residential, `NBA_PROXY_URL`)
- Grading pipeline internals (grade_props.py, nba_grading.py, _common_grade_data 6-tuple)
- Signal design (STREAK strongest at +21.4% lift; SLUMP relabeled DUE)
- `common.player_line_patterns` population via compute-patterns.yml nightly 07:30 UTC
- Odds API client mechanics specific to NBA (includeLinks=true per-event only)
- Two-stage lineup poll: stage 1 official JSON (starters), stage 2 boxscorepreviewv3 (full roster); PREVIEW_TIMEOUT=20s no retry; posToGroup() for position grouping
- Scheduled re-grading via refresh-lines.yml (12pm, 3pm, 6pm ET)

**`/web/nba/README.md`** (STATUS: live; skeleton at commit 95bdee1)
- Canonical component inventory (StatsTable, RosterTable, MatchupGrid, LiveBoxScore, TodayPropsSection, MatchupsTab, RefreshDataButton)
- Canonical UI invariants (compact stats columns, all-stats additions, colSpanTotal values, At a Glance defaults, props strip behavior, RosterTable "Confirmed" badge rules)
- Refresh polling intervals: scoreboard and box score 30s, live odds 60s
- FanDuel betslip linking conditions
- MIN format `mm:ss` with `*` prefix for starters
- Odds display (opening vs live with directional arrows and color coding)
- Player page layout: game dropdown, team pills, Matchups tab, Props tab, Stats
- Admin RefreshDataButton uses ADMIN_REFRESH_CODE

**`/database/nba/README.md`** (STATUS: live; skeleton at commit 063b7d5)
- Full table inventory (extract via etl/db_inventory.py if stale)
- `nba.player_box_score_stats.period` VARCHAR(2) constraint and valid values
- `common.daily_grades` schema v3: outcome_name + over_price + UNIQUE key including outcome_name
- `common.player_line_patterns` structure and semantics (lag-1 transition probabilities, 27,765 rows as of 2026-04-10)
- NBA odds backfill range (Mar 24 to Apr 3 complete as of 2026-04-02 reference)
- 135 unmapped players (all inactive, not blocking) decision

**`/infrastructure/README.md`** (STATUS: live; skeleton at commit 673b5a1)
- VM details: schnapp-runner-2, Central US, B2s_v2, Ubuntu 24.04, admin user schnapp-admin
- Python venv at ~/venv, ODBC pre-installed
- 1GB swap /swapfile persistent, swappiness=80
- Runner systemd service name: actions.runner.SchnappAPI-sports-modeling.schnapp-runner.service with Restart=always
- Flask and MCP systemd services, ports 5000 and 8000
- Cloudflare tunnel, tunnel restart recovery pattern
- install-mcp.yml redeploy cycle (~18 to 30 seconds after mcp/server.py changes)
- Schnapp Ops MCP tool list and auth token location
- Uptime Robot keep-alive at /api/ping every 30 min

**`/etl/_shared/README.md`** (STATUS: in development; skeleton at commit aa474be)
- Azure SQL cold-start retry pattern (3 retries, 45-60s waits)
- fast_executemany=True caveat (ETL bulk ok; grading engine uses its own engine)
- MERGE source dedup pattern to avoid error 8672
- CAST(bit AS INT) before SUM requirement
- "write Python script to /tmp/, execute with venv python, use pyodbc" pattern for DB one-offs via MCP shell_exec

### Decision log extraction
Legacy PROJECT_REFERENCE.md has a decision log section. For each decision with enduring impact, write an ADR in `/docs/DECISIONS.md` starting at ADR-0005. Known candidates:

- Grading schema v3 redesign (Over+Under rows, outcome_name in UNIQUE key)
- Signal redesign: STREAK as strongest; SLUMP relabeled DUE; FADE/COLD suppression rules
- FanDuel-only bookmaker decision
- Two-stage lineup poll (stage 2 always runs, no retry, 20s timeout)
- `common.player_line_patterns` as personal lag-1 transitions with season-hit-rate fallback
- Webshare proxy for stats.nba.com from Actions runners

### Method
Use `str_replace` on each component README's sections (Purpose, Key Concepts, Invariants). Never rewrite a whole file. After each file is updated, commit it individually. Append one summary CHANGELOG entry at the end of the step with tag `[nba][docs]`.

Do not delete `/PROJECT_REFERENCE.md` during Step 4. Retirement is Step 7.

---

## Step 5: MLB content migration plan

MLB design session outputs must land in the repo. Current state: designs exist only in this chat's context and (partially) in ADR-0003 and ADR-0004. The authoritative source files on Austin's computer are:

- `C:\Users\1stLake\OneDrive - Schnapp\mlbSavantV3.pbix` (extracted visual catalog is below)
- `C:\Users\1stLake\OneDrive - Schnapp\mlbStatQueries.docx` (M queries for legacy PBI)
- `C:\Users\1stLake\OneDrive - Schnapp\miscMLBinstructions.docx` (process notes)
- `C:\Users\1stLake\OneDrive - Schnapp\mlb-data\mlbSavantStatcast-2024-25.xlsx`
- `C:\Users\1stLake\OneDrive - Schnapp\mlb-data\mlbSavantStatcast-2025-26.xlsx`

### MLB visual catalog (extracted from mlbSavantV3.pbix, preserved verbatim here)

10 pages in the PBI. 8 confirmed to keep: Game, New, Extra, Criteria, EV, MAIN, VS, Proj. Plus a Pitcher Analysis page prototyped as "Duplicate of Extra".

**Page consolidation (ADR-0003)**: New, Extra, Criteria, and MAIN are four iterations of the same Player Analysis page with minor layout differences. They consolidate into one web page. MAIN is the canonical layout for extraction.

**Visuals on Player Analysis (MAIN canonical)**:

1. Player identity cards: `BATTER.player_name`, `BATTER.team_name`, `BATTER.gameDisplay`, `BATTER.gameTime`, `BATTER.battingOrder`
2. Predictions table: `BATTER._xH`, `BATTER._H_prob`, `BATTER._xR`, `BATTER._xRBI`, `BATTER._xBB`, `BATTER._xK`, `BATTER._x1B`, `BATTER._x2B`, `BATTER._xHR`, `BATTER._HR_prob`, `BATTER._xTB`, `BATTER._xXBH`, `BATTER._xHRR`
3. Per-game log: aggregated from PLAYS
4. Per-at-bat log: PLAYS per at-bat (unaggregated)
5. HR pattern card: `PLAYS."HR Pattern*"` fields
6. VS pitcher career summary + detail: from `Measure.vs*` matchup data
7. Pitcher season stats: `pitcherSeasonData`
8. Team overview pivot: combines PLAYS + BATTER + Measure.vs
9. Platoon split pivot: PLAYS filtered by `isNextPitcherHand`

**EV page visuals**:
- Team EV pivot (all batters, both teams)
- At-bat log with batter name column

**VS page visuals**:
- Team VS career pivot for entire lineup, filtered by home/away side (`BATTER.Side`)

**Proj page visuals**:
- Lineup-wide projection table combining `BATTER._x*` fields with L5AB EV + vs HR + Pattern HitRate

**Pitcher Analysis page (to preserve conceptually)**:
- Pitcher counterpart to Player Analysis. Was the "Duplicate of Extra" working copy in the PBI. Not in the keep list explicitly but retained as a web page concept.

### The 9 pre-aggregated entities (ADR-0004)

1. Upcoming games
2. Batter context per game
3. Batter projections per game
4. Player game stats
5. Player at-bat stats
6. Player trend/pattern stats (HR Pattern, L5AB, etc.)
7. Player platoon splits
8. Career batter vs pitcher matchup
9. Pitcher season stats

Pitch-level Statcast is ETL-internal only. Never queried at runtime by web.

### MLB data sources

- **MLB Stats API**: `https://statsapi.mlb.com/api/v1`. Primary endpoint `/game/{gameID}/withMetrics` returns box + season stats + play-by-play + pitch data in one call. Schedule endpoint with `hydrate=probablePitcher` for upcoming games.
- **Baseball Savant**: `baseballsavant.mlb.com/statcast_search` for Statcast pitch-level; `/gf?game_pk=` for live box score.
- **The Odds API**: same key as NBA, sport=`baseball_mlb`, FanDuel only.
- **Historical Excel exports**: the two xlsx files listed above.

### Step 5 execution plan

1. Create `/etl/mlb/_legacy_powerquery/` and archive the M code from `mlbStatQueries.docx` as `.pq` files. Reference only, not for import.
2. Populate `/etl/mlb/README.md` with full 9-entity specs including column lists. Pull column lists from the visual catalog in this file.
3. Populate `/database/mlb/README.md` with schema design for 9 tables plus pitch-level internal table.
4. Populate `/web/mlb/README.md` with page spec for Game, Player Analysis, Pitcher Analysis, EV, VS, Proj.
5. Write ADRs for any additional decisions that arise (legacy PBI page name retention vs. clean naming, Statcast storage location final call, etc.).

---

## Migration mechanics reminders

- **Always use `create_or_update_file` for markdown**, never `push_files`. User memory explicitly warns `push_files` corrupts newlines for anything beyond strict ASCII TS/JSON/YAML.
- **Always use `str_replace` on existing component READMEs**, never rewrite wholesale. The INVARIANTS section in each README is authoritative and is the anti-revert mechanism.
- **One CHANGELOG entry per session/step**, tagged `[scope][component]`, newest at top.
- **SHA must be fetched fresh** via `get_file_contents` before each `create_or_update_file` on an existing file. Stale SHAs after intervening commits will reject.
- **Azure SWA failed deploys are the supersede pattern**, not real failures. See first section above.

---

## Final step (Step 7) pre-flight checklist

Before deleting `/PROJECT_REFERENCE.md` and root `/CHANGELOG.md`:

1. Confirm no workflow file references either by path (`grep -r PROJECT_REFERENCE .github/workflows/` and same for CHANGELOG.md).
2. Confirm no code reads them (`grep -r PROJECT_REFERENCE etl/ mcp/ web/`).
3. Confirm `/docs/CHANGELOG.md` has migrated or explicitly archived the entries Austin cares about from the old root CHANGELOG.
4. Delete both files in a single commit with message `docs: retire legacy PROJECT_REFERENCE.md and root CHANGELOG.md`.
5. Delete this handoff file in the same or a follow-up commit.
