# Architecture Decision Records

Append-only. New ADRs get the next sequential number. ADRs are never rewritten, only superseded by a later ADR that references them.

Format:

```
ADR-NNNN [scope][component] Title
Date: YYYY-MM-DD
Context: Why this decision came up.
Decision: What was decided.
Consequences: What this implies for future work.
Supersedes: ADR-XXXX (optional)
```

---

## ADR-0001 [shared][docs] Adopt co-located README structure with central /docs/
Date: 2026-04-20

Context: A single PROJECT_REFERENCE.md at repo root grew past 30,000 characters. Updates between sessions took several minutes because the file had to be partially rewritten on each change. Sessions on one sport repeatedly reverted recent changes on another sport because the whole file was in context. Re-deriving troubleshooting steps was common because the why behind decisions was buried in long sections.

Decision: Adopt a structure where:

- Cross-cutting docs live in `/docs/` (README router, SESSION_PROTOCOL, CHANGELOG, DECISIONS, PRODUCT_BLUEPRINT, CONNECTIONS, GLOSSARY, ROADMAP).
- Component-specific docs live next to the code (`/etl/<sport>/README.md`, `/web/<sport>/README.md`, etc.) using a fixed template with STATUS, Purpose, Files, Key Concepts, Invariants, Recent Changes, Open Questions sections.
- CHANGELOG is read first every session and filtered by tag instead of reading any monolithic doc.
- DECISIONS captures the why, append-only.

Consequences:

- Session-end updates become a single-line CHANGELOG append plus an optional small `str_replace` on a component INVARIANTS section. Seconds instead of minutes.
- Cross-sport contamination during sessions ends because tag filtering scopes the read.
- The revert problem ends because INVARIANTS sections are explicit and CHANGELOG recency is read first.
- During the migration, both old and new systems coexist. Old PROJECT_REFERENCE.md and root CHANGELOG.md are not deleted until all content has been ported.

Migration order: foundation files first (this commit), then component skeletons, then NBA content migration from PROJECT_REFERENCE.md, then MLB content from the design conversation that produced ADRs 0003 and 0004, then NFL placeholder, then automation scripts, then legacy file removal.

---

## ADR-0002 [shared][docs] Code files stay flat in /etl/; doc subfolders are additive only
Date: 2026-04-20

Context: The original documentation structure proposal included `/etl/nba/`, `/etl/mlb/`, `/etl/nfl/` as full subfolders containing both code and docs. The actual repo has `/etl/` as a flat directory with files like `etl/nba_etl.py`, `etl/mlb_etl.py`, `etl/nfl_etl.py`, `etl/odds_etl.py`, `etl/grading.yml` etc. All 27 GitHub Actions workflows reference these files directly by their flat paths.

Decision: Code files do not move. Per-sport documentation lives in newly created subfolders alongside the existing flat code files: `etl/nba/README.md` exists in the same directory as `etl/nba_etl.py`. The subfolder is purely additive. The same approach applies to `web/`, `database/`, and `infrastructure/` where similar flat layouts exist.

Consequences:

- Zero risk of breaking workflows or imports during the migration.
- Future code reorganization (for example, moving `etl/nba_etl.py` into `etl/nba/etl.py`) remains an option but is decoupled from the documentation work and would require updating workflow references in lockstep.
- Doc subfolders coexist with flat code files in the same directory listing. Visually unusual but functionally clean.

---

## ADR-0003 [mlb][web] Single Player Analysis page replaces PBI's New, Extra, Criteria, MAIN
Date: 2026-04-20

Context: The legacy Power BI file `mlbSavantV3.pbix` contained four pages with nearly identical content: New, Extra, Criteria, and MAIN. They were iterations developed without cleanup of older versions. All four had the same set of visuals (predictions table, per-game log, per-at-bat log, HR pattern card, VS pitcher career card and table, pitcher season stats, team overview pivot, platoon split pivot) with minor layout variations. The pages confirmed by Austin to keep from his last mobile session were Game, New, Extra, Criteria, EV, MAIN, VS, and Proj, but New, Extra, Criteria, and MAIN are functionally one page.

Decision: The web app implements one Player Analysis page that consolidates the New, Extra, Criteria, and MAIN concepts. The exact page name in code is to be decided during the web build but conceptually it is "the player analysis view." Old PBI page names are not preserved.

Consequences:

- Single React component to build and maintain instead of four near-duplicates.
- Anyone referencing "the New page" or "Criteria page" from PBI history should be redirected to Player Analysis.
- The pages to keep, mapped to web concepts: Game (selector and nav), Player Analysis (formerly New, Extra, Criteria, and MAIN), EV (exit velocity team view), VS (lineup-wide career matchup), Proj (lineup projections), and a Pitcher Analysis page (pitcher counterpart to Player Analysis; was prototyped as "Duplicate of Extra" in PBI).

---

## ADR-0004 [mlb][etl][web] All MLB visual stats pre-aggregated, none computed at query time
Date: 2026-04-20

Context: The legacy Power BI file used DAX measures to aggregate Statcast pitch-level data into per-player, per-game, per-at-bat, and rolling-window views at display time. Power BI can do this because it loads the entire fact table into memory and computes against it interactively. A web app cannot afford runtime aggregation of pitch-level data on every page load: latency would be unacceptable and the database would be hammered.

Decision: The MLB ETL pre-aggregates all visual-feeding entities and stores them in dedicated tables. The pitch-level Statcast data remains an ETL-internal source and is not queried by the web app. The 9 entities identified during the visual catalog are: upcoming games, batter context per game, batter projections per game, player game stats, player at-bat stats, player trend/pattern stats, player platoon splits, career batter vs pitcher matchup, and pitcher season stats. These will be defined in detail in `/database/mlb/README.md` once that file is created during the migration.

Consequences:

- ETL becomes more complex: it must produce 9 derived tables on a recurring schedule, with rolling windows recomputed whenever new game data lands.
- Web pages stay fast because every visual reads from a purpose-built table with the data already shaped.
- Adding a new visual stat requires an ETL change to add a column or table, not just a query change.
- The legacy Power Query M code that performed equivalent transformations in PBI is preserved for reference but not for direct reimplementation; Python ETL targets the same final state but is structured differently.

---

## ADR-0005 [nba][database] Grading schema v3: Over and Under rows with outcome_name in UNIQUE key
Date: 2026-04-02

Context: The original `common.daily_grades` schema stored one row per `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value)` with an implicit Over orientation. Under grading was bolted on via a separate `best_price` CTE join at read time that tried to reattach Under prices at the web layer. That join was fragile and in practice attached Over prices to Under rows, so the At-a-Glance Under tab displayed wrong prices. Writing both directions into one row was rejected because the component-level grades (trend, momentum, pattern, matchup, regression) differ between Over and Under once inversion is applied.

Decision: Migrate `common.daily_grades` to schema v3 which adds `outcome_name VARCHAR(5)` (`'Over'` / `'Under'`) and `over_price INT` (direction-appropriate price, misnamed but kept for migration simplicity). UNIQUE key extends to `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value, outcome_name)`. `grade_props.py` writes both Over and Under rows for standard markets. Alternate lines remain Over-only. Web `getGrades` reads `dg.outcome_name` and `dg.over_price` directly from the table with no join to `odds`.

Consequences:

- Under tab in At a Glance shows correct prices.
- Row count per grading run roughly doubles for standard markets. Not a storage concern at current volume.
- The `over_price` column name is now a misnomer because it holds Under prices in Under rows. Rename deferred to avoid touching the grading engine and web routes simultaneously.
- The removed `best_price` CTE must not be reintroduced in any grading or reader query.

---

## ADR-0006 [nba][etl] Signal redesign: STREAK strongest; SLUMP relabeled DUE as bounce-back
Date: 2026-04-10

Context: The original signal set treated COLD and SLUMP as warnings against betting a miss streak. Backtest of resolved grades showed SLUMP actually had a positive lift (missed streaks tend to regress into hits), while STREAK (consecutive hits) had the strongest positive lift at +21.4%. The UI was actively labeling a profitable spot as a warning.

Decision: Keep STREAK as a positive signal fired when `momentum_grade > 70`. Relabel SLUMP as DUE, keep it as a positive bounce-back signal rendered green, and narrow its fire condition to `momentum_grade > 65 AND hr60 >= 0.35`. COLD and FADE remain player-level warning signals at legacy thresholds. Signal definitions live in `shared/signals.ts` and are shared across web and any future grading consumers.

Consequences:

- UI color coding of DUE reverses from red/gray (old SLUMP) to green.
- Backtest re-run is pending once enough new resolved outcomes have accumulated under the new signal set.
- The word "slump" should not appear in new code or UI. Anyone reviewing the old backtest should translate SLUMP to DUE.

---

## ADR-0007 [nba][mlb][etl] FanDuel-only bookmaker across all sports
Date: 2026-04-20

Context: The Odds API supports a dozen US bookmakers. Early ingestion pulled multiple books for price comparison. In practice FanDuel has the most complete prop line coverage in the markets we grade (player points, rebounds, assists, combos, alternates). Non-FanDuel rows complicated the grading join because line offerings and line values differed per book, and the web always displays the FanDuel price in betslip links regardless. Carrying non-FanDuel rows added storage and grading noise without changing any user-facing decision.

Decision: All NBA and MLB odds ingestion uses `bookmakers=fanduel`. No other bookmakers are written to `odds.*` tables. This applies to both upcoming and historical modes. Betslip deep links use FanDuel's event link format via `includeLinks=true` on the per-event Odds API endpoint.

Consequences:

- `odds.upcoming_player_props.bookmaker_key` is effectively a constant (`'fanduel'`). Queries can elide it but must still match on the column to preserve future-proofing.
- Reopening to additional bookmakers is possible later but would require downstream grading and web changes to pick a displayed price. Not trivially reversible.
- Odds API credit consumption is minimized by not pulling additional bookmakers we would discard.

---

## ADR-0008 [nba][etl] Two-stage lineup poll: unconditional Stage 2, no retry, 20s timeout
Date: 2026-04-04

Context: The original lineup poll used a single-stage call to the NBA's `boxscorepreviewv3` endpoint with 60-second timeouts and 3 retries. Three games in a cycle could consume 9 minutes worst-case, blowing past the 5-minute timeout on `refresh-data.yml`. Separately, official NBA lineup JSON (when available) had accurate Confirmed-vs-Projected starter designations that `boxscorepreviewv3` did not expose. When Stage 1 succeeded, Stage 2 was being skipped, leaving bench and inactive players unwritten.

Decision: Two stages, both run unconditionally.

- Stage 1 fetches the official NBA lineup JSON. Returns 5 starters per team with precise PG/SG/SF/PF/C positions and `lineup_status` of Confirmed or Projected.
- Stage 2 always fetches `boxscorepreviewv3` for the full roster (bench and inactive). Stage 1 starter designations override Stage 2 for overlapping players.
- `PREVIEW_TIMEOUT = 20` seconds. No retry. A single attempt is sufficient; 404 on live games is expected and handled.

Consequences:

- Full roster information is always written to `nba.daily_lineups`, not only starters.
- Lineup poll for three games fits comfortably inside the 5-minute refresh window.
- Historical data is preserved because DELETE runs only for games in the current poll cycle.
- Position strings in `nba.daily_lineups` are full values (PG, SG, SF, PF, C). Downstream consumers must use `posToGroup()` to bucket into G/F/C, never `position[0]` or `LEFT(position, 2)`.

---

## ADR-0009 [nba][etl] Personal lag-1 transition patterns with season-hit-rate fallback
Date: 2026-04-10

Context: The `pattern_grade` component was originally computed from a global reversal-rate table that treated all players as equivalent after a run of N hits or misses. Different players are streaky vs. reversionary in meaningfully different ways. A player-specific model was needed, but per-player pattern fitting requires minimum sample sizes and graceful fallback when a player has not crossed the threshold on a given line.

Decision: Introduce `common.player_line_patterns` keyed by `(player_id, market_key, line_value)`. Store lag-1 transition probabilities `p_hit_after_hit` and `p_hit_after_miss` computed from resolved outcomes. Create a row only when `n >= MIN_GAMES` (10). Store a transition probability only when that state has `>= MIN_TRANSITION_OBS` (3) observations. `compute-patterns.yml` refreshes the table nightly at 07:30 UTC. `grade_props.py` looks up the personal probabilities for each grade and falls back to the season hit rate when no pattern row exists.

Consequences:

- `_common_grade_data` changed from a 5-tuple to a 6-tuple (sixth element is the patterns table). Reverting to a 5-tuple would silently drop personal patterns from grading.
- `precompute_line_grades` iterates by `(player_id, market_key)` pair to reuse the stat sequence across line values, dropping outer iterations roughly 10x.
- `is_momentum_player`, `is_reversion_player`, `is_bouncy_player` BIT columns exist on the pattern table for downstream diagnostics. All aggregation of these requires `CAST(col AS INT)` before `SUM()`.
- Reference cardinality: 27,765 rows as of 2026-04-10.

---

## ADR-0010 [nba][etl] Webshare rotating residential proxy for stats.nba.com from Actions runners
Date: 2026-04-20

Context: `stats.nba.com` rate-limits aggressively and blocks many datacenter IP ranges, including Azure and most GitHub-hosted runner pools. Early runs against `stats.nba.com` from the runner returned empty payloads or outright blocks. `cdn.nba.com` (used by `nba_live.py` and the Flask runner) is public and does not block, so the proxy requirement is endpoint-specific.

Decision: All `stats.nba.com` calls from the ETL route through the Webshare rotating residential proxy via the `NBA_PROXY_URL` secret. The `leaguedashptstats` endpoint is the one exception and does not require the proxy. All `cdn.nba.com` calls go direct with no proxy. The proxy URL is kept in the VM's systemd environment file as well for consistency, though the Flask runner does not use it.

Consequences:

- `stats.nba.com` calls carry per-request proxy latency (~200-500 ms added) and a small ongoing Webshare cost.
- Proxy outages block NBA ETL but do not affect live scoreboard or live box score; the live path is CDN-only by design.
- If Webshare is ever retired, any replacement must be a rotating residential proxy service. Datacenter proxies have been observed to fail the same way direct Azure IPs do.

---

## ADR-0011 [mlb][etl][database] `mlb.games` stores today's scheduled games, not only Final
Date: 2026-04-20

Context: The MLB game strip on the web app needs to show matchups and start times before any game has gone Final, even on days when the nightly ETL has not produced new data. A pure "Final games only" store would leave the strip empty until the first game of the day finished. Adding a second table (`mlb.scheduled_games` vs `mlb.games`) was rejected because the web route would then need to UNION two tables and handle duplicate rows as games transitioned from scheduled to Final.

Decision: `etl/mlb_etl.py:load_todays_schedule` upserts today's regular-season games into `mlb.games` regardless of status. `game_status` carries the raw MLB status code (`Scheduled`, `Warmup`, `In Progress`, `Final`, etc.), which is normalized to `'F'` only when the game reaches Final state. `away_team_score` / `home_team_score` are populated where available, NULL otherwise. The web filters by `game_date` and relies on `game_status` for styling decisions.

Consequences:

- `mlb.games` is not strictly a historical table. Queries that need "completed games only" must filter `game_status IN ('F')` explicitly.
- The play-by-play loader's game-selection query already filters `game_status = 'F' AND game_type = 'R'` correctly and is unaffected.
- Today's games get upserted every nightly run plus any intraday refresh. They are overwritten atomically via the existing MERGE, so racing with a Final update is safe.
- No intraday refresh workflow exists today. A Final-state update only happens on the next nightly run, meaning scores can lag by up to 24 hours on the web. Flagged as an open question in `/etl/mlb/README.md`.

---

## ADR-0012 [mlb][etl][web] Pitch-level `mlb.play_by_play` stays ETL-internal; web reads aggregate views only
Date: 2026-04-20

Context: `mlb.play_by_play` stores one row per pitch with full Statcast pitch and hit data, roughly 300 rows per game. At current volume (roughly 2000 games per season) that is 600,000 rows per season and growing. A web page that queries raw pitch rows per render would hammer the DB and make cold-starts unacceptable. Power BI's DAX model could afford in-memory pitch aggregation; a web app with Azure SQL Serverless cannot.

Decision: Treat `mlb.play_by_play` as an ETL-internal source. The web app queries only aggregate derivations of it, never raw pitch rows. Current examples:

- `/api/mlb-linescore` groups by `(inning, is_top_inning)` with the `is_last_pitch = 1` predicate and returns a pre-shaped per-half-inning runs array
- `/api/mlb-atbats` filters to `is_last_pitch = 1 AND result_event_type IS NOT NULL` and returns one row per completed at-bat with only the last-pitch Statcast metrics

Future features needing pitch-level data (pitch log per at-bat, per-pitcher velocity distribution, etc.) should either be materialized into purpose-built tables via ETL (preferred) or be implemented as tightly scoped SQL aggregations that never return more than a few hundred rows.

Consequences:

- The web layer does not need to know about pitch-level data structure. Only aggregates.
- New visuals that demand pitch-level fidelity require an ETL change (new materialized table) rather than a web-layer change, reinforcing ADR-0004.
- If `mlb.play_by_play` grows to the point where even aggregate reads are slow, add indexes on `game_pk + is_last_pitch` and `game_pk + inning + is_top_inning`. The write path can tolerate index overhead because writes happen in batched inserts, not streaming.

---

## ADR-0013 [mlb][etl] Play-by-play uses direct INSERT, not MERGE, because diff-before-fetch guarantees new rows
Date: 2026-04-20

Context: The standard ETL upsert pattern in `etl/db.py:upsert` creates a `#stage_` temp table, populates it via `to_sql`, then runs a MERGE into the permanent table. This is safe and idempotent but carries per-row overhead from the MERGE comparison. For `mlb.play_by_play` at roughly 300 rows per game, 50 games per run, the MERGE overhead dominates wall-clock time even though every row is guaranteed new (games are pre-diffed against the destination).

Decision: `mlb_play_by_play.py` bypasses the staging/MERGE pattern and writes directly to the permanent table via `to_sql(if_exists='append')` with `fast_executemany=True`. The diff against `SELECT DISTINCT game_pk FROM mlb.play_by_play` runs once at the top of the loader and is the only idempotency check. Explicit `INSERT_DTYPES` (SQLAlchemy `VARCHAR(N)` mappings) are passed to `to_sql` to prevent pandas from inferring column widths from the first row in a batch and right-truncating longer rows later.

Consequences:

- Load is approximately 10x faster than MERGE through a slow engine.
- If a `game_pk` is partially loaded (process killed mid-game), a retry will see that `game_pk` in the destination and skip it — but the partial rows remain. Manual cleanup with `DELETE FROM mlb.play_by_play WHERE game_pk = X` is required before retry. No automatic partial-load detection.
- The pattern only applies when pre-diffing guarantees no key collisions. Using direct INSERT for a table that might have duplicate keys will raise a PK violation.
- `fast_executemany=True` for this loader is deliberate. Reverting to `get_engine_slow` would reintroduce the ~10x slowdown without meaningful benefit; `INSERT_DTYPES` already solves the VARCHAR truncation problem that motivates the slow engine elsewhere.

---

## ADR-0014 [nfl][etl][database] NFL uses schema-from-data (pandas inference + self-healing ALTER) instead of hand-written DDL
Date: 2026-04-20

Context: NBA and MLB tables have column lists defined explicitly — NBA in separate SQL migration files, MLB in row-dict keys inside each loader function. Both require a hand-written column list to be kept in sync with the API response. For NFL, the `nflreadpy` package wraps the nflverse data sources, which evolve season-over-season as new stats appear (new Next Gen Stats fields, new FTN charting columns, etc.). Hand-writing 50+ columns per table for seven tables, then keeping them current with upstream changes, would consume meaningful effort with no clear product benefit.

Decision: `etl/nfl_etl.py` defers schema to pandas + `nflreadpy`. On first run per table, `df.to_sql(if_exists='replace')` creates the table with column types inferred from the dataframe. On subsequent runs, `add_missing_columns()` diffs the dataframe columns against the live table and ALTERs in any new columns using a conservative type map (object → NVARCHAR(500), int64 → BIGINT, float64 → FLOAT, bool → TINYINT, datetime → DATETIME2). Every table gets an implicit `created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()` audit column on first creation. Upsert keys are hand-specified per loader function and never change; only columns evolve.

Consequences:

- No hand-maintained DDL files for NFL. `/database/nfl/` will never contain .sql schema scripts
- Adding a column requires zero code changes — the next ETL run picks it up
- Dropping or renaming a column is not supported by the inference mechanism. Manual intervention is required, which is acceptable because these are rare
- Column types can be loose (NVARCHAR(500) for anything object-typed). If a stricter type is needed for a specific query, do it at the query layer with CAST
- First-run behavior differs from NBA/MLB: the first run creates the tables. A completely clean database has no `nfl.*` tables until the ETL has run once successfully
- This pattern is scoped to NFL. Do not extend it to NBA or MLB without a specific reason — those schemas are stable enough that hand-written DDL is not the friction it is for NFL

---

## ADR-0015 [nfl][etl] Single source: `nflreadpy` package handles schedules, stats, charting, and rosters
Date: 2026-04-20

Context: NBA and MLB each call their canonical public APIs directly — `stats.nba.com` / `cdn.nba.com` for NBA, `statsapi.mlb.com` for MLB. NFL has no equivalent single official API with prop-research-grade granularity. The nflverse ecosystem (a community-maintained stack of R and Python packages) is the de facto canonical source for NFL data and aggregates from multiple upstream sources (NFL.com, Pro Football Reference, FTN Fantasy, nflverse itself) into a unified schema.

Decision: All NFL ETL data comes from `nflreadpy` (the Python binding to nflverse). Seven tables cover the full ingest: `load_schedules`, `load_players`, `load_player_stats(summary_level='week')`, `load_snap_counts`, `load_ftn_charting`, `load_rosters_weekly`, `load_team_stats(summary_level='week')`. No direct HTTP calls to NFL.com, PFR, FTN, or ESPN. `update_config(cache_mode='off')` is called at the top of every run because GitHub Actions runners have no persistent filesystem.

Consequences:

- NFL ETL depends on a single third-party Python package. If `nflreadpy` breaks or changes its API surface, the ETL breaks
- The data model is whatever `nflreadpy` exposes. Fields and column names come from the package's contract with nflverse, not from a custom data model
- Upstream outages (nflverse data lag, FTN unavailability) surface as per-table load failures caught by the fail-soft `run(name, fn)` wrapper. Other tables still load even if one upstream is down
- Play-by-play data (`nflreadpy.load_pbp`) is available but not currently loaded. Adding it would be a one-function call once a use case justifies it
- Odds data is NOT in `nflreadpy`. NFL odds would still come from the Odds API via a future `nfl_odds_etl.py` or an extension of the existing `odds_etl.py`

---

## ADR-0016 [shared][docs] Retire legacy docs to `/docs/_archive/` via git mv, preserving history
Date: 2026-04-20

Context: Step 7 of ADR-0001 called for retirement of `/PROJECT_REFERENCE.md` and `/CHANGELOG.md` at the repo root once their content had been migrated. Original plan in `/docs/MIGRATION_HANDOFF.md` was to delete both files. On final review, the root CHANGELOG turned out to be 756 lines of genuine engineering history (commit-by-commit decision log going back months) that was never fully migrated into `/docs/CHANGELOG.md` — the new log only covers the documentation restructure itself. Deletion would have lost that history from any non-git view of the repo.

Decision: Archive, do not delete. Both files moved to `/docs/_archive/` via `git mv` so git history is preserved as a rename rather than a delete + add. Each archived file gets an ARCHIVED banner at the top pointing readers to `/docs/README.md` and the component READMEs. Pre-flight checks before the move confirmed no workflows, scripts, or code in `etl/`, `mcp/`, `web/`, `grading/`, or `infrastructure/` reference either file by path.

Consequences:

- Historical engineering record is preserved and remains git-blame-able
- References in new READMEs that say "legacy root `/CHANGELOG.md`" are now technically incorrect path-wise and need a follow-up str_replace sweep to point at `/docs/_archive/CHANGELOG.md`. Not a correctness-critical issue because the intent (historical reference) is unchanged
- The legacy session protocol at the top of the old PROJECT_REFERENCE.md is superseded by `/docs/SESSION_PROTOCOL.md`. Readers who open the archive file see the banner first and know to follow the current protocol
- `/docs/MIGRATION_HANDOFF.md` becomes obsolete once Step 7 is complete and should be deleted in the same session as this ADR lands
- `.github/workflows/` and `.gitignore` were spot-checked and do not reference the archived paths
- Future reorganizations that might want to shorten the path can move the archive again; git mv keeps every past move discoverable via `git log --follow`

---

## ADR-0017 [shared][docs] Session protocol skill at `/docs/skills/session-protocol.md` separates prescriptive checklist from protocol rationale
Date: 2026-04-21

Context: `/docs/SESSION_PROTOCOL.md` defined the protocol (start, end, invariants, why) in one file. In practice, new sessions were tripping over things the protocol did not cover: mechanical hazards like `github:push_files` corrupting `.py` files, stale memory contradicting the repo, NFL context leaking into MLB work, mid-session changes (new infrastructure, new schema) not getting captured until end-of-session when they were forgotten. Adding all of that to `SESSION_PROTOCOL.md` would have turned a 60-line reference into a 300-line operations manual and diluted its role as the canonical definition. Separately, a fresh session's context window is most constrained at the start, so execution guidance needs to be short enough to absorb in one read.

Decision: Introduce a second file at `/docs/skills/session-protocol.md` that holds prescriptive execution guidance: a start-of-session checklist, a mid-session signals table (infrastructure change, schema change, roadmap shift, etc. each mapped to an owed update), an end-of-session checklist, mechanical guardrails, known session-boundary failure modes, and a bounded "when to deviate" list. The canonical protocol definition stays in `SESSION_PROTOCOL.md`; rationale stays there too under "Why this protocol exists." The two files reference each other: the protocol points at the skill for execution; the skill complements the protocol without duplicating it.

Consequences:

- Sessions read two small files at start (`/docs/README.md`, `/docs/SESSION_PROTOCOL.md`) and the skill (`/docs/skills/session-protocol.md`) instead of one medium file. Total read length is comparable, but the split makes the prescriptive content scannable.
- The mid-session signals table is a new structural piece. Any new signal category (e.g. "new third-party dependency added") should be appended to that table in the skill with its corresponding owed update.
- Mechanical guardrails (fresh SHA before create_or_update_file, no push_files for .py) live in the skill, not scattered across user memory. If new guardrails emerge from burned sessions, they go in the skill.
- The "when to deviate" section in the skill is capped at four bullets by design. Adding a fifth is a signal that deviation has become the norm and the protocol itself needs revision.
- `/docs/skills/` is now a directory. Future Claude-facing skill files (testing playbooks, debugging runbooks, etc.) go there. User-facing runbooks for infrastructure operations continue to live at `/infrastructure/runbooks/`.
- `/docs/README.md` now lists the skill under "Read first every session." The router is the one place where the reading order is authoritative; anyone editing the reading order edits the router.

---

## ADR-0018 [mlb][etl][database][web] First ADR-0004 derived entity: `mlb.player_at_bats` materialized in-lockstep with PBP writes
Date: 2026-04-21

Context: ADR-0004 committed to pre-aggregating all visual-feeding MLB entities instead of computing them at query time. Five of the nine entities had no materialization as of the Step 5 migration (including player at-bats). The old `/api/mlb-atbats` route violated ADR-0004 by running a filtered aggregation against `mlb.play_by_play` on every request (`is_last_pitch = 1 AND result_event_type IS NOT NULL` plus two joins to `mlb.players`). Shipping the first derived entity establishes the pattern for the other four, unblocks the future Player Analysis page's access path, and removes the last remaining runtime PBP aggregation the web app relied on.

Three subsidiary decisions within this ADR:

1. Materialization runs inline in `etl/mlb_play_by_play.py` after each PBP flush, not in a separate workflow. The two datasets must stay consistent; a separate workflow creates a window where PBP has a game and at-bats don't. Inline keeps the at-bats table automatically covered by any backfill run.
2. The diff for the materializer runs against `mlb.player_at_bats.game_pk`, not against the PBP diff that drives the fetch loop. This makes partial runs (PBP wrote, at-bats failed) self-healing on the next invocation, and makes `--rebuild-at-bats` the same code path with a different game set.
3. Names are NOT denormalized onto `mlb.player_at_bats`. An initial design that did denormalize produced 19.8% NULL `batter_name` and 32.4% NULL `pitcher_name` across the 5,092-game backfill because `mlb.players` is truncate-and-reload scoped to the current season, leaving 983 historical player IDs unresolvable. The web route joins `mlb.players` at read time instead. The joined table has under a thousand rows with a PK on `player_id`, so the cost is negligible.

Decision: Create `mlb.player_at_bats` with PK `at_bat_id = '{game_pk}-{at_bat_number}'`. Populate via `load_player_at_bats_for_games(engine, game_pks)` called inline after each PBP flush in `mlb_play_by_play.py`. Add `--rebuild-at-bats` CLI flag and matching `rebuild_at_bats` workflow_dispatch input on `mlb-pbp-etl.yml` for full rebuilds. Store only IDs — `batter_id` and `pitcher_id` — never names. Web `/api/mlb-atbats` reads this table directly and joins `mlb.players` at read time for names. Two indexes: `IX_player_at_bats_game_pk` for the web access path, `IX_player_at_bats_batter` on `(batter_id, game_date)` as the intended Player Analysis access path.

Initial backfill from 5,092 existing PBP games produced 384,040 at-bat rows in 76 seconds.

Consequences:

- `/api/mlb-atbats` no longer runs a filter-and-aggregate over pitch-level data on every request. ADR-0004's "no runtime aggregation of pitch-level data" invariant is enforced for this route
- Any future schema change to `mlb.player_at_bats` requires a full rebuild via `--rebuild-at-bats` after `DELETE FROM mlb.player_at_bats`. The flag skips the PBP fetch loop so it is safe to run independently
- Partial-run self-healing means a failure between PBP flush and at-bats materialization is automatically recovered on the next PBP workflow run, regardless of whether the next run is rebuild-mode or normal-mode
- The four remaining ADR-0004 entities (batter context, batter projections, trend/pattern, platoon splits, career BvP) follow this same pattern: direct INSERT, pre-diff against the destination, materialization lives in the ETL script that produces the source data
- ID-only storage is the right default for any future derived table that needs player references. Denormalizing names only works if the reference table covers the full historical time range, which `mlb.players` does not

---

## ADR-0019 [mlb][etl][database] Second ADR-0004 derived entity: `mlb.career_batter_vs_pitcher` via staged MERGE off `player_at_bats`
Date: 2026-04-21

Context: ADR-0018 shipped the first of the ADR-0004 derived entities (`mlb.player_at_bats`) and established the in-lockstep-with-PBP-writes materialization pattern. Career batter-vs-pitcher matchup was next in line because it is the data source for the planned VS page (ADR-0003) and its grain, source, and refresh cadence all needed to be decided before any more derived tables were added. Three questions had to be answered jointly: what grain, which source table, and which write strategy.

Three subsidiary decisions within this ADR:

1. **Source is `mlb.player_at_bats`, not `mlb.play_by_play`.** At-bats is already at the right grain with the right filter applied and indexed on `batter_id`. Using PBP directly would re-execute the `is_last_pitch = 1 AND result_event_type IS NOT NULL` filter on every materializer call, duplicating work already done. The cost is a derived-to-derived dependency, but the ordering is mechanical: player_at_bats writes first, career_bvp reads from it, within the same PBP flush cycle. Never the other direction.

2. **Write strategy is staged MERGE, not pre-diffed INSERT** (diverging from ADR-0013 and ADR-0018). Every flush can touch `(batter, pitcher)` pairs that already have rows from prior flushes and need updating. Pre-diffing doesn't apply because the idempotency unit isn't a new key, it's a recomputed aggregate. Incremental path: stage affected pairs, recompute their lifetime counts, MERGE. Rebuild path uses the same MERGE, chunked by batter_id. Note: SQL Server does not support `WHERE (col1, col2) IN (SELECT col1, col2 FROM ...)` tuple-IN syntax; the materializer uses an `#affected_pairs` temp table with INNER JOIN instead.

3. **PK is compound `(batter_id, pitcher_id)`, no synthetic key and no season dimension.** A synthetic `'{batter_id}-{pitcher_id}'` string adds ~40 bytes per row and zero functional value — queries naturally filter on one or both IDs. Lifetime is the entire point of this table; windowed views (last 3 matchups, last 5) get their own materialization when a consumer needs them, rather than inflating this row shape speculatively.

Decision: Create `mlb.career_batter_vs_pitcher` with compound PK `(batter_id, pitcher_id)` clustered, plus `IX_bvp_pitcher` on `(pitcher_id, batter_id)` for the reverse read path. Columns: PA, AB, H, 1B, 2B, 3B, HR, RBI, BB, SO, HBP, SF, TB counts plus AVG/OBP/SLG/OPS rates (pre-computed in the MERGE, stored so the web needs no arithmetic) plus `last_faced_date`. Populate via `load_career_bvp_for_games(engine, game_pks)` called inline in the PBP flush loop after `load_player_at_bats_for_games`. Add `--rebuild-bvp` CLI flag and matching `rebuild_bvp` workflow_dispatch input for full rebuilds, independent of `--rebuild-at-bats`. All counts derive from `result_event_type` via CASE WHEN aggregation; no schema change needed to `mlb.player_at_bats`.

Initial backfill from 384,040 at-bat rows produced 165,550 `(batter, pitcher)` pairs across 806 batters in approximately 6 seconds.

Event-type taxonomy baked into `BVP_AGGREGATE_SELECT`:

- Hits: `single`, `double`, `triple`, `home_run`
- Walks: `walk`, `intent_walk`
- Strikeouts: `strikeout`, `strikeout_double_play`
- HBP: `hit_by_pitch`
- Sac flies: `sac_fly`, `sac_fly_double_play`
- AB excludes: walks, intent walks, HBP, sac flies (both variants), sac bunts (both variants), `catcher_interf`

Consequences:

- `mlb.career_batter_vs_pitcher` is the first materialized table in the repo to use staged MERGE because its grain requires updates, not just appends. Future derived tables with the same property (player trend/pattern stats, career pitcher-vs-batter if separated) should use this same pattern rather than reinventing
- The three-sport shape of ADR-0013's "pre-diffed direct INSERT is always right" is now two-sport: direct INSERT for append-only derivations (play_by_play, player_at_bats), staged MERGE for aggregate derivations (career_batter_vs_pitcher). The decision rule is whether a key can need an update after initial write
- The `(batter_id, pitcher_id) IN (SELECT ...)` tuple-IN anti-pattern caught in the first commit of this change should be remembered. SQL Server needs the staged temp-table + JOIN pattern instead. Worth listing in `/docs/skills/session-protocol.md` mechanical guardrails if it recurs
- The `rebuild_bvp` and `rebuild_at_bats` workflow flags are independent and compose: setting both runs at-bats first (populating the source), then bvp (aggregating from the source). This ordering is encoded in `main()` and should not be reversed
- The four remaining ADR-0004 entities (batter context per game, batter projections per game, player trend/pattern stats, player platoon splits) reduce to three after this ADR lands. Player trend/pattern stats likely also want staged MERGE (rolling windows change as new games land); batter context and projections likely fit the append-only player_at_bats model (one row per batter per game). Platoon splits are a structural variant of career_batter_vs_pitcher and should share its write strategy
- Ratios are stored pre-computed in the permanent table. If a bug in the ratio math is ever found, the fix requires a full rebuild via `--rebuild-bvp` after a DELETE. No view layer abstracts this away. That is an intentional trade: the web reads are dumb and fast


## ADR-0017: Opportunity-based grading (2026-04-22)

**Context.** The existing grading stack (hit rate, trend, momentum, pattern, matchup, regression) was entirely outcome-based: it measured what happened (made stat cleared the line) without distinguishing "efficient with few attempts" from "inefficient with many attempts." A player shooting 12 threes a night on a 2.5 line got the same signal as one shooting 6, provided their hit rates matched.

**Decision.** Add six per-(player, market) opportunity grades to `common.daily_grades`:
- `opportunity_short_grade`, `opportunity_long_grade`: short-vs-long trend and long-vs-season trend on per-game opportunity.
- `opportunity_matchup_grade`: how much the opponent allows of this opportunity stat to this position group.
- `opportunity_streak_grade`: sign-based run vs player's own opportunity mean.
- `opportunity_volume_grade`, `opportunity_expected_grade`: threes-only parallel columns for raw `3PA` trend vs `3PA * 3PT%` trend.

Opportunity per market:
- Points / combos with P: `(FGA - 3PA) * r2 * 2 + 3PA * r3 * 3 + FTA * rft` where r2/r3/rft are per-player trailing shooting percentages.
- Rebounds / combos with R: `reb_chances` from `nba.player_rebound_chances`.
- Assists / combos with A: `potential_ast` from `nba.player_passing_stats`.
- Threes: `3PA * r3` primary, with raw `3PA` and `3PA * r3` as parallel volume/expected columns.
- Combo markets (PRA/PR/PA/RA) sum the component opportunities; missing components count as 0 via `sum(min_count=1)`.

Look-ahead bias is prevented by using shift(1) rolling averages: game G's opportunity uses percentages computed from games 1..G-1 only.

**Alternatives considered.** (a) Flat league-average FTA-to-possession coefficient (0.44) — rejected because it folds opportunity into a possession metric, which was not the question. (b) Per-player FTA-to-possession ratio from play-by-play — rejected as higher-complexity for marginal lift over weighting each attempt by its own expected points. (c) A new workflow and table for opportunity — rejected; source data (`player_box_score_stats`, `player_passing_stats`, `player_rebound_chances`) is already ingested daily by `nba_etl.py`, so no new ETL or cron is needed. The grading pipeline consumes the extra data on existing schedules.

**Consequences.**
- `common.daily_grades` gains 6 columns (nullable, non-breaking for existing readers; `web/lib/queries.ts:getGrades` already uses column-existence checks).
- `composite_grade` now averages 7-11 components instead of 5-6 depending on market and coverage. Historical rows are unchanged until re-graded by backfill.
- `fetch_matchup_defense` SQL widened to include opportunity ranks; slightly heavier but still a single query per grading run.
- Backfill dates before tracking data was populated will have NULL for rebound/assist opportunity grades.
- Blocks and steals have no opportunity grade (no per-player attempt rate).
- `precompute_opportunity_grades` uses `groupby().transform()` not `.apply(group_keys=False)` — the latter drops the grouping column in pandas 3.x.

**Supersedes.** Nothing. Extends the grading stack from ADR-0005.
