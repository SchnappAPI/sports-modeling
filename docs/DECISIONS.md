# Architecture Decision Records

Append-only. New ADRs use date-based identifiers (see "Numbering scheme" below). ADRs are never rewritten, only superseded by a later ADR that references them.

Format:

```
ADR-YYYYMMDD-N [scope][component] Title
Date: YYYY-MM-DD
Context: Why this decision came up.
Decision: What was decided.
Consequences: What this implies for future work.
Supersedes: ADR-XXXX (optional)
```

---

## Numbering scheme

ADRs 0001-0019 use sequential zero-padded numbers (the historical scheme).

Starting 2026-04-22, new ADRs use date-based identifiers: `ADR-YYYYMMDD-N` where
`N` is a counter for multiple ADRs on the same day (`-1`, `-2`, ...). The change
was made after a cross-session collision produced a duplicate ADR-0017. Date-based
identifiers are collision-free because a session checks for today's existing ADR
headers with a single grep rather than needing to know the highest number across
the whole file. The existing duplicate ADR-0017 (opportunity-based grading) is
left in place per the append-only, never-rewritten rule; future sessions should
recognize that context when referring to "ADR-0017."

End-of-session rule: grep the file for `^## ADR-YYYYMMDD-` with today's date; if
nothing matches use `-1`, otherwise use the next unused counter.

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

---

## ADR-20260422-1 [nba][grading] `--force` flag on `grade_props.py --mode backfill` to re-grade existing historical rows in place

Date: 2026-04-22

**Context.** ADR-0017 added six opportunity-based grade columns to `common.daily_grades`. Once the schema and code were deployed, newly-graded dates included the opportunity values, but the 173 historical dates already graded under the old schema had NULL opportunity columns. The existing `backfill` mode skipped those dates entirely via a `NOT EXISTS` clause in `run_backfill` whose purpose is to avoid re-grading dates that already had full coverage under the old (pre-opportunity) definition of "full." Without a way to opt out of that skip filter, the opportunity columns would never populate historically — the data would only exist forward from the deploy date.

**Decision.** Add a `--force` CLI flag to `grade_props.py --mode backfill` that removes the `NOT EXISTS` filter for the current invocation, causing every in-scope date to pass through `_compute_grades` and into `upsert_grades`. The MERGE in `upsert_grades` handles the re-grade automatically via its UPDATE branch when a row already exists for the `(game_date, player_id, market_key, line_value, outcome_name)` unique key, so no explicit DELETE-then-INSERT is needed. The archive trigger attached to `daily_grades` preserves the prior row version in `daily_grades_archive` before the UPDATE, giving us full history of what changed. The flag also threads through `grading.yml`'s `workflow_dispatch` inputs (`force: boolean`), into the env block, into the `ARGS` assembly, and into the redispatch payload so a multi-batch chain carries `--force` end-to-end. Default remains `false` — normal nightly grading never re-grades resolved dates.

**Alternatives considered.**

1. Run a standalone SQL UPDATE to backfill opportunity columns directly. Rejected because opportunity grades depend on per-player shift-1 rolling percentages computed across the full date range; a pure-SQL version would duplicate logic that already lives in `precompute_opportunity_grades`. Keeping the flag means one code path, one source of truth.
2. Hard-coded DELETE-then-reinsert of the 173 dates before re-dispatching backfill. Rejected because `upsert_grades` already handles UPDATE correctly via MERGE. Deleting first would throw away archive lineage for no benefit.
3. Two separate workflows, one for forward backfill and one for force-rewrite. Rejected because the batch-and-redispatch chain logic is identical; duplicating it doubles the surface area for the chain-control bugs that are easy to introduce.

**Consequences.**

- Backfill mode now has two distinct behaviors controlled by a single flag. Every future use of backfill must consider which behavior is needed. Default (no flag) is "fill gaps only" and is correct for nightly resolver runs. `--force` is "re-grade everything in scope" and is correct only when a new grade column has been added and existing rows must adopt it.
- `daily_grades_archive` will show a spike in row volume corresponding to the 173 re-graded dates. This is expected and not a bug — each UPDATE archives the old row version before writing the new one.
- `grading.yml`'s `workflow_dispatch` now has a `force` boolean input in addition to `mode`, `date`, `batch`, and `time_limit_minutes`. Any tool that dispatches this workflow (e.g. a future admin UI button) must know about the flag and default it to `false`.
- The flag is NBA-specific today because only NBA grading uses `grade_props.py`. If MLB grading reuses the same script pattern, the flag should port over unchanged. If MLB ships a separate script, it should adopt the same flag convention.

**Supersedes.** Nothing. Extends ADR-0017 by giving it a backfill path for historical rows.

---

## ADR-20260422-2 [shared][infra][docs] VM git pushes work directly via stored credentials; prior "403 forbidden" claim was stale memory, not repo state

Date: 2026-04-22

**Context.** User-facing project memory carried a long-standing instruction that git pushes from the VM (`schnapp-runner-2`) failed with HTTP 403 and that all commits had to route through the GitHub MCP or through Azure Cloud Shell with a personal access token. This instruction predates the introduction of the Schnapp Ops MCP and predates the stable credential store at `/home/schnapp-admin/.git-credentials`. A session on 2026-04-22 attempted a direct `sudo -u schnapp-admin git push origin main` from the VM and it succeeded on the first try, producing a clean commit on `main` with no 403. No repo file ever codified the 403 claim — it existed only in memory.

**Decision.** Document here, in the append-only decision log, that VM git pushes work via the credentials stored at `/home/schnapp-admin/.git-credentials`. The Schnapp Ops MCP can run full edit-commit-push workflows directly via `shell_exec`, bypassing both the GitHub MCP (which has file-type hazards around `.py` newlines and non-ASCII Unicode) and the Cloud Shell detour. User project memory has been updated to reflect this in the same session via `memory_user_edits`.

**Why it's an ADR.** This is the kind of fact that future sessions could otherwise re-derive wrong by trusting stale memory over repo state. The session-protocol skill already says "if memory contradicts `/docs/`, the repo wins," but that rule only works if the repo has an explicit counter-statement. This ADR is that counter-statement.

**Consequences.**

- Any future session that sees a 403-on-VM-push claim in memory or in a pasted primer should treat it as contradicted and trust the VM push path first. A fresh 403 would indicate a new credential problem (token revoked, expired, repo permissions changed), not a standing architectural constraint.
- The Schnapp Ops MCP's `shell_exec` is the preferred execution surface for Python, git, and any VM-side operation. GitHub MCP remains the right tool when the session is running without VM access, but from inside a Schnapp Ops session the VM path is cleaner.
- If the credential at `/home/schnapp-admin/.git-credentials` is ever revoked or rotated, this ADR still holds — the fact that VM push works as a class of operation is the invariant. Re-provisioning credentials restores it.
- No workflow or code change is made by this ADR. It is documentation of an operational fact.

**Supersedes.** Nothing at the repo level. Effectively retires a user-memory-only instruction that had no codified presence in `/docs/` or in any component README.

---

## ADR-20260423-1 [nba][grading] Composite formula reweighted to momentum/hr60/pattern; other components stored as context only

Date: 2026-04-23

**Context.** The existing composite grade averaged all non-null components with equal weight: hit rate, trend, momentum, pattern, matchup, regression, and (from ADR-0017) four opportunity grades. Grade-outcome correlation analysis on 1.04M resolved rows found that only two components have meaningful predictive lift: momentum_grade (28-point Won-vs-Lost gap) and hit_rate_60 (25-point gap). Pattern_grade has a 3-point gap and is retained as a tiebreaker and context signal. All other components — matchup (1.1 gap), regression (slightly negative), trend (effectively zero vs the standard line), all six opportunity grades (0.1 or less) — diluted the composite by pulling it toward 50 and caused the grade 90-100 bucket to collapse (hitting at only 46.4% under the old equal-weight formula, worse than grade 70-80).

**Decision.** Rewrite `compute_composite` signature to three arguments: `compute_composite(momentum, hit_rate_60, pattern)`. New weights: 40% momentum + 40% (hit_rate_60 * 100) + 20% pattern. Renormalize when any component is NULL so partial availability still produces a valid 0-100 value. All removed components (matchup, regression, trend, all six opportunity grades) remain computed and written to `common.daily_grades` as context columns — they are useful for display and future analysis but must not re-enter the composite mean without fresh calibration evidence.

**Calibration evidence.** Ran scipy gaussian_kde calibration on 94,029 records. Shift amplitude of -0.019 produces log-loss improvement of 0.000012 over zero shift — negligible. The correct mechanism for incorporating confidence is grade-weighted lookback window selection for KDE tier computation (see tier lines below), not location-shifting the distribution.

New composite grade vs actual hit rate with the reweighted formula (monotonic, no collapse):
- Grade 0-10: 20.8%, grade 40-50: 46.0%, grade 60-70: 60.9%, grade 80-90: 74.8%, grade 90-100: 82.4%

**Tier lines.** Added `compute_kde_tier_lines` and `common.player_tier_lines` table. KDE fitted on grade-weighted game log window (15 games composite>=80, 30 games 50-79, full season <50; normal dist fallback when n<10). Reflection boundary at 0 prevents negative-stat probability mass. Tier cutoffs: safe>=80%, value>=58%, high_risk>=28% with +150 or better market price, lotto>=7% with +400 or better and composite>=50. Blowout dampening applied at 50% of historical pts delta when spread>=10.5 for pts/combo markets.

**Why not equal-weight all non-null components.** The former equal-weight approach implicitly assumed each component carries equal signal. Calibration falsified this: matchup, regression, and trend variance across players closely follows random noise at the 0-1 gap range. Including them adds asymmetric downward pressure (they pull everything toward 50) for zero lift. When those components are near 50 (neutral) they are harmless; when a player has an extreme value on a noisy component it introduces a false signal.

**Consequences.**

- `compute_composite` in `grade_props.py` now takes exactly three arguments. Any caller that passes the old 10-argument signature will get a TypeError. The backfill run re-grades all 174 historical dates to populate `composite_grade` under the new formula.
- `common.player_tier_lines` is a new table (one row per player-market-game-date). Web consumers should read this table for tier line display rather than computing tiers client-side.
- The six opportunity grades and matchup/regression/trend columns remain on `common.daily_grades`. They are available for future exploration but are not in the composite. Adding any of them back to the composite requires calibration evidence showing positive lift.
- Blowout dampening is applied at 50% of the historical player delta, not 100%, to avoid over-penalizing a single game context. The 50% factor should be revisited once enough blowout-game outcomes have been resolved under the new tier framework.
- `fetch_player_blowout_profiles` requires `nba.games` to have `home_score`, `away_score`, `home_team_tricode`, `away_team_tricode` columns. These come from the CDN box score writer; their presence should be verified before any environment migration.

**Supersedes.** The equal-weight composite from ADR-0005 is superseded for composite_grade computation. ADR-0005's UNIQUE key and schema definitions remain in force.


---

## ADR-20260424-1 [shared][docs] STATUS taxonomy extended with `idle`; `planned` dropped from component vocabulary

Date: 2026-04-24

**Context.** The component-README STATUS line was previously restricted by `docs/GLOSSARY.md` to `live | in development | design phase | planned`. The 2026-04-24 audit surfaced NFL ETL as a real state not covered by the vocabulary: code exists, pipeline runs on schedule (Tuesday 09:00 UTC), but no active development has happened since first run and no downstream product consumer exists. Labeling this "in development" misrepresents both NFL (no work happening) and MLB (actual active work). The word "live" similarly overstated: technically the ETL runs, but no one consumes its output.

**Decision.** Extend the STATUS taxonomy with `idle` and drop `planned` from component STATUS vocabulary. Final five-state taxonomy:

- `live` — production, actively used (NBA today)
- `in development` — active work in progress, not yet considered live (MLB today)
- `idle` — infrastructure exists and may run on a schedule, but no active development and no downstream product consumer (NFL ETL + NFL database today)
- `design phase` — planning and specification underway, no code yet
- `not started` — no code and no active design

`planned` is retained as a roadmap concept but is no longer a valid component STATUS value. A component is either built (`live`/`in development`/`idle`) or pre-build (`design phase`/`not started`).

**Consequences.**

- `docs/GLOSSARY.md` amended with per-state examples so the taxonomy is self-anchoring.
- `etl/nfl/README.md`, `database/nfl/README.md`, `etl/README.md`, `database/README.md`, `docs/ROADMAP.md` realigned to use `idle` for NFL.
- Sessions must not invent new STATUS values. If none of the five fit, pick the closest and clarify in body text rather than coining new terms.

**Supersedes.** Nothing at the repo level; extends the vocabulary in `docs/GLOSSARY.md`.

---

## ADR-20260424-2 [shared][etl][database] Data integrity and completeness framework: invariants at write, mapping resolver, daily retry with 3-attempt cap

Date: 2026-04-24

**Context.** The project has accumulated data-integrity gaps that were accepted as tolerable but never systematically tracked or resolved:

- 135 NBA players in `odds.player_map` are unmapped. Previously accepted as "historically inactive, not on current rosters, not blocking grading." Not surfaced, not tracked, no path to resolution.
- `mlb.players` is truncate-reload scoped to the current season. 20-32% of historical `batter_id`/`pitcher_id` joins on `mlb.player_at_bats` and `mlb.career_batter_vs_pitcher` resolve to NULL names. Accepted via "join at read time."
- Sports ETL encounters upstream publication lag routinely. Nothing in the current system distinguishes "truly missing" from "hasn't appeared yet."
- No structured place where data-integrity issues surface. Problems are discovered ad-hoc during backtests or user reports.

Root causes of unexpected NULL values in a field that should have one, with the right response per cause:

1. Upstream publication lag — daily retry
2. Transient API error — already handled by `get_engine` retry (3 attempts × 45s)
3. Entity mapping gap — mapping resolver (not retry; retrying the same failed lookup is pointless)
4. Schema drift / new entity — daily retry (resolves after next reference-table pull)
5. Legitimately null (domain-conditional — e.g., no batted ball data on a strikeout; no opportunity_volume_grade for a rebounds market) — never retry; accept
6. Our parsing bug — human attention; retry masks it

A single generic "retry everything NULL" is wrong for multiple of these.

**Decision.** Three-layer framework. Each layer addresses a distinct failure mode.

**Layer 1 — Invariants enforced at write time.** Every table that opts in declares its "must-not-be-null" critical fields in a Python dict constant `CRITICAL_FIELDS` in `etl/integrity.py` (flat per ADR-0002). Before the MERGE/INSERT step, a shared `validate_row()` helper checks each row. Violating rows do NOT land in the production table; they go to `common.ingest_quarantine` with the full row payload as JSON, the failed invariant, the source workflow, and timestamps. The ETL workflow continues per-row (fail-soft) but logs quarantine counts.

Two kinds of "null is valid" need to be distinguished:

- **Stat-zero vs stat-null.** A stat column (PTS, REB, AST, hit counts, etc.) is never NULL when the row exists. A player who played 4 minutes and scored nothing has PTS=0, not PTS=NULL. If a row has NULL where the schema says stat, that is a Layer-1 violation — something went wrong in parsing or upstream delivery. DNP players produce no row (they do not appear in `nba.player_box_score_stats` at all), so there is nothing to "conditionally null" for them.
- **Domain-conditional nullability** is declared in `CRITICAL_FIELDS` as SQL-expression rules. Real examples in the current codebase: `nba.daily_grades.over_price` is NULL on bracket lines (only the center line in a bracket-expanded standard market carries the posted price); `nba.daily_grades.opportunity_volume_grade` and `opportunity_expected_grade` are NULL when `market_key` is not threes-related (other markets have no volume or expected-makes metric); `mlb.play_by_play.hit_launch_speed`, `hit_launch_angle`, and `hit_total_distance` are NULL when `is_hit_into_play = 0` (strikeouts, walks, HBP have no batted ball); `mlb.play_by_play.batter_id` is NULL on pickoff and caught-stealing events where no batter of record applies.

Any successful Layer 1 validation for a `(table_name, row_key)` also clears prior `common.ingest_quarantine` and `common.data_completeness_log` entries for that same key, regardless of whether the new write came from a scheduled ETL run, a retry workflow, or a manual fix. This keeps HEALTH.md accurate and closes resolved Issues without requiring every resolution path to explicitly touch the integrity tables.

**Layer 2 — Mapping resolver.** Entity-matching gaps go to `common.unmapped_entities` keyed by `(source_feed, entity_type, source_key)`. A nightly workflow `resolve-mappings.yml` attempts auto-resolution via exact match (case-insensitive), last-name + first-initial match, and normalized string distance against the relevant reference table. Any gap auto-resolved is logged with the resolution method. Any gap remaining after 3 nights of attempts raises a GitHub Issue labeled `data-integrity:unmapped` with source details and suggested candidates.

**Layer 3 — Retry cadence for upstream lag.** `common.data_completeness_log` keyed by `(table_name, row_key, column_name)` with `first_detected_at`, `last_attempt_at`, `attempt_count`, `resolved_at`, `notes`. Nightly workflow `retry-incomplete.yml` scans quarantine + production tables for declared-critical fields still NULL. For each, if `attempt_count < 3` AND `last_attempt_at < today`, reattempts — re-fetches from upstream for that specific row key, not a full ETL re-run. If resolved, marks `resolved_at` and moves row from quarantine to production. If attempt 3 fails, raises a GitHub Issue labeled `data-integrity:incomplete`.

**Retry cadence: one per day, max 3 attempts.** Sports data publication cycles are daily, not sub-daily. Exponential backoff within a day burns API quota against an upstream that cannot resolve faster. One-per-day aligns with existing ETL schedules (piggybacks on the next daily upstream state refresh). 3-day cap survives typical weekend publication delays (Fri ETL → Sat retry → Sun retry covers "data not posted until Monday"). Beyond 3 days, the root cause is almost never upstream lag; it is a mapping gap, an unrecognized legitimate null, or a parsing bug — all of which need human attention. Retrying further is wasted work.

**Surfacing: `docs/HEALTH.md` + auto-opened GitHub Issues.**

- `docs/HEALTH.md` is regenerated daily by a new `daily-health-report.yml` workflow and committed to `main`. Contents fall into two categories:
  - **Column-level integrity** from Layer 1: quarantine counts per table, unresolved mappings with age, incomplete-retry counts at each attempt level.
  - **Relational integrity** from scanned queries: one table's row counts aligned against another's expectations. Defined alongside `CRITICAL_FIELDS` in a sibling `RELATIONAL_CHECKS` dict. Real cases this must cover: games in `nba.schedule` with status >= 1 should have `nba.daily_lineups` rows covering both teams' rosters (typically 13-17 per team); games in `mlb.games` with status = 'F' should have corresponding `mlb.batting_stats` and `mlb.pitching_stats` rows; games in `nba.schedule` with status = 3 should have `nba.player_box_score_stats` rows for every player who appears in `nba.daily_lineups` with starter_status != 'Inactive'. A relational failure (e.g., playoff `boxscorepreviewv3` populating only 10 of 15 roster slots per team) does NOT block the ETL; it surfaces in HEALTH.md so the partial state is visible and re-fetches can be triggered.
  - **Downstream-inference safety rule (documented here so sessions do not re-derive it wrong):** "did this player play?" is answered by `minutes > 0` in `nba.player_box_score_stats` or by `starter_status != 'Inactive'` in `nba.daily_lineups`. It is NEVER answered by whether stat values are zero. PTS = 0 with MIN > 0 is a player who played and did not score; PTS = 0 with MIN = 0 is a player who was on the roster but did not take the floor. Any code or query that uses stat-zero as a proxy for non-participation is wrong.

Zero notification; pure reference. Session protocol will be amended to check HEALTH.md at start of any data-touching session.
- GitHub Issues are auto-opened only at the 3-attempt cap (either layer). Labels: `data-integrity:unmapped` and `data-integrity:incomplete`. Body is auto-generated with row details + suggested resolution path. Issues auto-close via the Layer-1 clearing rule above: when a subsequent validation succeeds for the same `(table, row_key)`, the clearing step also closes any open Issue tagged with that row key. This covers all three resolution paths (retry workflow success, regular ETL re-write, manual DB fix). Workflow permissions are scoped via `permissions: issues: write` in the workflow YAML on the default `GITHUB_TOKEN` — no long-lived PAT expansion.

**Consequences.**

- New tables: `common.ingest_quarantine`, `common.unmapped_entities`, `common.data_completeness_log`. DDL owned by `etl/integrity.py` (pattern matches existing Python-owned DDL).
- New workflows: `resolve-mappings.yml`, `retry-incomplete.yml`, `daily-health-report.yml`.
- New shared module: `etl/integrity.py` with `CRITICAL_FIELDS` catalog + `validate_row()` + `write_quarantine()` + `move_from_quarantine()`.
- Every existing ETL script gets a one-line change to route rows through `validate_row()` before upsert. Rollout per sport, NBA first (highest-volume, most test data), then MLB, then NFL.
- Every existing `common.*`, `nba.*`, `mlb.*`, `nfl.*` table must declare its critical fields in `CRITICAL_FIELDS`. One-time cataloging, 1-2 hours per sport done carefully (including conditional-nullability rules). A 30-minute shallow pass will miss real cases and produce false positives in HEALTH.md.
- `docs/HEALTH.md` becomes a canonical artifact. Session-start reading list is extended to include it.
- The 135 NBA unmapped players become a resolvable class of problem rather than accepted loss. Either they get resolved via the mapping resolver or get marked `legitimate_unresolvable = 1` (player no longer in any current feed, deliberately skipped).
- The `mlb.players` 20-32% NULL rate on historical name joins is a read-time join problem, not a write-time invariant problem. Rows in `mlb.player_at_bats` are fully valid at write time (both `batter_id` and `pitcher_id` are populated); the NULL appears only when joining to the current-season-scoped `mlb.players` at read time. Layer 1 does not catch it. It is tracked as a scanned metric in `docs/HEALTH.md` (percentage of `mlb.player_at_bats` rows where `batter_id` or `pitcher_id` does not resolve) and properly fixed by ADR-20260424-3 Initiative D's table-strategy change.

**First-run scope requirement (not deferred):** v1 must include a retroactive scan pass that applies each table's `CRITICAL_FIELDS` rules against existing production rows, writes violations to `common.data_completeness_log` with a `detected_retroactively = 1` flag, but does NOT move existing rows out of production. Forward-looking quarantine applies only to new writes. Without the retroactive scan, HEALTH.md would show zero issues on day one despite real integrity gaps in historical data — the file would be misleading by design.

**Open questions deferred to implementation.**

- Exact schema for `common.ingest_quarantine` (row_payload as NVARCHAR(MAX) JSON vs sparse columns) — decide during implementation.
- Whether to version the `CRITICAL_FIELDS` catalog (schema evolves; old quarantine rows need to re-validate against new invariants). Decide when second cataloging exercise runs.
- Whether `resolve-mappings.yml` should attempt LLM-assisted resolution for ambiguous fuzzy matches. Likely no for v1 — deterministic match first, human review for the rest.

**Supersedes.** The informal "accept 135 unmapped NBA players as tolerable loss" stance captured in `database/nba/README.md` and CHANGELOG. Under the new framework, unresolvable-after-3-attempts becomes an explicit state, not an implicit acceptance.

---

## ADR-20260424-3 [shared][docs] Streamlining initiative sequence: A → D → B → E → C

Date: 2026-04-24

**Context.** The 2026-04-24 audit surfaced five streamlining opportunities:

- **A — Data integrity and completeness framework** (designed in ADR-20260424-2).
- **B — Code reuse / DRY pass.** Unify the local `get_engine()` copy in `etl/nfl_etl.py` with `etl/db.py`. Promote reusable helpers (`clean_df()` from NFL, VARCHAR-width `INSERT_DTYPES` from MLB PBP). Consolidate workflow YAMLs where a single template + per-sport inputs would be cleaner than separate files.
- **C — Observability layer.** A nightly `daily-health-check.yml` on top of A's tables. Alerts on workflow failures in last 24h, stale schedules (e.g., NBA ETL missed 24h), row-count sanity checks (`daily_grades` should grow monotonically). Markdown summary to a known location.
- **D — MLB player table strategy.** Resolve the 20-32% NULL name rate on `mlb.player_at_bats`/`mlb.career_batter_vs_pitcher` joins. Either make `mlb.players` append-only with `last_seen_season`, or denormalize names onto the derived tables at ETL time. Decision-with-ADR when D activates.
- **E — ROADMAP structure.** Add a "Completed and idle" section so finished-but-inactive items (NFL ETL today, others later) have somewhere natural to live.

User directive: stop deferring; get these figured out.

**Decision.** Execute strictly sequentially in the order A → D → B → E → C.

**Rationale.**

- **A first.** Highest-value intervention; every later initiative benefits from A's infrastructure being in place. Starting elsewhere would create work that needs refactoring into A's pattern later.
- **D second.** The MLB NULL-name problem is the most acute active integrity loss. It is a specific application of A's framework, not a separate system. D validates A in production against a real pain point.
- **B third.** Low-risk consolidation once A has established the shared-module pattern (`etl/integrity.py`). Scope stays bounded: unify `get_engine` imports, promote verified-reusable helpers only, consolidate workflow YAMLs where duplication exceeds 80%.
- **E fourth.** 20-minute documentation-only change. Trivial once the other initiatives are underway.
- **C fifth.** Observability consumes A's tables and wants B's cleanup to reduce false-positive noise. Cheapest and best-informed last.

**Why strictly sequential, not parallel.** One developer with periodic Claude sessions. Parallelism creates merge conflicts on exactly the files that need coherent design (the new `etl/integrity.py`, the new `common.*` tables, the workflow YAMLs). Sequential execution is slower in theory but much more reliable in practice. Each initiative gets its own branch, its own implementation-level ADR, its own PR and merge.

**Consequences.**

- Exactly one initiative is "active" at a time. The next cannot start until the previous has landed on `main`.
- Each initiative gets a dedicated implementation ADR (ADR-20260424-2 already covers A's design; A's implementation gets a follow-up ADR when it lands).
- `docs/ROADMAP.md` "Active" section reflects the currently-active initiative. "Next up" holds the ordered queue.
- Estimated effort: A implementation is 4-6 hours of focused work (ADR done; new tables, new shared module, rollout to NBA). D is ~2-3 hours (apply A to MLB). B is 2-3 hours. E is 30 minutes. C is 3-4 hours. Total: ~5 focused sessions to reach a state where all five are resolved and the project is meaningfully easier to manage.

**Supersedes.** The "deferred indefinitely" status of items mentioned in prior Open Questions across several READMEs. Those deferrals are now scheduled, not deferred.

## ADR-20260424-4 — Tier-line discretion: filter, quantize, calibrate

**Status:** Proposed. Design-only; implementation pending Austin sign-off on the four open questions at the bottom.

**Context**

Live-product feedback from Austin on 2026-04-24: `common.player_tier_lines` surfaces lines that are unusable in practice. Four failure modes:

- Non-tradeable line values (e.g., 4.4 points) when all posted alternate lines are 0.5-increment. The current `compute_kde_tier_lines` interpolates freely from the KDE output instead of snapping to the posted board.
- Implied-odds overconfidence (lines at American -1400, implied probability >93%). Austin's bar: nothing stronger than -500 (implied ~83%) because post-vig EV collapses and the confidence creates a false-certainty UX.
- Unreasonable proximity to the posted center. Example cited: safe 5 points when the posted center is 18.5. The model picks the lowest line that clears a probability threshold regardless of where it sits on the distribution.
- Raw KDE probability does not match historical outcomes at the top of the composite-grade range. Backtest signal: as composite grades rise past ~85, actual hit rate flattens while displayed probability continues to rise.

Independent signal from the 2026-04-24 retroactive scan: 3,087 NULL safe_line rows exist in production. These are cases where the current code correctly declines to emit a Safe line because no posted alternate qualifies. That behavior is right; this ADR generalizes it to all four tiers and adds explicit discretion controls.

ADR-20260423-1 established the tier schema; this ADR modifies the line-selection function. Schema unchanged.

**Decision**

Four controls applied in sequence inside `grading.grade_props.compute_kde_tier_lines`:

1. **Posted-line source.** Tier lines must be chosen from the set of posted alternate lines for that `(player_id, market_key)`. Free-form KDE interpolation is removed. After the KDE produces a target probability for a tier threshold, pick the posted alt line whose KDE probability most closely satisfies the threshold. Every emitted line is tradeable on FanDuel as-is.

2. **Implied-odds ceiling.** Reject any posted line whose current American odds are stronger than -500 (implied probability > 0.833). Implementation: join the candidate line against `odds.upcoming_player_props.outcome_price` on the Over side. If every posted alt line at a tier fails this cap, that tier is NULL. "Too sure to bet" becomes a non-result rather than a surfaced line.

3. **Posted-center proximity band.** A tier line must lie within ±N% of the posted center (the standard, non-alternate line for that market). Initial N: 50%. Rejects candidates outside `[center * (1 - N), center * (1 + N)]`. Eliminates the safe-5-when-center-is-18.5 failure mode. Band may need per-market tuning — see Question A.

4. **Backtest-calibrated probability.** Replace raw KDE probability in the output with an empirically-calibrated probability derived from historical `grade → outcome` correlation. Process:
   - Build a calibration table (`common.grade_calibration` or an in-memory lookup rebuilt nightly) by bucketing resolved `common.daily_grades` rows by composite_grade and computing empirical hit rate per bucket.
   - Initial buckets: 0-25, 25-50, 50-65, 65-75, 75-85, 85-92, 92+.
   - Shrinkage factor for each bucket: `empirical_hit_rate / raw_kde_prob_mean`.
   - At tier-line emit time, multiply the raw probability by the bucket's shrinkage factor before writing to `safe_prob` / `value_prob` / etc. The displayed 78% then reflects 78% historical reality rather than model-optimistic 85%.

**Order of operations**

For each tier (Safe threshold ~0.75, Value ~0.58, HighRisk ~0.45, Lotto ~0.25):

1. Collect posted alt lines for `(player_id, market_key)` from `odds.upcoming_player_props`.
2. Filter by proximity band (control 3).
3. Filter by implied-odds ceiling (control 2).
4. Compute raw KDE probability for each surviving line.
5. Pick the line whose KDE probability is closest to the tier threshold from above (highest prob that still satisfies the tier).
6. Apply calibration shrinkage (control 4) to produce the emitted probability.
7. If zero lines survive steps 2-5, tier value is NULL.

**Consequences**

Positive:
- Every emitted tier line is tradeable; no 4.4-point anomalies.
- No "false certainty" at -500+ implied odds.
- Safe/Value/HighRisk/Lotto all stay within a reasonable band of the posted center.
- Displayed probability matches historical reality.
- Tier NULL becomes semantically clean: "no qualifying line at this tier" instead of "data gap."

Negative:
- Expected 30-50% reduction in emitted tier-line counts (exact figure TBD from first post-change run). User may perceive fewer "picks."
- Calibration dataset needs periodic rebuild as outcomes accumulate. Add a nightly workflow step or rebuild inside the grading run.
- Proximity band assumes the posted center line is always available. Add a RELATIONAL_CHECK: every graded market in the window should have a posted center line in `odds.upcoming_player_props`.
- Existing `common.player_tier_lines` rows remain under old logic until backfilled. Post-change backfill of the last 30 days is recommended (workflow: `grading.yml` with a force flag, limited to tier_lines regeneration).

**Open questions — answer before implementation**

A. **Proximity band width.** 50% is plausible for points (18.5 → 9-28). Too tight for low-value markets (blocks 1.5 → 0.75-2.25 excludes legitimate 0.5 and 2.5). Options: per-market band, stat-dependent band, or use standard deviation from player's recent games. Recommendation: per-market table, default 50%, overrides for `player_blocks`, `player_steals`, `player_threes` set to ±100% or a fixed 2-line spread.

B. **Calibration bucket granularity.** 7 buckets seems reasonable for 1M+ row dataset. Alternative: continuous shrinkage curve (isotonic regression on composite_grade). Continuous is more accurate but harder to explain; discrete buckets are inspectable. Recommendation: start with 7 discrete buckets, evaluate isotonic later.

C. **When every tier fails.** If no posted line satisfies any tier, emit an empty tier_lines row (all four tier values NULL) or emit nothing? Empty row preserves grade_date + player_id + market_key for joinability but wastes space. No-row is cleaner but breaks the "every graded player-market has a tier row" invariant. Recommendation: empty row with all NULLs, document invariant as "tier_lines row exists for every graded player-market; tier values are individually nullable."

D. **MLB scope.** MLB grading does not yet exist (ADR-20260424-3 sequencing). Build this ADR's changes as NBA-only for v1 and re-parameterize when MLB grading is implemented. Recommendation: NBA-only v1, parameterize on sport_key when MLB grading lands.

**Implementation plan once approved**

1. Write `grading/calibrate_grades.py` — computes empirical hit rate per composite_grade bucket from resolved `common.daily_grades`. Idempotent; writes `common.grade_calibration` or returns a DataFrame cached in-process.
2. Modify `compute_kde_tier_lines` in `grade_props.py` to apply the four controls in order.
3. Add RELATIONAL_CHECK `nba_graded_markets_have_posted_center` to `etl/integrity.py`.
4. Full-season walk-forward backfill via `grading.yml` mode=backfill force=true. Each historical date calibrates from prior-day evidence only (no leakage). [Amended 2026-04-24: original text said "last 30 days"; corrected per Austin's stated full-season position. The 30-day-only scope was a misreading of the goal — partial regrades mix two model versions and break correlation analysis.]
5. Monitor first week post-deploy: tier-line count per day, NULL rate per tier, user-reported weirdness.

**Related ADRs**
- ADR-20260423-1 (player_tier_lines initial design — schema unchanged by this ADR)
- ADR-20260424-2 (data integrity framework — adds a relational check covered here)
- ADR-20260424-3 (initiative sequence — this falls inside Initiative A's ongoing iteration)

## ADR-20260424-5 — Tier-line justification and breakout-detection (replaces ADR-4 control 3)

**Status:** Approved. Amends ADR-20260424-4 before implementation. Revised 2026-04-24 after Austin clarified: breakout opportunities are what the tool is TRYING to find, not what it should filter out.

**Context**

ADR-20260424-4 proposed a proximity band. ADR-5 v1 replaced that with a historical-justification filter ("player must have hit the threshold at least once"). Both framings are wrong. From Austin:

> Dont omit anything just because they have not done it before. For example a player may never have had 10 rebounds before, but has been averaging 18 rebound chances a game as of recently, or a player has never hit 15 points before, but as of lately they are taking more shots, getting more playing time, and shooting more efficiently. Those are just two examples of what i dont want to unintentionally hide from myself. Instead those are things i am trying to find.

The goal is identifying extremes-with-signal, including breakout candidates who have NEVER hit the line before but show leading indicators (opportunity, minutes, usage) trending in that direction. Any filter gating on historical outcomes suppresses the exact cases the tool should surface.

**Decision**

*Remove dense-grid line interpolation.* Tier lines are chosen from posted alternate lines only — no freely-generated 4.4pt lines. Implements ADR-4 control 1.

*Remove historical-justification filter entirely.* No precedent required to surface a tier line. Surface everything that has a posted alternate passing the -500 implied-odds cap. Trust the user, not a filter, to assess the evidence.

*Keep -500 implied-odds ceiling across all tiers.* Reject any posted line whose Over price is more negative than -500 (implied probability > 83.3%). Applies to Safe, Value, HighRisk, Lotto uniformly. Keeps the existing per-tier minimum-price floors for HighRisk (+150 or better) and Lotto (+400 or better) — these define what makes each tier that tier.

*Exclude blocks and steals.* TIER_EXCLUDED_MARKETS covers player_blocks, player_blocks_alternate, player_steals, player_steals_alternate. Bypass tier computation for these markets entirely.

*Surface raw evidence alongside calibrated probability.* Add 21 new columns to common.player_tier_lines across three groups:

Per-tier hit evidence (16 columns — 4 metrics × 4 tiers):
- `<tier>_hits_all` INT — games player hit at or above the line across all available history
- `<tier>_games_all` INT — denominator for the above
- `<tier>_hits_20` INT — hits in the last 20 games played by this player
- `<tier>_games_20` INT — denominator, usually 20 (less if player has fewer games on record)

Per-tier price (2 new columns; HighRisk and Lotto already had these):
- safe_price INT
- value_price INT

Per-player-market context (3 columns shared across tiers — the breakout-detection signals):
- recent_minutes_20 FLOAT — avg minutes per game in last 20
- recent_opportunity FLOAT — avg of market-appropriate opportunity metric, last 20
- historical_opportunity FLOAT — same metric over the full window (lets user compute the trend themselves)

Opportunity metric per market (from v1):
- points / points_alternate: FGA (shot attempts from box score)
- threes / threes_alternate: FG3A (three attempts from box score)
- rebounds / rebounds_alternate: reb_chances (from nba.player_rebound_chances)
- assists / assists_alternate: potential_ast (from nba.player_passing_stats)
- combos (PR, PA, RA, PRA and _alternate): NULL for v1, evaluated case-by-case in v2

*Apply isotonic calibration to emitted probabilities.* Fit isotonic regression on resolved common.daily_grades (composite_grade -> Won/Lost). Calibrated probability replaces raw KDE prob in tier_prob columns. Also publish discrete bucket view for inspection.

*When every tier fails.* Emit no row.

**Consequences**

Positive:
- Breakout candidates surface. Player with rising minutes and rising shot attempts but no historical hits at the line gets a tier line with 0/200 hits_all but 18.2 recent_opportunity vs 14.3 historical.
- User sees hits_all, hits_20, recent_minutes, recent_opportunity, calibrated probability together. They form the judgment.
- Non-tradeable line values eliminated. Every emitted line is on the posted board.
- False certainty eliminated. Implied-odds cap holds across all tiers.
- Calibrated probabilities match historical hit rates.

Negative:
- 21 new columns on common.player_tier_lines. Additive; existing rows NULL until regenerated.
- compute_kde_tier_lines needs per-tier hit counts and per-player-market opportunity values. Hit counts come free from stat_values already in hand; opportunity needs a separate fetch joining nba.player_rebound_chances and nba.player_passing_stats for rebound/assist markets.
- Tier row volume likely increases net-net (no historical filter removing rows).

**Implementation plan**

1. Schema migration: 21 additive columns via one-off workflow.
2. Calibration module: grading/calibrate_grades.py — fit isotonic regression on resolved daily_grades, publish bucket table for inspection.
3. Opportunity fetcher in grade_props.py: pull fga, fg3a, reb_chances, potential_ast per (player, game) over LOOKBACK_OPP.
4. Rewrite compute_kde_tier_lines per this ADR.
5. Update caller and upsert_tier_lines with new columns.
6. Full-season walk-forward backfill via grading.yml mode=backfill force=true. [Amended 2026-04-24: original text said "last 30 days"; corrected to match Austin's full-season walk-forward requirement. Walk-forward calibration via `as_of_date` parameter ensures each date uses only prior-day evidence.]
7. Monitor first week.

**Related**
- ADR-20260424-4 — controls 1, 2, 4 unchanged; control 3 replaced by this ADR.
- ADR-20260423-1 — tier_lines schema; this ADR extends additively.


## ADR-20260424-6 — Tier qualification redesign: Safe EV floor, HighRisk/Lotto OR-gate with breakout signal, 4 hit-context columns

**Status:** Approved 2026-04-24. Refines ADR-20260424-5. Replaces the strict probability gate for HighRisk and Lotto with an OR-gate that admits breakout cases. Adds an EV floor for Safe to prevent -EV "safe" props at very short prices. Adds 4 columns characterizing past hit games for similarity matching on the rare tiers.

**Context**

ADR-5 emits tier rows only when calibrated probability clears the tier's threshold (Safe ≥ 0.80, Value ≥ 0.58, HighRisk ≥ 0.28, Lotto ≥ 0.07). After running with ADR-5 in production, three problems became visible:

1. The strict probability gate hides exactly the cases Austin is trying to find. A player who has never hit 15 points has hits_all = 0, the KDE-fitted probability falls below the Lotto floor, the row never gets emitted. The 21 ADR-5 evidence columns never reach the user because the row was filtered before the columns could be populated. From Austin: *"a player may never have had 10 rebounds before, but has been averaging 18 rebound chances a game as of recently, or a player has never hit 15 points before, but as of lately they are taking more shots, getting more playing time, and shooting more efficiently. Those are just two examples of what i dont want to unintentionally hide from myself. Instead those are things i am trying to find."*

2. Safe rows can be -EV at the boundary. At -500 (the implied-odds ceiling) with 80% calibrated probability (the Safe threshold), per-dollar EV is -0.04. A row with both values at the floor surfaces as Safe even though it's a strictly losing bet. From Austin: *"It is rare that i will place props that are -1000... I am trying to find a balance. To find valuable props to bet on."*

3. The 4 evidence columns added in ADR-5 (hits_all, games_all, hits_20, games_20 per tier) tell the user *whether* past hits exist but not *under what conditions* they occurred. From Austin: *"identify the variables or factors that are consistent in games leading up to a player hitting one of these uncommon lines. are there any trends consistent and similar to the players situation now?"*

**Decision**

*Safe tier: add EV floor.* In addition to existing controls (probability ≥ 0.80, posted line, -500 implied-odds ceiling), require `safe_ev ≥ TIER_SAFE_EV_FLOOR` (-0.05 per dollar). Drops the -EV-by-construction Safe rows at the boundary. Higher-quality Safe rows are unaffected.

*HighRisk and Lotto: OR-gate qualification with breakout signal.* The price floors (+150 for HighRisk, +400 for Lotto) and -500 implied-odds ceiling are unchanged. The probability gate becomes a logical OR: surface the row if `calibrated_probability ≥ tier_threshold` OR `breakout_signal` is true.

`breakout_signal := (recent_opportunity ≥ historical_opportunity × 1.15) AND (recent_minutes_20 ≥ season_avg_minutes × 0.95)`

Where `historical_opportunity` is the player's mean of the market-relevant opportunity metric (FGA for points markets, FG3A for threes, reb_chances for rebounds, potential_ast for assists) over their full season-to-date history before grade_date. `recent_opportunity` is the same metric averaged over the last 20 games. `season_avg_minutes` is the player's mean minutes over the same season-to-date window.

This admits the breakout case explicitly: a player whose recent shot volume has stepped up by 15%+ while playing roughly their normal minutes will surface in HighRisk and Lotto even when the KDE-derived probability says no, because the trend signal indicates the line is now within reach.

The composite ≥ 50 gate currently restricting Lotto is also relaxed: Lotto qualifies if `(composite ≥ 50) OR breakout_signal`. Composite ≥ 50 was a coarse quality filter; the breakout signal is a more direct read of "this player has upside right now."

*Add 4 hit-context columns to common.player_tier_lines.* For each row where a HighRisk or Lotto line is emitted AND the player has at least one historical game where the stat met or exceeded that line, populate:

- `highrisk_hit_avg_min` FLOAT — average minutes played in past games where stat ≥ highrisk_line
- `highrisk_hit_avg_opp` FLOAT — average market-relevant opportunity in past games where stat ≥ highrisk_line
- `lotto_hit_avg_min`    FLOAT — average minutes in past games where stat ≥ lotto_line
- `lotto_hit_avg_opp`    FLOAT — average opportunity in past games where stat ≥ lotto_line

Computed from a stat-history-and-opportunity-history join keyed on (player_id, game_date) in the caller, then passed to compute_kde_tier_lines as an aligned DataFrame. Indices in the aligned frame let us identify the games where the line was hit and average the matching minutes/opportunity values. NULL when a tier line is emitted but no past hits exist (this is the breakout case — the qualifying logic surfaced the row without a hit precedent).

The user reads these alongside `recent_minutes_20` and `recent_opportunity` already on the row: a row showing past hits at 36 minutes and 18 FGA when today's projected role is 22 minutes and 11 FGA tells the user the past hits came from conditions not present tonight. The reverse — past hits at 22/11 when today is 22/11 — supports the play.

**Consequences**

Positive:
- Breakout candidates surface. Player with rising opportunity and stable minutes who has never hit the rare line gets a tier row with `hits_all = 0`, `recent_opportunity > historical_opportunity`, and the user sees both numbers and judges plausibility.
- -EV Safe rows are dropped at the boundary. The -500 cap was always implicitly +EV-adjacent at high probabilities; the explicit EV floor handles the boundary cleanly.
- Similarity matching on the rare tiers gets concrete numbers. Past-hit conditions are no longer hidden in aggregate hit counts.

Negative:
- 4 additional columns on common.player_tier_lines (49 total). Additive migration. Existing rows NULL until regenerated.
- Tier row volume is expected to increase modestly: rows that previously failed the strict HighRisk/Lotto probability gate but pass the breakout signal will now be emitted. Estimated 5-15% increase based on observed opportunity-trend distribution.
- Composite ≥ 50 relaxation for Lotto introduces a small number of rows that previously would not have surfaced; these are by design the breakout cases.
- The breakout signal computation requires `recent_opportunity`, `historical_opportunity`, `recent_minutes_20`, and a season-average minutes value to be available. When the player has insufficient game history (early season or thin sample), breakout_signal is False, falling back to probability-only qualification.

**Implementation plan**

1. Schema migration: add 4 additive columns via one-off workflow (`migrate-tier-lines-v4.yml`).
2. `grading/grade_props.py`:
   - Add constants: `TIER_SAFE_EV_FLOOR`, `BREAKOUT_OPP_RATIO`, `BREAKOUT_MIN_RATIO`.
   - Update `compute_kde_tier_lines`: new `aligned_history` kwarg (pd.DataFrame with cols stat, minutes, opportunity); compute season_avg_min and breakout_signal once at top of function; apply EV floor at Safe emission; apply OR-gate at HighRisk and Lotto emission; populate 4 new hit-context fields.
   - Update `ensure_tables`: ADD COLUMN IF NOT EXISTS for the 4 new columns.
   - Update `upsert_tier_lines`: extend ALL_COLS, create_cols_sql, and MERGE.
   - Update caller `grade_props_for_date`: build aligned_history DataFrame from stat_grp + opp_grp on game_date, pass into compute_kde_tier_lines, include 4 new column values in tier_rows.append.
3. Push, restart full-season walk-forward backfill with `--force`.
4. Validate: find a breakout case (zero past hits, qualifying via opportunity), a similarity match (past hits + today matches avg-hit-game conditions), and a Safe row dropped by the new EV floor.

**Related**
- ADR-20260424-5 — tier-line justification; this ADR refines its qualification logic and adds 4 columns to its 21.
- ADR-20260424-4 — controls 1, 2, 4 unchanged (posted lines only, -500 ceiling, isotonic calibration).
- ADR-20260423-1 — tier_lines schema; this ADR extends additively.


## ADR-20260425-1 [shared][docs] Live session cache: per-turn chat logging on dedicated `chat/*` branches

Date: 2026-04-25

**Context.** Long chats lose context on tab close, compaction, or mid-flow interruption. Existing protocol captures end-state via CHANGELOG and the why via ADRs, but neither captures the conversation that produced the why. For chats that produce repo changes, the discussion leading to the change has durable value: framing shifts, alternatives considered, edge cases raised. Manual handoff via primer is fragile and burns user effort. Three constraints shaped the design:

1. Latency overhead from per-turn writes is real on claude.ai chat (each commit re-uploads the full chat file via GitHub MCP). Short throwaway chats cannot pay that cost. Long substantive chats can.
2. Whether a chat is "substantive" is best determined mechanically, not heuristically. The cleanest test: did the chat cause a commit to a non-chats path? If yes, it earned the log. If no, no log was needed.
3. The skill must work on both claude.ai chat and Claude Code so that surface choice does not silently change behavior.

**Decision.** Introduce a user-installed skill `live-session-cache` (lives at `~/.claude/skills/live-session-cache/SKILL.md`, generic across all projects) plus a project-specific integration doc at `docs/skills/live-session-cache.md` that overrides defaults for this repo.

The skill operates in three modes:

- **`trigger` (default on claude.ai chat):** activates the moment Claude is about to write to any path other than `chats/`. Backfills the conversation up to that point as Turn 0 (full reconstruction, not summary), then logs per-turn going forward.
- **`always` (default on Claude Code):** activates at session start. Logs every substantive turn from turn 1.
- **`manual`:** activates only when the user invokes a trigger phrase like "start logging this chat". Manual phrases work as override in the other modes too.

Mode is selectable per-project via a `live-session-cache: <mode>` line in project memory.

Each turn block contains: User message verbatim, Reasoning (2-5 sentence summary), Response verbatim, optional Evolution Note (captures inflection points where a turn shifted approach without producing immediate code), State Delta (decisions, files touched with work-branch commit hashes, errors, open questions). Skip rules drop pure-acknowledgment turns.

Chat files live at `chats/in-progress/{slug}.md` while active; move to `chats/archive/{YYYY}/{MM}/{slug}.md` on wrap-and-merge. Branch convention: `chat/YYYY-MM-DD-{slug}` off `main`, always separate from the work branch.

End-of-chat options:

1. **Wrap and merge.** Final summary at top of file, move to archive path, open PR with title `chat: {slug}` and the summary as PR body, recommend squash-merge.
2. **Pause and continue.** Append `## Session paused — {timestamp}` block with current state. Leave branch open. Resumption flow detects open `chat/*` branches at start of next chat.
3. **Keep going.** Dismiss the warning.

Context-size monitoring fires soft warning around 60-70% of capacity (one-time note) and hard warning around 85% (presents the three options and waits).

**Alternatives considered.**

1. *Single growing log file on main, no branch.* Rejected because in-flight log pollutes main before the chat is done. Branch isolation lets us decide at wrap-up time whether the conversation lands.
2. *Chats live in a separate dedicated repo (`SchnappAPI/claude-chats`).* Rejected because chat history loses its proximity to the code it produced. With chats in the project repo, six months from now the conversation that produced a commit is in the same place as the commit.
3. *Always-on across both surfaces.* Rejected for claude.ai because per-turn full-file upload makes short chats expensive for no benefit. Trigger-based on claude.ai is the right tradeoff.
4. *Heuristic activation ("detect substantive content").* Rejected because the determination of "meaningful" introduces judgment risk. Binary "first non-chats write" is mechanical and matches user's actual definition of meaningful (per the originating conversation).
5. *Capture raw thinking blocks verbatim.* Rejected because thinking is internal scratch space without a stable handle. Reasoning summary is the available proxy.

**Consequences.**

- New folder `/chats/` becomes part of repo structure. Subfolders `in-progress/` and `archive/{YYYY}/{MM}/`.
- Chat branches accumulate during active chats. Wrap-and-merge or branch deletion is the only way they leave the repo.
- Squash-merge on wrap keeps `main` history clean despite N turn-level commits per chat.
- The chat log is supplementary, not a replacement for existing end-of-session protocol. CHANGELOG, ADR, INVARIANTS edits all still required. The chat log preserves the conversation that produced them.
- Mode `trigger` keeps zero overhead on chats that do not produce repo changes. Mode `always` on Claude Code is cheap because writes are local until session-end push.
- Project memory stays clean: one optional line for mode override.
- Project-specific rules (branch conventions, integration with CHANGELOG/ADR/INVARIANTS, redaction policy) live in `docs/skills/live-session-cache.md` and are read by the skill at activation. The repo doc takes precedence when defaults conflict.

**Open questions.** None for v1. Refinements expected after first real usage. Likely candidates for revision: context-size threshold percentages, whether the Evolution Note field needs further structure, whether wrap-and-merge should auto-tag the PR with anything for filtering.

**Supersedes.** Nothing. Extends the documentation system without modifying ADR-0001's core structure.

## ADR-20260425-2 [shared][web] DB-backed feature flags as the runtime visibility surface

Date: 2026-04-25

**Context.** Three operational needs converged. First, maintenance mode was a hardcoded constant in `web/middleware.ts` that required a commit-and-push (and a 90-second SWA redeploy) every time it flipped. Second, with NBA shipping ahead of MLB and MLB ahead of NFL, individual sports and sub-pages periodically need to be hidden from end users without removing the underlying code. Third, the existing inline `RefreshDataButton` on NBA pages required a separate `ADMIN_REFRESH_CODE` prompt every time, even though the same admin already had a session at `/admin`. A single source of truth that handles all three, editable from a phone in seconds, was preferable to growing the env-var surface.

**Decision.** Introduce `common.feature_flags` (`flag_key VARCHAR(100) PK`, `enabled BIT`) as the single runtime visibility surface. The DB is authoritative; no env vars, no Azure portal flips, no commits to toggle. Seed seven flags on creation (`maintenance_mode`, `sport.nba`, `sport.mlb`, `sport.nfl`, `page.nba.grades`, `page.nba.player`, `page.mlb.main`); future pages add one row plus one line in the admin UI list.

Reads happen in two places, both with a 60-second in-process cache so DB load is at most one read per minute per Azure Functions instance:

- `web/middleware.ts` reads `maintenance_mode` to drive the maintenance gate.
- `web/lib/feature-flags.ts` exports `isPageVisible(flagKey)` for server components. Sport pages call it with `sport.<x>`; sub-pages call it with `page.<x>.<y>`.

Three layered behaviors keep the design predictable:

1. **Cascade.** A `page.<sport>.<x>` lookup short-circuits to false if its parent `sport.<sport>` flag is explicitly disabled. Disabling NBA hides every NBA sub-page in one flip; sub-pages don't need their parent state duplicated.
2. **Admin bypass.** Any visitor with the `sb_unlock=go` cookie passes every gate. The cookie is set automatically by `/api/admin/*` on successful auth, so signing into `/admin` simultaneously authenticates and unlocks every disabled surface for that browser. The separate `?unlock=go` URL is preserved for the same flow without an admin sign-in.
3. **Fail open.** Any DB error in `loadFlags()` returns the previously-cached map (or an empty map on cold start). The site stays up if the database stalls; flag flips are best-effort, not a hard dependency.

Admin UI extends the existing `/admin` page with three tabs (Codes, Visibility, Tools). Visibility renders one row per flag with an enable/disable toggle hitting `/api/admin/flags`. Tools holds the relocated Refresh Data button, which now reads the admin session header instead of prompting for `ADMIN_REFRESH_CODE`. The discreet entry to `/admin` is a triple-tap on the top-left 32x32 corner of any page (zero visual surface, works identically on desktop and mobile).

**Tradeoffs.** The DB read adds latency to every page load (worst case ~50ms on a cold instance, sub-1ms on a warm cache). A flag flip takes up to 60 seconds to propagate across instances; this is acceptable for an operator tool, not for any user-facing latency-sensitive logic. The surface area is meaningfully larger than the constant it replaces: a DB table, two API routes, a server helper, an admin UI section, page-wrapper edits across every sport. The benefit is that toggles are live and reversible from a phone with no push.

**Alternatives considered.** Env vars in Azure SWA app settings: rejected because Azure portal flips are slow on mobile and the SWA settings page is awkward on a phone screen. A single global JSON in blob storage: viable but introduces a second source of truth that has to stay in sync with the DB-backed admin codes table that already exists. Per-sport hardcoded constants with deploys: same redeploy-on-flip problem as the maintenance constant; rejected for the same reason.

**Consequences.**

- `web/middleware.ts` no longer carries a `MAINTENANCE_ON` constant. The `web/README.md` Maintenance Gate section is updated to reflect the DB-backed flow.
- Adding a new gated page is a two-line change: insert one row into `common.feature_flags`, add one `if (!(await isPageVisible(...)))` line at the top of the page wrapper. The admin UI auto-renders the new flag in the Visibility tab on next load (it lists whatever rows exist in the table).
- The `ADMIN_REFRESH_CODE` env var stays defined for backward compatibility but is no longer the primary refresh entry point.
- Three commits landed the framework: `7c9d52f`, `4f5da59`, `7e65752`. See CHANGELOG 2026-04-25 for the file-level breakdown.

## ADR-20260425-3 [nba][grading] Two-cap calibration fix: thin-sample raw cap + calibrator output cap to control overconfidence at high-probability tail

Date: 2026-04-25

**Context.** Backtest on 72 ADR-6-rebuilt dates (88,922 resolved tier-line rows, later validated on 103,365 over a 143-date superset) showed two overlapping failure modes in the high-probability tail. First, players with thin history (`n_games < 10`, the `_kde_prob_above` normal-dist fallback) hit lines at 69.5% on the prob>=0.80 cohort while the model predicted 85.4%, a 15.9-point gap; the cohort with `n_games >= 10` showed only a 6.3-point gap on the same threshold. Second, the isotonic calibrator trained on the same data forced empirical rates of 65-78% in the raw 0.90+ buckets (combined n=135) UP to 84.7% via PAV pooling against the much larger 0.85-0.90 bucket (n=1814, empirical 82.3%); the calibrator then claimed 84.7% probability for inputs whose true rate was 65-78%. Both modes contributed to the Safe tier missing its 80% design target by 15.3 points overall.

**Decision.** Two surgical caps, neither of which drops Safe inventory:

1. **Thin-sample raw cap.** Add `KDE_THIN_SAMPLE_PROB_CAP = 0.85` to `grade_props.py`. In `_kde_prob_above`, both fallback branches (`n < KDE_MIN_GAMES` and the `except Exception` after KDE fitting fails) now return `min(prob, KDE_THIN_SAMPLE_PROB_CAP)`. This caps the worst overconfidence cases (Sam Merrill 3-game window producing 99.99% probabilities) without affecting the n>=10 majority.

2. **Calibrator output cap.** In `calibrate_grades.py` `fit_calibrator`, compute `max_well_sampled_rate = max(stats.hit_rate where n >= 200)` at fit time. This evaluates to ~0.8225 on the current dataset (raw 0.85-0.90 bucket, n=1814, empirical 82.3%). Apply `min(v, max_well_sampled_rate)` inside the `calibrator` closure after the standard PAV interpolation. The cap stops the model from claiming probabilities the data has never validated at scale, but stays above the 0.80 Safe threshold so Safe-qualifying rows still qualify.

The cap value is published as `max_well_sampled_rate` on `common.grade_calibration` so the new web transparency page can surface both the per-bucket empirical rates AND the global cap in effect.

**Tradeoffs.** Modest improvement, not a fix. Backtest simulation: Safe overall gap shrinks from -10.0 pts to -8.7 pts (1.3 pts narrower); Safe high-prob (>=0.80) gap shrinks from -10.2 pts to -7.8 pts. Realized return on Safe ($-0.04 per dollar) is unchanged because the same rows still bet, the model just stops misrepresenting their probability. The deeper structural problem - that Safe at -500 prices is breakeven only above ~83.3% hit rate, but the model can only deliver ~75% on the well-sampled tail - is not addressed here. That requires either tightening `IMPLIED_ODDS_CEILING` further or lowering `TIER_SAFE_PROB`, both deferred pending more backtest history.

**Alternatives considered.** Filter rows where `n_games < 15` entirely (Fix A in the simulation): improved Safe gap from -10.0 to -1.5 pts but dropped 38.6% of all tier inventory. The visibility cost was judged too high relative to the calibration improvement. Recompute calibrator with non-monotonic regression (allow the tail to drop): rejected as it adds complexity without addressing the thin-sample root cause. Threshold sensitivity analysis showed `n_games >= 5` captured most of the calibration gain that `>= 15` did, suggesting the issue is concentrated in the 0-4 game cohort, which the 0.85 raw cap suppresses without requiring row-level filtering.

**Consequences.**

- The running grading process picks up the changes on next iter of the backfill chain. In-flight processes do not (Python imports are loaded once).
- `common.grade_calibration` gains a new column `max_well_sampled_rate FLOAT NULL`, idempotently added by `publish_calibration_buckets` on first call after deploy.
- A web transparency page surfacing per-bucket calibration accuracy is a follow-on task (live ADR-2026-04-25-3 deliverable, not yet implemented at time of decision).
- After the in-flight backfill completes (currently ~87 of 143 ADR-6 dates), a re-run of the backtest should validate the simulated -2.4 pts improvement on prob>=0.80 cohort. Targets: Safe gap ~-7 pts overall, ~-7.8 pts at high-prob tail.


## ADR-20260426-1 [shared][infra][web] Maintenance-mode middleware returns 200, not 503
Date: 2026-04-26

Context: On 2026-04-25 18:36:30 UTC the `maintenance_mode` flag in `common.feature_flags` was toggled on, mid-deploy of an unrelated schema-stamping commit. Every Azure SWA deploy from that point forward failed with "Web app warm up timed out" — 13 consecutive deploy failures spanning ~14 hours, including a /transparency page, a calibrator-path change, the Mac-runner pilot doc commit, the home-picker rewrite, the home admin link visibility fix, and several CHANGELOG-only commits.

Root cause: middleware returned `NextResponse(MAINTENANCE_HTML, { status: 503, headers: { 'Retry-After': '3600' } })` to anonymous traffic when `maintenance_mode` was on. This is the technically-correct HTTP semantic for "service unavailable, retry later." Azure SWA, however, warms up new revisions by probing the root URL with anonymous requests. A 5xx response is interpreted as an unhealthy revision; SWA retries probes for roughly ten minutes and then fails the deploy. The flag was sticky (no automatic recovery), so every push went into the same trap. The operator (Austin) did not see the failure because the `sb_unlock` admin cookie bypasses the maintenance gate, so all manual visits looked normal. The prior session's "yellow banner on /admin when maintenance_mode is on" change was specifically built to surface this confusion, but that change was caught in the same broken-deploy cycle and never shipped.

Decision: Maintenance middleware returns HTTP 200 with the maintenance HTML body, dropping the 503 status and the `Retry-After: 3600` header. The maintenance HTML already carries `<meta name="robots" content="noindex,nofollow">`, so 200 does not cause SEO issues for the consumer-facing site (which is itself passcode-gated and not designed to be indexed).

Consequences:
- SWA deploys succeed regardless of maintenance state. Operators can toggle maintenance on/off freely without bricking the next push.
- The site loses the proper Retry-After signal during maintenance. In practice this signal was not respected by anything that mattered here: the PWA service worker is network-first for navigation and does not cache 503s; there are no automated monitors honoring Retry-After (Uptime Robot is paused per the 2026-04-23 change); and crawlers are excluded by the noindex meta tag.
- The "yellow banner on /admin when maintenance_mode is on" added in commit `27fd10f` remains the in-app indicator that maintenance is active, now accurately reflecting reality after deploys ship.
- An alternative considered and rejected: detecting SWA warmup probes by user-agent and bypassing maintenance only for them. Brittle (Azure does not document warmup user-agents), not worth the complexity given the noindex-driven safety of returning 200.
- Another alternative considered and rejected: a confirmation prompt on the admin maintenance toggle. This adds friction without addressing the root cause; the middleware fix removes the failure mode entirely.

## ADR-20260426-2 [shared][infra] Mac-runner workflow migration pattern
Date: 2026-04-26

Context: Mac-runner pilot (`mac-runner-pilot.yml`) had proven the runner-up-to-local-SQL chain end-to-end, but the pilot used a separate `local_db_inventory.py` script and `SQL_*` env vars to deliberately avoid colliding with the production `AZURE_SQL_*` vars. Migrating an actual production workflow surfaces a question the pilot deferred: how does the same Python script (built for Azure SQL) connect to the local container without source-code branching per runner? Three obstacles: (1) production scripts read `AZURE_SQL_*` env vars; the Mac runner has no such secrets defined and shouldn't, (2) Azure SQL uses a CA-signed cert so production scripts pin `TrustServerCertificate=no`, but the local container has only a self-signed cert and would fail under that pin, (3) Colima allocates 6 GiB total to the Linux VM, container limit ~5.78 GiB, but SQL Server's default `max server memory` is "all available" (2147483647) so a workload spike could OOM-kill the DB.

Decision:

1. **Workflow-level alias, not application-level abstraction.** Mac workflows source `/Users/schnapp/sql-server.env` and re-export `MSSQL_SA_PASSWORD` as `AZURE_SQL_PASSWORD`, plus literals `AZURE_SQL_SERVER=localhost,1433 / DATABASE=sports-modeling / USERNAME=sa`, all written into `$GITHUB_ENV`. The production script reads exactly the same env-var names on either runner and never knows which database it's hitting. Rejected alternative: defining a parallel `SQL_*` abstraction layer in `etl/db.py`. That would touch every production script and make the migration a multi-file refactor; the alias approach is one workflow YAML per migrated workflow.

2. **`TrustServerCertificate` becomes env-driven, default `"no"`.** Both `etl/db_inventory.py` and `etl/db.py:_build_conn_str` (when migrated next) read `AZURE_SQL_TRUST_CERT` with default `"no"`. Default-`no` preserves Azure SQL behavior bit-exact; the Mac workflow opts in with `AZURE_SQL_TRUST_CERT=yes`. Rejected alternative: provisioning a real trusted cert in the container. Higher complexity, ongoing cert rotation, and no security gain since the connection is over the local Docker bridge to `localhost`.

3. **`max server memory (MB) = 4500` on the container.** Set via `sp_configure` and persisted in the master DB inside the named volume `mssql-data` (survives container restart). Math: Colima 6 GiB total, container limit ~5.78 GiB, current SQL working set ~4 GiB; 4500 MB ≈ 4.39 GiB caps SQL's growth with ~1.4 GiB headroom for host processes. Picked 4500 over 5000 to leave room for sidecars and the Linux guest itself.

4. **`KeepAlive=true` on the launchd plist.** Hard crashes of the Runner.Listener no longer require manual `launchctl load`. Combined with `RunAtLoad`, the runner is fully self-healing across reboots and crashes.

Consequences:

- Migrating a read-only production workflow is now ~30 lines of YAML plus zero or one source-code line (the env-var read, only the first time per script). `db_inventory-mac.yml` proved this with side-by-side parity on all eight inventory checks (exact match on five, expected drift on `upp` and `dg` matching `/docs/PRIMER`'s drift list).
- Future write-path migrations get the same alias treatment plus whatever schema-specific care that workflow needs. The `etl/db.py:_build_conn_str` env-var read is the natural next change since most write-path scripts go through `get_engine`.
- Production scripts gain a one-character increase in surface area (`AZURE_SQL_TRUST_CERT` env-var read) per script that needs it. Default-`no` means VM behavior is unchanged.
- `etl/db_inventory.py` had a latent SQL bug (unqualified `sport_key` in a multi-table JOIN) that surfaced when the script first ran in months; fixed in the same session, see CHANGELOG `[shared][etl]` entry of 2026-04-26.
