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
