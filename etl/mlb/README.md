# MLB ETL

**STATUS:** partially live. 7 tables loaded nightly from MLB Stats API. 1 pitch-level table populated on demand, plus 1 derived at-bat table materialized in-lockstep. 4 derived entities from ADR-0004 remain.

## Purpose

Ingest MLB data from the MLB Stats API into Azure SQL. Current scope covers the raw and snapshot tables plus the first materialized at-bat table that downstream derived entities will be built on top of. The 9-entity vision in ADR-0004 describes the end state; as of 2026-04-21 five of those nine are in place (the eight originally shipped plus `mlb.player_at_bats`).

## Files

Code:

- `etl/mlb_etl.py` — main nightly ETL. Loads 7 tables in dependency order. Entry point for `mlb-etl.yml`
- `etl/mlb_play_by_play.py` — on-demand pitch-level loader for `mlb.play_by_play` + in-lockstep materializer for `mlb.player_at_bats`. Entry point for `mlb-pbp-etl.yml`
- `etl/mlb_batting_stats_migration.sql` — one-time DDL migration script for the batting stats table shape

Workflows:

- `.github/workflows/mlb-etl.yml` — nightly at 09:00 UTC (03:00 CST / 04:00 CDT). Manual dispatch accepts `backfill=true` to also load 2023-2025 seasons
- `.github/workflows/mlb-pbp-etl.yml` — workflow_dispatch only. Inputs: `batch` (default 50 games per run), `seasons` (default `2026`), `rebuild_at_bats` (default false; when true, skips PBP fetch and runs only the at-bats materializer against existing PBP data)

Local Power Query archive will be copied to `etl/mlb/_legacy_powerquery/` when available — the source file lives on the corporate machine (`mlbStatQueries.docx`) and has not been committed yet.

## Key Concepts

### Source endpoint

Nearly every read comes from one URL:

```
https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics
```

Returns box score, play-by-play, pitch metrics, hit data, and game metadata in a single response. The only other endpoints hit are `teams`, `sports_players`, `stats` (season totals), and `schedule`.

### Nightly ETL load order

`etl/mlb_etl.py` runs 7 loads in dependency order. Teams and players use truncate-and-reload; games and box scores use upsert; season snapshots use truncate-and-reload.

1. **`mlb.teams`** — current-season team reference. Truncate + reload
2. **Today's schedule** — `statsapi.schedule` for today with `sportId=1`, regular season only. Upserts into `mlb.games` regardless of status so the strip shows start times before games go Final
3. **`mlb.players`** — `sports_players` endpoint for each season in scope. Truncate + reload. Dedupes by `player_id` across seasons
4. **`mlb.games` + `mlb.batting_stats` + `mlb.pitching_stats`** — single pass through the schedule. For each Final regular-season game not already in `mlb.batting_stats`, fetches `/withMetrics`, builds rows for all three tables, flushes in batches of 100. Each flush MERGEs via `etl/db.py:upsert`
5. **`mlb.player_season_batting`** — `stats` endpoint with `group=hitting`, `stats=season`. Truncate + reload. Skipped if current season has not started
6. **`mlb.pitcher_season_stats`** — `stats` endpoint with `group=pitching`, `stats=season`. Truncate + reload. Skipped if current season has not started

### Schedule fetch is month-by-month

`fetch_schedule_months(season)` walks March through October one month at a time rather than a single full-season call. Prevents 503 errors on wide date ranges.

Filter on result: `game_type == 'R'` (regular season) **and** `status == 'Final'`.

### Incremental game loading

`load_games_and_box_scores` computes the delta against the destination table:

```sql
SELECT DISTINCT game_pk FROM mlb.batting_stats
```

Only games not in that set are processed. This is the only incremental checkpoint; all three tables (`games`, `batting_stats`, `pitching_stats`) are treated as complete-or-incomplete together for a given `game_pk`. If `batting_stats` has the game, all three are assumed loaded.

### Backfill flag

`python etl/mlb_etl.py --backfill` adds seasons 2023, 2024, 2025 to the box-score and player loops. Without the flag only the current season runs. Backfill is safe to rerun — incremental logic skips games already loaded.

### Play-by-play loader

`etl/mlb_play_by_play.py` is separate from the nightly ETL because it's heavy (hundreds of rows per game, Statcast metrics on every pitch). It is never run on a cron, only via `workflow_dispatch`.

Processing:

1. Read desired `game_pk` set from `mlb.games` where `game_status = 'F' AND game_type = 'R'` and year is in the requested seasons list
2. Read existing `game_pk` set from `mlb.play_by_play`
3. Diff. Process the oldest N new games (default 50)
4. For each new game, parse `playEvents` from `/withMetrics`. One row per pitch/pickoff/baserunning event
5. Flush every 5 games (~3000 rows per write)
6. After each flush, run the at-bats materializer against the games just written

Write strategy is **direct INSERT via `to_sql(if_exists='append')`**, not the MERGE staging pattern. Because every game in the batch is pre-diffed and guaranteed new, there is nothing to update — direct INSERT is ~10x faster. Uses `get_engine()` with `fast_executemany=True` and explicit `INSERT_DTYPES` for VARCHAR widths (pandas would otherwise infer widths from the first row and right-truncate on longer later rows).

### Play-by-play primary key

```
play_event_id = f"{game_pk}-{at_bat_number}-{play_event_index}"
```

Unique across the full table.

### Play-by-play table self-heals description widths

On each run, `ensure_table()` runs a DDL that widens `result_description` and `play_event_description` to `VARCHAR(1000)` if the existing column is narrower. Keeps the schema in sync without a separate migration step.

### At-bats materializer

`load_player_at_bats_for_games(engine, game_pks)` materializes `mlb.player_at_bats` from rows that were just written to `mlb.play_by_play`. Runs inline after each PBP flush, sourced straight from PBP via a SQL SELECT with the filter `is_last_pitch = 1 AND result_event_type IS NOT NULL`.

Diffs against `mlb.player_at_bats.game_pk` (not `mlb.play_by_play.game_pk`), so a partial run that landed PBP rows but failed to land at-bat rows gets the missing games on the next invocation. Same self-healing applies to manual retries after partial failures.

The materializer stores IDs only — no denormalized batter or pitcher names. `mlb.players` is current-season-scoped, so denormalizing names at write time would leave ~30% of historical rows with NULL. Web routes join `mlb.players` at read time instead.

### Rebuild at-bats mode

`python etl/mlb_play_by_play.py --rebuild-at-bats` (or `rebuild_at_bats=true` in the workflow dispatch) skips the PBP fetch loop entirely and runs the materializer against every `game_pk` currently in `mlb.play_by_play`. Used for the initial backfill after the table was introduced, and any future schema change that needs a full rebuild.

Does not delete existing rows. For a full rebuild (rather than gap-fill), `DELETE FROM mlb.player_at_bats` first; the self-heal logic will then re-insert everything.

### Play-by-play table self-heals name columns

`DDL_DROP_NAME_COLUMNS` in `ensure_table()` idempotently drops `batter_name` and `pitcher_name` from `mlb.player_at_bats` if present. These columns were part of an initial denormalized design and got removed the same day (2026-04-21) after the rebuild showed 20-32% NULL rates against historical data.

### Timezone

`mlb_etl.py` uses `date.today()` for "today's schedule" which is naive local time on the runner (UTC on the VM). `mlb-etl.yml` cron fires at 09:00 UTC = 03:00 CST / 04:00 CDT, early enough that yesterday's games are always Final.

## Invariants

- 7 tables in the nightly ETL. Adding or removing one requires updating both `etl/mlb_etl.py` load order and `database/mlb/README.md`
- `fast_executemany=True` for all MLB loads. `get_engine_slow` is not used here (it is only used by the NBA play-by-play grading engine)
- Incremental checkpoint for games is `mlb.batting_stats`. Do not change this without understanding that all three box score tables fall together
- `mlb_play_by_play.py` uses direct INSERT, not MERGE. Relies on pre-diffing against `mlb.play_by_play.game_pk`. If that invariant changes, the write strategy must change too
- `fetch_schedule_months` stays month-by-month. Single full-season fetches return 503s
- Today's schedule upsert runs **before** the player load so the strip is populated even on cold-start days
- Pitch-level play-by-play stays internal to the ETL for now. Web only reads aggregated views of it (`mlb-linescore`) or the materialized `mlb.player_at_bats`
- `mlb.player_at_bats` materialization runs inline in the same script as PBP writes. The at-bats diff runs against `mlb.player_at_bats` (not PBP), so partial runs self-heal. Do not fold the materializer into a separate workflow without preserving the self-heal property
- `mlb.player_at_bats` stores IDs only. Names get joined at read time. Do not re-add `batter_name` / `pitcher_name` columns — they were removed 2026-04-21 for 20-32% NULL rates on historical data

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][etl]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether the 4 remaining derived entities from ADR-0004 (batter context per game, batter projections, player trend/pattern stats, platoon splits, career batter-vs-pitcher) should be materialized as separate tables or computed in SQL views
- Whether to fold `mlb_play_by_play.py` into the nightly schedule under a separate cron vs. leave it as on-demand. Cost trade-off depends on runner-minutes load
- Whether the local Excel Statcast exports (`mlbSavantStatcast-*.xlsx`) should be migrated to the same shape as `mlb.play_by_play` or kept as a parallel Blob-stored dataset. See `/docs/ROADMAP.md`
- Whether to add a third workflow for intraday score refresh (today's games going Final during the day are not reflected in `mlb.games` until the next nightly run)
