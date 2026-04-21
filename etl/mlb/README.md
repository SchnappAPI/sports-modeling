# MLB ETL

**STATUS:** partially live. 7 tables loaded nightly from MLB Stats API. 1 pitch-level table populated on demand. 2 derived entities from ADR-0004 remain to be built on top of what exists.

## Purpose

Ingest MLB data from the MLB Stats API into Azure SQL. Current scope covers the raw and snapshot tables that downstream derived entities will be built on top of. The 9-entity vision in ADR-0004 describes the end state, not the current state.

## Files

Code:

- `etl/mlb_etl.py` — main nightly ETL. Loads 7 tables in dependency order. Entry point for `mlb-etl.yml`
- `etl/mlb_play_by_play.py` — on-demand pitch-level loader for `mlb.play_by_play`. Entry point for `mlb-pbp-etl.yml`
- `etl/mlb_batting_stats_migration.sql` — one-time DDL migration script for the batting stats table shape

Workflows:

- `.github/workflows/mlb-etl.yml` — nightly at 09:00 UTC (03:00 CST / 04:00 CDT). Manual dispatch accepts `backfill=true` to also load 2023-2025 seasons
- `.github/workflows/mlb-pbp-etl.yml` — workflow_dispatch only. Inputs: `batch` (default 50 games per run), `seasons` (default `2026`)

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

Write strategy is **direct INSERT via `to_sql(if_exists='append')`**, not the MERGE staging pattern. Because every game in the batch is pre-diffed and guaranteed new, there is nothing to update — direct INSERT is ~10x faster. Uses `get_engine()` with `fast_executemany=True` and explicit `INSERT_DTYPES` for VARCHAR widths (pandas would otherwise infer widths from the first row and right-truncate on longer later rows).

### Play-by-play primary key

```
play_event_id = f"{game_pk}-{at_bat_number}-{play_event_index}"
```

Unique across the full table.

### Play-by-play table self-heals description widths

On each run, `ensure_table()` runs a DDL that widens `result_description` and `play_event_description` to `VARCHAR(1000)` if the existing column is narrower. Keeps the schema in sync without a separate migration step.

### Timezone

`mlb_etl.py` uses `date.today()` for "today's schedule" which is naive local time on the runner (UTC on the VM). `mlb-etl.yml` cron fires at 09:00 UTC = 03:00 CST / 04:00 CDT, early enough that yesterday's games are always Final.

## Invariants

- 7 tables in the nightly ETL. Adding or removing one requires updating both `etl/mlb_etl.py` load order and `database/mlb/README.md`
- `fast_executemany=True` for all MLB loads. `get_engine_slow` is not used here (it is only used by the NBA play-by-play grading engine)
- Incremental checkpoint for games is `mlb.batting_stats`. Do not change this without understanding that all three box score tables fall together
- `mlb_play_by_play.py` uses direct INSERT, not MERGE. Relies on pre-diffing against `mlb.play_by_play.game_pk`. If that invariant changes, the write strategy must change too
- `fetch_schedule_months` stays month-by-month. Single full-season fetches return 503s
- Today's schedule upsert runs **before** the player load so the strip is populated even on cold-start days
- Pitch-level play-by-play stays internal to the ETL for now. Web only reads aggregated views of it (`mlb-linescore`, `mlb-atbats`)

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][etl]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether the 2 remaining derived entities from ADR-0004 (batter-context-per-game, batter-projections, player trend/pattern stats, platoon splits, career batter-vs-pitcher) should be materialized as separate tables or computed in SQL views. Currently none exist
- Whether to fold `mlb_play_by_play.py` into the nightly schedule under a separate cron vs. leave it as on-demand. Cost trade-off depends on runner-minutes load
- Whether the local Excel Statcast exports (`mlbSavantStatcast-*.xlsx`) should be migrated to the same shape as `mlb.play_by_play` or kept as a parallel Blob-stored dataset. See `/docs/ROADMAP.md`
- Whether to add a third workflow for intraday score refresh (today's games going Final during the day are not reflected in `mlb.games` until the next nightly run)
