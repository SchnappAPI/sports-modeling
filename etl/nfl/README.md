# NFL ETL

**STATUS:** code complete, not yet automated. `etl/nfl_etl.py` is a full 7-table ETL script ready to run. No workflow has been created and the script has never been executed against production, so no `nfl.*` tables exist in the database yet.

## Purpose

Ingest NFL data from `nflreadpy` (the nflverse Python wrapper) into Azure SQL. Seven tables cover schedule, players, weekly stats, snap counts, FTN charting, weekly rosters, and team game stats. The design philosophy differs from NBA and MLB: rather than hand-writing DDL for each table, the script infers schema from the API response on first run and self-heals via `ALTER TABLE ... ADD COLUMN` when new columns appear in later responses.

## Files

- `etl/nfl_etl.py` — complete 14 KB ETL script. Seven loader functions, one upsert helper, one clean_df helper

No workflow exists. To run this in production, `nfl-etl.yml` still needs to be written and committed to `.github/workflows/`.

No sub-scripts exist for play-by-play, live scores, or intraday updates. All data comes from the one `nflreadpy` package.

## Key Concepts

### Seven tables loaded

| Table | Source | Upsert key |
|-------|--------|-------------|
| `nfl.games` | `nflreadpy.load_schedules(season)` | `game_id` |
| `nfl.players` | `nflreadpy.load_players()` | `gsis_id` |
| `nfl.player_game_stats` | `nflreadpy.load_player_stats(season, summary_level='week')` | `(player_gsis_id, season, week, season_type)` |
| `nfl.snap_counts` | `nflreadpy.load_snap_counts(season)` | `(game_id, pfr_player_id)` |
| `nfl.ftn_charting` | `nflreadpy.load_ftn_charting(season)` | `(ftn_game_id, ftn_play_id)` |
| `nfl.rosters_weekly` | `nflreadpy.load_rosters_weekly(season)` | `(season, week, team, gsis_id)` |
| `nfl.team_game_stats` | `nflreadpy.load_team_stats(season, summary_level='week')` | `(season, week, season_type, team)` |

### Schema-from-data, not hand-written DDL

`nfl_etl.py:upsert()` checks `table_exists` first. If the table is missing, it runs `df.to_sql(if_exists='replace')` which lets pandas infer SQL column types from the dataframe, then ALTERs the table to add a `created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()` audit column. This is the only time `created_at` is touched; re-runs never overwrite it.

If the table exists, the script runs `add_missing_columns()` which diffs the dataframe columns against the live table and ALTERs in any new columns via a conservative type map (object → NVARCHAR(500), int64 → BIGINT, float64 → FLOAT, bool → TINYINT, datetime → DATETIME2). Unknown dtypes fall back to NVARCHAR(500). Then MERGE runs normally.

### `clean_df()` global cleanup

Every dataframe passes through `clean_df()` before any table-specific rename or drop:
- Empty strings replaced with None
- Boolean-like object columns (`{True, False}` or `{'True', 'False'}`) mapped to 0/1 integers
- Object columns where ≥90% of non-null values parse as numeric get coerced via `pd.to_numeric`

The 90% threshold prevents ID-like columns with numeric and alphanumeric IDs from being accidentally coerced. The column has to be mostly numeric to trigger conversion.

### Per-table pre-clean_df steps

Each loader function does minimal table-specific work before calling `clean_df()`:
- `load_games`: renames 16 columns from the API's short names (gameday, gsis, pfr, pff, espn, ftn, away_rest, home_rest, div_game, temp, wind, away_qb_id, home_qb_id, stadium), converts game_date to a date
- `load_players`: renames height/weight/ngs_status, drops rows with empty `gsis_id`
- `load_player_game_stats`: drops `headshot_url`, renames `player_id` to `player_gsis_id`
- `load_snap_counts`: renames `player` to `player_name`
- `load_ftn_charting`: renames `nflverse_game_id` to `game_id`, strips timezone from `date_pulled` (SQL Server rejects tz-aware datetimes)
- `load_rosters_weekly`: renames height/weight, parses `birth_date`, drops empty `gsis_id` rows
- `load_team_game_stats`: drops `game_id` (not part of the schema, a quirk of the API response)

### Season selection

`current_nfl_season()` returns the current year if month >= June, else last year. This accounts for the NFL league year running Feb-Feb but fantasy/stat seasons aligning with the calendar year from September. `--season YYYY` overrides.

### Local engine factory, not shared

`nfl_etl.py` defines its own `get_engine()` rather than importing from `etl/db.py`. Functionally equivalent (`fast_executemany=True`, same connection string shape), but deliberately duplicated to keep the script self-contained. Worth reconciling with `etl/_shared/` when the NFL build starts in earnest.

### Cache-off in CI

`update_config(cache_mode='off')` runs at the top of `main()`. Without this, `nflreadpy` attempts to cache downloads to disk; GitHub Actions runners have no persistent filesystem between jobs, so cache writes would fail silently and add latency.

### Fail-soft per table

Each table load runs inside a `run(name, fn)` wrapper that catches all exceptions, logs them, and continues. If any table fails, the script exits with status 1 at the end but still attempts every other table. This is intentional because `ftn_charting` in particular is third-party data and has had availability gaps.

## Invariants

- Do not hand-write DDL for the `nfl.*` tables. The schema comes from the `nflreadpy` API response. Adding a column means the next run picks it up automatically
- The schema inference only runs on empty tables. Once data exists, the schema drifts only via `ALTER TABLE ADD COLUMN`. Dropping or renaming a column requires manual intervention
- `gsis_id` is the canonical player identifier across all NFL tables. Do not assume `pfr_player_id` or `ftn_player_id` are interchangeable — they are separate identifier systems with partial overlap
- `nfl.players` drops rows with empty `gsis_id` before upsert. Do not change this; it would break the PK
- `clean_df()` runs on every dataframe. Do not bypass it per-table
- All loads are idempotent via MERGE with per-table upsert keys. Re-running is always safe
- `nfl_etl.py` is the only script in the NFL ETL today. Do not fold it into `nba_etl.py` or `mlb_etl.py`; the data model is different enough that a separate process makes sense

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nfl][etl]`. Historical entries before the restructure are in the legacy root `/CHANGELOG.md`.

## Open Questions

- **No workflow exists.** To move NFL to production, create `.github/workflows/nfl-etl.yml` with schedule + workflow_dispatch. Tuesday morning after the last Monday Night Football game would line up with the `nflreadpy` update cycle
- Whether NFL needs pattern-grading infrastructure analogous to `common.player_line_patterns` for NBA. NFL props are weekly (not per-game daily), so sample sizes per line are much smaller
- Whether `nfl.player_game_stats` is granular enough for the product vision or whether play-by-play (`nflreadpy.load_pbp`) is also needed. The 7 tables here cover the standard fantasy/prop research surface; pitch-level-equivalent data is a separate add-on
- Whether the local `get_engine()` should be replaced with an import from `etl/db.py` to align with NBA and MLB. Low priority until the second NFL script exists
- Whether to add `nfl-odds-etl.py` (analogous to `odds_etl.py` for NBA) or to extend the existing Odds API loader with NFL sport keys. `odds_etl.py` does mention NFL — that path has not been verified
- Web surface is not started. See `/web/nfl/README.md`
