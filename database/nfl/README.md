# NFL Database

**STATUS:** tables populated, not in active use. The `nfl.*` tables were created by the first successful run of `etl/nfl_etl.py` on 2026-04-21 and have not been actively worked on since. As of that first run: `nfl.games` (285 rows), `nfl.players` (24,376), `nfl.player_game_stats` (19,421), `nfl.snap_counts` (26,612), `nfl.ftn_charting` (47,316), `nfl.rosters_weekly` (46,820), `nfl.team_game_stats` (570). Schema will continue to evolve via `add_missing_columns()` on each subsequent scheduled run, but no downstream consumer queries these tables today.

## Purpose

The `nfl` schema will hold seven tables sourced from the `nflreadpy` package. Unlike NBA (hand-written DDL in multiple files) and MLB (DDL defined implicitly by dict keys in the loader functions), NFL's schema is inferred from the `nflreadpy` API responses by pandas `to_sql`. Column types are determined at table-creation time from the first dataframe the loader sees.

## Files

No DDL scripts exist. All schema creation and evolution happens inside `etl/nfl_etl.py`:
- `upsert()` creates missing tables via `df.to_sql(if_exists='replace')`
- `add_missing_columns()` ALTERs in new columns via a conservative type map when the API adds them between runs

Every table gets an implicit `created_at DATETIME2 NOT NULL DEFAULT GETUTCDATE()` audit column on first creation.

## Key Concepts

### Tables

| Table | Grain | Primary upsert key | Observed cardinality (as of 2026-04-21 first run) |
|-------|-------|-------------------|----------------------|
| `nfl.games` | One row per game | `game_id` | ~285 games per regular season + playoffs |
| `nfl.players` | One row per player | `gsis_id` | ~2500 active players across the league |
| `nfl.player_game_stats` | One row per player per week | `(player_gsis_id, season, week, season_type)` | ~50k rows per full season |
| `nfl.snap_counts` | One row per player per game (offense/defense/ST counts) | `(game_id, pfr_player_id)` | ~15k rows per full season |
| `nfl.ftn_charting` | One row per play, FTN Fantasy charting data | `(ftn_game_id, ftn_play_id)` | ~45k rows per full season |
| `nfl.rosters_weekly` | One row per player per team per week | `(season, week, team, gsis_id)` | ~55k rows per full season |
| `nfl.team_game_stats` | One row per team per week | `(season, week, season_type, team)` | ~570 rows per full season |

### Type inference rules

Because the schema is inferred, the actual column types depend on what pandas produces from each `nflreadpy` response. Known conservative mappings applied via `add_missing_columns()` for schema drift:

| Pandas dtype | SQL Server type |
|-------------|-----------------|
| `int64`, `Int64` | `BIGINT` |
| `int32`, `Int32` | `INT` |
| `float64`, `float32` | `FLOAT` |
| `bool` | `TINYINT` |
| `object` | `NVARCHAR(500)` |
| `datetime64[ns]`, `datetime64[us]` | `DATETIME2` |

Any column whose name contains "date" and has object dtype after explicit `.dt.date` conversion is typed as SQL `DATE`.

### Identifier systems

NFL uses multiple identifier systems simultaneously. Do not assume these are interchangeable:

- `gsis_id` — official NFL.com identifier. Canonical player key across `nfl.players`, `nfl.player_game_stats`, `nfl.rosters_weekly`. Format: `00-NNNNNNN`
- `pfr_player_id` — Pro Football Reference identifier. Used only in `nfl.snap_counts`
- `ftn_player_id` — FTN Fantasy identifier. Used only in `nfl.ftn_charting` if present
- `espn_id` — ESPN identifier. Typically stored on `nfl.players` but not used as a key
- `sleeper_id`, `yahoo_id`, `pfr_id` — various fantasy/reference identifiers stored on `nfl.players`

The cross-walk between these identifier systems is what `nfl.players` exists for. Queries that join `nfl.snap_counts` to `nfl.players` must go through `pfr_player_id` → gsis_id via a lookup.

### `nfl.games` identifier columns

Because NFL has more identifier overlap than NBA/MLB, the `games` table carries multiple game identifier systems: `game_id` (canonical nflverse), `gsis_id`, `pfr_game_id`, `pff_game_id`, `espn_id`, `ftn_id`. Queries should use `game_id` as the join key unless a specific external source is involved.

### `nfl.ftn_charting` timezone handling

`date_pulled` is stripped of timezone info before insert. SQL Server `DATETIME2` rejects tz-aware datetimes, and the type inference would have misinterpreted a tz-aware value. The stored values are effectively UTC with no offset indicator.

### Partial-load behavior

If a load fails mid-season (e.g. `ftn_charting` is temporarily unavailable), only that table is skipped. The other six tables still upsert their rows. `nfl_etl.py` exits with status 1 so monitoring catches the failure, but the database is left in a consistent state: every row that made it through is complete and correct.

## Invariants

- Tables are created by the ETL, not by hand-written DDL. Never add DDL scripts under `database/nfl/` or `etl/` that pre-create these tables
- `gsis_id` is the canonical player identifier for the NFL schema. Cross-system ID joins go through `nfl.players`
- Every table has a `created_at` audit column added on first creation. Do not drop it or try to populate it from the source data
- Upsert keys are per-table and must not change once a table exists. Changing a key format requires a manual table rebuild
- Do not rename columns once a table exists. The schema drift mechanism only adds columns, never renames or drops

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nfl][database]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether to add hand-written DDL that guarantees exact column types (e.g. `season_type` as CHAR(3)) rather than relying on pandas inference. Pro: type stability across cold-start runs. Con: duplicates logic that already exists in the API response
- Whether `nfl.games` needs a status column analogous to `nba.schedule.game_status` or `mlb.games.game_status`. `nflreadpy.load_schedules` returns completed game scores plus scheduled future games; the status distinction today is just "does `home_score` have a value."
- Whether to add a play-by-play table (`nfl.play_by_play` from `nflreadpy.load_pbp`). Not included in the current ETL; would roughly triple data volume
- Whether to namespace NFL-specific extensions to `common.*` (e.g. a future `common.player_line_patterns`-equivalent for NFL weekly props)
- Whether the `cachemode=off` setting in the ETL is a good long-term choice or whether a shared-volume cache on the self-hosted runner would cut `nflreadpy` download time on repeated runs
