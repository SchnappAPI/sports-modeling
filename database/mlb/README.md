# MLB Database

**STATUS:** 7 tables live (nightly-loaded reference, games, per-game box scores, season snapshots). 1 pitch-level table live (on-demand). 1 derived at-bat table live (materialized in-lockstep with pitch-level). 4 derived entities from ADR-0004 not yet implemented.

## Purpose

The `mlb` schema holds the current MLB dataset. Seven tables populate from the nightly ETL, two tables populate from the on-demand play-by-play loader (the pitch-level table and the derived at-bat table). All DDL for these 9 tables is owned by the two Python ETL scripts — there is no separate `.sql` migration file.

## Files

- `/etl/mlb_batting_stats_migration.sql` — one-time column migration for `mlb.batting_stats` (kept for reference; do not re-run)

Live DDL sources:

- `etl/mlb_etl.py` — implicit DDL via pandas `to_sql` for truncate-and-reload tables. Permanent tables' columns are defined by the row dict keys in each loader function
- `etl/mlb_play_by_play.py` — explicit `CREATE TABLE IF NOT EXISTS` and `ALTER COLUMN` statements in `DDL_CREATE`, `DDL_ALTER_DESCRIPTIONS`, `DDL_CREATE_AT_BATS`, `DDL_DROP_NAME_COLUMNS`, and `DDL_CREATE_AT_BATS_INDEXES`

## Key Concepts

### Tables that exist today

| Table | Purpose | Primary key | Load strategy |
|-------|---------|-------------|----------------|
| `mlb.teams` | Team reference | `team_id` | Truncate + reload |
| `mlb.players` | Player reference including bat_side and pitch_hand | `player_id` | Truncate + reload |
| `mlb.games` | One row per game (Final + today's scheduled) | `game_pk` | Upsert |
| `mlb.batting_stats` | Per-batter per-game box score | `batter_game_id` | Upsert |
| `mlb.pitching_stats` | Per-pitcher per-game box score | `pitcher_game_id` | Upsert |
| `mlb.player_season_batting` | Season cumulative batting snapshot | (no enforced PK; unique on `player_id`) | Truncate + reload |
| `mlb.pitcher_season_stats` | Season cumulative pitching snapshot | (no enforced PK; unique on `player_id`) | Truncate + reload |
| `mlb.play_by_play` | One row per pitch/baserunning event with Statcast metrics | `play_event_id` | Direct INSERT (pre-diffed) |
| `mlb.player_at_bats` | One row per completed at-bat, materialized from PBP | `at_bat_id` | Direct INSERT (pre-diffed, in-lockstep with PBP) |

### `mlb.teams`

Columns: `team_id`, `team_abbreviation`, `full_name`, `venue_id`.

### `mlb.players`

Columns: `player_id`, `player_name`, `team_id`, `position`, `bat_side`, `pitch_hand`.

`bat_side` codes: `L` | `R` | `S` (switch). `pitch_hand` codes: `L` | `R`.

No foreign key to `mlb.teams`. Historical box scores retain the team context via `batting_stats.team_id`, so `players.team_id` always reflects the current roster.

### `mlb.games`

One row per regular-season game. Final games plus today's scheduled games (see ADR-0011 below). Columns:

- Identity: `game_pk` (PK), `game_date`, `game_datetime`, `game_type` (`R`), `game_status` (`F` for Final, else MLB status code), `abstract_game_state`, `day_night`, `double_header`, `game_number`, `game_display` (`AWY@HME`), `venue_id`, `venue_name`
- Away team: `away_team_id`, `away_team_score`, `away_is_winner`, `away_pitcher_id`, `away_pitcher_name`, `away_pitcher_hand`
- Home team: `home_team_id`, `home_team_score`, `home_is_winner`, `home_pitcher_id`, `home_pitcher_name`, `home_pitcher_hand`
- Series metadata (currently NULL-filled): `is_tie`, `games_in_series`, `series_game_number`, `game_date_index`

### `mlb.batting_stats`

One row per batter per game. PK: `batter_game_id = '{player_id}-{game_pk}-{team_id}'`. Including `team_id` in the PK preserves trade history — a player who appeared for two teams in the same season has two distinct keys.

Full columns: `batter_game_id`, `game_pk`, `game_date`, `player_id`, `team_id`, `side` (`A`|`H`), `position`, `batting_order`, `games_played`, `plate_appearances`, `at_bats`, `runs`, `hits`, `doubles`, `triples`, `home_runs`, `total_bases`, `rbi`, `stolen_bases`, `walks`, `intentional_walks`, `strikeouts`, `hit_by_pitch`, `left_on_base`, `sac_bunts`, `sac_flies`, `fly_outs`, `ground_outs`, `air_outs`, `pop_outs`, `line_outs`, `batting_avg`, `obp`, `slg`, `ops`.

### `mlb.pitching_stats`

One row per pitcher per game. PK: `pitcher_game_id = '{player_id}-{game_pk}'`. No `team_id` in the PK because a pitcher does not switch teams mid-game.

Columns: `pitcher_game_id`, `game_pk`, `game_date`, `player_id`, `team_id`, `side`, `innings_pitched` (decimal; `.1` = 1/3, `.2` = 2/3), `hits_allowed`, `runs_allowed`, `earned_runs`, `walks`, `strikeouts`, `hr_allowed`, `era`, `pitches`, `strikes`, `note` (`SP` for starting pitcher, else NULL).

### `mlb.player_season_batting`

Full-season snapshot per player. Truncated and reloaded nightly. Columns:

`player_id`, `player_name`, `team_id`, `season_year`, `age`, `games_played`, `at_bats`, `plate_appearances`, `hits`, `doubles`, `triples`, `home_runs`, `runs`, `rbi`, `walks`, `intentional_walks`, `strikeouts`, `hit_by_pitch`, `stolen_bases`, `caught_stealing`, `stolen_base_pct`, `caught_stealing_pct`, `ground_into_double_play`, `total_bases`, `left_on_base`, `sac_bunts`, `sac_flies`, `ground_outs`, `air_outs`, `pitches_seen`, `batting_avg`, `obp`, `slg`, `ops`, `babip`, `ground_outs_to_air_outs`, `at_bats_per_hr`, `catchers_interference`.

### `mlb.pitcher_season_stats`

Full-season snapshot per pitcher. 58 columns. Covers core rate stats (`era`, `whip`, `strike_pct`, `win_pct`), per-9 metrics (`k_per_9`, `bb_per_9`, `h_per_9`, `runs_per_9`, `hr_per_9`), platoon-independent totals (`wins`, `losses`, `saves`, `blown_saves`, `holds`, `complete_games`, `shutouts`, `games_finished`), counts allowed (`hits_allowed`, `hr_allowed`, `walks`, `hit_by_pitch`, `stolen_bases_allowed`), and derived ratios (`strikeout_walk_ratio`, `pitches_per_inning`).

### `mlb.play_by_play`

One row per play event (pitch, pickoff attempt, stolen base) across all Final regular-season games loaded so far. PK: `play_event_id = '{game_pk}-{at_bat_number}-{play_event_index}'`.

Column families:

- Identity: `play_event_id`, `game_pk`, `game_date`, `at_bat_number`, `play_event_index`, `inning`, `is_top_inning`, `team_id`, `vs_team_id`, `away_team_id`, `home_team_id`, `venue_id`
- At-bat result (populated only on the last pitch of the at-bat): `result_event_type`, `result_description`, `result_rbi`, `result_is_out`, `at_bat_is_complete`, `at_bat_is_scoring_play`, `at_bat_has_out`, `at_bat_end_time`, `play_end_time`, `is_at_bat`, `is_plate_appearance`
- Matchup: `batter_id`, `batter_hand_code`, `batter_split`, `pitcher_id`, `pitcher_hand_code`, `pitcher_split`
- Event metadata: `play_id`, `play_event_type`, `is_pitch`, `is_base_running_play`, `pitch_number`, `pitch_call_code`, `pitch_type_code`, `play_event_description`
- Pitch outcome: `is_hit_into_play`, `is_strike`, `is_ball`, `is_out`, `runner_going`, `count_balls_strikes`, `count_outs`, `is_last_pitch`, `play_event_end_time`
- Statcast pitch data: `pitch_start_speed`, `pitch_end_speed`, `pitch_zone`, `strike_zone_top`, `strike_zone_bottom`
- Statcast hit data: `hit_launch_speed`, `hit_launch_angle`, `hit_total_distance`, `hit_trajectory`, `hit_hardness`, `hit_location`, `hit_probability`, `hit_bat_speed`, `home_run_ballparks`
- Audit: `created_at` (defaults to `GETUTCDATE()`)

### `mlb.player_at_bats`

One row per completed at-bat (the first of ADR-0004's derived entities to ship). Materialized from `mlb.play_by_play` using the filter `is_last_pitch = 1 AND result_event_type IS NOT NULL`. PK: `at_bat_id = '{game_pk}-{at_bat_number}'`.

Populated in-lockstep with `mlb.play_by_play` — after each PBP flush, the loader materializes at-bats for the games just written. The diff against existing rows runs against `mlb.player_at_bats.game_pk` (not PBP), so partial runs that landed PBP rows but not at-bat rows self-heal on the next invocation. Rebuild mode (`--rebuild-at-bats` in `mlb_play_by_play.py`) processes every game currently in PBP.

Columns: `at_bat_id`, `game_pk`, `game_date`, `at_bat_number`, `inning`, `is_top_inning`, `batter_id`, `pitcher_id`, `result_event_type`, `result_description`, `result_rbi`, `hit_launch_speed`, `hit_launch_angle`, `hit_total_distance`, `hit_trajectory`, `hit_hardness`, `hit_probability`, `hit_bat_speed`, `home_run_ballparks`, `away_team_id`, `home_team_id`, `created_at`.

IDs only — no denormalized batter or pitcher names. `mlb.players` is truncate-and-reload scoped to the current season, so roughly 30% of historical `pitcher_id`s and 20% of historical `batter_id`s would resolve to NULL if names were joined at write time. Web routes join `mlb.players` at read time instead; the table is under a thousand rows with a PK on `player_id`, so the join cost is negligible.

Indexes: `IX_player_at_bats_game_pk` (per-game web lookup), `IX_player_at_bats_batter` on `(batter_id, game_date)` (future Player Analysis access path).

### Tables from ADR-0004 that do not exist yet

Four of the nine visual-feeding entities from ADR-0004 still have no materialization:

- Batter context per game
- Batter projections per game
- Player trend and pattern stats
- Player platoon splits
- Career batter vs pitcher matchup

Player at-bat stats (the sixth missing entity at the time of ADR-0004) shipped 2026-04-21 as `mlb.player_at_bats`.

## Invariants

- `batter_game_id` includes `team_id` so mid-season trades create distinct rows. Do not change the PK format
- `pitcher_game_id` does **not** include `team_id` because pitchers do not switch teams mid-game
- `mlb.games` stores both Final games and today's scheduled (non-Final) games. The web strip depends on this
- Game PK is MLB's `game_pk` integer, not a synthetic key
- `mlb.play_by_play` result-related columns are NULL on all pitches except the last one of an at-bat (`is_last_pitch = 1`). Queries that need at-bat results must filter on that predicate
- Pitch-level Statcast stays internal to the ETL layer. Web reads only aggregate views of `mlb.play_by_play` (linescore) or the materialized `mlb.player_at_bats`, never raw pitch rows
- `mlb.player_at_bats` stores `batter_id` and `pitcher_id` only, never denormalized names. Names get joined from `mlb.players` at read time because `mlb.players` is current-season-scoped and denormalizing would leave ~30% of historical rows with NULL names
- `mlb.player_at_bats` has `at_bat_id = '{game_pk}-{at_bat_number}'` as PK. Do not change this format
- Materialization of `mlb.player_at_bats` runs in-lockstep with `mlb.play_by_play` writes. The at-bats diff runs separately against `mlb.player_at_bats` so partial runs self-heal

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][database]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether the 4 remaining ADR-0004 entities should be materialized as tables (pre-aggregated, keeps the runtime-no-aggregation invariant) or computed as SQL views (simpler, but violates ADR-0004)
- Whether the Azure Blob Statcast data (`schnappmlbdata`, ~4.17 GB Parquet, 2015-2026) should be ingested into `mlb.play_by_play` as a backfill, kept parallel for historical-only queries, or dropped once the nightly PBP ETL catches up
- Whether to add FK constraints across the 7 tables or leave them off. They were deliberately dropped when `teams` and `players` became truncate-reload, and re-adding them would require a different load strategy
