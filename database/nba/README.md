# NBA Database

**STATUS:** live.

## Purpose

The `nba` schema holds player, team, game, box score, lineup, and grade-input data. The `odds` schema holds FanDuel lines and event mappings that feed NBA grading. Grading outputs land in `common.daily_grades`. The personal pattern table `common.player_line_patterns` feeds the grading pipeline.

## Files

DDL is defined inline in the NBA ETL scripts (`etl/nba_etl.py`, `etl/odds_etl.py`, etc.). A current table inventory is produced via `etl/db_inventory.py`, triggered by `db_inventory.yml`. The workflow writes output to `/tmp/db_inventory_output.txt` on the VM; read it back with `shell_exec cat` through Schnapp Ops MCP.

## Key Concepts

### Shared rules (all schemas)

- Snake case everywhere. Every fact table has `created_at DATETIME2 DEFAULT GETUTCDATE()`
- `DELETE` not `TRUNCATE`. FK constraints block TRUNCATE
- Column is `minutes`, never `min`. `min` is reserved in SQL Server
- No `FullGame` period rows. Game totals are always `SUM` over quarter rows
- `BIT` columns must `CAST(col AS INT)` before `SUM()`. SQL Server rejects `SUM` directly on BIT

### NBA tables

| Table | Notes |
|-------|-------|
| `nba.schedule` | Canonical game list. All games regardless of status. `home_score` / `away_score` updated live by `nba_live.py`. Use this for any game-list query |
| `nba.games` | Completed games only. Populated when `game_date <= today` and `game_status = 3`. Box-score ETL's FK source |
| `nba.teams` | Hardcoded `STATIC_TEAMS` dict in ETL |
| `nba.players` | `player_id`, `player_name`, `team_id`, `team_tricode`, `roster_status` (1 = active), `position`. Position may be compound: G-F, F-G, C-F, F-C |
| `nba.daily_lineups` | Keyed by `player_name` + `team_tricode`. No `player_id` or `team_id`. `starter_status` in `{'Starter', 'Bench', 'Inactive'}`. Position values are full strings (PG, SG, SF, PF, C) for starters from official lineup JSON. Historical data preserved; DELETE runs only for games in the current poll cycle |
| `nba.player_box_score_stats` | PK `(game_id, player_id, period)`. `period` is `VARCHAR(2)`. Valid values: `'1Q'`, `'2Q'`, `'3Q'`, `'4Q'`, `'OT'`. `minutes` is DECIMAL. `fg3a` present |
| `nba.player_passing_stats` | `(player_id, game_date)`. Includes `potential_ast` |
| `nba.player_rebound_chances` | `(player_id, game_date)`. Includes `reb_chances` |

### `odds` tables (NBA-relevant)

| Table | Notes |
|-------|-------|
| `odds.event_game_map` | `event_id` → `game_id` + `game_date`. Written by `odds_etl.py` in upcoming mode |
| `odds.upcoming_events` | Today's events with `home_tricode`, `away_tricode`, `commence_time`, `game_id` |
| `odds.upcoming_player_props` | Today's FanDuel player prop lines. Key fields: `event_id`, `market_key`, `outcome_point`, `player_name`, `bookmaker_key`, `snap_ts`, `outcome_price`, `outcome_name`, `link VARCHAR(500)` |
| `odds.player_props` | Historical player prop lines |
| `odds.upcoming_game_lines` | Today's game lines (spread, total, moneyline) |
| `odds.player_map` | Maps odds player names to `nba.players.player_id` |

NBA odds backfill range complete Mar 24 - Apr 3, 2026 as of the 2026-04-02 reference point. The `mappings` mode has been run; 135 players remain unmapped. All are inactive or off current rosters. Not blocking grading and not a problem to fix.

### `common.daily_grades` (schema v3, migrated 2026-04-02)

Columns: `grade_id`, `grade_date`, `event_id`, `game_id`, `player_id`, `player_name`, `market_key`, `bookmaker_key`, `line_value`, `outcome_name VARCHAR(5)` (`'Over'` / `'Under'`), `over_price INT` (direction-appropriate price), `hit_rate_60`, `hit_rate_20`, `sample_size_60`, `sample_size_20`, `weighted_hit_rate`, `grade`, `trend_grade`, `momentum_grade`, `pattern_grade`, `matchup_grade`, `regression_grade`, `composite_grade`, `hit_rate_opp`, `sample_size_opp`, `created_at`.

UNIQUE key: `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value, outcome_name)`.

`grade_props.py` writes both Over and Under rows for standard markets. Alternate lines remain Over-only.

`getGrades` reads `dg.outcome_name` and `dg.over_price` directly from this table. There is no join to `odds` for prices. The removed `best_price` CTE attached Over prices to Under rows; do not reintroduce.

### `common.player_line_patterns`

PK `(player_id, market_key, line_value)`. Stores lag-1 transition probabilities per player-line.

Columns: `n`, `hr_overall`, `p_hit_after_hit`, `p_hit_after_miss`, `hit_momentum`, `miss_momentum`, `pattern_strength`, `is_momentum_player BIT`, `is_reversion_player BIT`, `is_bouncy_player BIT`, `last_updated`.

Rules:

- `MIN_GAMES = 10` to create a row
- `MIN_TRANSITION_OBS = 3` per state for `p_hit_after_hit` / `p_hit_after_miss`
- Updated nightly by `compute-patterns.yml` at 07:30 UTC
- Reference point: 27,765 rows on 2026-04-10
- Any aggregate of `is_*` columns must use `SUM(CAST(col AS INT))`

### Other `common` tables

- `common.user_codes`, `common.user_activations`, `common.demo_config` - passcode auth and demo mode (demo codes fix to 2026-03-30; live codes unrestricted)

### Connection and reliability

Server: `sports-modeling-server.database.windows.net`, DB `sports-modeling`, user `sqladmin`. Tier `GP_S_Gen5_2` Serverless. Auto-pauses; first connection after pause 20-60s cold start. Free offer applied so the auto-pause delay cannot be changed. Firewall allows `0.0.0.0 - 255.255.255.255` plus Allow Azure Services (required for GitHub Actions runners).

Connections use SQLAlchemy + pyodbc with ODBC Driver 18. ETL uses `fast_executemany=True`. Grading engine has its own engine instance with `fast_executemany=False` to prevent NVARCHAR(MAX) truncation. Retry logic: 3 attempts with 45s waits.

Uptime Robot pings `https://schnapp.bet/api/ping` every 30 min to keep the DB from cold-starting during active hours. `keepalive.yml` is dispatch-only.

## Invariants

Do not revert without an ADR.

- `nba.player_box_score_stats.period` is `VARCHAR(2)`. Only `'1Q'`, `'2Q'`, `'3Q'`, `'4Q'`, `'OT'`. Do not insert longer strings
- `nba.schedule` is the canonical game list. `nba.games` holds only finals
- `nba.games` filter uses `game_date <= today`, not `< today`. Today's finals must enter `nba.games` so FK allows same-day box-score writes
- `nba.daily_lineups` is keyed by `player_name` + `team_tricode`. Positions for starters are full strings (PG, SG, SF, PF, C). Historical rows preserved
- `common.daily_grades` UNIQUE key includes `outcome_name`
- `getGrades` reads `outcome_name` and `over_price` directly from `common.daily_grades`. Never join odds for prices
- `common.player_line_patterns` updated nightly by `compute-patterns.yml` at 07:30 UTC
- `BIT` columns require `CAST(col AS INT)` before `SUM()`
- `fast_executemany=True` in ETL, `False` in the grading engine. Do not unify
- FanDuel-only bookmaker. `bookmaker_key = 'fanduel'` is the only value expected in `odds.*`

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][database]`. Historical entries before the restructure are in the legacy root `/CHANGELOG.md`.

## Open Questions

- Whether the 135 unmapped inactive players need periodic cleanup or can stay indefinitely
- Whether pitch-level-equivalent MLB Statcast storage warrants a similar separation of runtime-queried vs internal-only tables for NBA (currently not needed)
