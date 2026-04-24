# NBA Database

**STATUS:** live.

## Purpose

The `nba` schema holds player, team, game, box score, lineup, and grade-input data. The `odds` schema holds FanDuel lines and event mappings that feed NBA grading. Grading outputs land in `common.daily_grades`. The personal pattern table `common.player_line_patterns` feeds the grading pipeline.

## Files

DDL is defined inline in the NBA ETL scripts (`etl/nba_etl.py`, `etl/odds_etl.py`, etc.) and in `grading/grade_props.py` (`ensure_tables`). A current table inventory is produced via `etl/db_inventory.py`, triggered by `db_inventory.yml`. The workflow writes output to `/tmp/db_inventory_output.txt` on the VM; read it back with `shell_exec cat` through Schnapp Ops MCP.

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
| `odds.game_lines` | Historical game lines (spread, total, moneyline). Read by `fetch_game_spreads` for blowout risk computation on backfill runs |
| `odds.player_map` | Maps odds player names to `nba.players.player_id` |

NBA odds backfill range complete Mar 24 - Apr 3, 2026 as of the 2026-04-02 reference point. The `mappings` mode has been run; 135 players remain unmapped. All are inactive or off current rosters. Not blocking grading and not a problem to fix.

### `common.daily_grades` (schema v3, migrated 2026-04-02)

Defined in `grading/grade_props.py:ensure_tables`. Full column list:

- Identity: `grade_id INT IDENTITY`, `grade_date DATE`, `event_id VARCHAR(50)`, `game_id VARCHAR(15) NULL`, `player_id BIGINT NULL`, `player_name NVARCHAR(100)`
- Market: `market_key VARCHAR(100)`, `bookmaker_key VARCHAR(50)`, `line_value DECIMAL(6,1)`, `outcome_name VARCHAR(5) DEFAULT 'Over'` (`'Over'` / `'Under'`), `over_price INT NULL` (direction-appropriate price, name kept for migration simplicity)
- Hit rates: `hit_rate_60 FLOAT`, `hit_rate_20 FLOAT`, `sample_size_60 INT`, `sample_size_20 INT`, `weighted_hit_rate FLOAT`, `grade FLOAT`
- Component grades: `trend_grade FLOAT`, `momentum_grade FLOAT`, `pattern_grade FLOAT`, `matchup_grade FLOAT`, `regression_grade FLOAT`, `composite_grade FLOAT`. Since ADR-20260423-1 only `momentum_grade`, `hit_rate_60` (scaled), and `pattern_grade` enter `composite_grade`; the others are stored as context only
- Opportunity grades (added 2026-04-22, ADR-0017): `opportunity_short_grade FLOAT`, `opportunity_long_grade FLOAT`, `opportunity_matchup_grade FLOAT`, `opportunity_streak_grade FLOAT`, `opportunity_volume_grade FLOAT` (threes only; NULL elsewhere), `opportunity_expected_grade FLOAT` (threes only; NULL elsewhere). See `/etl/nba/README.md` for definitions. Since ADR-20260423-1 these are stored as context only and are NOT in `composite_grade`
- Opponent: `hit_rate_opp FLOAT`, `sample_size_opp INT`
- Resolution: `outcome VARCHAR(5) NULL` (`'Won'` / `'Lost'` / `NULL`). Populated by `grade_props.py --mode outcomes` as a pure SQL `UPDATE` after games go Final (`nba.schedule.game_status = 3`)
- Audit: `created_at DATETIME2 DEFAULT GETUTCDATE()`

UNIQUE key: `uq_daily_grades_v3` on `(grade_date, event_id, player_id, market_key, bookmaker_key, line_value, outcome_name)`.

Writes follow a staging + `MERGE` pattern (temp table `#stage_grades`, then `MERGE common.daily_grades AS t USING #stage_grades AS s`). Source rows are deduplicated in Python before staging to avoid error 8672.

`grade_props.py` writes both Over and Under rows for standard markets. Alternate lines remain Over-only. Standard markets also bracket-expand (5 lines each side of the posted line at 1.0 increments); `drop_bracket_lines_covered_by_alts` removes overlaps before grading.

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

### `common.player_tier_lines` (added 2026-04-23, ADR-20260423-1)

PK `tier_id` IDENTITY. UNIQUE on `(grade_date, game_id, player_id, market_key)`. One row per player-market-game-date.

Columns: `composite_grade`, `kde_window INT` (15/30/82 = lookback games used), `blowout_dampened BIT`, `safe_line / safe_prob`, `value_line / value_prob`, `highrisk_line / highrisk_prob / highrisk_price`, `lotto_line / lotto_prob / lotto_price`, `created_at`.

Tier definitions (calibrated 2026-04-23):

- **Safe**: P(stat > line) >= 0.80 from KDE on grade-weighted game log
- **Value**: P >= 0.58, line above safe_line
- **High Risk**: P >= 0.28, market price >= +150 available within 0.5 of model line
- **Lotto**: P >= 0.07, market price >= +400 available within 0.5 of model line, composite_grade >= 50

KDE window: composite >= 80 uses last 15 games (player peaking), 50-79 uses last 30 games, < 50 uses full season. Normal dist fallback when n < 10. Reflection boundary at 0 prevents negative-stat probability mass.

Written by `grade_props.py` via `upsert_tier_lines` during all grading modes (upcoming, intraday, backfill). Over rows only. NULL tier values mean no qualifying market price exists at that threshold for this player-market.

Reference point: populated for all 174 historical dates by backfill run 2026-04-23. Current live state 2026-04-24: 191K rows across 2025-10-27 to 2026-04-23.

#### Tier effectiveness (backtest 2026-04-24)

Evaluated 176,346 tier rows against actual box score outcomes across 16 NBA markets.

| Tier | n | Actual hit % | Model predicted % | Design % | Calibration |
|------|---|--------------|-------------------|----------|-------------|
| Safe | 164,718 | 85.9% | 82.8% | 80% | Well-calibrated; slightly conservative |
| Value | 167,066 | 61.0% | 60.8% | 58% | Well-calibrated |
| High Risk | 67,479 | 19.8% | 31.5% | 28% | Overconfident by 12 points; breakeven at +348 is 27.8%, so -EV at current prices |
| Lotto | 49,248 | 6.4% | 12.6% | 7% | Hit rate on target; model probability overconfident; -EV at avg +1,189 |

Probability calibration is reliable in the 50-90% range (e.g., 80-90% predicted → 86.7% actual) and degrades at both extremes. Below 50% the model overestimates by 10-20 points. Above 90% the model predicts 99%+ but actual hit rate is 69.5% (the same overconfidence-at-extremes pattern the composite grade exhibited before the formula change).

Per-market variation: combo markets (PRA, PR, PA, RA and their alternates) outperform the Safe target by 9-11 points (89-92% actual). Points and rebounds land on target (85-86% actual). Three-point markets underperform Safe at 74-76% because the discrete low-count distribution is smoothed more than KDE can reliably represent.

Blowout dampening improves calibration when applied. Dampened rows hit Safe at 91.4% vs 88.7% not-dampened; Value at 68.0% vs 62.9%.

Brier scores: Safe 0.127, Value 0.239, High Risk 0.174, Lotto 0.065.

Coverage: 97.1% of `common.daily_grades` player-market-games produce a tier row. The 2.9% missing are players below the `KDE_MIN_GAMES = 10` threshold with fewer than 3 observations in their game log.

### Other `common` tables

- `common.user_codes`, `common.user_activations`, `common.demo_config` - passcode auth and demo mode (demo codes fix to 2026-03-30; live codes unrestricted)

### Connection and reliability

Server: `sports-modeling-server.database.windows.net`, DB `sports-modeling`, user `sqladmin`. Tier `GP_S_Gen5_2` Serverless. Auto-pause delay is configurable and was set to 60 minutes on 2026-04-24 (any prior note claiming the Free offer locks this setting is stale — it is configurable via the portal). First connection after pause 20-60s cold start. Firewall allows `0.0.0.0 - 255.255.255.255` plus Allow Azure Services (required for GitHub Actions runners).

Connections use SQLAlchemy + pyodbc with ODBC Driver 18. ETL uses `fast_executemany=True`. Grading engine has its own engine instance with `fast_executemany=False` to prevent NVARCHAR(MAX) truncation. Retry logic in `grading/grade_props.py:get_engine`: 3 attempts with 60s waits.

Uptime Robot previously pinged `https://schnapp.bet/api/ping` every 30 min to keep the DB warm during active hours, but was paused 2026-04-23 to let auto-pause actually take effect and cut continuous-compute billing. `keepalive.yml` is dispatch-only and should not be rescheduled without making a deliberate decision to reverse the tradeoff.

## Invariants

Do not revert without an ADR.

- `nba.player_box_score_stats.period` is `VARCHAR(2)`. Only `'1Q'`, `'2Q'`, `'3Q'`, `'4Q'`, `'OT'`. Do not insert longer strings
- `nba.schedule` is the canonical game list. `nba.games` holds only finals
- `nba.games` filter uses `game_date <= today`, not `< today`. Today's finals must enter `nba.games` so FK allows same-day box-score writes
- `nba.daily_lineups` is keyed by `player_name` + `team_tricode`. Positions for starters are full strings (PG, SG, SF, PF, C). Historical rows preserved
- `common.daily_grades` UNIQUE key includes `outcome_name`. `outcome` column is populated by `grade_props.py --mode outcomes`, not by the upcoming/intraday grading paths
- `common.daily_grades.composite_grade` follows the 40/40/20 formula (ADR-20260423-1). The matchup, regression, trend, and opportunity columns are stored but not in the composite mean
- `getGrades` reads `outcome_name` and `over_price` directly from `common.daily_grades`. Never join odds for prices
- `common.player_tier_lines` UNIQUE key is `(grade_date, game_id, player_id, market_key)`. Over-side only. Written in lockstep with `common.daily_grades` inside `grade_props.py`
- `common.player_line_patterns` updated nightly by `compute-patterns.yml` at 07:30 UTC
- `BIT` columns require `CAST(col AS INT)` before `SUM()`
- `fast_executemany=True` in ETL, `False` in the grading engine. Do not unify
- FanDuel-only bookmaker. `bookmaker_key = 'fanduel'` is the only value expected in `odds.*`

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][database]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- High Risk and Lotto tiers are -EV at their current price floors. Either raise `TIER_HIGHRISK_MIN_PRICE` and `TIER_LOTTO_MIN_PRICE` (mechanical but loses some rows), or apply an isotonic calibration pass on model probability before tier cutoff selection (more correct, more work). Deferred.
- Three-point markets calibrate worse than points/rebounds/assists. A discrete Poisson or negative-binomial fit may be more appropriate than KDE for low-count stats.
- Whether the 135 unmapped inactive players need periodic cleanup or can stay indefinitely.
- Whether pitch-level-equivalent MLB Statcast storage warrants a similar separation of runtime-queried vs internal-only tables for NBA (currently not needed).
