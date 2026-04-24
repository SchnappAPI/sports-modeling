# NBA ETL

**STATUS:** live.

## Purpose

Ingests NBA data from `stats.nba.com`, `cdn.nba.com`, and The Odds API. Produces box scores, rosters, game logs, odds lines, lineups, and grades. All production ingestion runs in Python on the self-hosted GitHub Actions runner (`schnapp-runner-2`) and writes to Azure SQL.

## Files

ETL code lives flat in `/etl/` per ADR-0002. Grading code lives in `/grading/` as a separate top-level folder.

ETL (`/etl/`):

- `etl/nba_etl.py` - main ETL entry point. Box scores, PT stats, schedule, rosters, players
- `etl/nba_live.py` - today-only schedule refresh from the public CDN scoreboard. Does not write live per-player rows to the DB
- `etl/odds_etl.py` - odds ingestion, shared with MLB, dispatches by sport
- `etl/lineup_poll.py` - two-stage lineup polling
- `etl/runner.py` - Flask live-data service on the VM, port 5000
- `etl/compute_patterns.py` - nightly recomputation of `common.player_line_patterns`
- `etl/db.py` - shared upsert via staging + `MERGE`
- `etl/db_inventory.py` - prints table inventory, used by `db_inventory.yml`
- `etl/game_day_gate.py`, `etl/gate_check.py` - cron gating helpers
- `etl/lineup_cleanup.py`, `etl/nba_clear.py` - dispatch-only maintenance
- `etl/signal_backtest.py`, `etl/streak_analysis.py` - analysis scripts
- `etl/seed_user_codes.py`, `etl/migrate_common_teams.py` - one-shot migrations

Grading (`/grading/`):

- `grading/grade_props.py` - grading entry point. Modes: `upcoming`, `intraday`, `backfill`, `outcomes`
- `grading/migrate_grades_v2.py` - legacy migration helper

Workflows under `.github/workflows/` orchestrate these scripts on schedule. The NBA-relevant ones are listed in the Invariants section below.

## Key Concepts

### Data source split

- **`stats.nba.com`** requires the Webshare rotating residential proxy (`NBA_PROXY_URL` secret). All `nba_etl.py` endpoints route through the proxy. PT stats (`leaguedashptstats`) is the one exception and does not use the proxy.
- **`cdn.nba.com`** is public and does not use the proxy. `nba_live.py` reads `todaysScoreboard_00.json` from the CDN. The Flask runner reads `boxscore_{gameId}.json` from the CDN on demand. Live per-player data is never persisted to the DB.

### Box score ingestion

`nba_etl.py` uses `playergamelogs`, with five calls per run (one per period: 1Q, 2Q, 3Q, 4Q, OT). Overtime uses `Period=""` + `GameSegment=Overtime`. The `fg3a` field is included. Teams come from a hardcoded `STATIC_TEAMS` dict to eliminate an HTTP dependency that previously failed behind the proxy. Players come from `playerindex` via the proxy.

### Grading pipeline

Order of operations across workflows:

1. `odds-etl.yml` fetches FanDuel lines
2. `grading.yml` runs on `workflow_run` after `odds-etl.yml` succeeds; calls `python grading/grade_props.py --mode upcoming`
3. `compute-grade-outcomes.yml` resolves Won/Lost after games finish; calls `grade_props.py --mode outcomes`
4. `compute-patterns.yml` runs nightly at 07:30 UTC to refresh `common.player_line_patterns` from resolved outcomes

`grade_props.py` has four modes:

- `upcoming` - grade today's standard + alternate lines, Over and Under. Also writes `common.player_tier_lines` for today's player-markets via `upsert_tier_lines`.
- `intraday` - re-grade only player-market pairs whose posted line has moved since last grade (used by `refresh-data.yml`). Also refreshes affected tier rows.
- `backfill` - grade historical dates in batches; re-dispatches itself until `nothing to do`. Accepts `--force` to remove the `NOT EXISTS` skip filter so already-graded dates re-grade in place via the MERGE UPDATE path. Used when a new grade column is added and existing rows must be refilled (ADR-0017 opportunity backfill, ADR-20260422-1). Also rewrites tier rows for the re-graded dates.
- `outcomes` - pure SQL `UPDATE` to set `outcome` = `'Won'` / `'Lost'` on resolved rows

`_common_grade_data` returns a 7-tuple: `(history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns, opp_df)`. `patterns` is the personal pattern table keyed by `(player_id, market_key, line_value)`; `opp_df` is the per-game opportunity frame used by the opportunity grades. Never revert to a 5-tuple or 6-tuple form.

`grade_props_for_date` returns a `(grade_rows, tier_rows)` tuple. `run_upcoming`, `run_intraday`, and `run_backfill` all call `upsert_grades` and `upsert_tier_lines` in sequence.

### Grade components (actual code, not description)

All scores are 0-100 floats. Constants live at the top of `grade_props.py`:

- `weighted_hit_rate`: `0.60 * hit_rate_20 + 0.40 * hit_rate_60` when `sample_size_20 >= MIN_SAMPLE (5)`, else `hit_rate_60` only. Stored as a probability; `grade` column is this times 100 rounded
- `trend_grade`: `50 + (mean(last 10) - mean(last 30)) / mean(last 30) * 150`, clamped to [0, 100]. Requires at least `TREND_MIN = 3` obs. Stored as context only; NOT in composite.
- `momentum_grade`: reads the player's personal lag-1 transition probability from `common.player_line_patterns` and scales to 0-100. Uses `p_hit_after_hit` when the player is on a hit streak, `p_hit_after_miss` on a miss streak. Falls back to `hit_rate_60 * 100 + streak * 2` on a hit streak or `hit_rate_60 * 100` on a miss streak when no personal pattern exists. Score interpretation: 80 means an 80% personal probability of the next game hitting. **In composite at 40% weight.**
- `pattern_grade`: `pattern_strength * 300`, clamped to [0, 100], plus a sample-size bonus up to 20 points (scaling with `n` above 10). Measures how predictable the player's pattern is, not a reversal rate. **In composite at 20% weight.**
- `matchup_grade`: `(30 - defense_rank + 1) / 30 * 100`. Defense rank by position group (G/F/C) vs today's opponent. Rank 1 = most allowed = highest score. Stored as context only; NOT in composite.
- `regression_grade`: z-score of `last 10` vs full-season mean, transformed to `50 - z * 25` and clamped to [0, 100]. Stored as context only; NOT in composite.
- `composite_grade` (ADR-20260423-1, **40/40/20 formula**): weighted mean = `0.40 * momentum_grade + 0.40 * (hit_rate_60 * 100) + 0.20 * pattern_grade`. Only non-NULL components contribute, weights are renormalized if any are NULL. Returns None if all three are NULL. **The old equal-weight composite of all components is gone.** Calibration found that `matchup_grade`, `regression_grade`, `trend_grade`, and all six opportunity grades (below) have near-zero predictive lift (Won-vs-Lost mean gaps of 0.1-1.1 points vs 25-28 points for momentum and hr60), so they are stored as context columns but removed from the composite mean. For Under rows, the three in-composite components invert via `100 - value` before the weighted sum.

#### Opportunity grades (added 2026-04-22, ADR-0017; removed from composite ADR-20260423-1)

Six per-(player, market) components derived from per-game attempt and tracking data, not made-stat outcomes. All 0-100, 50 = neutral; Under rows invert via `100 - value`. **Stored as context columns only; not folded into `composite_grade`.** They still appear in `common.daily_grades` for display and future diagnostic use.

- `opportunity_short_grade`: short-vs-long trend on the player per-game opportunity value. Scaling matches `trend_grade`: `50 + (mean(last 10) - mean(last 30)) / mean(last 30) * 150`, clamped
- `opportunity_long_grade`: long-vs-season trend on the same metric: `50 + (mean(last 30) - season_mean) / season_mean * 150`, clamped
- `opportunity_matchup_grade`: how much the opponent allows of this market's opportunity stats to the player's position group. Averages component-level ranks from the extended `fetch_matchup_defense` (`rank_opp_pts`, `rank_opp_fg3m`, `rank_reb_chances`, `rank_potential_ast`), then scales `(30 - avg_rank + 1) / 30 * 100`
- `opportunity_streak_grade`: sign-based run count of games above/below player's season opportunity mean. Minimum run length `OPP_STREAK_MIN = 2`. Delta = `run * 6` capped at ±30 from neutral 50
- `opportunity_volume_grade`: threes-only. Short-vs-long trend on raw `3PA` (pure attempt volume)
- `opportunity_expected_grade`: threes-only. Short-vs-long trend on `3PA * rolling 3PT%` (expected made threes)

Per-market opportunity definitions (`MARKET_OPP_COMPONENTS`):

| Market | Opportunity = |
|--------|----------------|
| points / points_alternate | `(FGA - 3PA) * r2 * 2 + 3PA * r3 * 3 + FTA * rft` |
| rebounds / rebounds_alternate | `reb_chances` |
| assists / assists_alternate | `potential_ast` |
| threes / threes_alternate | `3PA * r3` (+ parallel volume/expected columns) |
| PRA / PR / PA / RA (+ alternates) | sum of component opportunities |

`r2`, `r3`, `rft` are per-player trailing shooting percentages: shift-1 rolling(10, min_periods=3) mean of per-game pcts with expanding-season fallback. Shift-1 prevents look-ahead bias; game G's percentages use games 1..G-1 only.

Blocks and steals markets have no opportunity grades (no per-player attempt rate). Those rows keep the six columns NULL.

Backfill dates before `player_passing_stats` and `player_rebound_chances` were populated will have NULL rebounds/assists opportunity. Combo markets (PRA/PR/PA/RA) treat missing components as 0 via `sum(min_count=1)`, so they still produce a (degraded) opportunity grade.

All six components invert for Under rows (`100 - value`). Rising trend is bad for an under.

### KDE tier lines (added 2026-04-23, ADR-20260423-1)

`compute_kde_tier_lines` fits a kernel density estimate to the player's game log for each (player, market, game) and walks a dense candidate-line grid to find the four tier cutoffs. One row per player-market-game-date is written to `common.player_tier_lines` by `upsert_tier_lines`. See `database/nba/README.md` for the schema.

KDE fitting:

- `scipy.stats.gaussian_kde` with Scott bandwidth
- Reflection boundary at 0 to prevent negative-stat probability mass (the sample is mirrored around 0 before fitting; the upper-tail integral is doubled to correct for the mirror)
- Grade-weighted lookback via `_select_kde_window`: composite >= 80 uses last 15 games, 50-79 uses last 30, under 50 uses full season
- Normal distribution fallback when fewer than `KDE_MIN_GAMES = 10` observations exist

Tier cutoff constants (top of `grade_props.py`):

- `TIER_SAFE_PROB = 0.80`: Safe is the highest candidate line where `P(stat > line) >= 0.80`. No market price requirement.
- `TIER_VALUE_PROB = 0.58`: Value is the highest candidate line above Safe where `P >= 0.58`. No market price requirement.
- `TIER_HIGHRISK_PROB = 0.28` + `TIER_HIGHRISK_MIN_PRICE = 150`: highest line above Value where `P >= 0.28` AND a market price of +150 or better exists within 0.5 of the candidate line. NULL when no qualifying price exists.
- `TIER_LOTTO_PROB = 0.07` + `TIER_LOTTO_MIN_PRICE = 400` + composite >= 50: highest line above High Risk where `P >= 0.07` AND +400 or better exists within 0.5 AND the player's composite is at least 50. The composite gate prevents pure-rarity long-shots from surfacing without any signal.

Blowout dampening (points and combo markets only):

- `fetch_game_spreads` / `fetch_upcoming_game_spreads` read pre-game spreads from `odds.game_lines` and `odds.upcoming_game_lines`
- `fetch_player_blowout_profiles` precomputes each player's historical points delta in blowout losses vs close games from `nba.player_box_score_stats` + `nba.games`
- When the pre-game spread is >= 10.5 and the player is on the projected losing team, half of the blowout-loss delta is subtracted from every value in the KDE input sample before fitting. The resulting tier lines come out lower. `blowout_dampened = 1` is written to the tier row.
- Backtest confirms the adjustment improves calibration: dampened rows hit Safe at 91.4% vs 88.7% not-dampened, Value at 68.0% vs 62.9% (ADR-20260423-1)

Calibration (backtest against 176K actual outcomes, 2025-10-27 to 2026-04-22):

- Safe: 85.9% actual vs 80% design. Well-calibrated.
- Value: 61.0% actual vs 58% design. Well-calibrated.
- High Risk: 19.8% actual vs 28% design. Overconfident; at average +348 the breakeven is 27.8%, so the tier is currently -EV at its prices. Use as "interesting line" indicator, not as a bet recommendation.
- Lotto: 6.4% actual vs 7% design. Hit rate on target; model probability (12.6% average) is overconfident. Also -EV at current prices.
- Probability miscalibration is concentrated at the extremes. The 50-90% predicted range is accurate; below 50% the model overestimates by 10-20 points; above 90% it collapses back to ~70% actual.
- Three-point markets calibrate worse than points/rebounds/assists because of the discrete low-count distribution KDE smooths over.

### Bracket expansion and Under grading

Standard markets (`player_points`, `player_rebounds`, `player_points_rebounds_assists`, etc.) expand into a line bracket: `BRACKET_STEPS = 5` on each side of the posted line in `BRACKET_INCREMENT = 1.0` steps. Only the center line (step 0) carries the actual posted price; bracket lines have `over_price = NULL`.

Alternate markets (`*_alternate`) are posted at fixed grids defined in `ALT_GRIDS` and never bracket-expand. `drop_bracket_lines_covered_by_alts` removes standard bracket lines whose stat + line already appears in an alternate row for the same player.

Under grading applies only to standard markets. Under prices come from `odds.upcoming_player_props` where `outcome_name = 'Under'`, matched to the same `(player_id, market_key, line_value)` as the Over. Alternate lines remain Over-only.

`precompute_line_grades` iterates by `(player_id, market_key)` pair, loads the stat sequence once, and fans across line values. Roughly 560 outer iterations vs. 6,200 in the per-line design it replaced.

### Signal display (web only)

Signals are a UI concept and live in `web/lib/signals.ts`. The grading pipeline does not import them; it writes raw component grades and the web computes chips from those at render time.

Two signal families:

- **Player-level** (same across every line for this player): `HOT` if `trend_grade > 72`, `COLD` if `trend_grade < 28`, `DUE` if `regression_grade > 72`, `FADE` if `regression_grade < 28`. HOT suppresses FADE. DUE suppresses COLD.
- **Line-level** (per posted line): `STREAK` when `momentum_grade > 70`. `SLUMP` (displayed as the green DUE chip) when `momentum_grade > 65` and `hit_rate_60 >= 0.35` and `STREAK` did not fire. Note: this line-level DUE is a different signal from the player-level DUE above.

`LONGSHOT` is a cell-level value signal flagged when `over_price > 250`, `hit_rate_20 > 0`, and `hit_rate_60 >= 0.20`.

STREAK was the strongest positive signal in the last backtest (+21.4% lift per the April 2026 session); a re-run is pending under the personal-pattern grading.

### `common.player_line_patterns`

Populated nightly by `compute-patterns.yml` at 07:30 UTC. Stores lag-1 transition probabilities (`p_hit_after_hit`, `p_hit_after_miss`) per `(player_id, market_key, line_value)`. Rules:

- `MIN_GAMES = 10` to create a row
- `MIN_TRANSITION_OBS = 3` per state before a transition probability is stored
- Grading reads these directly via `fetch_player_patterns` and falls back to a season-hit-rate baseline when no pattern row exists

### Odds API client

- Bookmaker is FanDuel only (`bookmakers=fanduel`). See ADR-0007.
- `includeLinks=true` is valid only on the per-event endpoint (`/v4/sports/{sport}/events/{event_id}/odds`). Not valid on the bulk endpoint.
- Event-level links write to `odds.upcoming_player_props.link VARCHAR(500)` and surface as tappable FanDuel betslip deep links in the web UI when the game is still open.
- Missing cells in a props table (for example, a 5+ PTS line shown as a dash) reflect Odds API feed coverage, not an ingestion bug. FanDuel's native app may display lines that the Odds API does not return.
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`. Upcoming mode writes to `odds.event_game_map` and runs nightly for `days-ahead=1`.

### Two-stage lineup poll

`lineup_poll.py` runs in two stages. Both invariant:

- **Stage 1**: official NBA lineup JSON. Returns 5 starters per team with precise PG/SG/SF/PF/C positions. `lineup_status` is `Confirmed` or `Projected`.
- **Stage 2**: `boxscorepreviewv3` for the full roster (bench + inactive). Always runs, unconditionally on Stage 1's outcome. Stage 1 starter designations override Stage 2 for overlapping players.
- `PREVIEW_TIMEOUT = 20s`, no retry. Single attempt is sufficient; 404 on live games is expected and handled.
- `BETWEEN_GAMES_DELAY = 0.5s`.
- Position strings written to `nba.daily_lineups` are full (PG, SG, SF, PF, C) for starters. Bench and inactive players have `position` empty — the NBA daily lineups JSON and `boxscorepreviewv3` both set position only for the five starters. Consumers that need rotation-role information (6th man, 7th man) cannot derive it from this table; see Open Questions below.
- Consumers must use `posToGroup()` (PG/SG → G, SF/PF → F, C → C, compound values by `LEFT(1)`). Never `position[0]`.
- Runs inside every cycle of `nba-game-day.yml` and inside `refresh-data.yml` with `--hours-ahead 6`.

### Scheduled re-grading

`refresh-lines.yml` runs at 17:00, 20:00, and 23:00 UTC (12 PM, 3 PM, 6 PM ET) daily. It refreshes FanDuel lines and re-runs grading in `upcoming` mode. Also callable via `workflow_dispatch`.

`refresh-data.yml` is `workflow_dispatch`-only. Triggered from the web app's Refresh Data button via `POST /api/refresh-data`, which validates `ADMIN_REFRESH_CODE` and dispatches the workflow via the GitHub Actions REST API. Runs four steps: live box score + schedule (`nba_live.py`), odds (`odds_etl.py --mode upcoming`), grading (`grade_props.py --mode intraday`), lineup poll (`lineup_poll.py --hours-ahead 6`).

## Invariants

Do not revert these without a superseding ADR.

- Grading code lives under `/grading/`, not `/etl/`. The entry point is `grading/grade_props.py`.
- `common.daily_grades` has `outcome_name` (Over/Under), `over_price`, and `outcome` (Won/Lost/NULL). UNIQUE key includes `outcome_name`.
- `precompute_line_grades` iterates by `(player_id, market_key)` pair, not per line value.
- Under components invert via `100 - value`.
- Standard markets bracket-expand via `BRACKET_STEPS = 5` at `BRACKET_INCREMENT = 1.0`. Alternate markets do not.
- Alternate markets are Over-only. Under grading is standard-markets-only.
- `drop_bracket_lines_covered_by_alts` runs before grading to avoid duplicate lines.
- Lineup poll Stage 2 always runs.
- Lineup poll `PREVIEW_TIMEOUT = 20s` with no retry.
- Position grouping uses `posToGroup()`, never `position[0]` or `LEFT(position, 2)`.
- `nba.daily_lineups.position` is populated only for starters. Bench and inactive players have `position` empty in both the NBA daily lineups JSON and `boxscorepreviewv3`. Do not infer rotation role from position emptiness; use season-level minutes from `nba.player_box_score_stats` instead.
- `includeLinks=true` is only valid on the Odds API per-event endpoint.
- Bookmaker is FanDuel only.
- `stats.nba.com` calls route through the Webshare proxy. `cdn.nba.com` calls do not.
- `nba_live.py` never writes live per-player rows to the DB. Live data is served from the Flask runner off the CDN.
- `compute-patterns.yml` runs nightly at 07:30 UTC.
- `grading.yml` is triggered by `workflow_run` on `odds-etl.yml` success. Do not reintroduce a fixed time buffer.
- `refresh-data.yml` uses grading mode `intraday`, not `upcoming`, so only moved lines are re-graded.
- Opportunity grades live on `common.daily_grades` (six columns prefixed `opportunity_`). ADR-0017. Under rows invert via `100 - value` like every other grade. ADR-20260423-1 removed them from the composite mean.
- `fetch_matchup_defense` produces 5 opportunity ranks (`rank_opp_pts`, `rank_opp_fg3a`, `rank_opp_fg3m`, `rank_reb_chances`, `rank_potential_ast`) in addition to the stat ranks. Never drop those; `precompute_opportunity_grades` reads them directly.
- `_common_grade_data` returns a 7-tuple; the seventh element is `opp_df`. Do not revert to 6-tuple.
- `MARKET_OPP_COMPONENTS` is the single source of truth for which components contribute to which market's opportunity metric. Modify only alongside a CHANGELOG note.
- Opportunity grading uses `groupby().transform()` (pandas 3.x safe); never use `groupby(group_keys=False).apply()`, which drops the grouping column in pandas 3.x.
- `grade_props.py --mode backfill --force` re-grades already-graded dates via MERGE UPDATE. The archive trigger in `upsert_grades` preserves old row versions. Use only when a new component requires refilling historical rows; omit `--force` for normal nightly backfill of newly-resolved dates.
- `compute_composite` takes exactly three arguments: `(momentum, hit_rate_60, pattern)`. Any caller passing the old multi-arg signature will TypeError. ADR-20260423-1.
- `compute_kde_tier_lines` uses reflection at 0 and grade-weighted window selection. Do not change `KDE_MIN_GAMES = 10` without re-running the full-season backfill to revalidate calibration.
- `grade_props_for_date` returns `(grade_rows, tier_rows)`. `run_upcoming`, `run_intraday`, and `run_backfill` all call `upsert_grades` then `upsert_tier_lines`. Neither may be skipped.
- Blowout dampening only applies to points and combo markets (`player_points`, `player_points_*`, and their `_alternate` variants). Rebounds, assists, and threes are never dampened.

Active NBA workflows:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `nba-game-day.yml` | 09:30 UTC daily + every 15 min 00:00-06:00 + every 15 min 22:00-23:59 UTC | Live scoreboard refresh, odds refresh, grading, lineup poll |
| `nba-etl.yml` | 09:00 UTC daily | Box scores, PT stats, schedule, rosters |
| `odds-etl.yml` | 10:00 UTC daily | Today's FanDuel lines |
| `grading.yml` | `workflow_run` after `odds-etl.yml` succeeds, plus `workflow_dispatch` for backfill | `grade_props.py --mode upcoming` (or `--mode backfill`) |
| `nba-backfill.yml` | Dispatched by `nba-game-day.yml` when a game goes Final | Odds + grade backfill |
| `refresh-lines.yml` | Cron at 17/20/23 UTC + `workflow_dispatch` | Odds refresh + `grade_props.py --mode upcoming` |
| `refresh-data.yml` | `workflow_dispatch` (from web via `/api/refresh-data` with `ADMIN_REFRESH_CODE`) | Four-step full refresh including `grade_props.py --mode intraday` |
| `compute-grade-outcomes.yml` | Scheduled + `workflow_dispatch` | `grade_props.py --mode outcomes` |
| `compute-patterns.yml` | 07:30 UTC nightly + `workflow_dispatch` | Update `common.player_line_patterns` |
| `restart-flask.yml` | `workflow_dispatch` | Restart `schnapp-flask.service` |
| `install-mcp.yml` | `workflow_dispatch` | Install or update MCP server on VM |

Retired (dispatch-only, do not reschedule): `pregame-refresh.yml`, `nba-live.yml`, `lineup-poll.yml`, `keepalive.yml`. `keepalive.yml` was originally replaced by Uptime Robot, which was paused 2026-04-23 — the DB now auto-pauses after 60 minutes of idle, accepted as a tradeoff for billing savings. Do not reschedule `keepalive.yml` without making a deliberate decision to reverse that tradeoff.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][etl]`. Historical entries before the documentation restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Signal backtest re-run is pending once enough resolved outcomes have accumulated under the personal-pattern grading (`etl/signal_backtest.py`, `signal-backtest.yml`).
- Availability and projected minutes are the dominant unmodeled risks in the tier model (2026-04-24 investigation). DNPs are 4.6% of all tier rows and hit at 0%; sub-20-minute games are another 12% and hit well below design on every tier. Restricting the backtest to confirmed starters with 30+ actual minutes pushes Safe to 92.3% (vs 80% design) and Lotto to 9.1% (vs 7% design, +EV at average +1,189). Both three-point Safe miscalibration and the 90%+ probability collapse are largely downstream of this. Fix requires a pre-game availability gate plus a minutes prior into `compute_kde_tier_lines`.
- NBA lineup backfill for completed games (see `/docs/ROADMAP.md`): rewrite `nba.daily_lineups` rows for all `game_status = 3` games from the NBA daily lineups JSON so every completed game has `lineup_status = 'Confirmed'` with authoritative Starter / Bench / Inactive roles. Prerequisite for the availability gate.
- Rotation-role / 6th-man identification is deferred (2026-04-24). The plan when work resumes: compute each player's season-level mean minutes in games where they did not start, grouped by team, from `nba.player_box_score_stats`. Feed the result into the tier-line minutes prior. Cannot come from `nba.daily_lineups` because non-starters have no position data there (the NBA daily lineups JSON sets `position` only on the five starters). See ROADMAP "Next up".
- High Risk and Lotto tiers are -EV at their market price floors in the current aggregate backtest. Segmentation work above suggests the calibration gap closes substantially with availability + minutes gating. Re-evaluate whether tier-probability isotonic calibration or price-floor raises are still needed after those fixes land. See tier effectiveness evaluation summary in `database/nba/README.md` and CHANGELOG 2026-04-24.
- Three-point markets calibrate worse than points/rebounds/assists at the Safe tier specifically (-17 point gap). KDE over-smoothing a discrete low-count distribution is the likely cause; a Poisson or negative-binomial fit may be more appropriate for low-count stats. HighRisk and Lotto threes calibrate comparable to singles, so the fix can be Safe-specific if needed.
- Extraction of common ingestion helpers into `etl/_shared.py` is deferred until MLB and NFL converge on the same patterns.
