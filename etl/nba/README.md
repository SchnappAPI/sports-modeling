# NBA ETL

**STATUS:** live.

## Purpose

Ingests NBA data from `stats.nba.com`, `cdn.nba.com`, and The Odds API. Produces box scores, rosters, game logs, odds lines, lineups, and grades. All production ingestion runs in Python on the self-hosted GitHub Actions runner (`schnapp-runner-2`) and writes to Azure SQL.

## Files

Code files live flat in `/etl/` per ADR-0002.

- `etl/nba_etl.py` - main ETL entry point. Box scores, PT stats, schedule, rosters, players
- `etl/nba_live.py` - today-only schedule refresh from the public CDN scoreboard. Does not write live per-player rows to the DB
- `etl/nba_grading.py`, `etl/grade_props.py` - grading pipeline
- `etl/odds_etl.py` - odds ingestion, shared with MLB, dispatches by sport
- `etl/lineup_poll.py` - two-stage lineup polling
- `etl/runner.py` - Flask live-data service on the VM, port 5000
- `etl/compute_patterns.py` - nightly recomputation of `common.player_line_patterns`
- `etl/db.py` - shared upsert via staging + `MERGE`

Workflows under `.github/workflows/` orchestrate these scripts on schedule. The NBA-relevant ones are listed in the Invariants section below.

## Key Concepts

### Data source split

- **`stats.nba.com`** requires the Webshare rotating residential proxy (`NBA_PROXY_URL` secret). All `nba_etl.py` endpoints route through the proxy. PT stats (`leaguedashptstats`) is the one exception and does not use the proxy.
- **`cdn.nba.com`** is public and does not use the proxy. `nba_live.py` reads `todaysScoreboard_00.json` from the CDN. The Flask runner reads `boxscore_{gameId}.json` from the CDN on demand. Live per-player data is never persisted to the DB.

### Box score ingestion

`nba_etl.py` uses `playergamelogs`, with five calls per run (one per period: 1Q, 2Q, 3Q, 4Q, OT). Overtime uses `Period=""` + `GameSegment=Overtime`. The `fg3a` field is included. Teams come from a hardcoded `STATIC_TEAMS` dict to eliminate an HTTP dependency that previously failed behind the proxy. Players come from `playerindex` via the proxy.

### Grading pipeline

Order of operations, one step triggering the next:

1. `odds-etl.yml` fetches FanDuel lines
2. `grading.yml` runs on `workflow_run` after odds-etl succeeds; calls `grade_props.py run_upcoming`
3. `compute-grade-outcomes.yml` resolves Won/Lost after games finish
4. `compute-patterns.yml` runs nightly at 07:30 UTC to refresh `common.player_line_patterns` from resolved outcomes

`_common_grade_data` returns a 6-tuple. The sixth element is the personal pattern table keyed by `(player_id, market_key, line_value)`. Grade components live:

- `weighted_hit_rate`: 60% L20 + 40% L60, falls back to L60 when L20 sample < 5
- `trend_grade`: L10 mean vs L30 mean, centered at 50
- `momentum_grade`: consecutive hit or miss streak, log-scaled
- `pattern_grade`: historical reversal rate after runs of current streak length
- `matchup_grade`: defense rank for player position vs today's opponent (rank 1 = most allowed)
- `regression_grade`: z-score of recent L10 vs full season
- `composite_grade`: equal-weighted mean of all non-NULL components

All components invert for Under rows (`100 - value`). Rising trend is bad for an under.

`precompute_line_grades` iterates by `(player_id, market_key)` pair, loads the stat sequence once, and fans across line values. Roughly 560 outer iterations vs. 6,200 in the per-line design it replaced.

### Signal design

Signals are sourced from `shared/signals.ts` and are computed on grade outputs.

- **STREAK** is the strongest positive signal (+21.4% lift in the last backtest). Fires when `momentum_grade > 70`.
- **DUE** (formerly labeled SLUMP) is a bounce-back signal for miss streaks. Fires when `momentum_grade > 65 AND hr60 >= 0.35`. Rendered green in the UI.
- HOT, COLD, FADE are player-level and follow legacy thresholds.

### `common.player_line_patterns`

Populated nightly by `compute-patterns.yml` at 07:30 UTC. Stores lag-1 transition probabilities (`p_hit_after_hit`, `p_hit_after_miss`) per `(player_id, market_key, line_value)`. Rules:

- `MIN_GAMES = 10` to create a row
- `MIN_TRANSITION_OBS = 3` per state before a transition probability is stored
- Grading reads these directly and falls back to season hit rate when no pattern row exists

### Odds API client

- Bookmaker is FanDuel only (`bookmakers=fanduel`). See ADR-0007.
- `includeLinks=true` is valid only on the per-event endpoint (`/v4/sports/{sport}/events/{event_id}/odds`). Not valid on the bulk endpoint.
- Event-level links write to `odds.upcoming_player_props.link VARCHAR(500)` and surface as tappable FanDuel betslip deep links in the web UI when the game is still open.
- Missing cells in a props table (e.g., a 5+ PTS line that shows as a dash) reflect Odds API feed coverage, not an ingestion bug. FanDuel's native app may display lines that the Odds API does not return.
- Modes: `discover`, `probe`, `backfill`, `mappings`, `upcoming`. Upcoming mode writes to `odds.event_game_map` and runs nightly for `days-ahead=1`.

### Two-stage lineup poll

`lineup_poll.py` runs in two stages. Both invariant:

- **Stage 1**: official NBA lineup JSON. Returns 5 starters per team with precise PG/SG/SF/PF/C positions. `lineup_status` is `Confirmed` or `Projected`.
- **Stage 2**: `boxscorepreviewv3` for the full roster (bench + inactive). Always runs, unconditionally on Stage 1's outcome. Stage 1 starter designations override Stage 2 for overlapping players.
- `PREVIEW_TIMEOUT = 20s`, no retry. Single attempt is sufficient; 404 on live games is expected and handled.
- `BETWEEN_GAMES_DELAY = 0.5s`.
- Position strings written to `nba.daily_lineups` are full (PG, SG, SF, PF, C). Consumers must use `posToGroup()` (PG/SG → G, SF/PF → F, C → C, compound values by `LEFT(1)`). Never `position[0]`.
- Runs inside every cycle of `nba-game-day.yml` and inside `refresh-data.yml`.

### Scheduled re-grading

`refresh-lines.yml` runs at 12:00 PM, 3:00 PM, and 6:00 PM ET daily. It refreshes FanDuel lines and re-runs grading. Unauthenticated and also callable from the web via `POST /api/refresh-lines`.

`refresh-data.yml` is the admin-only full refresh (live box score + odds + grading + lineup poll). Requires `ADMIN_REFRESH_CODE`. Called from the in-app Refresh Data button.

## Invariants

Do not revert these without a superseding ADR.

- `_common_grade_data` returns a 6-tuple. The sixth element is patterns. Never revert to the 5-tuple form.
- `common.daily_grades` has `outcome_name` (Over/Under) and `over_price`. UNIQUE key includes `outcome_name`.
- `precompute_line_grades` iterates by `(player_id, market_key)` pair, not per line value.
- Under components invert via `100 - value`.
- Lineup poll Stage 2 always runs.
- Lineup poll `PREVIEW_TIMEOUT = 20s` with no retry.
- Position grouping uses `posToGroup()`, never `position[0]` or `LEFT(position, 2)`.
- `includeLinks=true` is only valid on the Odds API per-event endpoint.
- Bookmaker is FanDuel only.
- `stats.nba.com` calls route through the Webshare proxy. `cdn.nba.com` calls do not.
- `nba_live.py` never writes live per-player rows to the DB. Live data is served from the Flask runner off the CDN.
- `compute-patterns.yml` runs nightly at 07:30 UTC.
- `grading.yml` is triggered by `workflow_run` on `odds-etl.yml` success. Do not reintroduce a fixed time buffer.

Active NBA workflows:

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| `nba-game-day.yml` | 09:30 UTC daily + every 15 min 00:00-06:00 + every 15 min 22:00-23:59 UTC | Live scoreboard refresh, odds refresh, grading, lineup poll |
| `nba-etl.yml` | 09:00 UTC daily | Box scores, PT stats, schedule, rosters |
| `odds-etl.yml` | 10:00 UTC daily | Today's FanDuel lines |
| `grading.yml` | `workflow_run` after `odds-etl.yml` succeeds | Grade today's props |
| `nba-backfill.yml` | Dispatched by `nba-game-day.yml` when a game goes Final | Odds + grade backfill |
| `refresh-lines.yml` | `POST /api/refresh-lines` | Unauthenticated odds + grade refresh |
| `refresh-data.yml` | `POST /api/refresh-data` with `ADMIN_REFRESH_CODE` | Full four-step refresh |
| `compute-patterns.yml` | 07:30 UTC nightly + `workflow_dispatch` | Update `common.player_line_patterns` |
| `restart-flask.yml` | `workflow_dispatch` | Restart `schnapp-flask.service` |
| `install-mcp.yml` | `workflow_dispatch` | Install or update MCP server on VM |

Retired (dispatch-only, do not reschedule): `pregame-refresh.yml`, `nba-live.yml`, `lineup-poll.yml`, `keepalive.yml` (replaced by Uptime Robot).

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nba][etl]`. Historical entries before the documentation restructure are in the legacy root `/CHANGELOG.md`.

## Open Questions

- Signal backtest re-run is pending once enough resolved outcomes have accumulated under the personal-pattern grading.
- Extraction of common ingestion helpers into `etl/_shared.py` is deferred until MLB and NFL converge on the same patterns.
