# MLB Web

**STATUS:** partially live. Single `/mlb` page with date picker, game strip, and per-game detail (Box Score + Exit Velo tabs). 6-page vision from ADR-0003 not yet realized — only the game-detail foundation exists.

## Purpose

MLB pages implementing the product blueprint for baseball. Today the UI covers game-level viewing; player-level analysis, lineup-wide views, and projections are roadmap.

## Files

Live page + components:

- `web/app/mlb/page.tsx` — thin Suspense wrapper around `MlbPageInner`
- `web/app/mlb/MlbPageInner.tsx` — top-level client component. Owns the date picker, game strip, and active-game routing (`?gameId=&date=` URL params)
- `web/app/mlb/MlbGameTabs.tsx` — per-game detail. Contains four inline sub-components: `Linescore`, `BatterTable`, `PitcherTable`, `ExitVeloTable`. Two tabs: Box Score and Exit Velo

API routes:

- `web/app/api/mlb-games/route.ts` — reads `mlb.games` joined to `mlb.teams`, filtered by `?date=` (defaults to Central Time today). Used by `MlbPageInner` to populate the strip
- `web/app/api/mlb-boxscore/route.ts` — reads `mlb.batting_stats` and `mlb.pitching_stats` in parallel by `?gamePk=`. Returns `{ batters, pitchers }`
- `web/app/api/mlb-linescore/route.ts` — checks whether `mlb.play_by_play` has data for the game, then derives per-half-inning runs from scoring plays (`is_last_pitch=1`) and R/H summaries from `mlb.batting_stats`. Returns `{ innings, summary, hasPbp }`
- `web/app/api/mlb-atbats/route.ts` — reads `mlb.player_at_bats` directly (one row per completed at-bat, pre-filtered and indexed by `game_pk`) and LEFT JOINs `mlb.players` twice for batter/pitcher names at read time. Returns `{ atBats }`

No shared component library yet. Nothing from `/web/components/` is MLB-aware. All MLB UI lives in the three files above.

## Key Concepts

### Game strip

Horizontal scrolling row of game cells. Each cell shows away/home abbreviations stacked vertically, with scores for Final games and game-time or status text below. Active game has a darker background. URL is the source of truth: `?gameId={pk}&date={yyyy-mm-dd}`.

Date control: text input bound to `selectedDate`, plus `‹` / `›` buttons that shift one day at a time. `todayLocal()` is browser-local; date-picker writes in `YYYY-MM-DD`.

On date change, default selection is the first non-Final game (for browsing today's live slate), falling back to the first game. Explicit clicks set `isExplicitSelection.current = true` so the date change doesn't clobber the user's choice.

### Game detail: Box Score tab

Five sub-tables arranged per team (away first, then home):

1. Linescore (if PBP data exists for this game): inning-by-inning runs + R/H totals
2. Away Batting: AB, R, H, 2B, 3B, HR, RBI, BB, K, SB, AVG, OBP, SLG, OPS
3. Home Batting: same columns
4. Away Pitching: IP, H, R, ER, BB, K, HR, ERA, P-S (pitches-strikes)
5. Home Pitching: same columns

Batter table formatting:
- Players with any hit get a faint green row background (`bg-green-950/10`)
- Hits column bolds and brightens when > 0
- HR column turns yellow when > 0
- Batting order shown as a small prefix (`Math.floor(battingOrder / 100)` gives the 1-9 slot; `battingOrder % 100` distinguishes starter from substitute)
- Substitutes (same slot, different player) get a `+` marker and dimmed name

Pitcher table:
- Starting pitcher (`note = 'SP'`) shown brighter; relievers dimmer
- IP formatted as decimal with outs: `6.0`, `6.1` (1 out), `6.2` (2 outs)
- P-S column: `pitches-strikes` (e.g., `97-62`), dash when pitch data missing
- HR column turns yellow when > 0

### Game detail: Exit Velo tab

Per-at-bat Statcast table, filtered to plays with either exit velocity data or a result type. Grouped into two tables (away at-bats, home at-bats). Relies entirely on `mlb.play_by_play` being populated for the game — if not, the tab shows "Exit velocity data not yet available. Run the play-by-play ETL to load it."

Columns: Batter, Pitcher, Inn, Result, EV (exit velocity), LA (launch angle), Dist (feet), xBA (hit probability).

Data source: `mlb.player_at_bats` (materialized from `mlb.play_by_play` at ETL time with the filter `is_last_pitch = 1 AND result_event_type IS NOT NULL`). The route joins `mlb.players` at read time for batter/pitcher names — that can't be denormalized onto the table because `mlb.players` is current-season-scoped. `hasPbp` from `/api/mlb-linescore` is still the authoritative availability check; if a game hasn't been through the PBP loader, `mlb.player_at_bats` also has nothing for it.

Color coding:
- Exit velo: 100+ red, 95+ orange, 90+ yellow, else default
- Result type: home runs yellow, hits green, strikeouts red, else default

Away vs home filter: `isAway = teamId === awayTeamId`; away at-bats happen when `is_top_inning = true`.

### Which games have PBP data

`mlb.play_by_play` is a separate on-demand loader (`mlb-pbp-etl.yml`). The backfill is partial — not every Final game has pitch data. `hasPbp` from `/api/mlb-linescore` is the authoritative check; the Exit Velo tab will cleanly degrade for games without data. `mlb.player_at_bats` is materialized in-lockstep with `mlb.play_by_play`, so the same set of games is covered.

### Timezone

`MlbPageInner.todayLocal()` is browser-local (the client's tz). `mlb-games/route.ts:todayCT()` defaults the API to Central Time when no `?date` is provided. These two can disagree for users outside Central; the URL query takes precedence once a date is explicitly selected.

### 6-page vision (ADR-0003)

The ADR-0003 page plan remains the target:

- **Game** — the current `/mlb` page. Live
- **Player Analysis** — consolidation of legacy PBI pages New, Extra, Criteria, and MAIN. Not started
- **EV** — team-wide exit velocity view. Partially addressed by the current Exit Velo tab but scoped to a single game. Full team view not started
- **VS** — lineup-wide career matchup. Not started
- **Proj** — lineup projections. Not started
- **Pitcher Analysis** — pitcher counterpart to Player Analysis. Not started

Five of six pages still need data that depends on the remaining ADR-0004 entities. The Game page can ship and iterate today; the others are blocked on database work. `mlb.player_at_bats` (shipped 2026-04-21) covers one of those prerequisites and its index on `(batter_id, game_date)` is the intended access path for Player Analysis.

## Invariants

Do not revert without an ADR.

- Only one MLB top-level route: `/mlb`. All game viewing is date + gameId query params, not separate routes
- URL is source of truth for selected game and date. `MlbPageInner` reads `?gameId=` and `?date=` from `useSearchParams`
- Box Score tab always renders; Exit Velo tab gracefully falls back when `hasPbp=false`
- Starting pitchers come from `mlb.pitching_stats.note = 'SP'`. Do not attempt to derive from innings pitched or batting-order context
- IP display uses MLB notation (`.1` = 1 out, `.2` = 2 outs), never decimal thirds (`.333`, `.667`)
- `/api/mlb-linescore` derives runs from `mlb.play_by_play` scoring plays, not from `mlb.batting_stats`. Hits come from `batting_stats` because PBP does not have a reliable hit-count aggregate
- `/api/mlb-atbats` reads from `mlb.player_at_bats` (not `mlb.play_by_play`). Do not revert to the PBP aggregate query — the materialization exists precisely to keep ADR-0004's no-runtime-aggregation invariant
- `/api/mlb-atbats` joins `mlb.players` at read time for batter and pitcher names. Names are not denormalized onto `mlb.player_at_bats` because `mlb.players` is current-season-scoped
- MLB shares no components with NBA. If something feels reusable, put it under `/web/_shared/` first

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][web]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether to add a Pitch Log sub-tab under Box Score that shows every pitch with type, velocity, location, and result. Data is already in `mlb.play_by_play`
- Whether to surface win probability changes per at-bat on the Exit Velo tab
- When to start the Player Analysis page — now unblocked on at-bat access (via `mlb.player_at_bats` + its `batter_id, game_date` index), but still blocked on `player_trend_stats` materialization
- Mobile layout for the 13-zone hot/cold grid (still a question from the original skeleton; deferred until the page is built)
- Whether to pull opening-day 2023-2024-2025 historical games into PBP as a one-time backfill before the 2026 season gets deep
