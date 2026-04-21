# MLB Web

**STATUS:** partially live. `/mlb` page with date picker, game strip, and a view switcher (Game, VS enabled; EV, Proj, Player, Pitcher "coming soon"). Game view shows per-game detail (Box Score + Exit Velo tabs). VS view shows lineup-wide career batter-vs-pitcher matchup. 4 of the 6 ADR-0003 pages still to build.

## Purpose

MLB pages implementing the product blueprint for baseball. Today the UI covers game-level viewing and lineup-wide career matchup; player-level analysis, team-wide exit velocity, and projections are roadmap.

## Files

Live page + components:

- `web/app/mlb/page.tsx` — thin Suspense wrapper around `MlbPageInner`
- `web/app/mlb/MlbPageInner.tsx` — top-level client component. Owns the date picker, view switcher, game strip, and active-game routing (`?view=&gameId=&date=` URL params)
- `web/app/mlb/MlbGameTabs.tsx` — Game view body. Contains four inline sub-components: `Linescore`, `BatterTable`, `PitcherTable`, `ExitVeloTable`. Two tabs: Box Score and Exit Velo
- `web/app/mlb/MlbVsView.tsx` — VS view body. Two lineup tables (away batters vs home SP, home batters vs away SP) with career stats per row

API routes:

- `web/app/api/mlb-games/route.ts` — reads `mlb.games` joined to `mlb.teams`, filtered by `?date=` (defaults to Central Time today). Used by `MlbPageInner` to populate the strip
- `web/app/api/mlb-boxscore/route.ts` — reads `mlb.batting_stats` and `mlb.pitching_stats` in parallel by `?gamePk=`. Returns `{ batters, pitchers }`
- `web/app/api/mlb-linescore/route.ts` — checks whether `mlb.play_by_play` has data for the game, then derives per-half-inning runs from scoring plays (`is_last_pitch=1`) and R/H summaries from `mlb.batting_stats`. Returns `{ innings, summary, hasPbp }`
- `web/app/api/mlb-atbats/route.ts` — reads `mlb.player_at_bats` directly (one row per completed at-bat, pre-filtered and indexed by `game_pk`) and LEFT JOINs `mlb.players` twice for batter/pitcher names at read time. Returns `{ atBats }`
- `web/app/api/mlb-bvp/route.ts` — reads `mlb.career_batter_vs_pitcher` directly for the matchup pairs implied by a given `?gamePk=`. Two roundtrips: one UNION ALL across `mlb.pitching_stats` (SP only) + `mlb.batting_stats` (starting lineup only) to get both teams' starters, one indexed SELECT on `mlb.career_batter_vs_pitcher` for the ≤18 relevant `(batter, pitcher)` pairs. Returns `{ awaySP, homeSP, awayLineup, homeLineup, earliestDataDate, available }`

No shared component library yet. Nothing from `/web/components/` is MLB-aware. All MLB UI lives in the four files above.

## Key Concepts

### View switcher

Six-tab row directly below the date header, above the game strip. Driven by `?view=` URL param with values `game`, `vs`, `ev`, `proj`, `player`, `pitcher`. Default and fallback is `game`. Currently `game` and `vs` are enabled; the other four render as disabled muted labels until they're built.

The game strip stays persistent across all views because every view is keyed off a selected game. Changing dates drops `gameId` (a game ID from yesterday is meaningless today) but preserves `view`. Changing games preserves both `view` and `date`. `MlbPageInner.buildUrl()` is the single URL-patching helper that diffs against current searchParams rather than rebuilding the query string from scratch.

### Game strip

Horizontal scrolling row of game cells. Each cell shows away/home abbreviations stacked vertically, with scores for Final games and game-time or status text below. Active game has a darker background. URL is the source of truth: `?view={view}&gameId={pk}&date={yyyy-mm-dd}`.

Date control: text input bound to `selectedDate`, plus `‹` / `›` buttons that shift one day at a time. `todayLocal()` is browser-local; date-picker writes in `YYYY-MM-DD`.

On date change, default selection is the first non-Final game (for browsing today's live slate), falling back to the first game. Explicit clicks set `isExplicitSelection.current = true` so the date change doesn't clobber the user's choice.

### Game view: Box Score tab

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

### Game view: Exit Velo tab

Per-at-bat Statcast table, filtered to plays with either exit velocity data or a result type. Grouped into two tables (away at-bats, home at-bats). Relies entirely on `mlb.play_by_play` being populated for the game — if not, the tab shows "Exit velocity data not yet available. Run the play-by-play ETL to load it."

Columns: Batter, Pitcher, Inn, Result, EV (exit velocity), LA (launch angle), Dist (feet), xBA (hit probability).

Data source: `mlb.player_at_bats` (materialized from `mlb.play_by_play` at ETL time with the filter `is_last_pitch = 1 AND result_event_type IS NOT NULL`). The route joins `mlb.players` at read time for batter/pitcher names — that can't be denormalized onto the table because `mlb.players` is current-season-scoped. `hasPbp` from `/api/mlb-linescore` is still the authoritative availability check; if a game hasn't been through the PBP loader, `mlb.player_at_bats` also has nothing for it.

Color coding:
- Exit velo: 100+ red, 95+ orange, 90+ yellow, else default
- Result type: home runs yellow, hits green, strikeouts red, else default

Away vs home filter: `isAway = teamId === awayTeamId`; away at-bats happen when `is_top_inning = true`.

### VS view

Two stacked tables for the selected game: "Away Lineup" (nine starting batters from the away team vs the home team's starting pitcher) and "Home Lineup" (nine starting batters from the home team vs the away team's starting pitcher). Same layout as the Box Score batter tables — one row per batter, stats across the row, batting-order slot number as a prefix — but the stats are lifetime career-vs-pitcher numbers from `mlb.career_batter_vs_pitcher` rather than this-game numbers.

Columns: Batter (with batting-order slot, L/R/S hand code, position), PA, AB, H, HR, RBI, BB, K, AVG, OBP, SLG, OPS, Last (last-faced date M/D/YY).

Starters are identified from `mlb.batting_stats` where `batting_order % 100 = 0` (drops substitutes) and `mlb.pitching_stats` where `note = 'SP'`. SPs are labeled with their hand (LHP/RHP) next to the pitcher name. Batters are labeled L/R/S for their bat side.

Zero-PA rows (the batter has never faced this pitcher in our loaded data) still appear in batting-order position with dimmed styling and dashes across stat columns. HR > 0 rows get a faint yellow tint. No AVG/OBP/SLG/OPS rendering for rows with AB = 0 (NULL renders as `-`).

Availability: the route returns `available: false` when either team lacks an SP row, which happens pre-game or for non-Final games. The component renders a clear message in that case. A Final game with no play-by-play loaded yields `available: true` with mostly zero-PA rows, plus a footer note that the PBP backfill is partial.

### Which games have PBP data

`mlb.play_by_play` is a separate on-demand loader (`mlb-pbp-etl.yml`). The backfill is partial — not every Final game has pitch data. `hasPbp` from `/api/mlb-linescore` is the authoritative check; the Exit Velo tab will cleanly degrade for games without data. `mlb.player_at_bats` is materialized in-lockstep with `mlb.play_by_play`, so the same set of games is covered. `mlb.career_batter_vs_pitcher` is also materialized in-lockstep from `mlb.player_at_bats`, so it too is covered for the same game set.

The VS view reads career totals aggregated across **all** loaded games, not only the currently selected game. So a hitter's lifetime BvP row includes every matchup against this pitcher from every game that's been through the PBP loader — which may or may not include the selected game's historical backdrop.

### Timezone

`MlbPageInner.todayLocal()` is browser-local (the client's tz). `mlb-games/route.ts:todayCT()` defaults the API to Central Time when no `?date` is provided. These two can disagree for users outside Central; the URL query takes precedence once a date is explicitly selected.

### 6-page vision (ADR-0003)

The ADR-0003 page plan remains the target:

- **Game** — Live. Date picker, game strip, per-game Box Score + Exit Velo tabs
- **VS** — Live (2026-04-21). Lineup-wide career matchup against opposing SP. Reads `mlb.career_batter_vs_pitcher` directly
- **Player Analysis** — consolidation of legacy PBI pages New, Extra, Criteria, and MAIN. Not started; data dependencies partially satisfied (at-bats access path live, career BvP access path live via `/api/mlb-bvp`; player trend/pattern stats still pending)
- **EV** — team-wide exit velocity view. Partially addressed by the current Exit Velo tab but scoped to a single game. Full team view not started
- **Proj** — lineup projections. Not started
- **Pitcher Analysis** — pitcher counterpart to Player Analysis. Not started

Two of six pages are live. Of the remaining four, EV can be built today (data is in `mlb.player_at_bats`), Proj and Player Analysis need the remaining ADR-0004 entities, and Pitcher Analysis is roughly symmetric with Player Analysis.

## Invariants

Do not revert without an ADR.

- Only one MLB top-level route: `/mlb`. All view selection is via `?view=`, not separate routes
- URL is source of truth for selected view, game, and date. `MlbPageInner` reads `?view=`, `?gameId=`, and `?date=` from `useSearchParams`. When changing dates, `gameId` drops but `view` is preserved
- Unknown or disabled `?view=` values fall back to `game`. `parseView()` is the single filter; do not bypass it when reading view state
- Box Score tab always renders; Exit Velo tab gracefully falls back when `hasPbp=false`
- Starting pitchers come from `mlb.pitching_stats.note = 'SP'`. Do not attempt to derive from innings pitched or batting-order context
- Starting batters come from `mlb.batting_stats` where `batting_order % 100 = 0`. Substitutes (`batting_order % 100 != 0`) are excluded from the VS view lineup intentionally
- IP display uses MLB notation (`.1` = 1 out, `.2` = 2 outs), never decimal thirds (`.333`, `.667`)
- `/api/mlb-linescore` derives runs from `mlb.play_by_play` scoring plays, not from `mlb.batting_stats`. Hits come from `batting_stats` because PBP does not have a reliable hit-count aggregate
- `/api/mlb-atbats` reads from `mlb.player_at_bats` (not `mlb.play_by_play`). Do not revert to the PBP aggregate query — the materialization exists precisely to keep ADR-0004's no-runtime-aggregation invariant
- `/api/mlb-atbats` joins `mlb.players` at read time for batter and pitcher names. Names are not denormalized onto `mlb.player_at_bats` because `mlb.players` is current-season-scoped
- `/api/mlb-bvp` reads `mlb.career_batter_vs_pitcher` directly, never aggregating `mlb.player_at_bats` or `mlb.play_by_play` at request time. Same rationale as `/api/mlb-atbats`: ADR-0004 / ADR-0019
- `/api/mlb-bvp` joins `mlb.players` at read time for batter and SP names, same pattern as `/api/mlb-atbats`
- MLB shares no components with NBA. If something feels reusable, put it under `/web/_shared/` first

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][web]`. Historical entries before the restructure are in the archived `/docs/_archive/CHANGELOG.md`.

## Open Questions

- Whether to add a Pitch Log sub-tab under Box Score that shows every pitch with type, velocity, location, and result. Data is already in `mlb.play_by_play`
- Whether to surface win probability changes per at-bat on the Exit Velo tab
- Whether VS should gain a single-pair mode (pick any batter + any pitcher, not tied to a game). Route already supports this shape; UI doesn't yet
- Whether VS should add recent-window columns (last-3 matchups, last-5 matchups). Requires a new materialization; deferred per ADR-0019
- When to start the Player Analysis page — now unblocked on both at-bat access and career matchup access, but still blocked on `player_trend_stats` materialization
- Mobile layout for the 13-zone hot/cold grid (still a question from the original skeleton; deferred until the page is built)
- Whether to pull opening-day 2023-2024-2025 historical games into PBP as a one-time backfill before the 2026 season gets deep. Would also grow `mlb.career_batter_vs_pitcher` row count significantly
- Whether the `earliestDataDate` field in `/api/mlb-bvp` is useful or should be removed. Currently computed but not consumed by the component; see the route's comment for the caveat about it being the MIN of per-pair MAXes
