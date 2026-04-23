# Roadmap

Deliberately brief. Detailed task tracking lives in component READMEs under "Open Questions" sections.

## Active

- **Documentation restructure** (complete, 2026-04-20). Migrated from monolithic `PROJECT_REFERENCE.md` to co-located component READMEs with central `/docs/`. See ADR-0001 and ADR-0016. Archived originals at `/docs/_archive/`.
- **NFL ETL** (live, 2026-04-21). 7 tables populated via `nflreadpy` on a Tuesday 09:00 UTC schedule. See `etl/nfl/README.md`.
- **MLB derived entities** (2 of 5 missing shipped, 2026-04-21). `mlb.player_at_bats` and `mlb.career_batter_vs_pitcher` both materialized in-lockstep with `mlb.play_by_play`. Both have live web routes (`/api/mlb-atbats`, `/api/mlb-bvp`). Three remain: batter context per game, batter projections per game, and player trend/pattern stats plus player platoon splits (may share a table). ADR-0018 and ADR-0019 establish the two patterns the remaining entities should follow — direct INSERT for append-only grains, staged MERGE for aggregate grains where rows need UPDATE on re-aggregation.
- **MLB VS page** (live, 2026-04-21). Second ADR-0003 page to ship. `/api/mlb-bvp` + `MlbVsView.tsx` + view-switcher integration in `MlbPageInner.tsx`. Shows each team's starting lineup vs opposing SP with lifetime BvP stats. See `web/mlb/README.md`.
- **MLB EV team view page** (live, 2026-04-21). Third ADR-0003 page to ship. `/api/mlb-ev` + `MlbEvView.tsx` + view-switcher toggle. Season-to-date exit velocity summary for both teams' starters, excluding the current game; tap-to-expand per-at-bat detail. No new materialized table — indexed aggregation on `mlb.player_at_bats` at request time. See `web/mlb/README.md`.

## Next up

- **NBA Trends Grid**. New third tab on `/nba/game/[gameId]` alongside Box Score and Matchups. Surfaces `common.player_tier_lines` (populated by ADR-20260423-1 grading rewrite) as a per-game grid showing each player's Safe / Value / High Risk / Lotto line values for the selected market, plus their per-game stat history with hit/miss indicators against the currently-posted FanDuel line. Row groups by team and lineup status (Starters, Bench, Inactive). Stat and game-window toggles. Period toggle (Q1, Q2, etc.) affects hit/miss on game columns only; tier line values stay full-game. API route `/api/tier-grid` first, then `TierGrid.tsx`, then wire into game page tab switcher. See `web/nba/README.md` once built.
- **MLB Player Analysis page**. Fourth ADR-0003 page. Partially data-unblocked (at-bat access and career BvP access both live) but the core trend/pattern visuals need `player_trend_stats` first, so this is blocked on ETL. Could start the page shell without those visuals if the priority shifts.
- **MLB `player_trend_stats` materialization**. Highest-value remaining derived entity; unblocks Player Analysis. Likely staged-MERGE pattern like career-BvP since rolling windows recompute as new games land.
- **NFL web surface**. ETL is live but no web layer exists yet. Parallel design session like the MLB visual catalog: identify what visuals matter, what stats feed them, what the pre-aggregation layer needs to produce. See `web/nfl/README.md`.
- **NFL odds ingestion**. `odds_etl.py` reportedly mentions NFL sport keys but has not been verified. Decide whether to extend it or add a dedicated `nfl-odds-etl.py`. See `etl/nfl/README.md` open questions.

## On the horizon (no active work yet)

- **Subscription / payment layer**. Stripe is the likely choice. Architecture is scoped but not started. Triggers a passcode model rework since payment-gated access replaces the current passcode-gate.
- **MLB pattern quality monitoring**. Once MLB grading is live and the MLB equivalent of `common.player_line_patterns` populates, NBA-style monitoring should follow.
- **PWA pinning**. Home screen URL should pin to `schnapp.bet/nba` (clean URL) rather than a date-specific path. Minor task; defer until the PWA install flow gets attention.

## Decisions deferred

These came up during planning conversations and were explicitly not decided:

- **Multi-bookmaker support**. FanDuel only for now. Rationale captured in ADR-0007.
- **Public Statcast Excel exports vs. live API for MLB historical data**. Currently both exist (`mlb-data/mlbSavantStatcast-2024-25.xlsx` etc. on local Windows machine). Need to decide whether the ETL relies on local Excel exports or pulls fresh from Savant for historical seasons.
