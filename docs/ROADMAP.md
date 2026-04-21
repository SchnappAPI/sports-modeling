# Roadmap

Deliberately brief. Detailed task tracking lives in component READMEs under "Open Questions" sections.

## Active

- **Documentation restructure** (complete, 2026-04-20). Migrated from monolithic `PROJECT_REFERENCE.md` to co-located component READMEs with central `/docs/`. See ADR-0001 and ADR-0016. Archived originals at `/docs/_archive/`.
- **NFL ETL** (live, 2026-04-21). 7 tables populated via `nflreadpy` on a Tuesday 09:00 UTC schedule. See `etl/nfl/README.md`.
- **MLB derived entities** (2 of 5 missing shipped, 2026-04-21). `mlb.player_at_bats` and `mlb.career_batter_vs_pitcher` both materialized in-lockstep with `mlb.play_by_play`. `/api/mlb-atbats` already reads from `mlb.player_at_bats`; a future `/api/mlb-bvp` will read from `mlb.career_batter_vs_pitcher`. Three remain: batter context per game, batter projections per game, and player trend/pattern stats plus player platoon splits (may share a table). ADR-0018 and ADR-0019 establish the two patterns the remaining entities should follow — direct INSERT for append-only grains, staged MERGE for aggregate grains where rows need UPDATE on re-aggregation.

## Next up

- **MLB build**. 6 of the 9 ADR-0004 entities are live (the 4 originally mapped plus `mlb.player_at_bats` plus `mlb.career_batter_vs_pitcher`). 3 derived entities remain, each blocking specific ADR-0003 pages. Of the 6 ADR-0003 pages: Game is live, VS is now fully data-unblocked (career BvP shipped with dual access paths), Player Analysis is partially unblocked (at-bat access path exists; still blocked on `player_trend_stats`), EV/Proj/Pitcher Analysis are fully blocked on remaining entities. Next data-layer target should be `player_trend_stats` to unblock Player Analysis, which is the highest-value ADR-0003 page. Next web-layer target could be the VS page since its data is now fully in place. See `/database/mlb/README.md` and `/web/mlb/README.md`.
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
