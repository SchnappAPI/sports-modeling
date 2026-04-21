# Roadmap

Deliberately brief. Detailed task tracking lives in component READMEs under "Open Questions" sections.

## Active

- **Documentation restructure** (complete, 2026-04-20). Migrated from monolithic `PROJECT_REFERENCE.md` to co-located component READMEs with central `/docs/`. See ADR-0001 and ADR-0016. Archived originals at `/docs/_archive/`.
- **NFL ETL** (live, 2026-04-21). 7 tables populated via `nflreadpy` on a Tuesday 09:00 UTC schedule. See `etl/nfl/README.md`.
- **MLB derived entities** (1 of 5 missing shipped, 2026-04-21). `mlb.player_at_bats` materialized in-lockstep with `mlb.play_by_play`; `/api/mlb-atbats` swapped to read from it. Four remain: batter context per game, batter projections per game, player trend/pattern stats, player platoon splits, career batter-vs-pitcher matchup. ADR-0018 establishes the pattern the others should follow (inline materialization, diff-against-destination self-heal, ID-only storage).

## Next up

- **MLB build**. 5 of the 9 ADR-0004 entities are live (the 8 originally shipped plus `mlb.player_at_bats`). 4 derived entities remain, each blocking specific ADR-0003 pages. Of the 6 ADR-0003 pages: Game is live, Player Analysis is partially unblocked (at-bat access path exists via `IX_player_at_bats_batter` on `(batter_id, game_date)`, but still blocked on `player_trend_stats`), EV/VS/Proj/Pitcher Analysis are fully blocked on remaining entities. See `/database/mlb/README.md` and `/web/mlb/README.md`.
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
