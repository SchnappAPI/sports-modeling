# Roadmap

Deliberately brief. Detailed task tracking lives in component READMEs under "Open Questions" sections.

## Active

- **Documentation restructure** (complete, 2026-04-20). Migrated from monolithic `PROJECT_REFERENCE.md` to co-located component READMEs with central `/docs/`. See ADR-0001 and ADR-0016. Archived originals at `/docs/_archive/`.

## Next up after migration completes

- **MLB build**. Design phase complete; ETL, database tables, and web components all need to be built. See `/database/mlb/README.md` and `/web/mlb/README.md` once those are populated for the 9-entity catalog and visual inventory. ADR-0004 captures the architectural commitment to pre-aggregated stats.
- **NFL planning**. No design work started. Next step is a parallel design session like the MLB visual catalog: identify what visuals matter, what stats feed them, what the pre-aggregation layer needs to produce.

## On the horizon (no active work yet)

- **Subscription / payment layer**. Stripe is the likely choice. Architecture is scoped but not started. Triggers a passcode model rework since payment-gated access replaces the current passcode-gate.
- **MLB pattern quality monitoring**. Once MLB grading is live and the MLB equivalent of `common.player_line_patterns` populates, NBA-style monitoring should follow.
- **PWA pinning**. Home screen URL should pin to `schnapp.bet/nba` (clean URL) rather than a date-specific path. Minor task; defer until the PWA install flow gets attention.

## Decisions deferred

These came up during planning conversations and were explicitly not decided:

- **Multi-bookmaker support**. FanDuel only for now. Rationale captured in ADR-0007.
- **Public Statcast Excel exports vs. live API for MLB historical data**. Currently both exist (`mlb-data/mlbSavantStatcast-2024-25.xlsx` etc. on local Windows machine). Need to decide whether the ETL relies on local Excel exports or pulls fresh from Savant for historical seasons.
