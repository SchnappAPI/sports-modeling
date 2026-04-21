# NFL Web

**STATUS:** not started. No `/web/app/nfl/` route, no components, no `/api/nfl-*` routes. This is the only entirely-unimplemented layer of the three sports.

## Purpose

Will implement the product blueprint for football. The backend is ready to support it — `etl/nfl_etl.py` loads 7 tables from nflreadpy (see `/etl/nfl/README.md`) — but nothing queries them yet.

## Files

None yet.

## Key Concepts

None defined. When web work starts, the natural first deliverables are:

- `/web/app/nfl/page.tsx` — thin Suspense wrapper following the `/mlb` pattern
- `/web/app/nfl/NflPageInner.tsx` — top-level page. Week picker (not date picker, since NFL is weekly), game strip
- `/web/app/nfl/NflGameTabs.tsx` — per-game detail

API routes will go in `/web/app/api/nfl-*/` following the `/api/mlb-*` pattern:
- `/api/nfl-games?week=&season=`
- `/api/nfl-boxscore?gameId=`
- `/api/nfl-player-stats?gameId=` or `?playerId=`

None of this exists. The blueprint is documented in `/docs/PRODUCT_BLUEPRINT.md`.

## Invariants

None yet. Once work starts, inherit the cross-sport patterns:

- URL is the source of truth for selected week and game
- No shared components with NBA or MLB until a proven need for sharing exists; start isolated, refactor later
- All visual stats come from pre-aggregated tables (ADR-0004). The 7 `nfl.*` tables should largely satisfy this at launch; FTN charting in particular is already play-level and pre-aggregated to per-play rows

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[nfl][web]`. Historical entries before the restructure are in the legacy root `/CHANGELOG.md`.

## Open Questions

- Week picker UI: the NBA/MLB date picker doesn't map directly. A season + week selector is closer to how NFL is actually consumed
- Whether to land a Game page first (parallels MLB `/mlb` launch) or go straight to a Player Analysis page since the weekly grain makes per-game detail less relevant
- Whether NFL props need the same "At a Glance" matrix that NBA has, given how few props FanDuel posts per player per week
- Whether to import props via `odds_etl.py` extensions or a separate `nfl_odds_etl.py`. The odds ingestion path for NFL has not been verified
