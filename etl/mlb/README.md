# MLB ETL

**STATUS:** design phase. Data sources and 9 visual-feeding entities are cataloged. Implementation is scaffolded but not built out.

## Purpose

Will ingest MLB data from the MLB Stats API, Baseball Savant Statcast, and The Odds API. Produces 9 pre-aggregated entities that feed the web UI directly (see ADR-0004).

## Files

Scaffolded code in `/etl/`:

- `etl/mlb_etl.py` - main ETL entry point (scaffold)
- `etl/mlb_load_todays_schedule.py` - schedule loader (scaffold)
- `etl/mlb/_legacy_powerquery/` - archive of the Power BI M queries from `mlbSavantV3.pbix` (will be created during Step 5)

## Key Concepts

Per `/docs/DECISIONS.md` ADR-0004, the ETL produces 9 entities:

1. Upcoming games
2. Batter context per game
3. Batter projections per game
4. Player game stats
5. Player at-bat stats
6. Player trend and pattern stats
7. Player platoon splits
8. Career batter vs pitcher matchup
9. Pitcher season stats

Primary API endpoint: `https://statsapi.mlb.com/api/v1/game/{gameID}/withMetrics` returns box scores, season stats, play-by-play, and pitch data in a single call. Most MLB ETL reads come from this one URL.

Statcast pitch-level data is ETL-internal and is never queried directly by the web app.

## Invariants

- All 9 visual-feeding entities are pre-aggregated by the ETL. No runtime aggregation in queries serving the web layer (ADR-0004).
- Pitch-level Statcast stays internal to the ETL.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][etl]`.

## Open Questions

- Local Excel exports (`mlb-data/mlbSavantStatcast-2024-25.xlsx`, etc.) vs. live Baseball Savant API for historical seasons. See `/docs/ROADMAP.md`.
- Whether rolling-window stats recompute on a full rebuild each run or incrementally.
- Exact column lists for each of the 9 entities, to be finalized before implementation starts.
