# MLB Database

**STATUS:** design phase. Schema decisions driven by `/docs/DECISIONS.md` ADR-0004 (all visual stats pre-aggregated).

## Purpose

The `mlb` schema will hold the 9 entities that feed the web UI, plus any internal-only Statcast pitch-level storage.

## Files

No DDL scripts yet. Table designs will follow from the 9-entity specification in `/etl/mlb/README.md`.

## Key Concepts

Per ADR-0004, the 9 visual-feeding entities:

1. Upcoming games
2. Batter context per game
3. Batter projections per game
4. Player game stats
5. Player at-bat stats
6. Player trend and pattern stats
7. Player platoon splits
8. Career batter vs pitcher matchup
9. Pitcher season stats

Pitch-level Statcast data is ETL-internal only. An intermediate table such as `mlb.statcast_pitches` may exist but is not queried by the web.

## Invariants

- No runtime aggregation in any query feeding the web layer (ADR-0004).
- Pitch-level Statcast stays internal to the ETL.

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][database]`.

## Open Questions

- Exact table names and column lists for each of the 9 entities.
- Whether MLB-specific extensions to `common.*` should be namespaced to `mlb.*` (for example, `mlb.player_line_patterns` if that concept applies to baseball).
- Storage format for Statcast intermediate data: Azure Blob (the existing `schnappmlbdata` container, ~4.17 GB Parquet) vs. Azure SQL. The current separation is intentional; the decision stays deferred until a concrete use case appears.
