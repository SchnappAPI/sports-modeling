# MLB Web

**STATUS:** design phase. Visuals cataloged from `mlbSavantV3.pbix`. Page consolidation decided (ADR-0003). No pages built yet.

## Purpose

MLB pages implementing the product blueprint for baseball.

## Files

No MLB web components yet.

## Key Concepts

Per `/docs/DECISIONS.md` ADR-0003, the pages to build:

- **Game** - selector and nav; entry point for a single matchup
- **Player Analysis** - consolidates the legacy PBI pages New, Extra, Criteria, and MAIN into one page. Visuals: predictions table, per-game log, per-at-bat log, HR pattern card, VS pitcher career card and table, pitcher season stats, team overview pivot, platoon split pivot
- **EV** - exit velocity team view
- **VS** - lineup-wide career matchup
- **Proj** - lineup projections
- **Pitcher Analysis** - pitcher counterpart to Player Analysis (formerly "Duplicate of Extra" in the PBI)

All visuals read from pre-aggregated tables per ADR-0004. No runtime aggregation of Statcast in queries.

The connected-visual pattern applies: selecting a batter on the Player Analysis page updates every visual on that page simultaneously. See `/docs/PRODUCT_BLUEPRINT.md`.

## Invariants

- Single Player Analysis page, not four near-duplicates (ADR-0003)
- No runtime aggregation in queries feeding the web layer (ADR-0004)
- The shared shell from `/web/_shared/` is not modified to accommodate MLB-specific stat columns

## Recent Changes

See `/docs/CHANGELOG.md` filtered by `[mlb][web]`.

## Open Questions

- Internal section layout of Player Analysis (consolidation is decided; section order and grouping are not)
- Whether Hot/Cold zones and spray chart belong on Player Analysis or a separate visualization page
- Mobile layout for the 13-zone hot/cold grid
