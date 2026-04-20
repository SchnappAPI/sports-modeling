# Architecture Decision Records

Append-only. New ADRs get the next sequential number. ADRs are never rewritten, only superseded by a later ADR that references them.

Format:

```
ADR-NNNN [scope][component] Title
Date: YYYY-MM-DD
Context: Why this decision came up.
Decision: What was decided.
Consequences: What this implies for future work.
Supersedes: ADR-XXXX (optional)
```

---

## ADR-0001 [shared][docs] Adopt co-located README structure with central /docs/
Date: 2026-04-20

Context: A single PROJECT_REFERENCE.md at repo root grew past 30,000 characters. Updates between sessions took several minutes because the file had to be partially rewritten on each change. Sessions on one sport repeatedly reverted recent changes on another sport because the whole file was in context. Re-deriving troubleshooting steps was common because the why behind decisions was buried in long sections.

Decision: Adopt a structure where:

- Cross-cutting docs live in `/docs/` (README router, SESSION_PROTOCOL, CHANGELOG, DECISIONS, PRODUCT_BLUEPRINT, CONNECTIONS, GLOSSARY, ROADMAP).
- Component-specific docs live next to the code (`/etl/<sport>/README.md`, `/web/<sport>/README.md`, etc.) using a fixed template with STATUS, Purpose, Files, Key Concepts, Invariants, Recent Changes, Open Questions sections.
- CHANGELOG is read first every session and filtered by tag instead of reading any monolithic doc.
- DECISIONS captures the why, append-only.

Consequences:

- Session-end updates become a single-line CHANGELOG append plus an optional small `str_replace` on a component INVARIANTS section. Seconds instead of minutes.
- Cross-sport contamination during sessions ends because tag filtering scopes the read.
- The revert problem ends because INVARIANTS sections are explicit and CHANGELOG recency is read first.
- During the migration, both old and new systems coexist. Old PROJECT_REFERENCE.md and root CHANGELOG.md are not deleted until all content has been ported.

Migration order: foundation files first (this commit), then component skeletons, then NBA content migration from PROJECT_REFERENCE.md, then MLB content from the design conversation that produced ADRs 0003 and 0004, then NFL placeholder, then automation scripts, then legacy file removal.

---

## ADR-0002 [shared][docs] Code files stay flat in /etl/; doc subfolders are additive only
Date: 2026-04-20

Context: The original documentation structure proposal included `/etl/nba/`, `/etl/mlb/`, `/etl/nfl/` as full subfolders containing both code and docs. The actual repo has `/etl/` as a flat directory with files like `etl/nba_etl.py`, `etl/mlb_etl.py`, `etl/nfl_etl.py`, `etl/odds_etl.py`, `etl/grading.yml` etc. All 27 GitHub Actions workflows reference these files directly by their flat paths.

Decision: Code files do not move. Per-sport documentation lives in newly created subfolders alongside the existing flat code files: `etl/nba/README.md` exists in the same directory as `etl/nba_etl.py`. The subfolder is purely additive. The same approach applies to `web/`, `database/`, and `infrastructure/` where similar flat layouts exist.

Consequences:

- Zero risk of breaking workflows or imports during the migration.
- Future code reorganization (for example, moving `etl/nba_etl.py` into `etl/nba/etl.py`) remains an option but is decoupled from the documentation work and would require updating workflow references in lockstep.
- Doc subfolders coexist with flat code files in the same directory listing. Visually unusual but functionally clean.

---

## ADR-0003 [mlb][web] Single Player Analysis page replaces PBI's New, Extra, Criteria, MAIN
Date: 2026-04-20

Context: The legacy Power BI file `mlbSavantV3.pbix` contained four pages with nearly identical content: New, Extra, Criteria, and MAIN. They were iterations developed without cleanup of older versions. All four had the same set of visuals (predictions table, per-game log, per-at-bat log, HR pattern card, VS pitcher career card and table, pitcher season stats, team overview pivot, platoon split pivot) with minor layout variations. The pages confirmed by Austin to keep from his last mobile session were Game, New, Extra, Criteria, EV, MAIN, VS, and Proj, but New, Extra, Criteria, and MAIN are functionally one page.

Decision: The web app implements one Player Analysis page that consolidates the New, Extra, Criteria, and MAIN concepts. The exact page name in code is to be decided during the web build but conceptually it is "the player analysis view." Old PBI page names are not preserved.

Consequences:

- Single React component to build and maintain instead of four near-duplicates.
- Anyone referencing "the New page" or "Criteria page" from PBI history should be redirected to Player Analysis.
- The pages to keep, mapped to web concepts: Game (selector and nav), Player Analysis (formerly New, Extra, Criteria, and MAIN), EV (exit velocity team view), VS (lineup-wide career matchup), Proj (lineup projections), and a Pitcher Analysis page (pitcher counterpart to Player Analysis; was prototyped as "Duplicate of Extra" in PBI).

---

## ADR-0004 [mlb][etl][web] All MLB visual stats pre-aggregated, none computed at query time
Date: 2026-04-20

Context: The legacy Power BI file used DAX measures to aggregate Statcast pitch-level data into per-player, per-game, per-at-bat, and rolling-window views at display time. Power BI can do this because it loads the entire fact table into memory and computes against it interactively. A web app cannot afford runtime aggregation of pitch-level data on every page load: latency would be unacceptable and the database would be hammered.

Decision: The MLB ETL pre-aggregates all visual-feeding entities and stores them in dedicated tables. The pitch-level Statcast data remains an ETL-internal source and is not queried by the web app. The 9 entities identified during the visual catalog are: upcoming games, batter context per game, batter projections per game, player game stats, player at-bat stats, player trend/pattern stats, player platoon splits, career batter vs pitcher matchup, and pitcher season stats. These will be defined in detail in `/database/mlb/README.md` once that file is created during the migration.

Consequences:

- ETL becomes more complex: it must produce 9 derived tables on a recurring schedule, with rolling windows recomputed whenever new game data lands.
- Web pages stay fast because every visual reads from a purpose-built table with the data already shaped.
- Adding a new visual stat requires an ETL change to add a column or table, not just a query change.
- The legacy Power Query M code that performed equivalent transformations in PBI is preserved for reference but not for direct reimplementation; Python ETL targets the same final state but is structured differently.
