# Changelog

Append-only. Newest at top. One entry per session-end.

Format:

```
YYYY-MM-DD [scope][component] One-line summary. See /path/README.md.
```

Tag taxonomy is defined in `/docs/README.md`. Filter entries by relevant tags rather than reading top-to-bottom.

Historical entries from before the documentation restructure are archived in the legacy `/CHANGELOG.md` at the repo root. That file is no longer appended to as of the migration date below; new work lands here.

---

2026-04-20 [nba][docs][infra] Step 4 post-migration audit and fixes: corrected VM size to Standard B1s in infrastructure/README.md; fixed etl/nba/README.md grading file paths (grade_props.py lives in /grading/ not /etl/, and nba_grading.py does not exist) plus rewrote grade component descriptions (momentum_grade is personal lag-1 transitions not log-scaled streaks, pattern_grade is pattern_strength scaled not a reversal rate) and documented bracket expansion + four grading modes; fixed web/nba/README.md Files section (TodayPropsSection and MatchupsTab are not standalone) and added signal logic section distinguishing player-level DUE from line-level SLUMP; added outcome column to database/nba/README.md common.daily_grades spec; disambiguated three engine variants in etl/_shared/README.md (etl/db.py:get_engine, etl/db.py:get_engine_slow, grading/grade_props.py:get_engine).

2026-04-20 [nba][docs] Completed /docs/ Step 4: migrated NBA content from legacy /PROJECT_REFERENCE.md into etl/nba, web/nba, database/nba, infrastructure, and etl/_shared READMEs. Added ADRs 0005-0010 capturing grading v3, signal redesign, FanDuel-only, two-stage lineup poll, player_line_patterns, and Webshare proxy. Legacy /PROJECT_REFERENCE.md remains until Step 7 retirement.

2026-04-20 [shared][docs] Added /docs/MIGRATION_HANDOFF.md to preserve MLB visual catalog and full migration plan across session boundary. Read before resuming Step 4 or later. File is temporary; delete after Step 7.

2026-04-20 [shared][docs] Completed /docs/ Step 3: added 16 component README skeletons covering ETL, database, web, and infrastructure across all sports. NBA READMEs reference legacy PROJECT_REFERENCE.md as authoritative until Step 4 content migration.

2026-04-20 [shared][docs] Completed /docs/ Step 2: added PRODUCT_BLUEPRINT, CONNECTIONS, GLOSSARY, ROADMAP. Central /docs/ folder is now complete; component README scaffolding starts in Step 3.

2026-04-20 [shared][docs] Created /docs/ foundation: README router, SESSION_PROTOCOL, CHANGELOG (this file), DECISIONS with ADRs 0001-0004. Legacy PROJECT_REFERENCE.md and root CHANGELOG.md remain in place during migration. See /docs/DECISIONS.md ADR-0001.
