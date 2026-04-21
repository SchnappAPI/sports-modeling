# Changelog

Append-only. Newest at top. One entry per session-end.

Format:

```
YYYY-MM-DD [scope][component] One-line summary. See /path/README.md.
```

Tag taxonomy is defined in `/docs/README.md`. Filter entries by relevant tags rather than reading top-to-bottom.

Historical entries from before the documentation restructure are preserved at `/docs/_archive/CHANGELOG.md`. That file is no longer appended to; new work lands here.

---

2026-04-21 [nfl][etl] Replaced deprecated `datetime.utcnow()` with `datetime.now(timezone.utc)` in `etl/nfl_etl.py` ahead of first NFL ETL run. Single call site in `current_nfl_season()` (handoff note said 9, actual count was 1). Import updated to include `timezone`. No behavioral change: both expressions produce the same year/month for the season comparison. See etl/nfl/README.md.

2026-04-20 [shared][docs] Completed /docs/ Step 7: retired legacy /PROJECT_REFERENCE.md and root /CHANGELOG.md. Both files moved to /docs/_archive/ via git mv so commit history is preserved as rename, not delete + add. Archived files got an ARCHIVED banner pointing readers at the active docs. Deleted /docs/MIGRATION_HANDOFF.md (its job was done). Swept 15 files to replace stale "legacy root /CHANGELOG.md" references with /docs/_archive/CHANGELOG.md. Updated /docs/README.md, /docs/SESSION_PROTOCOL.md, /docs/ROADMAP.md to reflect completion rather than in-progress status. Added ADR-0016 documenting the archive (not delete) decision and the rationale (756 lines of genuine engineering history in the old CHANGELOG that was not migrated into /docs/CHANGELOG.md). The documentation restructure described in ADR-0001 is now complete.

2026-04-20 [nfl][docs][etl][database][web] Completed /docs/ Step 6: updated NFL READMEs from the "planning / everything is TBD" skeletons to reflect actual state. etl/nfl_etl.py turned out to be fully written (14 KB, 7 tables via nflreadpy: games, players, player_game_stats, snap_counts, ftn_charting, rosters_weekly, team_game_stats) but never wired to a workflow, so no nfl.* tables exist yet. etl/nfl/README.md now documents schema-from-data inference, clean_df global cleanup, per-table upsert keys, fail-soft error handling, and the local get_engine inconsistency with etl/db.py. database/nfl/README.md documents target schema of all 7 tables with pandas-to-SQL type map and the multi-identifier-system reality of NFL data (gsis_id canonical, pfr_player_id, ftn_player_id, espn_id separate). web/nfl/README.md kept intentionally short because no web surface exists, with specific pointers for where to start when work begins. Added ADR-0014 (schema-from-data is NFL-only) and ADR-0015 (nflreadpy as single source). Primary open question captured in docs: no nfl-etl.yml workflow exists, so the first-ever NFL ETL run is blocked on creating that file.

2026-04-20 [mlb][docs][etl][database][web] Completed /docs/ Step 5: migrated MLB content from legacy PROJECT_REFERENCE.md and from direct source reads into etl/mlb, database/mlb, and web/mlb READMEs. MLB turned out to be further along than the Step 3 skeletons suggested. etl/mlb/README.md documents 7 nightly tables (teams, players, games, batting_stats, pitching_stats, player_season_batting, pitcher_season_stats) plus on-demand mlb.play_by_play. database/mlb/README.md has column-level specs for all 8 tables with PK formats, primary key rationale, and the 5 ADR-0004 derived entities still missing. web/mlb/README.md captures the live /mlb page (date picker, game strip, Box Score + Exit Velo tabs, four /api/mlb-* routes) and the 6-page ADR-0003 roadmap with status per page. Added ADRs 0011 (mlb.games mixed-state), 0012 (pitch-level stays ETL-internal), 0013 (direct INSERT not MERGE for play-by-play).

2026-04-20 [nba][web][infra] Fixed stale /api/games Flask URL. RUNNER_URL was hardcoded to the old VM's IP (20.109.181.21, retired with schnapp-runner on West US 2 B2s_v2), which silently broke live CDN score overlay on the NBA page (routes always fell through to source='db'). Changed to https://live.schnapp.bet to match /api/scoreboard and /api/live-boxscore which already use the Cloudflare-proxied subdomain. Verified via MCP: both /ping and /scoreboard return correctly through the subdomain. Also updated web/nba/README.md and infrastructure/README.md to document the live.schnapp.bet subdomain, the proxied-vs-DNS-only split across all four schnapp.bet subdomains, and a matching invariant that web routes must never hardcode VM IPs.

2026-04-20 [nba][docs][infra] Second audit pass verified against source: Flask binds to 0.0.0.0:5000 (not 127.0.0.1), and /ping is unauthenticated (only /scoreboard and /boxscore check X-Runner-Key). Corrected infrastructure/README.md. /api/games does not drive the game list from Flask; the DB is always source of truth and CDN is overlay-only on today's live games. Corrected web/nba/README.md and added the full API route catalog plus the Central-vs-Eastern timezone split between web and ETL. Flagged /api/games stale Flask IP (20.109.181.21 vs 172.173.126.81) as an open question.

2026-04-20 [nba][docs][infra] Step 4 post-migration audit and fixes: corrected VM size to Standard B1s in infrastructure/README.md; fixed etl/nba/README.md grading file paths (grade_props.py lives in /grading/ not /etl/, and nba_grading.py does not exist) plus rewrote grade component descriptions (momentum_grade is personal lag-1 transitions not log-scaled streaks, pattern_grade is pattern_strength scaled not a reversal rate) and documented bracket expansion plus four grading modes; fixed web/nba/README.md Files section (TodayPropsSection and MatchupsTab are not standalone) and added signal logic section distinguishing player-level DUE from line-level SLUMP; added outcome column to database/nba/README.md common.daily_grades spec; disambiguated three engine variants in etl/_shared/README.md (etl/db.py:get_engine, etl/db.py:get_engine_slow, grading/grade_props.py:get_engine).

2026-04-20 [nba][docs] Completed /docs/ Step 4: migrated NBA content from legacy /PROJECT_REFERENCE.md into etl/nba, web/nba, database/nba, infrastructure, and etl/_shared READMEs. Added ADRs 0005-0010 capturing grading v3, signal redesign, FanDuel-only, two-stage lineup poll, player_line_patterns, and Webshare proxy. Legacy /PROJECT_REFERENCE.md remains until Step 7 retirement.

2026-04-20 [shared][docs] Added /docs/MIGRATION_HANDOFF.md to preserve MLB visual catalog and full migration plan across session boundary. Read before resuming Step 4 or later. File is temporary; delete after Step 7.

2026-04-20 [shared][docs] Completed /docs/ Step 3: added 16 component README skeletons covering ETL, database, web, and infrastructure across all sports. NBA READMEs reference legacy PROJECT_REFERENCE.md as authoritative until Step 4 content migration.

2026-04-20 [shared][docs] Completed /docs/ Step 2: added PRODUCT_BLUEPRINT, CONNECTIONS, GLOSSARY, ROADMAP. Central /docs/ folder is now complete; component README scaffolding starts in Step 3.

2026-04-20 [shared][docs] Created /docs/ foundation: README router, SESSION_PROTOCOL, CHANGELOG (this file), DECISIONS with ADRs 0001-0004. Legacy PROJECT_REFERENCE.md and root CHANGELOG.md remain in place during migration. See /docs/DECISIONS.md ADR-0001.
