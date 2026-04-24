# Roadmap

Deliberately brief. Detailed task tracking lives in component READMEs under "Open Questions" sections.

## Active

- **NBA Trends Grid** (live, 2026-04-23). Seventh tab on `/nba?gameId=...&tab=trends`. Surfaces `common.player_tier_lines` (ADR-20260423-1) as a per-game grid with Safe / Value / High Risk / Lotto line values, current FanDuel standard line, and per-game stat history colored against that standard line. URL-driven state (`?stat=`, `?window=`). See `web/nba/README.md`.
- **Documentation restructure** (complete, 2026-04-20). Migrated from monolithic `PROJECT_REFERENCE.md` to co-located component READMEs with central `/docs/`. See ADR-0001 and ADR-0016. Archived originals at `/docs/_archive/`.
- **NFL ETL** (idle, 2026-04-21). 7 tables populated via `nflreadpy` on a Tuesday 09:00 UTC schedule. No downstream product consumer — the pipeline runs unattended. See `etl/nfl/README.md`.
- **MLB derived entities** (2 of 5 missing entities materialized, 2026-04-21). `mlb.player_at_bats` and `mlb.career_batter_vs_pitcher` both materialized in-lockstep with `mlb.play_by_play`. Both have backing web routes (`/api/mlb-atbats`, `/api/mlb-bvp`) that are in development, not live. Three remain: batter context per game, batter projections per game, and player trend/pattern stats plus player platoon splits (may share a table). ADR-0018 and ADR-0019 establish the two patterns the remaining entities should follow — direct INSERT for append-only grains, staged MERGE for aggregate grains where rows need UPDATE on re-aggregation.
- **MLB VS page** (in development, 2026-04-21). Second ADR-0003 page to reach the repo. `/api/mlb-bvp` + `MlbVsView.tsx` + view-switcher integration in `MlbPageInner.tsx`. Shows each team's starting lineup vs opposing SP with lifetime BvP stats. See `web/mlb/README.md`.
- **MLB EV team view page** (in development, 2026-04-21). Third ADR-0003 page to reach the repo. `/api/mlb-ev` + `MlbEvView.tsx` + view-switcher toggle. Season-to-date exit velocity summary for both teams' starters, excluding the current game; tap-to-expand per-at-bat detail. No new materialized table — indexed aggregation on `mlb.player_at_bats` at request time. See `web/mlb/README.md`.

## Next up

- **Initiative A: Data integrity and completeness framework** (design phase, 2026-04-24). Three-layer system: invariants at write time (quarantine bad rows before they land), nightly mapping resolver (auto-fix entity-match gaps, surface unresolvable ones), and daily retry with 3-attempt cap for upstream-lag cases. Unresolved rows after 3 attempts become GitHub Issues with `data-integrity:*` labels. `docs/HEALTH.md` as zero-disruption reference surface. See ADR-20260424-2 for full design.
- **Initiative D: MLB player table strategy**. Resolve the 20-32% NULL name rate on `mlb.player_at_bats` and `mlb.career_batter_vs_pitcher` joins. Blocked on A (applies A's framework to the MLB-specific pain point). Decision-with-ADR when D activates.
- **Initiative B: Code reuse / DRY pass**. Unify `get_engine()` between `etl/nfl_etl.py` and `etl/db.py`; promote reusable helpers once A's shared-module pattern is established; consolidate duplicate workflow YAMLs where duplication exceeds 80%. Blocked on A establishing the shared-module pattern.
- **Initiative E: ROADMAP structure**. Add a "Completed and idle" section so finished-but-inactive items (NFL ETL today) have somewhere natural to live. 20-minute doc change. Blocked on A and D for ordering reasons only.
- **Initiative C: Observability layer**. Nightly health check on A's tables: workflow failures in last 24h, stale schedules, row-count sanity. Blocked on A (needs A's tables) and benefits from B (reduces false-positive noise).
- **NBA lineup backfill for completed games**. Rewrite `nba.daily_lineups` rows for all `game_status = 3` games using the NBA daily lineups JSON as the authoritative source, so every completed game has `lineup_status = 'Confirmed'` with Starter / Bench / Inactive roles per NBA data, not our live-poll snapshot. Unblocks availability gating in tier-line generation. Investigation on 2026-04-24 found that 80% of DNP tier rows either had no lineup row at all or were flagged Inactive when tier lines were generated; another batch of sub-20-minute rows came from bench players whose KDE fit assumed full minutes. See CHANGELOG 2026-04-24 and `etl/nba/README.md` open questions.
- **NBA tier-model minutes prior / rotation-role identification**. Paused 2026-04-24 pending lineup backfill. Use `nba.player_box_score_stats` to compute each player's season-level mean minutes when not starting, grouped by team, to identify rotation roles (6th man, 7th man, end-of-bench). Feed this as a pre-game minutes prior into `compute_kde_tier_lines` so the distribution is conditioned on expected playing time rather than all-games-equal. Daily lineups JSON cannot supply this: it has positions only for starters, so bench and inactive players have no position data. See `etl/nba/README.md` open questions.
- **MLB Player Analysis page**. Fourth ADR-0003 page. Partially data-unblocked (at-bat access and career BvP access are both built but in development) but the core trend/pattern visuals need `player_trend_stats` first, so this is blocked on ETL. Could start the page shell without those visuals if the priority shifts.
- **MLB `player_trend_stats` materialization**. Highest-value remaining derived entity; unblocks Player Analysis. Likely staged-MERGE pattern like career-BvP since rolling windows recompute as new games land.
- **NFL web surface**. ETL pipeline runs but no web layer exists yet and no active work is happening on NFL. Parallel design session like the MLB visual catalog: identify what visuals matter, what stats feed them, what the pre-aggregation layer needs to produce. See `web/nfl/README.md`.
- **NFL odds ingestion**. `odds_etl.py` reportedly mentions NFL sport keys but has not been verified. Decide whether to extend it or add a dedicated `nfl-odds-etl.py`. See `etl/nfl/README.md` open questions.

## On the horizon (no active work yet)

- **Subscription / payment layer**. Stripe is the likely choice. Architecture is scoped but not started. Triggers a passcode model rework since payment-gated access replaces the current passcode-gate.
- **MLB pattern quality monitoring**. Once MLB grading is live and the MLB equivalent of `common.player_line_patterns` populates, NBA-style monitoring should follow.
- **PWA pinning**. Home screen URL should pin to `schnapp.bet/nba` (clean URL) rather than a date-specific path. Minor task; defer until the PWA install flow gets attention.

## Decisions deferred

These came up during planning conversations and were explicitly not decided:

- **Multi-bookmaker support**. FanDuel only for now. Rationale captured in ADR-0007.
- **Public Statcast Excel exports vs. live API for MLB historical data**. Currently both exist (`mlb-data/mlbSavantStatcast-2024-25.xlsx` etc. on local Windows machine). Need to decide whether the ETL relies on local Excel exports or pulls fresh from Savant for historical seasons.

## 2026-04-24 Phase 3 update

Initiative A (data integrity framework) Phase 3 complete. Catalog reconciled: 272,703 → 29 violations, all remaining are known All-Star / non-team-sport edges. MLB odds daily cadence restored.

**Active:**
- ADR-20260424-5 code-complete and smoke-tested (run 24914385748 wrote 264 tier_lines cleanly). Next: force-regrade last 30 days via grading.yml backfill mode with force=true, then inspect common.grade_calibration buckets and new tier_lines columns (hits_all, hits_20, recent_opportunity, historical_opportunity) to validate.
- Tier-line discretion work must be re-evaluated after backfill: confirm calibrator fits produce sensible buckets, confirm opportunity signals surface breakout cases (rising recent_opportunity vs historical_opportunity), confirm no regressions in volume.
- Monitor 10:00 UTC odds-etl.yml for MLB upcoming ingest (ongoing watch since the cron --sport all change).

**Next-up (in order):**
1. Implement ADR-20260424-4 once questions A-D are answered. Estimate: one working session for compute_kde_tier_lines rewrite + calibration script + 30-day backfill.
2. ADR for sample-size/eligibility-gate redesign (item #1 from 2026-04-24 discussion). Replaces fixed-60-game hit_rate window with time-based window + rotation-role gate.
3. Historical mapping backfill pass (item #4): 810 unmapped player names + 3,310 unmapped event_game_map rows. Writes a resolver pass using expanded name-normalization and cross-season roster lookup.
4. Phase 5 wire validate_and_filter() into NBA ETL / odds ETL / grading. Deferred behind tier-line and sample-size work so those changes inherit enforcement rather than fight it.

**Deferred:**
- Initiative D (MLB null-name fix) — folded into item #3 / item #4 historical backfill.
- Initiative E (ROADMAP structure standardization) — low priority; lived with current format for now.
- MLB grading engine build — large, depends on #1 and #2 settling first so MLB inherits the improved design.

