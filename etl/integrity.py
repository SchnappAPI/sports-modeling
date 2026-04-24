# etl/integrity.py
"""
Data integrity and completeness framework.

See /docs/DECISIONS.md ADR-20260424-2 for the full design. This module
implements Layer 1 (invariants enforced at write time), the state tables
for Layer 2 (mapping resolver) and Layer 3 (daily retry), and helpers
consumed by the daily health report workflow.

Three layers, each addressing a distinct failure mode:

1. Layer 1 — per-row column-level invariants. Rows violating CRITICAL_FIELDS
   rules are written to `common.ingest_quarantine` instead of the destination
   table. Opt-in: tables not listed in CRITICAL_FIELDS pass through unchanged.

2. Layer 2 — entity mapping gaps tracked in `common.unmapped_entities`.
   Populated by ETL scripts when a source-feed entity (e.g., an odds-feed
   player name) cannot be resolved to a canonical id. Nightly resolver
   workflow attempts deterministic matches; escalates to a GitHub Issue
   after 3 attempts.

3. Layer 3 — daily retry state in `common.data_completeness_log`. Populated
   by the retroactive scan and by quarantine entries. The retry workflow
   reattempts the missing data once per day, max 3 attempts, then escalates.

Usage in an ETL script:

    from etl.integrity import validate_and_filter

    valid_rows = validate_and_filter(rows, 'nba.schedule', engine,
                                     source_workflow='nba-etl.yml')
    upsert(engine, pd.DataFrame(valid_rows), 'nba', 'schedule', keys=['game_id'])

HEALTH.md generation and retroactive scans live in separate functions
consumed by `daily-health-report.yml`.

Invariants documented in ADR-20260424-2:
- Stat-zero is NOT stat-null. PTS=0 is a valid value; PTS=NULL is a violation.
- "Did this player play?" is answered by MIN > 0 or starter_status != 'Inactive',
  never by stat-zero inference.
- Retroactive scan flags existing violations but does NOT move production rows.
- Successful Layer-1 validation clears prior quarantine + log entries for the
  same (table, row_key), regardless of which resolution path fixed the row.
"""

import json
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import bindparam, text

# Binary player-prop markets — yes/no props where outcome_point is always NULL
# (first TD scorer, first basket, double-double, etc.). Used by CRITICAL_FIELDS
# predicates for odds.player_props and odds.upcoming_player_props.
_BINARY_PLAYER_MARKETS = frozenset([
    "batter_first_home_run",
    "player_1st_td",
    "player_anytime_td",
    "player_double_double",
    "player_first_basket",
    "player_last_td",
    "player_triple_double",
])
_BINARY_PLAYER_MARKETS_SQL = "(" + ",".join(
    f"'{m}'" for m in sorted(_BINARY_PLAYER_MARKETS)
) + ")"



# =====================================================================
# CRITICAL_FIELDS catalog
# =====================================================================
# Populated during the cataloging pass (Phase 2 of Initiative A).
# Structure per table:
#
#   'schema.table': {
#       'row_key': [pk_col, ...],           # columns that form the row identifier
#       'always_required': [col, ...],      # must never be NULL
#       'required_when': {
#           col: {
#               'py_predicate': callable(row) -> bool,   # write-time enforcement
#               'sql_predicate': 'SQL fragment using row columns',  # scan-time
#               'description': 'Human-readable reason',
#           },
#           ...
#       },
#   }
#
# Keep empty until Phase 2 so no existing ETL is affected by the module
# landing. Opt-in enforcement means tables not listed here pass through.
CRITICAL_FIELDS: Dict[str, Dict[str, Any]] = {

    # =================================================================
    # nba schema
    # =================================================================

    "nba.schedule": {
        "row_key": ["game_id"],
        "always_required": [
            "game_id", "game_date", "game_status",
            "home_team_id", "away_team_id",
            "home_team_tricode", "away_team_tricode",
        ],
        "required_when": {
            "home_score": {
                "py_predicate": lambda r: r.get("game_status") is not None and int(r["game_status"]) >= 2,
                "sql_predicate": "t.game_status >= 2",
                "description": "home_score required once game has started or finished",
            },
            "away_score": {
                "py_predicate": lambda r: r.get("game_status") is not None and int(r["game_status"]) >= 2,
                "sql_predicate": "t.game_status >= 2",
                "description": "away_score required once game has started or finished",
            },
        },
    },

    "nba.games": {
        "row_key": ["game_id"],
        "always_required": [
            "game_id", "game_date",
            "home_team_id", "away_team_id",
            "home_score", "away_score",
        ],
        "required_when": {},
    },

    "nba.teams": {
        "row_key": ["team_id"],
        "always_required": ["team_id", "team_tricode", "team_name"],
        "required_when": {},
    },

    "nba.players": {
        "row_key": ["player_id"],
        "always_required": ["player_id", "player_name"],
        "required_when": {
            "team_id": {
                "py_predicate": lambda r: r.get("roster_status") == 1,
                "sql_predicate": "t.roster_status = 1",
                "description": "active-roster players (roster_status=1) must have team_id",
            },
            "team_tricode": {
                "py_predicate": lambda r: r.get("roster_status") == 1,
                "sql_predicate": "t.roster_status = 1",
                "description": "active-roster players (roster_status=1) must have team_tricode",
            },
        },
    },

    "nba.daily_lineups": {
        # Keyed by player_name + team_tricode per database/nba/README.md.
        # No player_id on this table.
        "row_key": ["game_id", "player_name", "team_tricode"],
        "always_required": [
            "game_id", "game_date", "player_name", "team_tricode",
            "starter_status", "lineup_status",
        ],
        "required_when": {
            # Per etl/nba/README.md: position is populated only for starters.
            # Bench and inactive rows legitimately have position empty or NULL
            # because NBA daily lineups JSON + boxscorepreviewv3 set position
            # only on the five starters per team.
            "position": {
                "py_predicate": lambda r: r.get("starter_status") == "Starter",
                "sql_predicate": "t.starter_status = 'Starter'",
                "description": "starters must have position; bench/inactive legitimately null",
            },
        },
    },

    "nba.player_box_score_stats": {
        # Row exists only for players who appeared. DNP players produce no row.
        # Every stat column is 0-or-positive; NULL is a violation, not a DNP
        # signal. "Did this player play?" is answered by minutes > 0, never
        # by stat-zero inference (ADR-20260424-2).
        "row_key": ["game_id", "player_id", "period"],
        "always_required": [
            "game_id", "player_id", "period", "team_id", "minutes",
            "pts", "reb", "ast", "stl", "blk", "tov", "pf",
            "fgm", "fga", "ftm", "fta", "fg3m", "fg3a",
            "oreb", "dreb",
        ],
        "required_when": {},
    },

    "nba.player_passing_stats": {
        "row_key": ["player_id", "game_date"],
        "always_required": ["player_id", "game_date", "potential_ast"],
        "required_when": {},
    },

    "nba.player_rebound_chances": {
        "row_key": ["player_id", "game_date"],
        "always_required": ["player_id", "game_date", "reb_chances"],
        "required_when": {},
    },

    # =================================================================
    # odds schema (cross-sport; NBA-primary today, MLB uses same tables)
    # =================================================================

    "odds.event_game_map": {
        # game_id is not always_required: 3,310 historical rows (2023-2025) have
        # NULL game_id from periods before schedule coverage. Forward-looking
        # mapping gaps are covered by the nba_events_mapped_24h relational check
        # and by the mapping resolver (Layer 2). Historical backfill is tracked
        # separately — see ROADMAP.
        "row_key": ["event_id"],
        "always_required": ["event_id", "sport_key", "game_date"],
        "required_when": {},
    },

    "odds.upcoming_events": {
        # game_id not always_required: unmapped events get written before
        # mapping resolves. Unmapped entities are tracked via
        # record_unmapped_entity on common.unmapped_entities.
        "row_key": ["event_id"],
        "always_required": [
            "event_id", "sport_key",
            "home_tricode", "away_tricode",
            "commence_time",
        ],
        "required_when": {},
    },

    "odds.upcoming_player_props": {
        # link is nullable: populated only from per-event Odds API endpoint;
        # bulk-endpoint rows legitimately have NULL link per docs.
        # outcome_point: see _BINARY_PLAYER_MARKETS — binary yes/no markets
        # have no line value.
        "row_key": ["event_id", "market_key", "outcome_point", "player_name", "outcome_name", "snap_ts"],
        "always_required": [
            "event_id", "market_key", "player_name",
            "bookmaker_key", "snap_ts", "outcome_price", "outcome_name",
        ],
        "required_when": {
            "outcome_point": {
                "py_predicate": lambda r: r.get("market_key") not in _BINARY_PLAYER_MARKETS,
                "sql_predicate": f"t.market_key NOT IN {_BINARY_PLAYER_MARKETS_SQL}",
                "description": "outcome_point required for line-based markets, not binary yes/no",
            },
        },
    },

    "odds.player_props": {
        # outcome_point is NULL for binary yes/no player markets (first TD,
        # first basket, double-double, triple-double, first home run). For all
        # line-based markets it is required. See _BINARY_PLAYER_MARKETS.
        "row_key": ["event_id", "market_key", "outcome_point", "player_name", "outcome_name", "snap_ts"],
        "always_required": [
            "event_id", "market_key", "player_name",
            "bookmaker_key", "snap_ts", "outcome_price", "outcome_name",
        ],
        "required_when": {
            "outcome_point": {
                "py_predicate": lambda r: r.get("market_key") not in _BINARY_PLAYER_MARKETS,
                "sql_predicate": f"t.market_key NOT IN {_BINARY_PLAYER_MARKETS_SQL}",
                "description": "outcome_point required for line-based markets, not binary yes/no",
            },
        },
    },

    "odds.upcoming_game_lines": {
        "row_key": ["event_id", "market_key", "outcome_name", "outcome_point", "snap_ts"],
        "always_required": [
            "event_id", "market_key", "bookmaker_key", "snap_ts",
            "outcome_price", "outcome_name",
        ],
        "required_when": {
            "outcome_point": {
                # Spreads and totals have a point; moneyline (h2h) does not.
                "py_predicate": lambda r: r.get("market_key") in ("spreads", "totals"),
                "sql_predicate": "t.market_key IN ('spreads', 'totals')",
                "description": "spread/total lines require outcome_point; h2h legitimately null",
            },
        },
    },

    "odds.game_lines": {
        "row_key": ["event_id", "market_key", "outcome_name", "outcome_point", "snap_ts"],
        "always_required": [
            "event_id", "market_key", "bookmaker_key", "snap_ts",
            "outcome_price", "outcome_name",
        ],
        "required_when": {
            "outcome_point": {
                "py_predicate": lambda r: r.get("market_key") in ("spreads", "totals"),
                "sql_predicate": "t.market_key IN ('spreads', 'totals')",
                "description": "spread/total lines require outcome_point; h2h legitimately null",
            },
        },
    },

    "odds.player_map": {
        # Canonical mapping between odds-feed player names and player_ids.
        # match_method = 'no_match' is a legitimate sentinel state meaning
        # "we tried, found nothing". Those rows have NULL player_id +
        # matched_name by design — they are Layer-2 tracking, not broken data.
        "row_key": ["odds_player_name", "sport_key"],
        "always_required": ["odds_player_name", "sport_key", "match_method"],
        "required_when": {
            "player_id": {
                "py_predicate": lambda r: r.get("match_method") != "no_match",
                "sql_predicate": "t.match_method <> 'no_match'",
                "description": "player_id required when we successfully matched the name",
            },
            "matched_name": {
                "py_predicate": lambda r: r.get("match_method") != "no_match",
                "sql_predicate": "t.match_method <> 'no_match'",
                "description": "matched_name required when we successfully matched",
            },
        },
    },

    # =================================================================
    # common schema
    # =================================================================

    "common.teams": {
        # Cross-sport team reference. sport_key distinguishes NBA/MLB/NFL.
        # tricode is the 3-letter abbr; team_name is the full name.
        # participant_id is the canonical id used by downstream consumers.
        "row_key": ["team_id", "sport_key"],
        "always_required": ["team_id", "sport_key", "tricode", "team_name", "participant_id"],
        "required_when": {},
    },

    "common.user_codes": {
        # mode distinguishes live/demo codes; active is the enable flag.
        "row_key": ["code"],
        "always_required": ["code", "mode", "active"],
        "required_when": {},
    },

    "common.demo_config": {
        "row_key": ["sport"],
        "always_required": ["sport", "demo_date"],
        "required_when": {},
    },

    "common.player_line_patterns": {
        # p_hit_after_hit / p_hit_after_miss are legitimately NULL when fewer
        # than MIN_TRANSITION_OBS = 3 observations in that state (per
        # etl/nba/README.md). No invariant on them.
        "row_key": ["player_id", "market_key", "line_value"],
        "always_required": [
            "player_id", "market_key", "line_value",
            "n", "hr_overall", "last_updated",
        ],
        "required_when": {},
    },

    "common.daily_grades": {
        # UQ is (grade_date, event_id, player_id, market_key, bookmaker_key,
        # line_value, outcome_name). player_id is declared NULL in schema but
        # enforced required here — a grade row with NULL player_id indicates
        # an unmapped odds-feed player and should not reach production.
        #
        # over_price is legitimately NULL on bracket lines (only center line
        # in a bracket-expanded standard market carries the posted price).
        # composite/trend/matchup/regression/momentum/pattern_grade and all
        # six opportunity_* grades are legitimately NULL under various
        # data-availability conditions (grade_props.py + ADR-20260423-1).
        # outcome is legitimately NULL until games resolve. No invariants
        # on any of these columns.
        #
        # hit_rate_60 and grade are conditional on sample_size_60 > 0. Rows
        # with sample_size_60 = 0 (typically rookies or returning players with
        # no games in the 60-game window) legitimately have NULL hit_rate_60
        # and grade. composite_grade is still computed from other components
        # (trend/matchup/regression) and is not gated on sample size.
        # See 2026-04-24 investigation and the pending redesign of the
        # eligibility gate (ROADMAP: replace fixed 60-game window with
        # % of available games threshold).
        "row_key": ["grade_date", "event_id", "player_id", "market_key", "bookmaker_key", "line_value", "outcome_name"],
        "always_required": [
            "grade_id", "grade_date", "event_id",
            "player_id", "player_name", "game_id",
            "market_key", "bookmaker_key",
            "line_value", "outcome_name",
            "sample_size_60",
        ],
        "required_when": {
            "hit_rate_60": {
                "py_predicate": lambda r: r.get("sample_size_60") is not None and int(r["sample_size_60"]) > 0,
                "sql_predicate": "t.sample_size_60 > 0",
                "description": "hit_rate_60 required only when we have >=1 prior game in the window",
            },
            "grade": {
                "py_predicate": lambda r: r.get("sample_size_60") is not None and int(r["sample_size_60"]) > 0,
                "sql_predicate": "t.sample_size_60 > 0",
                "description": "grade required only when sample_size_60 > 0 (grade uses hit_rate)",
            },
        },
    },

    "common.player_tier_lines": {
        # ALL four tiers (Safe/Value/HighRisk/Lotto) are legitimately NULL
        # when no posted alternate line satisfies the tier threshold. High-
        # composite players often have NULL safe_line because the lowest
        # posted alternate is already past the safe probability cutoff —
        # the model has nothing to recommend at that tier.
        # Tier-line discretion redesign (2026-04-24) will further gate these
        # on reasonableness (no -500+ implied odds, posted-line sanity checks).
        "row_key": ["grade_date", "game_id", "player_id", "market_key"],
        "always_required": [
            "tier_id", "grade_date", "game_id", "player_id", "market_key",
            "kde_window", "blowout_dampened",
        ],
        "required_when": {},
    },
}


# =====================================================================
# RELATIONAL_CHECKS catalog
# =====================================================================
# Cross-table row-count alignment queries, scanned daily by the health
# report workflow. Distinct from Layer 1 because these are row-count
# invariants across tables, not column-level invariants within a row.
#
# Structure:
#   'check_name': {
#       'description': 'Human-readable',
#       'query': 'SELECT ... returning violating rows (COUNT used in report)',
#       'severity': 'warn' | 'error',
#   }
#
# Example (populated in Phase 2):
#   'nba_lineup_coverage': {
#       'description': 'Every started game should have >= 20 daily_lineups rows.',
#       'query': '''
#           SELECT s.game_id, COUNT(dl.player_id) AS lineup_rows
#           FROM nba.schedule s
#           LEFT JOIN nba.daily_lineups dl ON dl.game_id = s.game_id
#           WHERE s.game_status >= 1 AND s.game_date >= DATEADD(day, -30, GETUTCDATE())
#           GROUP BY s.game_id
#           HAVING COUNT(dl.player_id) < 20
#       ''',
#       'severity': 'warn',
#   }
RELATIONAL_CHECKS: Dict[str, Dict[str, Any]] = {

    # Every started game in the last 30 days should have lineup rows for both
    # teams. Typical NBA roster is 13-17 per team; 20 total is a conservative
    # lower bound that catches "we only got the five starters" (seen on the
    # 2026-04-23 playoff regression when boxscorepreviewv3 populated only 10
    # starters per team).
    "nba_lineup_coverage_30d": {
        "description": "nba.schedule games with status>=1 in last 30d should have >=20 daily_lineups rows",
        "query": """
            SELECT s.game_id, s.game_date,
                   COUNT(dl.player_name) AS lineup_rows
            FROM nba.schedule s
            LEFT JOIN nba.daily_lineups dl ON dl.game_id = s.game_id
            WHERE s.game_status >= 1
              AND s.game_date >= DATEADD(day, -30, GETUTCDATE())
            GROUP BY s.game_id, s.game_date
            HAVING COUNT(dl.player_name) < 20
            ORDER BY s.game_date DESC
        """,
        "severity": "warn",
    },

    # Every completed game should have box score rows. Lack = ETL failure.
    "nba_boxscores_missing_30d": {
        "description": "nba.schedule games with status=3 in last 30d should have player_box_score_stats rows",
        "query": """
            SELECT s.game_id, s.game_date
            FROM nba.schedule s
            LEFT JOIN (
                SELECT DISTINCT game_id FROM nba.player_box_score_stats
            ) bs ON bs.game_id = s.game_id
            WHERE s.game_status = 3
              AND s.game_date >= DATEADD(day, -30, GETUTCDATE())
              AND bs.game_id IS NULL
            ORDER BY s.game_date DESC
        """,
        "severity": "error",
    },

    # Every box score player should have a corresponding lineup row for that
    # game. A player appearing in the box score without a lineup row is a gap.
    "nba_boxscore_lineup_alignment_30d": {
        "description": "every box score player should have a daily_lineups row for that game",
        "query": """
            SELECT DISTINCT bs.game_id, bs.player_id, p.player_name
            FROM nba.player_box_score_stats bs
            LEFT JOIN nba.players p ON p.player_id = bs.player_id
            LEFT JOIN nba.daily_lineups dl
                ON dl.game_id = bs.game_id
               AND dl.player_name = p.player_name
            INNER JOIN nba.schedule s ON s.game_id = bs.game_id
            WHERE s.game_date >= DATEADD(day, -30, GETUTCDATE())
              AND dl.player_name IS NULL
        """,
        "severity": "warn",
    },

    # Active-roster players must have a team.
    "nba_active_players_have_team": {
        "description": "nba.players with roster_status=1 must have team_id",
        "query": """
            SELECT player_id, player_name
            FROM nba.players
            WHERE roster_status = 1
              AND team_id IS NULL
        """,
        "severity": "error",
    },

    # Every nba.games row must have a matching schedule entry.
    "nba_games_have_schedule": {
        "description": "every nba.games row must have a matching nba.schedule entry",
        "query": """
            SELECT g.game_id
            FROM nba.games g
            LEFT JOIN nba.schedule s ON s.game_id = g.game_id
            WHERE s.game_id IS NULL
        """,
        "severity": "error",
    },

    # NBA odds events within 24h of commence should have a game_id mapping.
    "odds_nba_events_mapped_24h": {
        "description": "odds.upcoming_events basketball_nba near commence should be mapped",
        "query": """
            SELECT e.event_id, e.commence_time
            FROM odds.upcoming_events e
            LEFT JOIN odds.event_game_map m ON m.event_id = e.event_id
            WHERE e.sport_key = 'basketball_nba'
              AND e.commence_time <= DATEADD(hour, 24, GETUTCDATE())
              AND e.commence_time >= DATEADD(day, -7, GETUTCDATE())
              AND m.event_id IS NULL
        """,
        "severity": "warn",
    },

    # daily_grades and player_tier_lines ship in lockstep per ADR-20260423-1.
    # A grade with no matching tier row = tier computation failed.
    "nba_grades_have_tier_lines_7d": {
        "description": "daily_grades rows in last 7d should have matching player_tier_lines (Over side)",
        "query": """
            SELECT DISTINCT g.grade_date, g.game_id, g.player_id, g.market_key
            FROM common.daily_grades g
            LEFT JOIN common.player_tier_lines t
                ON t.grade_date = g.grade_date
               AND t.game_id = g.game_id
               AND t.player_id = g.player_id
               AND t.market_key = g.market_key
            WHERE g.grade_date >= DATEADD(day, -7, GETUTCDATE())
              AND g.outcome_name = 'Over'
              AND g.player_id IS NOT NULL
              AND g.game_id IS NOT NULL
              AND t.tier_id IS NULL
        """,
        "severity": "warn",
    },
}


# =====================================================================
# DDL for the three framework tables
# =====================================================================
# Single idempotent script. Safe to run repeatedly; CREATE IF NOT EXISTS
# semantics via sys.objects checks. Indexes are filtered on the
# resolved_at IS NULL partition because that is the hot path for every
# workflow that reads these tables.

DDL_STATEMENTS: List[str] = [
    # ----- common.ingest_quarantine -----
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID('common.ingest_quarantine') AND type = 'U')
    BEGIN
        CREATE TABLE common.ingest_quarantine (
            quarantine_id      BIGINT IDENTITY PRIMARY KEY,
            table_name         VARCHAR(100)    NOT NULL,
            row_key            NVARCHAR(500)   NOT NULL,
            row_payload        NVARCHAR(MAX)   NOT NULL,
            failed_invariant   VARCHAR(200)    NOT NULL,
            source_workflow    VARCHAR(100)    NULL,
            first_seen_at      DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            last_retry_at      DATETIME2       NULL,
            retry_count        INT             NOT NULL DEFAULT 0,
            resolved_at        DATETIME2       NULL,
            resolution_notes   NVARCHAR(MAX)   NULL
        );
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_quarantine_table_key_open'
                     AND object_id = OBJECT_ID('common.ingest_quarantine'))
    BEGIN
        CREATE INDEX IX_quarantine_table_key_open
            ON common.ingest_quarantine(table_name, row_key)
            WHERE resolved_at IS NULL;
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_quarantine_retry_ready'
                     AND object_id = OBJECT_ID('common.ingest_quarantine'))
    BEGIN
        CREATE INDEX IX_quarantine_retry_ready
            ON common.ingest_quarantine(last_retry_at, retry_count)
            WHERE resolved_at IS NULL;
    END
    """,
    # ----- common.unmapped_entities -----
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID('common.unmapped_entities') AND type = 'U')
    BEGIN
        CREATE TABLE common.unmapped_entities (
            unmapped_id           BIGINT IDENTITY PRIMARY KEY,
            source_feed           VARCHAR(100)    NOT NULL,
            entity_type           VARCHAR(50)     NOT NULL,
            source_key            NVARCHAR(500)   NOT NULL,
            source_context        NVARCHAR(MAX)   NULL,
            first_seen_at         DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            last_seen_at          DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            seen_count            INT             NOT NULL DEFAULT 1,
            candidate_match       NVARCHAR(500)   NULL,
            candidate_method      VARCHAR(50)     NULL,
            candidate_confidence  FLOAT           NULL,
            resolved_mapping      NVARCHAR(500)   NULL,
            resolved_at           DATETIME2       NULL,
            resolution_notes      NVARCHAR(MAX)   NULL,
            retry_count           INT             NOT NULL DEFAULT 0,
            CONSTRAINT UQ_unmapped_entities UNIQUE (source_feed, entity_type, source_key)
        );
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_unmapped_unresolved'
                     AND object_id = OBJECT_ID('common.unmapped_entities'))
    BEGIN
        CREATE INDEX IX_unmapped_unresolved
            ON common.unmapped_entities(retry_count, last_seen_at)
            WHERE resolved_at IS NULL;
    END
    """,
    # ----- common.data_completeness_log -----
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID('common.data_completeness_log') AND type = 'U')
    BEGIN
        CREATE TABLE common.data_completeness_log (
            log_id                  BIGINT IDENTITY PRIMARY KEY,
            table_name              VARCHAR(100)    NOT NULL,
            row_key                 NVARCHAR(500)   NOT NULL,
            column_name             VARCHAR(100)    NOT NULL,
            first_detected_at       DATETIME2       NOT NULL DEFAULT GETUTCDATE(),
            last_attempt_at         DATETIME2       NULL,
            attempt_count           INT             NOT NULL DEFAULT 0,
            resolved_at             DATETIME2       NULL,
            detected_retroactively  BIT             NOT NULL DEFAULT 0,
            notes                   NVARCHAR(MAX)   NULL,
            CONSTRAINT UQ_completeness_log UNIQUE (table_name, row_key, column_name)
        );
    END
    """,
    """
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'IX_completeness_retry_ready'
                     AND object_id = OBJECT_ID('common.data_completeness_log'))
    BEGIN
        CREATE INDEX IX_completeness_retry_ready
            ON common.data_completeness_log(last_attempt_at, attempt_count)
            WHERE resolved_at IS NULL;
    END
    """,
]


def ensure_tables(engine) -> None:
    """Create the three framework tables and their indexes if missing. Idempotent."""
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))


# =====================================================================
# Write-time validation (Layer 1)
# =====================================================================

def _row_key_str(row: Dict[str, Any], key_cols: List[str]) -> str:
    """Build a stable string representation of a row key for logging/indexing."""
    return "|".join(f"{c}={row.get(c)}" for c in key_cols)


def validate_row(table_name: str, row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Check a row against CRITICAL_FIELDS for its table.

    Returns (is_valid, failed_invariant). failed_invariant is None if valid.
    Tables not listed in CRITICAL_FIELDS always pass (opt-in enforcement).
    """
    rules = CRITICAL_FIELDS.get(table_name)
    if not rules:
        return True, None

    for col in rules.get("always_required", []):
        if row.get(col) is None:
            return False, f"always_required:{col}"

    for col, spec in rules.get("required_when", {}).items():
        pred = spec.get("py_predicate")
        if not callable(pred):
            continue
        try:
            should_be_present = bool(pred(row))
        except Exception:
            should_be_present = False
        if should_be_present and row.get(col) is None:
            return False, f"required_when:{col}:{spec.get('description', 'predicate_true')}"

    return True, None


def write_quarantine(
    engine,
    table_name: str,
    row: Dict[str, Any],
    failed_invariant: str,
    source_workflow: Optional[str] = None,
) -> None:
    """Record a single failed row in common.ingest_quarantine."""
    rules = CRITICAL_FIELDS.get(table_name, {})
    key_cols = rules.get("row_key", [])
    row_key = _row_key_str(row, key_cols) if key_cols else (
        json.dumps({k: str(v) for k, v in row.items() if v is not None}, default=str)[:500]
    )
    payload = json.dumps(row, default=str)
    sql = text("""
        INSERT INTO common.ingest_quarantine
            (table_name, row_key, row_payload, failed_invariant, source_workflow)
        VALUES (:t, :k, :p, :i, :w)
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"t": table_name, "k": row_key, "p": payload,
                           "i": failed_invariant, "w": source_workflow})


def validate_and_filter(
    rows: Iterable[Dict[str, Any]],
    table_name: str,
    engine,
    source_workflow: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Main ETL integration point. Pass rows through invariant checks before upsert.

    Valid rows are returned. Invalid rows are quarantined and NOT returned.
    Any previously-open quarantine or completeness-log entries for a
    newly-valid row_key are cleared and Issues matching the row_key close
    (workflow-side responsibility to watch the cleared state).

    Tables not in CRITICAL_FIELDS pass through unchanged (opt-in). For
    those tables, no DB round trip is incurred.
    """
    rules = CRITICAL_FIELDS.get(table_name)
    if not rules:
        return list(rows)

    key_cols = rules.get("row_key", [])
    rows_list = list(rows)

    # Pre-fetch open row_keys for this table so per-row "was this previously
    # quarantined?" checks become O(1) set membership rather than DB round trips.
    open_keys: set = set()
    if key_cols:
        with engine.connect() as conn:
            q_result = conn.execute(
                text("SELECT DISTINCT row_key FROM common.ingest_quarantine "
                     "WHERE table_name = :t AND resolved_at IS NULL"),
                {"t": table_name},
            )
            open_keys.update(r[0] for r in q_result)
            l_result = conn.execute(
                text("SELECT DISTINCT row_key FROM common.data_completeness_log "
                     "WHERE table_name = :t AND resolved_at IS NULL"),
                {"t": table_name},
            )
            open_keys.update(r[0] for r in l_result)

    valid: List[Dict[str, Any]] = []
    keys_to_clear: List[str] = []
    for row in rows_list:
        ok, failed = validate_row(table_name, row)
        if ok:
            if key_cols:
                rk = _row_key_str(row, key_cols)
                if rk in open_keys:
                    keys_to_clear.append(rk)
            valid.append(row)
        else:
            write_quarantine(engine, table_name, row, failed, source_workflow)

    # Batch-clear open entries for newly-valid rows. One UPDATE per table.
    if keys_to_clear:
        clear_sql_q = text("""
            UPDATE common.ingest_quarantine
            SET resolved_at = GETUTCDATE(),
                resolution_notes = COALESCE(resolution_notes, '')
                                   + ' | cleared by successful re-validation'
            WHERE table_name = :t AND row_key IN :keys AND resolved_at IS NULL
        """).bindparams(bindparam("keys", expanding=True))
        clear_sql_l = text("""
            UPDATE common.data_completeness_log
            SET resolved_at = GETUTCDATE(),
                notes = COALESCE(notes, '')
                        + ' | cleared by successful re-validation'
            WHERE table_name = :t AND row_key IN :keys AND resolved_at IS NULL
        """).bindparams(bindparam("keys", expanding=True))
        with engine.begin() as conn:
            conn.execute(clear_sql_q, {"t": table_name, "keys": list(set(keys_to_clear))})
            conn.execute(clear_sql_l, {"t": table_name, "keys": list(set(keys_to_clear))})

    return valid


# =====================================================================
# Retroactive scan (Layer 3 seed)
# =====================================================================

def retroactive_scan(engine, dry_run: bool = True) -> Dict[str, Dict[str, int]]:
    """
    For each table in CRITICAL_FIELDS, detect production rows that violate
    always_required or required_when invariants.

    Two modes:
      dry_run=True (default)  — read-only. Returns per-table, per-column
                                 violation counts. No writes to the log.
                                 Safe to run anytime.
      dry_run=False           — writes one row per violation to
                                 common.data_completeness_log via MERGE
                                 (idempotent against UQ_completeness_log).
                                 detected_retroactively=1 on all rows.

    Per-table isolation: if a SQL error hits one table (bad column name,
    bad predicate), it is captured under results[table_name]["__error__"]
    and the scan continues with other tables.

    Does NOT move existing production rows — retroactive movement is unsafe.
    The scan is visibility-only regardless of mode.

    Returns {table_name: {"always:col" or "conditional:col": count, ...}}.
    Tables with no violations return an empty dict.
    """
    results: Dict[str, Dict[str, int]] = {}
    for table_name, rules in CRITICAL_FIELDS.items():
        col_counts: Dict[str, int] = {}
        always_required = rules.get("always_required", [])
        required_when = rules.get("required_when", {})
        key_cols = rules.get("row_key", [])

        if not key_cols:
            results[table_name] = col_counts
            continue

        key_expr = " + '|' + ".join(
            f"'{k}=' + ISNULL(CAST(t.[{k}] AS NVARCHAR(50)), 'NULL')"
            for k in key_cols
        )

        try:
            ctx = engine.connect() if dry_run else engine.begin()
            with ctx as conn:

                # always_required
                for col in always_required:
                    if dry_run:
                        n = conn.execute(text(
                            f"SELECT COUNT(*) FROM {table_name} t WHERE t.[{col}] IS NULL"
                        )).scalar() or 0
                    else:
                        merge_sql = text(f"""
                            MERGE common.data_completeness_log AS tgt
                            USING (
                                SELECT :tbl AS table_name,
                                       {key_expr} AS row_key,
                                       :col AS column_name
                                FROM {table_name} t
                                WHERE t.[{col}] IS NULL
                            ) AS src
                            ON tgt.table_name = src.table_name
                               AND tgt.row_key = src.row_key
                               AND tgt.column_name = src.column_name
                            WHEN NOT MATCHED BY TARGET THEN
                                INSERT (table_name, row_key, column_name,
                                        detected_retroactively, notes)
                                VALUES (src.table_name, src.row_key, src.column_name,
                                        1, :notes);
                        """)
                        r = conn.execute(merge_sql, {
                            "tbl": table_name, "col": col,
                            "notes": "Retroactive: always_required",
                        })
                        n = r.rowcount or 0
                    if n:
                        col_counts[f"always:{col}"] = n

                # required_when (sql_predicate path; py_predicate is write-time only)
                for col, spec in required_when.items():
                    sql_pred = spec.get("sql_predicate")
                    if not sql_pred:
                        continue
                    description = spec.get("description", "required_when predicate")
                    if dry_run:
                        n = conn.execute(text(
                            f"SELECT COUNT(*) FROM {table_name} t "
                            f"WHERE t.[{col}] IS NULL AND ({sql_pred})"
                        )).scalar() or 0
                    else:
                        merge_sql = text(f"""
                            MERGE common.data_completeness_log AS tgt
                            USING (
                                SELECT :tbl AS table_name,
                                       {key_expr} AS row_key,
                                       :col AS column_name
                                FROM {table_name} t
                                WHERE t.[{col}] IS NULL
                                  AND ({sql_pred})
                            ) AS src
                            ON tgt.table_name = src.table_name
                               AND tgt.row_key = src.row_key
                               AND tgt.column_name = src.column_name
                            WHEN NOT MATCHED BY TARGET THEN
                                INSERT (table_name, row_key, column_name,
                                        detected_retroactively, notes)
                                VALUES (src.table_name, src.row_key, src.column_name,
                                        1, :notes);
                        """)
                        r = conn.execute(merge_sql, {
                            "tbl": table_name, "col": col,
                            "notes": f"Retroactive: required_when ({description})",
                        })
                        n = r.rowcount or 0
                    if n:
                        col_counts[f"conditional:{col}"] = n
        except Exception as e:
            col_counts["__error__"] = str(e)[:300]

        results[table_name] = col_counts
    return results


# =====================================================================
# Entity-mapping helpers (Layer 2)
# =====================================================================

def record_unmapped_entity(
    engine,
    source_feed: str,
    entity_type: str,
    source_key: str,
    source_context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Record a source-feed entity that could not be resolved to a canonical id.

    Idempotent via UQ_unmapped_entities: repeat calls bump seen_count and
    refresh last_seen_at. Separate resolver workflow handles match attempts
    and issue escalation.
    """
    context_json = json.dumps(source_context, default=str) if source_context else None
    sql = text("""
        MERGE common.unmapped_entities AS t
        USING (SELECT :feed AS source_feed, :etype AS entity_type, :key AS source_key) AS s
        ON t.source_feed = s.source_feed
           AND t.entity_type = s.entity_type
           AND t.source_key = s.source_key
        WHEN MATCHED AND t.resolved_at IS NULL THEN
            UPDATE SET last_seen_at = GETUTCDATE(),
                       seen_count = t.seen_count + 1,
                       source_context = COALESCE(:ctx, t.source_context)
        WHEN NOT MATCHED THEN
            INSERT (source_feed, entity_type, source_key, source_context)
            VALUES (:feed, :etype, :key, :ctx);
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"feed": source_feed, "etype": entity_type,
                           "key": source_key, "ctx": context_json})
