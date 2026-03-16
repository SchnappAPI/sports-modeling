"""
nba_etl.py
NBA data pipeline for the sports modeling database.
Runs exclusively in GitHub Actions. Never runs locally.
Requires NBA_PROXY_URL for all stats.nba.com requests.

Run modes:
  python nba_etl.py                  Process oldest BATCH unloaded games.
  python nba_etl.py --batch 100      Override batch size.
  python nba_etl.py --season 2023-24 Target a prior season.
  python nba_etl.py --load-rosters   Force roster reload even if players exist.

Batch design:
  Each run discovers all completed games for the season, subtracts games already
  loaded (presence in nba.games), and processes the oldest BATCH remaining ones.
  Repeated runs walk through the full season. Once complete, nightly runs exit
  after teams and roster checks.

Teams (nba.teams):
  Loaded from TeamInfoCommon called once per team (30 proxied calls).
  Provides: nba_team_id, nba_team_name, team_city, team_abbreviation,
  conference, division, w, l, conf_rank, div_rank.
  Runs every ETL run so wins/losses and ranks stay current.

Players (nba.players):
  Loaded from CommonTeamRoster called once per team (30 proxied calls).
  Provides: nba_player_id, player_name, position, jersey_num, height,
  weight, birth_date, age, experience, school, nba_team.
  Only loads when nba.players is empty OR --load-rosters is passed.
  Players who appear in box score data but are not on any current roster
  (e.g. mid-season trades, two-ways) are seeded via _seed_players as fallback.

Quarter-level box score design:
  player_box_score_stats and team_box_score_stats store one row per player/team
  per period. Valid quarter values: Q1, Q2, Q3, Q4, OT.
  OT is a single summed row across all overtime periods.
  BoxScoreTraditionalV3 is called with RangeType=0, StartPeriod=N, EndPeriod=N.
  RangeType=0 with period bounds correctly isolates each period.
  RangeType=2 with start_range/end_range=0 is WRONG and returns all zeros.

Potential assists (not stored):
  No game-level potential assists endpoint exists in the NBA API.
  Per-player game totals can be derived from player_box_score_matchups:
    SELECT game_id, person_id_off AS player_id,
           SUM(matchup_potential_assists) AS potential_assists
    FROM nba.player_box_score_matchups
    GROUP BY game_id, person_id_off

Tables written:
  nba.teams                     TeamInfoCommon x30, every run.
  nba.players                   CommonTeamRoster x30, first run or --load-rosters.
  nba.games                     One row per game. Presence = fully loaded marker.
  nba.player_box_score_stats    Quarter-level player stats (Q1/Q2/Q3/Q4/OT).
  nba.team_box_score_stats      Quarter-level team stats (Q1/Q2/Q3/Q4/OT).
  nba.player_tracking_stats     Game-level advanced + tracking metrics.
  nba.player_box_score_hustle   Game-level hustle stats.
  nba.player_box_score_matchups Per-game offensive/defensive matchup pairs.
  nba.matchup_position_stats    Stats allowed by each team to each position group.

API calls per run:
  30  TeamInfoCommon (one per team, every run)
  30  CommonTeamRoster (one per team, first run or --load-rosters only)
  1   LeagueGameFinder
  N   ScoreboardV3 (one per unique game date in batch)
  Per game (up to 9 for regulation, +1 per OT period):
    4   BoxScoreTraditionalV3 (Q1-Q4)
    N   BoxScoreTraditionalV3 (OT periods until empty)
    1   BoxScoreAdvancedV3
    1   BoxScorePlayerTrackV3
    1   BoxScoreHustleV2
    1   BoxScoreMatchupsV3

Secrets required:
  NBA_PROXY_URL, AZURE_SQL_SERVER, AZURE_SQL_DATABASE,
  AZURE_SQL_USERNAME, AZURE_SQL_PASSWORD
"""

import argparse
import os
import time
import logging

import pandas as pd
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv3,
    boxscoreadvancedv3,
    boxscoreplayertrackv3,
    boxscorehustlev2,
    boxscorematchupsv3,
    scoreboardv3,
    teaminfocommon,
    commonteamroster,
)
from nba_api.stats.static import teams as static_teams

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROXY_URL          = os.environ.get("NBA_PROXY_URL")
API_DELAY          = 0.30  # seconds between API calls
RETRY_WAIT         = 5     # seconds before retry
RETRY_COUNT        = 3     # attempts per API call
DEFAULT_BATCH      = 30    # games per run
DB_CONNECT_RETRIES = 3     # Azure SQL auto-pause resume attempts
DB_CONNECT_WAIT    = 60    # seconds between DB connection attempts

# ---------------------------------------------------------------------------
# Database engine with auto-pause retry
# ---------------------------------------------------------------------------
def get_engine():
    """
    Builds SQLAlchemy engine and validates the connection. Retries up to
    DB_CONNECT_RETRIES times to handle Azure SQL serverless auto-pause resume.
    """
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=True)

    for attempt in range(1, DB_CONNECT_RETRIES + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(
                f"DB connection attempt {attempt}/{DB_CONNECT_RETRIES} failed: {exc}"
            )
            if attempt < DB_CONNECT_RETRIES:
                log.info(f"  Waiting {DB_CONNECT_WAIT}s for Azure SQL to resume...")
                time.sleep(DB_CONNECT_WAIT)

    raise RuntimeError(
        f"Could not connect to Azure SQL after {DB_CONNECT_RETRIES} attempts."
    )

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
DDL_STATEMENTS = [

    # nba.teams
    # Source: TeamInfoCommon (one call per team, every run).
    # Columns are exactly what the API returns. No external mapping columns.
    # w, l, conf_rank, div_rank reflect the current season at time of load.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.teams') AND type = 'U')
    CREATE TABLE nba.teams (
        nba_team_id   BIGINT       NOT NULL,
        nba_team      CHAR(3)      NOT NULL,
        nba_team_name VARCHAR(60)  NOT NULL,
        team_city     VARCHAR(40)  NULL,
        conference    VARCHAR(10)  NULL,
        division      VARCHAR(20)  NULL,
        w             SMALLINT     NULL,
        l             SMALLINT     NULL,
        conf_rank     SMALLINT     NULL,
        div_rank      SMALLINT     NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_teams   PRIMARY KEY (nba_team_id),
        CONSTRAINT uq_nba_team    UNIQUE      (nba_team)
    )
    """,

    # nba.players
    # Source: CommonTeamRoster (one call per team, first run or --load-rosters).
    # Players missing from roster but present in box score data are seeded
    # via _seed_players with minimal fields as a fallback.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.players') AND type = 'U')
    CREATE TABLE nba.players (
        nba_player_id BIGINT        NOT NULL,
        player_name   VARCHAR(100)  NOT NULL,
        position      VARCHAR(10)   NULL,
        jersey_num    VARCHAR(5)    NULL,
        height        VARCHAR(10)   NULL,
        weight        VARCHAR(10)   NULL,
        birth_date    DATE          NULL,
        age           DECIMAL(5,1)  NULL,
        experience    VARCHAR(5)    NULL,
        school        VARCHAR(100)  NULL,
        nba_team_id   BIGINT        NULL,
        nba_team      CHAR(3)       NULL,
        created_at    DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_players      PRIMARY KEY (nba_player_id),
        CONSTRAINT fk_nba_players_team FOREIGN KEY (nba_team_id)
            REFERENCES nba.teams (nba_team_id)
    )
    """,

    # nba.games
    # Writing this row is the fully loaded marker.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.games') AND type = 'U')
    CREATE TABLE nba.games (
        game_id       VARCHAR(15)  NOT NULL,
        game_date     DATE         NOT NULL,
        game_code     VARCHAR(30)  NULL,
        game_display  VARCHAR(20)  NULL,
        home_team_id  BIGINT       NULL,
        home_team     CHAR(3)      NULL,
        away_team_id  BIGINT       NULL,
        away_team     CHAR(3)      NULL,
        season_year   CHAR(7)      NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_games      PRIMARY KEY (game_id),
        CONSTRAINT fk_nba_games_home FOREIGN KEY (home_team_id)
            REFERENCES nba.teams (nba_team_id),
        CONSTRAINT fk_nba_games_away FOREIGN KEY (away_team_id)
            REFERENCES nba.teams (nba_team_id)
    )
    """,

    # nba.player_box_score_stats
    # Quarter-level. quarter IN ('Q1','Q2','Q3','Q4','OT').
    # Source: BoxScoreTraditionalV3 with RangeType=0, StartPeriod=N, EndPeriod=N.
    # DNP players (non-blank comment field) are excluded.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_box_score_stats') AND type = 'U')
    CREATE TABLE nba.player_box_score_stats (
        game_id           VARCHAR(15)   NOT NULL,
        player_id         BIGINT        NOT NULL,
        quarter           VARCHAR(5)    NOT NULL,
        first_name        VARCHAR(60)   NULL,
        last_name         VARCHAR(60)   NULL,
        team_id           BIGINT        NULL,
        team_abbreviation CHAR(3)       NULL,
        position          VARCHAR(10)   NULL,
        minutes           VARCHAR(20)   NULL,
        fgm               SMALLINT      NULL,
        fga               SMALLINT      NULL,
        fg_pct            DECIMAL(6,4)  NULL,
        fg3m              SMALLINT      NULL,
        fg3a              SMALLINT      NULL,
        fg3_pct           DECIMAL(6,4)  NULL,
        ftm               SMALLINT      NULL,
        fta               SMALLINT      NULL,
        ft_pct            DECIMAL(6,4)  NULL,
        oreb              SMALLINT      NULL,
        dreb              SMALLINT      NULL,
        reb               SMALLINT      NULL,
        ast               SMALLINT      NULL,
        stl               SMALLINT      NULL,
        blk               SMALLINT      NULL,
        tov               SMALLINT      NULL,
        pf                SMALLINT      NULL,
        pts               SMALLINT      NULL,
        plus_minus        DECIMAL(6,1)  NULL,
        created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pbss        PRIMARY KEY (game_id, player_id, quarter),
        CONSTRAINT fk_nba_pbss_game   FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_pbss_player FOREIGN KEY (player_id)
            REFERENCES nba.players (nba_player_id)
    )
    """,

    # nba.team_box_score_stats
    # Quarter-level. Same structure as player_box_score_stats.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.team_box_score_stats') AND type = 'U')
    CREATE TABLE nba.team_box_score_stats (
        game_id           VARCHAR(15)   NOT NULL,
        team_id           BIGINT        NOT NULL,
        quarter           VARCHAR(5)    NOT NULL,
        team_abbreviation CHAR(3)       NULL,
        minutes           VARCHAR(20)   NULL,
        fgm               SMALLINT      NULL,
        fga               SMALLINT      NULL,
        fg_pct            DECIMAL(6,4)  NULL,
        fg3m              SMALLINT      NULL,
        fg3a              SMALLINT      NULL,
        fg3_pct           DECIMAL(6,4)  NULL,
        ftm               SMALLINT      NULL,
        fta               SMALLINT      NULL,
        ft_pct            DECIMAL(6,4)  NULL,
        oreb              SMALLINT      NULL,
        dreb              SMALLINT      NULL,
        reb               SMALLINT      NULL,
        ast               SMALLINT      NULL,
        stl               SMALLINT      NULL,
        blk               SMALLINT      NULL,
        tov               SMALLINT      NULL,
        pf                SMALLINT      NULL,
        pts               SMALLINT      NULL,
        plus_minus        DECIMAL(6,1)  NULL,
        created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_tbss      PRIMARY KEY (game_id, team_id, quarter),
        CONSTRAINT fk_nba_tbss_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_tbss_team FOREIGN KEY (team_id)
            REFERENCES nba.teams (nba_team_id)
    )
    """,

    # nba.player_tracking_stats
    # Game-level. Merges BoxScoreAdvancedV3 and BoxScorePlayerTrackV3.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_tracking_stats') AND type = 'U')
    CREATE TABLE nba.player_tracking_stats (
        game_id                VARCHAR(15)   NOT NULL,
        player_id              BIGINT        NOT NULL,
        team_id                BIGINT        NULL,
        usage_pct              DECIMAL(6,4)  NULL,
        off_rating             DECIMAL(7,3)  NULL,
        def_rating             DECIMAL(7,3)  NULL,
        net_rating             DECIMAL(7,3)  NULL,
        pace                   DECIMAL(7,3)  NULL,
        pie                    DECIMAL(7,4)  NULL,
        true_shooting_pct      DECIMAL(6,4)  NULL,
        efg_pct                DECIMAL(6,4)  NULL,
        speed                  DECIMAL(6,3)  NULL,
        distance               DECIMAL(8,3)  NULL,
        touches                INT           NULL,
        passes_made            INT           NULL,
        secondary_ast          INT           NULL,
        ft_ast                 INT           NULL,
        reb_chances            INT           NULL,
        oreb_chances           INT           NULL,
        dreb_chances           INT           NULL,
        contested_fgm          INT           NULL,
        contested_fga          INT           NULL,
        contested_fg_pct       DECIMAL(6,4)  NULL,
        uncontested_fgm        INT           NULL,
        uncontested_fga        INT           NULL,
        uncontested_fg_pct     DECIMAL(6,4)  NULL,
        defended_at_rim_fgm    INT           NULL,
        defended_at_rim_fga    INT           NULL,
        defended_at_rim_fg_pct DECIMAL(6,4)  NULL,
        created_at             DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pts        PRIMARY KEY (game_id, player_id),
        CONSTRAINT fk_nba_pts_game   FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_pts_player FOREIGN KEY (player_id)
            REFERENCES nba.players (nba_player_id)
    )
    """,

    # nba.player_box_score_hustle
    # Game-level. Source: BoxScoreHustleV2.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_box_score_hustle') AND type = 'U')
    CREATE TABLE nba.player_box_score_hustle (
        game_id                      VARCHAR(15)  NOT NULL,
        player_id                    BIGINT       NOT NULL,
        team_id                      BIGINT       NULL,
        team_tricode                 CHAR(3)      NULL,
        points                       SMALLINT     NULL,
        contested_shots              SMALLINT     NULL,
        contested_shots_2pt          SMALLINT     NULL,
        contested_shots_3pt          SMALLINT     NULL,
        deflections                  SMALLINT     NULL,
        charges_drawn                SMALLINT     NULL,
        screen_assists               SMALLINT     NULL,
        screen_assist_points         SMALLINT     NULL,
        loose_balls_recovered_off    SMALLINT     NULL,
        loose_balls_recovered_def    SMALLINT     NULL,
        loose_balls_recovered_total  SMALLINT     NULL,
        offensive_box_outs           SMALLINT     NULL,
        defensive_box_outs           SMALLINT     NULL,
        box_out_player_team_rebounds SMALLINT     NULL,
        box_out_player_rebounds      SMALLINT     NULL,
        box_outs                     SMALLINT     NULL,
        created_at                   DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pbsh        PRIMARY KEY (game_id, player_id),
        CONSTRAINT fk_nba_pbsh_game   FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id),
        CONSTRAINT fk_nba_pbsh_player FOREIGN KEY (player_id)
            REFERENCES nba.players (nba_player_id)
    )
    """,

    # nba.player_box_score_matchups
    # Game-level. Source: BoxScoreMatchupsV3.
    # NOTE: potential assists per player per game can be derived from this table:
    #   SELECT game_id, person_id_off AS player_id,
    #          SUM(matchup_potential_assists) AS potential_assists
    #   FROM nba.player_box_score_matchups
    #   GROUP BY game_id, person_id_off
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_box_score_matchups') AND type = 'U')
    CREATE TABLE nba.player_box_score_matchups (
        game_id                      VARCHAR(15)   NOT NULL,
        team_id                      BIGINT        NULL,
        team_tricode                 CHAR(3)       NULL,
        person_id_off                BIGINT        NOT NULL,
        first_name_off               VARCHAR(60)   NULL,
        family_name_off              VARCHAR(60)   NULL,
        jersey_num_off               VARCHAR(5)    NULL,
        person_id_def                BIGINT        NOT NULL,
        first_name_def               VARCHAR(60)   NULL,
        family_name_def              VARCHAR(60)   NULL,
        jersey_num_def               VARCHAR(5)    NULL,
        position_def                 VARCHAR(10)   NULL,
        matchup_minutes              VARCHAR(20)   NULL,
        matchup_minutes_sort         DECIMAL(8,4)  NULL,
        partial_possessions          DECIMAL(8,3)  NULL,
        pct_defender_total_time      DECIMAL(6,4)  NULL,
        pct_offensive_total_time     DECIMAL(6,4)  NULL,
        pct_total_time_both_on       DECIMAL(6,4)  NULL,
        switches_on                  SMALLINT      NULL,
        player_points                SMALLINT      NULL,
        team_points                  SMALLINT      NULL,
        matchup_assists              SMALLINT      NULL,
        matchup_potential_assists    SMALLINT      NULL,
        matchup_turnovers            SMALLINT      NULL,
        matchup_blocks               SMALLINT      NULL,
        matchup_fgm                  SMALLINT      NULL,
        matchup_fga                  SMALLINT      NULL,
        matchup_fg_pct               DECIMAL(6,4)  NULL,
        matchup_fg3m                 SMALLINT      NULL,
        matchup_fg3a                 SMALLINT      NULL,
        matchup_fg3_pct              DECIMAL(6,4)  NULL,
        help_blocks                  SMALLINT      NULL,
        help_fgm                     SMALLINT      NULL,
        help_fga                     SMALLINT      NULL,
        help_fg_pct                  DECIMAL(6,4)  NULL,
        matchup_ftm                  SMALLINT      NULL,
        matchup_fta                  SMALLINT      NULL,
        shooting_fouls               SMALLINT      NULL,
        created_at                   DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pbsm      PRIMARY KEY (game_id, person_id_off, person_id_def),
        CONSTRAINT fk_nba_pbsm_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id)
    )
    """,

    # nba.matchup_position_stats
    # Derived in-memory from Q1-Q4 player rows per game.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.matchup_position_stats') AND type = 'U')
    CREATE TABLE nba.matchup_position_stats (
        game_id             VARCHAR(15)   NOT NULL,
        game_date           DATE          NOT NULL,
        defending_team_id   BIGINT        NOT NULL,
        defending_team_abbr CHAR(3)       NULL,
        position_group      VARCHAR(10)   NOT NULL,
        player_count        SMALLINT      NULL,
        total_fgm           SMALLINT      NULL,
        total_fga           SMALLINT      NULL,
        total_fg3m          SMALLINT      NULL,
        total_fg3a          SMALLINT      NULL,
        total_ftm           SMALLINT      NULL,
        total_fta           SMALLINT      NULL,
        total_oreb          SMALLINT      NULL,
        total_dreb          SMALLINT      NULL,
        total_reb           SMALLINT      NULL,
        total_ast           SMALLINT      NULL,
        total_stl           SMALLINT      NULL,
        total_blk           SMALLINT      NULL,
        total_tov           SMALLINT      NULL,
        total_pf            SMALLINT      NULL,
        total_pts           SMALLINT      NULL,
        created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_mps      PRIMARY KEY (game_id, defending_team_id, position_group),
        CONSTRAINT fk_nba_mps_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id)
    )
    """,
]


def ensure_tables(engine):
    """Create all NBA tables that do not yet exist. Safe to run repeatedly."""
    with engine.begin() as conn:
        for stmt in DDL_STATEMENTS:
            conn.execute(text(stmt))
    log.info("Schema verified.")


# ---------------------------------------------------------------------------
# Safe type helpers
# ---------------------------------------------------------------------------
def safe_float(val):
    import math
    try:
        if val is None:
            return None
        if isinstance(val, float):
            return None if (math.isnan(val) or math.isinf(val)) else val
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s if s else None


def safe_date(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val).date()
    except Exception:
        return None


def safe_pct(num, den):
    n, d = safe_int(num), safe_int(den)
    if n is None or d is None or d == 0:
        return None
    return round(n / d, 4)


# ---------------------------------------------------------------------------
# API retry wrapper
# ---------------------------------------------------------------------------
def api_call(fn, label):
    """Call fn() up to RETRY_COUNT times. Sleeps API_DELAY after success."""
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            result = fn()
            time.sleep(API_DELAY)
            return result
        except Exception as exc:
            log.warning(f"  {label} attempt {attempt}/{RETRY_COUNT} failed: {exc}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_WAIT)
    log.error(f"  {label} failed after {RETRY_COUNT} attempts, skipping")
    return None


# ---------------------------------------------------------------------------
# Generic MERGE upsert
# ---------------------------------------------------------------------------
def _clean_val(v):
    """
    Sanitize a single value for pyodbc. Converts numpy scalars, float nan,
    float inf, and pandas NA types all to Python None. Converts surviving
    numpy numeric types to plain Python int or float so pyodbc never sees
    a numpy dtype.
    """
    import math
    import numpy as np
    if v is None:
        return None
    # Catch pandas NA / NaT
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    # Catch float nan / inf
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    # Coerce numpy integer types to Python int
    if isinstance(v, (np.integer,)):
        return int(v)
    # Coerce numpy float types to Python float, then re-check nan/inf
    if isinstance(v, (np.floating,)):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    return v


def upsert(df, engine, schema, table, pk_cols):
    """
    MERGE upsert. Sanitizes every value through _clean_val before sending
    to pyodbc, handling numpy scalars, float nan/inf, and pandas NA across
    all column dtypes.
    """
    if df is None or df.empty:
        return
    # Apply _clean_val element-wise across the entire DataFrame
    records = [
        {col: _clean_val(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]
    non_pk    = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = (
        ", ".join(f"tgt.{c} = src.{c}" for c in non_pk)
        if non_pk else f"tgt.{pk_cols[0]} = tgt.{pk_cols[0]}"
    )
    merge_sql = f"""
        MERGE {schema}.{table} AS tgt
        USING (VALUES ({val_list})) AS src ({col_list})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
    """
    with engine.begin() as conn:
        conn.execute(text(merge_sql), records)


# ---------------------------------------------------------------------------
# Step 1: Load teams from TeamInfoCommon (30 proxied calls, every run)
# ---------------------------------------------------------------------------
def load_teams(engine, season):
    """
    Calls TeamInfoCommon once per team. Upserts nba.teams with fields from
    TeamInfoCommon.team_info_common: TEAM_ID, TEAM_ABBREVIATION, TEAM_NAME,
    TEAM_CITY, TEAM_CONFERENCE, TEAM_DIVISION.
    W, L, CONF_RANK, DIV_RANK come from TeamInfoCommon.team_season_ranks
    (first row, current season).
    """
    log.info(f"Loading nba.teams via TeamInfoCommon for season {season}")
    all_team_stubs = static_teams.get_teams()
    rows = []

    for stub in all_team_stubs:
        team_id   = stub["id"]
        team_abbr = stub["abbreviation"]
        ep = api_call(
            lambda tid=team_id: teaminfocommon.TeamInfoCommon(
                team_id=tid,
                season_nullable=season,
                proxy=PROXY_URL,
            ),
            f"TeamInfoCommon {team_abbr}",
        )
        if ep is None:
            continue
        try:
            info_df  = ep.team_info_common.get_data_frame()
            ranks_df = ep.team_season_ranks.get_data_frame()

            if info_df.empty:
                log.warning(f"  TeamInfoCommon returned no data for {team_abbr}")
                continue

            r = info_df.iloc[0]
            w = l = conf_rank = div_rank = None
            if not ranks_df.empty:
                rr        = ranks_df.iloc[0]
                w         = safe_int(rr.get("W"))   if "W"         in rr.index else None
                l         = safe_int(rr.get("L"))   if "L"         in rr.index else None
                conf_rank = safe_int(rr.get("CONF_RANK")) if "CONF_RANK" in rr.index else None
                div_rank  = safe_int(rr.get("DIV_RANK"))  if "DIV_RANK"  in rr.index else None

            rows.append({
                "nba_team_id":   safe_int(r.get("TEAM_ID")),
                "nba_team":      safe_str(r.get("TEAM_ABBREVIATION")),
                "nba_team_name": safe_str(r.get("TEAM_NAME")),
                "team_city":     safe_str(r.get("TEAM_CITY")),
                "conference":    safe_str(r.get("TEAM_CONFERENCE")),
                "division":      safe_str(r.get("TEAM_DIVISION")),
                "w":             w,
                "l":             l,
                "conf_rank":     conf_rank,
                "div_rank":      div_rank,
            })
        except Exception as exc:
            log.warning(f"  TeamInfoCommon parse failed for {team_abbr}: {exc}")

    if rows:
        upsert(pd.DataFrame(rows), engine, "nba", "teams", ["nba_team_id"])
        log.info(f"  {len(rows)} teams upserted")
    else:
        log.warning("  No team rows produced. Check TeamInfoCommon responses.")


# ---------------------------------------------------------------------------
# Step 2: Load players from CommonTeamRoster (30 proxied calls, conditional)
# ---------------------------------------------------------------------------
def players_table_empty(engine):
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(1) FROM nba.players"))
        return result.scalar() == 0


def load_players(engine, season):
    """
    Calls CommonTeamRoster once per team. Upserts nba.players from the
    CommonTeamRoster dataset. Fields: PLAYER_ID, PLAYER (full name),
    POSITION, NUM (jersey), HEIGHT, WEIGHT, BIRTH_DATE, AGE, EXP, SCHOOL.
    Also captures TeamID and team abbreviation from the team stub.
    """
    log.info(f"Loading nba.players via CommonTeamRoster for season {season}")
    all_team_stubs = static_teams.get_teams()

    # Build a team_id -> nba_team lookup from what we just loaded into nba.teams
    team_abbr_map = {}
    try:
        with engine.connect() as conn:
            for row in conn.execute(text("SELECT nba_team_id, nba_team FROM nba.teams")):
                team_abbr_map[row[0]] = row[1]
    except Exception:
        pass

    rows = []
    for stub in all_team_stubs:
        team_id   = stub["id"]
        team_abbr = stub["abbreviation"]
        ep = api_call(
            lambda tid=team_id: commonteamroster.CommonTeamRoster(
                team_id=tid,
                season=season,
                proxy=PROXY_URL,
            ),
            f"CommonTeamRoster {team_abbr}",
        )
        if ep is None:
            continue
        try:
            roster_df = ep.common_team_roster.get_data_frame()
            if roster_df.empty:
                continue
            for _, row in roster_df.iterrows():
                pid = safe_int(row.get("PLAYER_ID"))
                if pid is None:
                    continue
                rows.append({
                    "nba_player_id": pid,
                    "player_name":   safe_str(row.get("PLAYER")) or "Unknown",
                    "position":      safe_str(row.get("POSITION")),
                    "jersey_num":    safe_str(row.get("NUM")),
                    "height":        safe_str(row.get("HEIGHT")),
                    "weight":        safe_str(row.get("WEIGHT")),
                    "birth_date":    safe_date(row.get("BIRTH_DATE")),
                    "age":           safe_float(row.get("AGE")),
                    "experience":    safe_str(row.get("EXP")),
                    "school":        safe_str(row.get("SCHOOL")),
                    "nba_team_id":   safe_int(row.get("TeamID")),
                    "nba_team":      team_abbr_map.get(safe_int(row.get("TeamID")), team_abbr),
                })
        except Exception as exc:
            log.warning(f"  CommonTeamRoster parse failed for {team_abbr}: {exc}")

    if rows:
        upsert(pd.DataFrame(rows), engine, "nba", "players", ["nba_player_id"])
        log.info(f"  {len(rows)} players upserted")
    else:
        log.warning("  No player rows produced. Check CommonTeamRoster responses.")


# ---------------------------------------------------------------------------
# Step 3: Discover all completed game IDs for the season
# ---------------------------------------------------------------------------
def get_all_season_game_ids(season):
    """Returns [(game_id, game_date), ...] sorted oldest first. Excludes preseason."""
    log.info(f"Fetching all game IDs for season {season}")
    ep = api_call(
        lambda: leaguegamefinder.LeagueGameFinder(
            season_nullable=season,
            league_id_nullable="00",
            proxy=PROXY_URL,
        ),
        "LeagueGameFinder",
    )
    if ep is None:
        return []
    df = ep.get_data_frames()[0]
    if df.empty:
        log.warning("LeagueGameFinder returned no games")
        return []
    pairs = df[["GAME_ID","GAME_DATE"]].drop_duplicates("GAME_ID").values.tolist()
    result = [
        (str(gid), pd.to_datetime(gdate).date())
        for gid, gdate in pairs
        if not str(gid).startswith("001")
    ]
    result.sort(key=lambda x: x[1])
    log.info(f"  Found {len(result)} completed games in season {season}")
    return result


# ---------------------------------------------------------------------------
# Step 4: Filter to games not yet loaded
# ---------------------------------------------------------------------------
def get_unloaded_games(all_pairs, engine):
    with engine.connect() as conn:
        loaded = {
            row[0] for row in
            conn.execute(text("SELECT DISTINCT game_id FROM nba.games"))
        }
    unloaded = [p for p in all_pairs if p[0] not in loaded]
    log.info(f"  {len(loaded)} already loaded, {len(unloaded)} remaining")
    return unloaded


# ---------------------------------------------------------------------------
# Step 5: ScoreboardV3 metadata for batch dates
# ---------------------------------------------------------------------------
def fetch_scoreboard_metadata(target_dates, season):
    """
    Returns dict keyed by game_id.
    ScoreboardV3: .game_header one row/game, .line_score away=iloc[0] home=iloc[1].
    home_team_id / away_team_id stored as BIGINT to match nba.teams PK.
    """
    metadata = {}
    for game_date in sorted(set(target_dates)):
        date_str = game_date.strftime("%Y-%m-%d")
        sb = api_call(
            lambda d=date_str: scoreboardv3.ScoreboardV3(
                game_date=d, league_id="00", proxy=PROXY_URL,
            ),
            f"ScoreboardV3 {date_str}",
        )
        if sb is None:
            continue
        try:
            headers_df = sb.game_header.get_data_frame()
            lines_df   = sb.line_score.get_data_frame()
            headers_df["gameId"] = headers_df["gameId"].astype(str)
            lines_df["gameId"]   = lines_df["gameId"].astype(str)

            for _, hdr in headers_df.iterrows():
                gid     = str(hdr["gameId"])
                game_ls = lines_df[lines_df["gameId"] == gid]
                away_abbr = away_tid = home_abbr = home_tid = None

                if len(game_ls) >= 2:
                    away_row  = game_ls.iloc[0]
                    home_row  = game_ls.iloc[1]
                    away_abbr = safe_str(away_row.get("teamTricode"))
                    away_tid  = safe_int(away_row.get("teamId"))
                    home_abbr = safe_str(home_row.get("teamTricode"))
                    home_tid  = safe_int(home_row.get("teamId"))
                elif len(game_ls) == 1:
                    home_abbr = safe_str(game_ls.iloc[0].get("teamTricode"))
                    home_tid  = safe_int(game_ls.iloc[0].get("teamId"))

                metadata[gid] = {
                    "game_date":    game_date,
                    "game_code":    safe_str(hdr.get("gameCode")),
                    "game_display": f"{away_abbr}@{home_abbr}" if away_abbr else None,
                    "home_team_id": home_tid,
                    "home_team":    home_abbr,
                    "away_team_id": away_tid,
                    "away_team":    away_abbr,
                    "season_year":  season[:7],
                }
        except Exception as exc:
            log.warning(f"  ScoreboardV3 parse failed for {date_str}: {exc}")

    log.info(f"  Scoreboard metadata fetched for {len(metadata)} game(s)")
    return metadata


# ---------------------------------------------------------------------------
# Quarter box score helpers
# ---------------------------------------------------------------------------
def _trad_player_rows(game_id, quarter_label, df):
    """
    Extract player rows from BoxScoreTraditionalV3 PlayerStats DataFrame.
    DNP players (non-blank comment field) are excluded.
    """
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        comment = safe_str(row.get("comment")) or ""
        if comment:
            continue
        pid = safe_int(row.get("personId"))
        if pid is None:
            continue
        rows.append({
            "game_id":           game_id,
            "player_id":         pid,
            "quarter":           quarter_label,
            "first_name":        safe_str(row.get("firstName")),
            "last_name":         safe_str(row.get("familyName")),
            "team_id":           safe_int(row.get("teamId")),
            "team_abbreviation": safe_str(row.get("teamTricode")),
            "position":          safe_str(row.get("position")),
            "minutes":           safe_str(row.get("minutes")),
            "fgm":               safe_int(row.get("fieldGoalsMade")),
            "fga":               safe_int(row.get("fieldGoalsAttempted")),
            "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
            "fg3m":              safe_int(row.get("threePointersMade")),
            "fg3a":              safe_int(row.get("threePointersAttempted")),
            "fg3_pct":           safe_float(row.get("threePointersPercentage")),
            "ftm":               safe_int(row.get("freeThrowsMade")),
            "fta":               safe_int(row.get("freeThrowsAttempted")),
            "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
            "oreb":              safe_int(row.get("reboundsOffensive")),
            "dreb":              safe_int(row.get("reboundsDefensive")),
            "reb":               safe_int(row.get("reboundsTotal")),
            "ast":               safe_int(row.get("assists")),
            "stl":               safe_int(row.get("steals")),
            "blk":               safe_int(row.get("blocks")),
            "tov":               safe_int(row.get("turnovers")),
            "pf":                safe_int(row.get("foulsPersonal")),
            "pts":               safe_int(row.get("points")),
            "plus_minus":        safe_float(row.get("plusMinusPoints")),
        })
    return rows


def _trad_team_rows(game_id, quarter_label, df):
    """Extract team rows from BoxScoreTraditionalV3 TeamStats DataFrame."""
    rows = []
    if df is None or df.empty:
        return rows
    for _, row in df.iterrows():
        tid = safe_int(row.get("teamId"))
        if tid is None:
            continue
        rows.append({
            "game_id":           game_id,
            "team_id":           tid,
            "quarter":           quarter_label,
            "team_abbreviation": safe_str(row.get("teamTricode")),
            "minutes":           safe_str(row.get("minutes")),
            "fgm":               safe_int(row.get("fieldGoalsMade")),
            "fga":               safe_int(row.get("fieldGoalsAttempted")),
            "fg_pct":            safe_float(row.get("fieldGoalsPercentage")),
            "fg3m":              safe_int(row.get("threePointersMade")),
            "fg3a":              safe_int(row.get("threePointersAttempted")),
            "fg3_pct":           safe_float(row.get("threePointersPercentage")),
            "ftm":               safe_int(row.get("freeThrowsMade")),
            "fta":               safe_int(row.get("freeThrowsAttempted")),
            "ft_pct":            safe_float(row.get("freeThrowsPercentage")),
            "oreb":              safe_int(row.get("reboundsOffensive")),
            "dreb":              safe_int(row.get("reboundsDefensive")),
            "reb":               safe_int(row.get("reboundsTotal")),
            "ast":               safe_int(row.get("assists")),
            "stl":               safe_int(row.get("steals")),
            "blk":               safe_int(row.get("blocks")),
            "tov":               safe_int(row.get("turnovers")),
            "pf":                safe_int(row.get("foulsPersonal")),
            "pts":               safe_int(row.get("points")),
            "plus_minus":        safe_float(row.get("plusMinusPoints")),
        })
    return rows


def _sum_ot_player_rows(game_id, ot_periods_data):
    """Sum all OT period player DataFrames into one OT row per player."""
    if not ot_periods_data:
        return []
    all_rows = []
    for player_df, _ in ot_periods_data:
        all_rows.extend(_trad_player_rows(game_id, "OT", player_df))
    if not all_rows:
        return []

    df = pd.DataFrame(all_rows)
    count_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                  "oreb","dreb","reb","ast","stl","blk","tov","pf","pts","plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    meta_cols = ["game_id","player_id","quarter","first_name","last_name",
                 "team_id","team_abbreviation","position","minutes"]
    agg_meta   = df.groupby("player_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("player_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"player_id": "_pid"}, inplace=True)
    agg_meta["_pid"] = agg_meta["player_id"]
    merged = agg_meta.merge(agg_counts, on="_pid").drop(columns=["_pid"])
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"
    return merged.to_dict(orient="records")


def _sum_ot_team_rows(game_id, ot_periods_data):
    """Sum all OT period team DataFrames into one OT row per team."""
    if not ot_periods_data:
        return []
    all_rows = []
    for _, team_df in ot_periods_data:
        all_rows.extend(_trad_team_rows(game_id, "OT", team_df))
    if not all_rows:
        return []

    df = pd.DataFrame(all_rows)
    count_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                  "oreb","dreb","reb","ast","stl","blk","tov","pf","pts","plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    meta_cols = ["game_id","team_id","quarter","team_abbreviation","minutes"]
    agg_meta   = df.groupby("team_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("team_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"team_id": "_tid"}, inplace=True)
    agg_meta["_tid"] = agg_meta["team_id"]
    merged = agg_meta.merge(agg_counts, on="_tid").drop(columns=["_tid"])
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"
    return merged.to_dict(orient="records")


# ---------------------------------------------------------------------------
# _seed_players: FK safety net for players not in nba.players
# ---------------------------------------------------------------------------
def _seed_players(rows, engine):
    """
    INSERT-only MERGE. Seeds nba.players rows so FK constraints on child tables
    are satisfied. Only fires for players not already present. Minimal fields only.
    CommonTeamRoster load at start of season provides the real data.
    """
    seed_sql = """
        MERGE nba.players AS tgt
        USING (VALUES (:nba_player_id, :player_name))
              AS src (nba_player_id, player_name)
        ON tgt.nba_player_id = src.nba_player_id
        WHEN NOT MATCHED THEN INSERT
            (nba_player_id, player_name, created_at)
        VALUES (src.nba_player_id, src.player_name, GETUTCDATE());
    """
    seed_rows = []
    for r in rows:
        pid = r.get("player_id") or r.get("nba_player_id")
        if pid is None:
            continue
        fn   = r.get("first_name") or ""
        ln   = r.get("last_name") or ""
        name = (fn + " " + ln).strip() or "Unknown"
        seed_rows.append({"nba_player_id": pid, "player_name": name})
    if not seed_rows:
        return
    with engine.begin() as conn:
        conn.execute(text(seed_sql), seed_rows)


# ---------------------------------------------------------------------------
# Step 6: Process one game
# ---------------------------------------------------------------------------
def process_game(game_id, game_date, game_meta, engine):
    log.info(f"  Processing {game_id} ({game_date})")

    meta = game_meta.get(game_id)
    games_row = {
        "game_id":       game_id,
        "game_date":     meta["game_date"]    if meta else game_date,
        "game_code":     meta["game_code"]    if meta else None,
        "game_display":  meta["game_display"] if meta else None,
        "home_team_id":  meta["home_team_id"] if meta else None,
        "home_team":     meta["home_team"]    if meta else None,
        "away_team_id":  meta["away_team_id"] if meta else None,
        "away_team":     meta["away_team"]    if meta else None,
        "season_year":   meta["season_year"]  if meta else None,
    }
    try:
        upsert(pd.DataFrame([games_row]), engine, "nba", "games", ["game_id"])
    except Exception as exc:
        log.error(f"  games upsert failed for {game_id}: {exc}")
        return

    if not meta:
        log.warning(f"  No scoreboard metadata for {game_id}, stub games row written")

    # ------------------------------------------------------------------
    # BoxScoreTraditionalV3 -> Q1, Q2, Q3, Q4, OT
    # RangeType=0, StartPeriod=N, EndPeriod=N correctly isolates each period.
    # ------------------------------------------------------------------
    all_player_rows = []  # Q1-Q4 rows used for matchup position aggregation

    for period_num, quarter_label in [(1,"Q1"),(2,"Q2"),(3,"Q3"),(4,"Q4")]:
        ep = api_call(
            lambda p=period_num: boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=p,
                end_period=p,
                range_type=0,
                start_range=0,
                end_range=0,
                proxy=PROXY_URL,
            ),
            f"BoxScoreTraditionalV3 {game_id} {quarter_label}",
        )
        if ep is None:
            continue
        try:
            p_df = ep.player_stats.get_data_frame()
            t_df = ep.team_stats.get_data_frame()
        except Exception as exc:
            log.warning(f"  Traditional parse failed {game_id} {quarter_label}: {exc}")
            continue

        p_rows = _trad_player_rows(game_id, quarter_label, p_df)
        t_rows = _trad_team_rows(game_id, quarter_label, t_df)

        if p_rows:
            _seed_players(p_rows, engine)
            upsert(pd.DataFrame(p_rows), engine,
                   "nba","player_box_score_stats",["game_id","player_id","quarter"])
            all_player_rows.extend(p_rows)

        if t_rows:
            upsert(pd.DataFrame(t_rows), engine,
                   "nba","team_box_score_stats",["game_id","team_id","quarter"])

        log.info(f"    {quarter_label}: {len(p_rows)} player, {len(t_rows)} team")

    # OT periods: sleep only when data returned, no sleep on empty check
    ot_periods_data = []
    ot_period = 5
    while True:
        try:
            ep_ot = boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=ot_period,
                end_period=ot_period,
                range_type=0,
                start_range=0,
                end_range=0,
                proxy=PROXY_URL,
            )
            ot_p_df = ep_ot.player_stats.get_data_frame()
            ot_t_df = ep_ot.team_stats.get_data_frame()
        except Exception:
            break

        if ot_p_df is None or ot_p_df.empty:
            break
        has_data = (
            ot_p_df["minutes"].notna().any()
            if "minutes" in ot_p_df.columns else False
        )
        if not has_data:
            break

        time.sleep(API_DELAY)
        ot_periods_data.append((ot_p_df, ot_t_df))
        ot_period += 1

    if ot_periods_data:
        ot_p_rows = _sum_ot_player_rows(game_id, ot_periods_data)
        ot_t_rows = _sum_ot_team_rows(game_id, ot_periods_data)
        if ot_p_rows:
            _seed_players(ot_p_rows, engine)
            upsert(pd.DataFrame(ot_p_rows), engine,
                   "nba","player_box_score_stats",["game_id","player_id","quarter"])
        if ot_t_rows:
            upsert(pd.DataFrame(ot_t_rows), engine,
                   "nba","team_box_score_stats",["game_id","team_id","quarter"])
        log.info(
            f"    OT ({len(ot_periods_data)} period(s) summed): "
            f"{len(ot_p_rows)} player, {len(ot_t_rows)} team"
        )

    # ------------------------------------------------------------------
    # BoxScoreAdvancedV3 + BoxScorePlayerTrackV3 -> player_tracking_stats
    # ------------------------------------------------------------------
    advanced_by_player  = {}
    tracking_by_player  = {}

    adv_ep = api_call(
        lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(
            game_id=game_id, proxy=PROXY_URL),
        f"BoxScoreAdvancedV3 {game_id}",
    )
    if adv_ep is not None:
        try:
            for _, row in adv_ep.player_stats.get_data_frame().iterrows():
                pid = safe_int(row.get("personId"))
                if pid is None:
                    continue
                advanced_by_player[pid] = {
                    "usage_pct":         safe_float(row.get("usagePercentage")),
                    "off_rating":        safe_float(row.get("offensiveRating")),
                    "def_rating":        safe_float(row.get("defensiveRating")),
                    "net_rating":        safe_float(row.get("netRating")),
                    "pace":              safe_float(row.get("pace")),
                    "pie":               safe_float(row.get("PIE")),
                    "true_shooting_pct": safe_float(row.get("trueShootingPercentage")),
                    "efg_pct":           safe_float(row.get("effectiveFieldGoalPercentage")),
                }
        except Exception as exc:
            log.warning(f"  BoxScoreAdvancedV3 parse failed for {game_id}: {exc}")

    trk_ep = api_call(
        lambda: boxscoreplayertrackv3.BoxScorePlayerTrackV3(
            game_id=game_id, proxy=PROXY_URL),
        f"BoxScorePlayerTrackV3 {game_id}",
    )
    if trk_ep is not None:
        try:
            for _, row in trk_ep.player_stats.get_data_frame().iterrows():
                pid = safe_int(row.get("personId"))
                if pid is None:
                    continue
                tracking_by_player[pid] = {
                    "team_id":                safe_int(row.get("teamId")),
                    "speed":                  safe_float(row.get("speed")),
                    "distance":               safe_float(row.get("distance")),
                    "touches":                safe_int(row.get("touches")),
                    "passes_made":            safe_int(row.get("passes")),
                    "secondary_ast":          safe_int(row.get("secondaryAssists")),
                    "ft_ast":                 safe_int(row.get("freeThrowAssists")),
                    "reb_chances":            safe_int(row.get("reboundChancesTotal")),
                    "oreb_chances":           safe_int(row.get("reboundChancesOffensive")),
                    "dreb_chances":           safe_int(row.get("reboundChancesDefensive")),
                    "contested_fgm":          safe_int(row.get("contestedFieldGoalsMade")),
                    "contested_fga":          safe_int(row.get("contestedFieldGoalsAttempted")),
                    "contested_fg_pct":       safe_float(row.get("contestedFieldGoalPercentage")),
                    "uncontested_fgm":        safe_int(row.get("uncontestedFieldGoalsMade")),
                    "uncontested_fga":        safe_int(row.get("uncontestedFieldGoalsAttempted")),
                    "uncontested_fg_pct":     safe_float(row.get("uncontestedFieldGoalsPercentage")),
                    "defended_at_rim_fgm":    safe_int(row.get("defendedAtRimFieldGoalsMade")),
                    "defended_at_rim_fga":    safe_int(row.get("defendedAtRimFieldGoalsAttempted")),
                    "defended_at_rim_fg_pct": safe_float(row.get("defendedAtRimFieldGoalPercentage")),
                }
        except Exception as exc:
            log.warning(f"  BoxScorePlayerTrackV3 parse failed for {game_id}: {exc}")

    all_pids = set(advanced_by_player) | set(tracking_by_player)
    if all_pids:
        tracking_rows = []
        for pid in all_pids:
            r = {"game_id": game_id, "player_id": pid}
            r.update(advanced_by_player.get(pid, {}))
            r.update(tracking_by_player.get(pid, {}))
            tracking_rows.append(r)
        _seed_players(tracking_rows, engine)
        upsert(pd.DataFrame(tracking_rows), engine,
               "nba","player_tracking_stats",["game_id","player_id"])
        log.info(f"    Tracking: {len(tracking_rows)}")

    # ------------------------------------------------------------------
    # BoxScoreHustleV2 -> player_box_score_hustle
    # ------------------------------------------------------------------
    hustle_ep = api_call(
        lambda: boxscorehustlev2.BoxScoreHustleV2(
            game_id=game_id, proxy=PROXY_URL),
        f"BoxScoreHustleV2 {game_id}",
    )
    if hustle_ep is not None:
        try:
            hustle_rows = []
            for _, row in hustle_ep.player_stats.get_data_frame().iterrows():
                pid = safe_int(row.get("personId"))
                if pid is None:
                    continue
                hustle_rows.append({
                    "game_id":                      game_id,
                    "player_id":                    pid,
                    "team_id":                      safe_int(row.get("teamId")),
                    "team_tricode":                 safe_str(row.get("teamTricode")),
                    "points":                       safe_int(row.get("points")),
                    "contested_shots":              safe_int(row.get("contestedShots")),
                    "contested_shots_2pt":          safe_int(row.get("contestedShots2pt")),
                    "contested_shots_3pt":          safe_int(row.get("contestedShots3pt")),
                    "deflections":                  safe_int(row.get("deflections")),
                    "charges_drawn":                safe_int(row.get("chargesDrawn")),
                    "screen_assists":               safe_int(row.get("screenAssists")),
                    "screen_assist_points":         safe_int(row.get("screenAssistPoints")),
                    "loose_balls_recovered_off":    safe_int(row.get("looseBallsRecoveredOffensive")),
                    "loose_balls_recovered_def":    safe_int(row.get("looseBallsRecoveredDefensive")),
                    "loose_balls_recovered_total":  safe_int(row.get("looseBallsRecoveredTotal")),
                    "offensive_box_outs":           safe_int(row.get("offensiveBoxOuts")),
                    "defensive_box_outs":           safe_int(row.get("defensiveBoxOuts")),
                    "box_out_player_team_rebounds": safe_int(row.get("boxOutPlayerTeamRebounds")),
                    "box_out_player_rebounds":      safe_int(row.get("boxOutPlayerRebounds")),
                    "box_outs":                     safe_int(row.get("boxOuts")),
                })
            if hustle_rows:
                _seed_players(hustle_rows, engine)
                upsert(pd.DataFrame(hustle_rows), engine,
                       "nba","player_box_score_hustle",["game_id","player_id"])
                log.info(f"    Hustle: {len(hustle_rows)}")
        except Exception as exc:
            log.warning(f"  BoxScoreHustleV2 parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # BoxScoreMatchupsV3 -> player_box_score_matchups
    # ------------------------------------------------------------------
    matchups_ep = api_call(
        lambda: boxscorematchupsv3.BoxScoreMatchupsV3(
            game_id=game_id, proxy=PROXY_URL),
        f"BoxScoreMatchupsV3 {game_id}",
    )
    if matchups_ep is not None:
        try:
            matchup_rows = []
            for _, row in matchups_ep.player_stats.get_data_frame().iterrows():
                pid_off = safe_int(row.get("personIdOff"))
                pid_def = safe_int(row.get("personIdDef"))
                if pid_off is None or pid_def is None:
                    continue
                matchup_rows.append({
                    "game_id":                   game_id,
                    "team_id":                   safe_int(row.get("teamId")),
                    "team_tricode":              safe_str(row.get("teamTricode")),
                    "person_id_off":             pid_off,
                    "first_name_off":            safe_str(row.get("firstNameOff")),
                    "family_name_off":           safe_str(row.get("familyNameOff")),
                    "jersey_num_off":            safe_str(row.get("jerseyNumOff")),
                    "person_id_def":             pid_def,
                    "first_name_def":            safe_str(row.get("firstNameDef")),
                    "family_name_def":           safe_str(row.get("familyNameDef")),
                    "jersey_num_def":            safe_str(row.get("jerseyNumDef")),
                    "position_def":              safe_str(row.get("positionDef")),
                    "matchup_minutes":           safe_str(row.get("matchupMinutes")),
                    "matchup_minutes_sort":      safe_float(row.get("matchupMinutesSort")),
                    "partial_possessions":       safe_float(row.get("partialPossessions")),
                    "pct_defender_total_time":   safe_float(row.get("percentageDefenderTotalTime")),
                    "pct_offensive_total_time":  safe_float(row.get("percentageOffensiveTotalTime")),
                    "pct_total_time_both_on":    safe_float(row.get("percentageTotalTimeBothOn")),
                    "switches_on":               safe_int(row.get("switchesOn")),
                    "player_points":             safe_int(row.get("playerPoints")),
                    "team_points":               safe_int(row.get("teamPoints")),
                    "matchup_assists":           safe_int(row.get("matchupAssists")),
                    "matchup_potential_assists": safe_int(row.get("matchupPotentialAssists")),
                    "matchup_turnovers":         safe_int(row.get("matchupTurnovers")),
                    "matchup_blocks":            safe_int(row.get("matchupBlocks")),
                    "matchup_fgm":               safe_int(row.get("matchupFieldGoalsMade")),
                    "matchup_fga":               safe_int(row.get("matchupFieldGoalsAttempted")),
                    "matchup_fg_pct":            safe_float(row.get("matchupFieldGoalsPercentage")),
                    "matchup_fg3m":              safe_int(row.get("matchupThreePointersMade")),
                    "matchup_fg3a":              safe_int(row.get("matchupThreePointersAttempted")),
                    "matchup_fg3_pct":           safe_float(row.get("matchupThreePointersPercentage")),
                    "help_blocks":               safe_int(row.get("helpBlocks")),
                    "help_fgm":                  safe_int(row.get("helpFieldGoalsMade")),
                    "help_fga":                  safe_int(row.get("helpFieldGoalsAttempted")),
                    "help_fg_pct":               safe_float(row.get("helpFieldGoalsPercentage")),
                    "matchup_ftm":               safe_int(row.get("matchupFreeThrowsMade")),
                    "matchup_fta":               safe_int(row.get("matchupFreeThrowsAttempted")),
                    "shooting_fouls":            safe_int(row.get("shootingFouls")),
                })
            if matchup_rows:
                upsert(pd.DataFrame(matchup_rows), engine,
                       "nba","player_box_score_matchups",
                       ["game_id","person_id_off","person_id_def"])
                log.info(f"    Matchups: {len(matchup_rows)}")
        except Exception as exc:
            log.warning(f"  BoxScoreMatchupsV3 parse failed for {game_id}: {exc}")

    # Matchup position aggregation (in-memory, no extra API call)
    if all_player_rows:
        _aggregate_matchup(all_player_rows, game_id, game_date, engine)


# ---------------------------------------------------------------------------
# Matchup position aggregation helper
# ---------------------------------------------------------------------------
def _aggregate_matchup(player_rows, game_id, game_date, engine):
    """
    Aggregate Q1-Q4 player rows into stats-allowed-by-defending-team
    per position group. OT rows excluded (regulation defensive signals only).
    """
    df = pd.DataFrame(player_rows)
    if df.empty or "team_id" not in df.columns:
        return

    df = df[df["quarter"].isin(["Q1","Q2","Q3","Q4"])]
    if df.empty:
        return

    team_ids = df["team_id"].dropna().unique().tolist()
    if len(team_ids) != 2:
        return

    team_abbr_map = (
        df[["team_id","team_abbreviation"]]
        .drop_duplicates("team_id")
        .set_index("team_id")["team_abbreviation"]
        .to_dict()
    )

    numeric_cols = ["fgm","fga","fg3m","fg3a","ftm","fta",
                    "oreb","dreb","reb","ast","stl","blk","tov","pf","pts"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    matchup_rows = []
    for _, row in df.iterrows():
        att_team = row["team_id"]
        if att_team not in team_ids:
            continue
        def_team = team_ids[0] if att_team == team_ids[1] else team_ids[1]
        pos      = str(row.get("position") or "").strip() or "UNKNOWN"
        matchup_rows.append({
            "game_id":             game_id,
            "game_date":           game_date,
            "defending_team_id":   def_team,
            "defending_team_abbr": team_abbr_map.get(def_team, ""),
            "position_group":      pos,
            **{c: int(row[c]) for c in numeric_cols},
        })

    if not matchup_rows:
        return

    agg_df  = pd.DataFrame(matchup_rows)
    grouped = (
        agg_df.groupby(["game_id","game_date","defending_team_id",
                        "defending_team_abbr","position_group"])[numeric_cols]
        .sum().reset_index()
    )
    counts = (
        agg_df.groupby(["game_id","game_date","defending_team_id",
                        "defending_team_abbr","position_group"])
        .size().reset_index(name="player_count")
    )
    final_df = grouped.merge(
        counts,
        on=["game_id","game_date","defending_team_id",
            "defending_team_abbr","position_group"],
    )
    final_df = final_df.rename(columns={c: f"total_{c}" for c in numeric_cols})
    upsert(final_df, engine, "nba","matchup_position_stats",
           ["game_id","defending_team_id","position_group"])
    log.info(f"    Matchup positions: {len(final_df)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="NBA ETL for the sports modeling database"
    )
    parser.add_argument(
        "--batch", type=int, default=DEFAULT_BATCH,
        help=f"Games to process per run (default: {DEFAULT_BATCH})",
    )
    parser.add_argument(
        "--season", type=str, default="2024-25",
        help="NBA season in YYYY-YY format (default: 2024-25)",
    )
    parser.add_argument(
        "--load-rosters", action="store_true",
        help="Force CommonTeamRoster reload even if nba.players is not empty",
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning("NBA_PROXY_URL not set. stats.nba.com requests will be blocked.")

    engine = get_engine()
    ensure_tables(engine)

    # Teams load every run so W/L and ranks stay current
    load_teams(engine, args.season)

    # Players load only when table is empty or forced
    if args.load_rosters or players_table_empty(engine):
        load_players(engine, args.season)
    else:
        log.info("nba.players already populated, skipping CommonTeamRoster load.")

    all_pairs      = get_all_season_game_ids(args.season)
    unloaded_pairs = get_unloaded_games(all_pairs, engine)
    batch_pairs    = unloaded_pairs[:args.batch]

    if not batch_pairs:
        log.info("No unloaded games found. ETL complete.")
        return

    remaining_after = len(unloaded_pairs) - len(batch_pairs)
    log.info(
        f"Batch: {len(batch_pairs)} game(s) to process, "
        f"{remaining_after} will remain after this run."
    )

    target_dates = list({gdate for _, gdate in batch_pairs})
    game_meta    = fetch_scoreboard_metadata(target_dates, args.season)

    for game_id, game_date in batch_pairs:
        process_game(game_id, game_date, game_meta, engine)

    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
