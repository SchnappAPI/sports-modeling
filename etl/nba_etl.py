"""
nba_etl.py
NBA data pipeline for the sports modeling database.
Runs exclusively in GitHub Actions. Never runs locally.
Requires NBA_PROXY_URL for all stats.nba.com requests.

Run modes:
  python nba_etl.py                      Loads the oldest BATCH_SIZE unloaded games.
  python nba_etl.py --batch 50           Overrides the default batch size.
  python nba_etl.py --season 2023-24     Targets a specific season (default: 2024-25).

Design:
  Each run discovers all completed games for the target season, subtracts games
  already loaded (those with a row in nba.games), and processes the oldest
  BATCH_SIZE remaining ones. Repeated runs walk through the full season in order.
  Once every game is loaded each nightly run finds nothing new and exits cleanly.
  Games are never re-loaded. A game_id in nba.games means all child rows exist.

Quarter-level box score design:
  player_box_score_stats and team_box_score_stats store one row per player/team
  per quarter. Valid quarter values: Q1, Q2, Q3, Q4, OT.
  OT is a single combined row summing all overtime periods.
  Full-game totals are derived in Power BI or SQL by summing the quarter rows.
  BoxScoreTraditionalV3 is called with RangeType=2, StartPeriod=N, EndPeriod=N
  for each quarter individually. OT periods (5, 6, ...) are fetched until the
  response is empty, then all OT period stats per player are summed into one row.

Tables written per run:
  nba.teams                      Upserted from static data on every run (no HTTP).
  nba.players                    Upserted from box score data after all games load.
  nba.games                      One row per game. Presence = fully loaded marker.
  nba.play_by_play               Raw PBP events. PK: game_id + action_number.
  nba.player_box_score_stats     Quarter-level player box score (Q1/Q2/Q3/Q4/OT).
  nba.team_box_score_stats       Quarter-level team box score (Q1/Q2/Q3/Q4/OT).
  nba.player_tracking_stats      Game-level tracking + advanced metrics per player.
  nba.player_box_score_hustle    Game-level hustle stats per player.
  nba.player_box_score_matchups  Per-game offensive/defensive player matchup pairs.
  nba.game_rotation              Per-game player stints (in/out times, pts, +/-).
  nba.matchup_position_stats     Stats allowed by each team to each position per game.
  nba.player_season_stats        Season-to-date per-game averages. Upserted each run.
  nba.lineup_stats               Season-to-date 5-man lineup stats. Upserted each run.
  nba.gravity_leaders            Season gravity scores per player. Upserted each run.

API calls per game (up to 12, all proxied):
  1-4.  BoxScoreTraditionalV3 x4   Q1, Q2, Q3, Q4 -> player_box_score_stats,
                                                      team_box_score_stats
  5+.   BoxScoreTraditionalV3 xN   OT periods until empty, summed into one OT row
  Next. PlayByPlayV3               -> nba.play_by_play
  Next. BoxScoreAdvancedV3         -> merged into nba.player_tracking_stats
  Next. BoxScorePlayerTrackV3      -> merged into nba.player_tracking_stats
  Next. BoxScoreHustleV2           -> nba.player_box_score_hustle
  Next. BoxScoreMatchupsV3         -> nba.player_box_score_matchups
  Next. GameRotation               -> nba.game_rotation

Once per run (not per game, proxied):
  ScoreboardV3               -> nba.games metadata for each target date
  LeagueGameFinder           -> game ID discovery
  LeagueDashPlayerStats      -> nba.player_season_stats
  LeagueDashLineups          -> nba.lineup_stats
  GravityLeaders             -> nba.gravity_leaders

Once per run (no HTTP, embedded in package):
  static_teams.get_teams()   -> nba.teams

Secrets required (GitHub Actions env vars):
  NBA_PROXY_URL
  AZURE_SQL_SERVER
  AZURE_SQL_DATABASE
  AZURE_SQL_USERNAME
  AZURE_SQL_PASSWORD

Deprecation notes (nba_api v1.11.4):
  ScoreboardV2           Deprecated for 2025-26. Not used.
  PlayByPlayV2           Returns empty JSON. Not used.
  BoxScoreSummaryV2      Stopped returning data after 4/10/2025. Not used.
  BoxScorePlayerTrackV2  Retired by NBA. Not used. Replaced by V3.
  TeamGameLog/Logs       Deprecated by NBA with no replacement.

v1.11.2 fix note (BoxScoreTraditionalV3):
  Named accessors (.player_stats, .team_stats) are used throughout to avoid
  any dependence on dataset index order.

Known API quirk (LeagueGameFinder):
  The game_id_nullable parameter is silently ignored by the NBA API.
  Always filter results client-side.
"""

import argparse
import os
import time
import logging
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text

from nba_api.stats.endpoints import (
    leaguegamefinder,
    boxscoretraditionalv3,
    boxscoreadvancedv3,
    boxscoreplayertrackv3,
    boxscorehustlev2,
    boxscorematchupsv3,
    gamerotation,
    playbyplayv3,
    scoreboardv3,
    leaguedashplayerstats,
    leaguedashlineups,
    gravityleaders,
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
PROXY_URL     = os.environ.get("NBA_PROXY_URL")
API_DELAY     = 0.75   # seconds between API calls
RETRY_WAIT    = 5      # seconds before a retry attempt
RETRY_COUNT   = 3      # retry attempts per call
DEFAULT_BATCH = 30     # games per run if --batch not specified

# ---------------------------------------------------------------------------
# Database engine
# ---------------------------------------------------------------------------
def get_engine():
    server   = os.environ["AZURE_SQL_SERVER"]
    database = os.environ["AZURE_SQL_DATABASE"]
    username = os.environ["AZURE_SQL_USERNAME"]
    password = os.environ["AZURE_SQL_PASSWORD"]
    conn_str = (
        f"mssql+pyodbc://{username}:{password}"
        f"@{server}/{database}"
        "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
    )
    return create_engine(conn_str, fast_executemany=True)

# ---------------------------------------------------------------------------
# DDL: exact schema the ETL writes, nothing extra
# ---------------------------------------------------------------------------
DDL_STATEMENTS = [

    # nba.teams
    # Static data source. PK = nba_team (3-char abbreviation).
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.teams') AND type = 'U')
    CREATE TABLE nba.teams (
        nba_team      CHAR(3)      NOT NULL,
        nba_team_id   BIGINT       NOT NULL,
        nba_team_name VARCHAR(60)  NOT NULL,
        conference    VARCHAR(10)  NULL,
        roto_team     CHAR(3)      NULL,
        espn_team     CHAR(3)      NULL,
        espn_team_id  INT          NULL,
        aywt_team     CHAR(4)      NULL,
        aywt_team_id  INT          NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_teams   PRIMARY KEY (nba_team),
        CONSTRAINT uq_nba_team_id UNIQUE      (nba_team_id)
    )
    """,

    # nba.players
    # Populated from box score data. Extra mapping columns (espn, odds_api)
    # left NULL by this ETL and populated by other processes.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.players') AND type = 'U')
    CREATE TABLE nba.players (
        nba_player_id        BIGINT        NOT NULL,
        player_name          VARCHAR(100)  NOT NULL,
        espn_player_id       BIGINT        NULL,
        odds_api_player_name VARCHAR(100)  NULL,
        position             CHAR(5)       NULL,
        nba_team             CHAR(3)       NULL,
        player_team          VARCHAR(110)  NULL,
        is_current           BIT           NULL,
        created_at           DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_players      PRIMARY KEY (nba_player_id),
        CONSTRAINT fk_nba_players_team FOREIGN KEY (nba_team)
            REFERENCES nba.teams (nba_team)
    )
    """,

    # nba.games
    # One row per game. Writing this row is the "fully loaded" marker.
    # espn_game_id left NULL by this ETL; populated by other processes.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.games') AND type = 'U')
    CREATE TABLE nba.games (
        game_id       VARCHAR(15)  NOT NULL,
        espn_game_id  VARCHAR(20)  NULL,
        game_date     DATE         NOT NULL,
        game_datetime DATETIME2    NULL,
        game_code     VARCHAR(30)  NULL,
        game_sequence INT          NULL,
        game_display  VARCHAR(20)  NULL,
        home_team     CHAR(3)      NULL,
        home_team_id  VARCHAR(15)  NULL,
        away_team     CHAR(3)      NULL,
        away_team_id  VARCHAR(15)  NULL,
        season_year   CHAR(7)      NULL,
        created_at    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_games      PRIMARY KEY (game_id),
        CONSTRAINT fk_nba_games_home FOREIGN KEY (home_team)
            REFERENCES nba.teams (nba_team),
        CONSTRAINT fk_nba_games_away FOREIGN KEY (away_team)
            REFERENCES nba.teams (nba_team)
    )
    """,

    # nba.play_by_play
    # Raw PBP events from PlayByPlayV3. All camelCase fields.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.play_by_play') AND type = 'U')
    CREATE TABLE nba.play_by_play (
        game_id         VARCHAR(15)   NOT NULL,
        action_number   INT           NOT NULL,
        period          INT           NULL,
        clock           VARCHAR(20)   NULL,
        team_id         BIGINT        NULL,
        team_tricode    CHAR(3)       NULL,
        person_id       BIGINT        NULL,
        player_name     VARCHAR(100)  NULL,
        player_name_i   VARCHAR(50)   NULL,
        x_legacy        DECIMAL(6,1)  NULL,
        y_legacy        DECIMAL(6,1)  NULL,
        shot_distance   DECIMAL(6,1)  NULL,
        shot_result     VARCHAR(10)   NULL,
        is_field_goal   BIT           NULL,
        score_home      INT           NULL,
        score_away      INT           NULL,
        points_total    INT           NULL,
        location        VARCHAR(5)    NULL,
        description     VARCHAR(500)  NULL,
        action_type     VARCHAR(50)   NULL,
        sub_type        VARCHAR(50)   NULL,
        video_available BIT           NULL,
        action_id       INT           NULL,
        created_at      DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pbp      PRIMARY KEY (game_id, action_number),
        CONSTRAINT fk_nba_pbp_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id)
    )
    """,

    # nba.player_box_score_stats
    # Quarter-level. quarter values: Q1, Q2, Q3, Q4, OT.
    # OT is a single row summing all overtime periods.
    # Source: BoxScoreTraditionalV3 called per period (RangeType=2).
    # All camelCase field names from the V3 API.
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
        comment           VARCHAR(100)  NULL,
        jersey_num        VARCHAR(5)    NULL,
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
    # Quarter-level. Same quarter structure as player_box_score_stats.
    # Source: BoxScoreTraditionalV3 TeamStats dataset called per period.
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
            REFERENCES nba.games (game_id)
    )
    """,

    # nba.player_tracking_stats
    # Game-level. Merges BoxScoreAdvancedV3 and BoxScorePlayerTrackV3.
    # All camelCase field names from V3 APIs.
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
    # Game-level. Source: BoxScoreHustleV2. All camelCase field names.
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
    # Game-level. Source: BoxScoreMatchupsV3. All camelCase field names with
    # Off/Def suffixes. One row per (offensive player, defensive player) pair.
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

    # nba.game_rotation
    # Game-level. Source: GameRotation (legacy UPPER_CASE field names).
    # One row per player stint. Times in tenths of a second from tip-off.
    # PK includes in_time_real to handle multiple stints per player.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.game_rotation') AND type = 'U')
    CREATE TABLE nba.game_rotation (
        game_id        VARCHAR(15)   NOT NULL,
        team_id        BIGINT        NOT NULL,
        team_city      VARCHAR(30)   NULL,
        team_name      VARCHAR(30)   NULL,
        person_id      BIGINT        NOT NULL,
        player_first   VARCHAR(60)   NULL,
        player_last    VARCHAR(60)   NULL,
        in_time_real   INT           NOT NULL,
        out_time_real  INT           NULL,
        player_pts     SMALLINT      NULL,
        pt_diff        SMALLINT      NULL,
        usg_pct        DECIMAL(6,4)  NULL,
        created_at     DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_gr      PRIMARY KEY (game_id, team_id, person_id, in_time_real),
        CONSTRAINT fk_nba_gr_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id)
    )
    """,

    # nba.matchup_position_stats
    # Derived in-memory from quarter-level player rows after each game.
    # Stats allowed by the defending team to each position group in that game.
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

    # nba.player_season_stats
    # Season-to-date per-game averages. Source: LeagueDashPlayerStats PerGame.
    # Legacy UPPER_CASE field names. Rank columns excluded.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.player_season_stats') AND type = 'U')
    CREATE TABLE nba.player_season_stats (
        player_id         BIGINT        NOT NULL,
        season            CHAR(7)       NOT NULL,
        season_type       VARCHAR(20)   NOT NULL,
        player_name       VARCHAR(100)  NULL,
        team_id           BIGINT        NULL,
        team_abbreviation CHAR(3)       NULL,
        age               DECIMAL(5,1)  NULL,
        gp                SMALLINT      NULL,
        w                 SMALLINT      NULL,
        l                 SMALLINT      NULL,
        w_pct             DECIMAL(6,4)  NULL,
        min               DECIMAL(6,2)  NULL,
        fgm               DECIMAL(6,2)  NULL,
        fga               DECIMAL(6,2)  NULL,
        fg_pct            DECIMAL(6,4)  NULL,
        fg3m              DECIMAL(6,2)  NULL,
        fg3a              DECIMAL(6,2)  NULL,
        fg3_pct           DECIMAL(6,4)  NULL,
        ftm               DECIMAL(6,2)  NULL,
        fta               DECIMAL(6,2)  NULL,
        ft_pct            DECIMAL(6,4)  NULL,
        oreb              DECIMAL(6,2)  NULL,
        dreb              DECIMAL(6,2)  NULL,
        reb               DECIMAL(6,2)  NULL,
        ast               DECIMAL(6,2)  NULL,
        tov               DECIMAL(6,2)  NULL,
        stl               DECIMAL(6,2)  NULL,
        blk               DECIMAL(6,2)  NULL,
        blka              DECIMAL(6,2)  NULL,
        pf                DECIMAL(6,2)  NULL,
        pfd               DECIMAL(6,2)  NULL,
        pts               DECIMAL(6,2)  NULL,
        plus_minus        DECIMAL(7,2)  NULL,
        nba_fantasy_pts   DECIMAL(8,2)  NULL,
        dd2               SMALLINT      NULL,
        td3               SMALLINT      NULL,
        updated_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_pss PRIMARY KEY (player_id, season, season_type)
    )
    """,

    # nba.lineup_stats
    # Season-to-date per-game averages for 5-man lineups.
    # Source: LeagueDashLineups GroupQuantity=5 PerGame.
    # group_id is the five player IDs joined by a hyphen.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.lineup_stats') AND type = 'U')
    CREATE TABLE nba.lineup_stats (
        group_id          VARCHAR(100)  NOT NULL,
        season            CHAR(7)       NOT NULL,
        season_type       VARCHAR(20)   NOT NULL,
        group_name        VARCHAR(200)  NULL,
        team_id           BIGINT        NULL,
        team_abbreviation CHAR(3)       NULL,
        gp                SMALLINT      NULL,
        w                 SMALLINT      NULL,
        l                 SMALLINT      NULL,
        w_pct             DECIMAL(6,4)  NULL,
        min               DECIMAL(8,2)  NULL,
        fgm               DECIMAL(6,2)  NULL,
        fga               DECIMAL(6,2)  NULL,
        fg_pct            DECIMAL(6,4)  NULL,
        fg3m              DECIMAL(6,2)  NULL,
        fg3a              DECIMAL(6,2)  NULL,
        fg3_pct           DECIMAL(6,4)  NULL,
        ftm               DECIMAL(6,2)  NULL,
        fta               DECIMAL(6,2)  NULL,
        ft_pct            DECIMAL(6,4)  NULL,
        oreb              DECIMAL(6,2)  NULL,
        dreb              DECIMAL(6,2)  NULL,
        reb               DECIMAL(6,2)  NULL,
        ast               DECIMAL(6,2)  NULL,
        tov               DECIMAL(6,2)  NULL,
        stl               DECIMAL(6,2)  NULL,
        blk               DECIMAL(6,2)  NULL,
        blka              DECIMAL(6,2)  NULL,
        pf                DECIMAL(6,2)  NULL,
        pfd               DECIMAL(6,2)  NULL,
        pts               DECIMAL(6,2)  NULL,
        plus_minus        DECIMAL(7,2)  NULL,
        updated_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_ls PRIMARY KEY (group_id, season, season_type)
    )
    """,

    # nba.gravity_leaders
    # Season gravity scores per player. Source: GravityLeaders.
    # Gravity = how much defensive attention a player draws above expected.
    # UPPER_CASE field names matching NBAStatsGravityLeadersParser.LEADER_FIELDS.
    """
    IF NOT EXISTS (SELECT 1 FROM sys.objects
                   WHERE object_id = OBJECT_ID(N'nba.gravity_leaders') AND type = 'U')
    CREATE TABLE nba.gravity_leaders (
        player_id                         BIGINT        NOT NULL,
        season                            CHAR(7)       NOT NULL,
        season_type                       VARCHAR(20)   NOT NULL,
        first_name                        VARCHAR(60)   NULL,
        last_name                         VARCHAR(60)   NULL,
        team_id                           BIGINT        NULL,
        team_abbreviation                 CHAR(3)       NULL,
        team_name                         VARCHAR(40)   NULL,
        team_city                         VARCHAR(40)   NULL,
        frames                            INT           NULL,
        gravity_score                     DECIMAL(10,4) NULL,
        avg_gravity_score                 DECIMAL(10,4) NULL,
        on_ball_perimeter_frames          INT           NULL,
        on_ball_perimeter_gravity_score   DECIMAL(10,4) NULL,
        avg_on_ball_perimeter_gravity     DECIMAL(10,4) NULL,
        off_ball_perimeter_frames         INT           NULL,
        off_ball_perimeter_gravity_score  DECIMAL(10,4) NULL,
        avg_off_ball_perimeter_gravity    DECIMAL(10,4) NULL,
        on_ball_interior_frames           INT           NULL,
        on_ball_interior_gravity_score    DECIMAL(10,4) NULL,
        avg_on_ball_interior_gravity      DECIMAL(10,4) NULL,
        off_ball_interior_frames          INT           NULL,
        off_ball_interior_gravity_score   DECIMAL(10,4) NULL,
        avg_off_ball_interior_gravity     DECIMAL(10,4) NULL,
        games_played                      SMALLINT      NULL,
        minutes                           DECIMAL(8,1)  NULL,
        pts                               DECIMAL(6,2)  NULL,
        reb                               DECIMAL(6,2)  NULL,
        ast                               DECIMAL(6,2)  NULL,
        updated_at                        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
        CONSTRAINT pk_nba_gl PRIMARY KEY (player_id, season, season_type)
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
    try:
        return (
            float(val)
            if val not in (None, "", "None")
            and not (isinstance(val, float) and pd.isna(val))
            else None
        )
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return (
            int(val)
            if val not in (None, "", "None")
            and not (isinstance(val, float) and pd.isna(val))
            else None
        )
    except (ValueError, TypeError):
        return None


def safe_str(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val).strip() or None


def safe_bit(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return int(bool(val))
    except (ValueError, TypeError):
        return None


def safe_pct(num, den):
    """Compute a percentage from two counts, returning None if denominator is 0."""
    n = safe_int(num)
    d = safe_int(den)
    if n is None or d is None or d == 0:
        return None
    return round(n / d, 4)


# ---------------------------------------------------------------------------
# Retry wrapper
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
def _to_none(val):
    """
    Convert NaN, NaT, inf, or None to Python None so pyodbc sends NULL.
    Handles float nan/inf, numpy scalars, and pandas NA types.
    """
    if val is None:
        return None
    if isinstance(val, float):
        import math
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def upsert(df, engine, schema, table, pk_cols):
    """MERGE upsert. NaN/NaT -> None. fast_executemany=True on engine."""
    if df is None or df.empty:
        return
    df = df.apply(lambda col: col.map(_to_none))
    non_pk    = [c for c in df.columns if c not in pk_cols]
    col_list  = ", ".join(df.columns)
    val_list  = ", ".join(f":{c}" for c in df.columns)
    on_clause = " AND ".join(f"tgt.{c} = src.{c}" for c in pk_cols)
    update_set = (
        ", ".join(f"tgt.{c} = src.{c}" for c in non_pk)
        if non_pk
        else f"tgt.{pk_cols[0]} = tgt.{pk_cols[0]}"
    )
    merge_sql = f"""
        MERGE {schema}.{table} AS tgt
        USING (VALUES ({val_list})) AS src ({col_list})
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({val_list});
    """
    with engine.begin() as conn:
        conn.execute(text(merge_sql), df.to_dict(orient="records"))


# ---------------------------------------------------------------------------
# Step 1: Teams from static data (no HTTP, no proxy cost)
# ---------------------------------------------------------------------------
def load_teams(engine):
    log.info("Loading nba.teams from static data")
    raw  = static_teams.get_teams()
    rows = [
        {
            "nba_team":      t["abbreviation"],
            "nba_team_id":   t["id"],
            "nba_team_name": t["full_name"],
            "conference":    None,
            "roto_team":     t["abbreviation"],
            "espn_team":     t["abbreviation"],
            "espn_team_id":  None,
            "aywt_team":     t["abbreviation"],
            "aywt_team_id":  None,
        }
        for t in raw
    ]
    upsert(pd.DataFrame(rows), engine, "nba", "teams", ["nba_team"])
    log.info(f"  {len(rows)} teams upserted")


# ---------------------------------------------------------------------------
# Step 2: Discover all completed game IDs for the season
# ---------------------------------------------------------------------------
def get_all_season_game_ids(season):
    """
    Returns [(game_id, game_date), ...] sorted oldest first.
    Preseason IDs start with '001' and are excluded.
    """
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
    pairs = (
        df[["GAME_ID", "GAME_DATE"]]
        .drop_duplicates("GAME_ID")
        .values.tolist()
    )
    result = [
        (str(gid), pd.to_datetime(gdate).date())
        for gid, gdate in pairs
        if not str(gid).startswith("001")
    ]
    result.sort(key=lambda x: x[1])
    log.info(f"  Found {len(result)} completed games in season {season}")
    return result


# ---------------------------------------------------------------------------
# Step 3: Filter to games not yet in nba.games
# ---------------------------------------------------------------------------
def get_unloaded_games(all_pairs, engine):
    with engine.connect() as conn:
        loaded = {
            row[0]
            for row in conn.execute(text("SELECT DISTINCT game_id FROM nba.games"))
        }
    unloaded = [p for p in all_pairs if p[0] not in loaded]
    log.info(f"  {len(loaded)} already loaded, {len(unloaded)} remaining")
    return unloaded


# ---------------------------------------------------------------------------
# Step 4: Fetch ScoreboardV3 metadata for all dates in the batch
# ---------------------------------------------------------------------------
def fetch_scoreboard_metadata(target_dates, season):
    """
    ScoreboardV3 typed DataSet accessors:
      .game_header.get_data_frame()  one row per game
      .line_score.get_data_frame()   two rows per game: away=iloc[0], home=iloc[1]
    Returns dict keyed by game_id (str).
    """
    metadata = {}
    for game_date in sorted(set(target_dates)):
        date_str = game_date.strftime("%Y-%m-%d")
        sb = api_call(
            lambda d=date_str: scoreboardv3.ScoreboardV3(
                game_date=d,
                league_id="00",
                proxy=PROXY_URL,
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
                    away_tid  = safe_str(away_row.get("teamId"))
                    home_abbr = safe_str(home_row.get("teamTricode"))
                    home_tid  = safe_str(home_row.get("teamId"))
                elif len(game_ls) == 1:
                    home_abbr = safe_str(game_ls.iloc[0].get("teamTricode"))
                    home_tid  = safe_str(game_ls.iloc[0].get("teamId"))

                metadata[gid] = {
                    "game_date":    game_date,
                    "game_code":    safe_str(hdr.get("gameCode")),
                    "game_display": f"{away_abbr}@{home_abbr}" if away_abbr else None,
                    "home_team":    home_abbr,
                    "home_team_id": home_tid,
                    "away_team":    away_abbr,
                    "away_team_id": away_tid,
                    "season_year":  season[:7],
                }
        except Exception as exc:
            log.warning(f"  ScoreboardV3 parse failed for {date_str}: {exc}")

    log.info(f"  Scoreboard metadata fetched for {len(metadata)} game(s)")
    return metadata


# ---------------------------------------------------------------------------
# Quarter-level box score helpers
# ---------------------------------------------------------------------------
def _trad_player_rows(game_id, quarter_label, df):
    """
    Extract player stat rows from a BoxScoreTraditionalV3 PlayerStats DataFrame
    for a single period or summed-OT. Returns a list of dicts.
    DNP players (non-blank comment) are excluded.
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
            "comment":           comment or None,
            "jersey_num":        safe_str(row.get("jerseyNum")),
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
    """Extract team stat rows from a BoxScoreTraditionalV3 TeamStats DataFrame."""
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
    """
    Sum all OT period player DataFrames into one row per player labeled 'OT'.
    ot_periods_data is a list of (player_df, team_df) tuples, one per OT period.
    Counting stats are summed. Percentages are recomputed from summed made/attempted.
    plus_minus is summed.
    Returns a list of player row dicts.
    """
    if not ot_periods_data:
        return []

    # Collect all player rows across all OT periods
    all_rows = []
    for player_df, _ in ot_periods_data:
        all_rows.extend(_trad_player_rows(game_id, "OT", player_df))

    if not all_rows:
        return []

    # Group by player_id and sum counting stats
    df = pd.DataFrame(all_rows)
    count_cols = ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
                  "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "pts",
                  "plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    # Take first occurrence of metadata fields (they are identical across periods)
    meta_cols = ["game_id", "player_id", "quarter", "first_name", "last_name",
                 "team_id", "team_abbreviation", "position", "comment",
                 "jersey_num", "minutes"]
    agg_meta = df.groupby("player_id")[meta_cols].first().reset_index(drop=True)
    agg_counts = df.groupby("player_id")[count_cols].sum().reset_index()
    agg_counts.rename(columns={"player_id": "_pid"}, inplace=True)
    agg_meta["_pid"] = agg_meta["player_id"]
    merged = agg_meta.merge(agg_counts, on="_pid").drop(columns=["_pid"])

    # Recompute percentages from summed made/attempted
    merged["fg_pct"]  = merged.apply(lambda r: safe_pct(r["fgm"],  r["fga"]),  axis=1)
    merged["fg3_pct"] = merged.apply(lambda r: safe_pct(r["fg3m"], r["fg3a"]), axis=1)
    merged["ft_pct"]  = merged.apply(lambda r: safe_pct(r["ftm"],  r["fta"]),  axis=1)
    merged["quarter"] = "OT"

    return merged.to_dict(orient="records")


def _sum_ot_team_rows(game_id, ot_periods_data):
    """
    Sum all OT period team DataFrames into one row per team labeled 'OT'.
    ot_periods_data is a list of (player_df, team_df) tuples, one per OT period.
    """
    if not ot_periods_data:
        return []

    all_rows = []
    for _, team_df in ot_periods_data:
        all_rows.extend(_trad_team_rows(game_id, "OT", team_df))

    if not all_rows:
        return []

    df = pd.DataFrame(all_rows)
    count_cols = ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
                  "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "pts",
                  "plus_minus"]
    for c in count_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    meta_cols = ["game_id", "team_id", "quarter", "team_abbreviation", "minutes"]
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
# Step 5: Process one game
# ---------------------------------------------------------------------------
def process_game(game_id, game_date, game_meta, engine):
    log.info(f"  Processing {game_id} ({game_date})")

    # Write nba.games row first. This is both the FK anchor for all child tables
    # and the "fully loaded" marker that prevents re-processing on future runs.
    meta = game_meta.get(game_id)
    games_row = {
        "game_id":       game_id,
        "espn_game_id":  None,
        "game_date":     meta["game_date"]    if meta else game_date,
        "game_datetime": None,
        "game_code":     meta["game_code"]    if meta else None,
        "game_sequence": None,
        "game_display":  meta["game_display"] if meta else None,
        "home_team":     meta["home_team"]    if meta else None,
        "home_team_id":  meta["home_team_id"] if meta else None,
        "away_team":     meta["away_team"]    if meta else None,
        "away_team_id":  meta["away_team_id"] if meta else None,
        "season_year":   meta["season_year"]  if meta else None,
    }
    try:
        upsert(pd.DataFrame([games_row]), engine, "nba", "games", ["game_id"])
    except Exception as exc:
        log.error(f"  games upsert failed for {game_id}: {exc}")
        return

    if not meta:
        log.warning(f"  No scoreboard metadata for {game_id}, stub game row written")

    # ------------------------------------------------------------------
    # 5a. BoxScoreTraditionalV3  ->  Q1, Q2, Q3, Q4, OT
    #
    # Called with RangeType=2, StartPeriod=N, EndPeriod=N for each quarter.
    # This isolates each period exactly. RangeType=0 would give cumulative totals.
    #
    # OT detection: call period 5 first. If the player DataFrame is non-empty
    # and at least one player has non-None minutes, the game went to OT.
    # Continue fetching OT periods (6, 7, ...) until the response is empty.
    # Sum all OT DataFrames into single OT rows before writing.
    #
    # Named accessors: .player_stats, .team_stats (avoids index-order dependency
    # which was broken before v1.11.2).
    # ------------------------------------------------------------------
    all_player_rows = []   # collected across Q1-Q4 for matchup aggregation

    for period_num, quarter_label in [(1, "Q1"), (2, "Q2"), (3, "Q3"), (4, "Q4")]:
        ep = api_call(
            lambda p=period_num: boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=p,
                end_period=p,
                range_type=2,
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
                   "nba", "player_box_score_stats",
                   ["game_id", "player_id", "quarter"])
            all_player_rows.extend(p_rows)

        if t_rows:
            upsert(pd.DataFrame(t_rows), engine,
                   "nba", "team_box_score_stats",
                   ["game_id", "team_id", "quarter"])

        log.info(f"    {quarter_label}: {len(p_rows)} player rows, {len(t_rows)} team rows")

    # OT: fetch periods 5, 6, ... until empty
    ot_periods_data = []
    ot_period = 5
    while True:
        ep_ot = api_call(
            lambda p=ot_period: boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                start_period=p,
                end_period=p,
                range_type=2,
                start_range=0,
                end_range=0,
                proxy=PROXY_URL,
            ),
            f"BoxScoreTraditionalV3 {game_id} OT{ot_period - 4}",
        )
        if ep_ot is None:
            break
        try:
            ot_p_df = ep_ot.player_stats.get_data_frame()
            ot_t_df = ep_ot.team_stats.get_data_frame()
        except Exception:
            break

        # An empty DataFrame or all-None minutes means this period did not happen
        if ot_p_df is None or ot_p_df.empty:
            break
        has_data = (
            ot_p_df["minutes"].notna().any()
            if "minutes" in ot_p_df.columns
            else False
        )
        if not has_data:
            break

        ot_periods_data.append((ot_p_df, ot_t_df))
        ot_period += 1

    if ot_periods_data:
        ot_p_rows = _sum_ot_player_rows(game_id, ot_periods_data)
        ot_t_rows = _sum_ot_team_rows(game_id, ot_periods_data)

        if ot_p_rows:
            _seed_players(ot_p_rows, engine)
            upsert(pd.DataFrame(ot_p_rows), engine,
                   "nba", "player_box_score_stats",
                   ["game_id", "player_id", "quarter"])
            all_player_rows.extend(ot_p_rows)

        if ot_t_rows:
            upsert(pd.DataFrame(ot_t_rows), engine,
                   "nba", "team_box_score_stats",
                   ["game_id", "team_id", "quarter"])

        log.info(
            f"    OT ({len(ot_periods_data)} period(s) summed): "
            f"{len(ot_p_rows)} player rows, {len(ot_t_rows)} team rows"
        )

    # ------------------------------------------------------------------
    # 5b. PlayByPlayV3  ->  nba.play_by_play
    # Accessor: .play_by_play.get_data_frame(). All camelCase fields.
    # ------------------------------------------------------------------
    pbp_ep = api_call(
        lambda: playbyplayv3.PlayByPlayV3(game_id=game_id, proxy=PROXY_URL),
        f"PlayByPlayV3 {game_id}",
    )
    if pbp_ep is not None:
        try:
            pbp_df = pbp_ep.play_by_play.get_data_frame()
            pbp_rows = []
            for _, row in pbp_df.iterrows():
                an = safe_int(row.get("actionNumber"))
                if an is None:
                    continue
                pbp_rows.append({
                    "game_id":        game_id,
                    "action_number":  an,
                    "period":         safe_int(row.get("period")),
                    "clock":          safe_str(row.get("clock")),
                    "team_id":        safe_int(row.get("teamId")),
                    "team_tricode":   safe_str(row.get("teamTricode")),
                    "person_id":      safe_int(row.get("personId")),
                    "player_name":    safe_str(row.get("playerName")),
                    "player_name_i":  safe_str(row.get("playerNameI")),
                    "x_legacy":       safe_float(row.get("xLegacy")),
                    "y_legacy":       safe_float(row.get("yLegacy")),
                    "shot_distance":  safe_float(row.get("shotDistance")),
                    "shot_result":    safe_str(row.get("shotResult")),
                    "is_field_goal":  safe_bit(row.get("isFieldGoal")),
                    "score_home":     safe_int(row.get("scoreHome")),
                    "score_away":     safe_int(row.get("scoreAway")),
                    "points_total":   safe_int(row.get("pointsTotal")),
                    "location":       safe_str(row.get("location")),
                    "description":    safe_str(row.get("description")),
                    "action_type":    safe_str(row.get("actionType")),
                    "sub_type":       safe_str(row.get("subType")),
                    "video_available":safe_bit(row.get("videoAvailable")),
                    "action_id":      safe_int(row.get("actionId")),
                })
            if pbp_rows:
                upsert(pd.DataFrame(pbp_rows), engine,
                       "nba", "play_by_play", ["game_id", "action_number"])
                log.info(f"    PBP: {len(pbp_rows)} rows")
        except Exception as exc:
            log.warning(f"  PBP parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 5c. BoxScoreAdvancedV3 + BoxScorePlayerTrackV3
    #     -> merged into nba.player_tracking_stats
    #
    # Both are game-level (no period parameter). Advanced provides usage,
    # ratings, pace, PIE, TS%, EFG%. PlayerTrack provides speed, distance,
    # touches, passes, reb chances, contested/uncontested FG, defended-at-rim.
    # ------------------------------------------------------------------
    advanced_by_player = {}
    adv_ep = api_call(
        lambda: boxscoreadvancedv3.BoxScoreAdvancedV3(
            game_id=game_id, proxy=PROXY_URL
        ),
        f"BoxScoreAdvancedV3 {game_id}",
    )
    if adv_ep is not None:
        try:
            adv_df = adv_ep.player_stats.get_data_frame()
            for _, row in adv_df.iterrows():
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

    tracking_by_player = {}
    trk_ep = api_call(
        lambda: boxscoreplayertrackv3.BoxScorePlayerTrackV3(
            game_id=game_id, proxy=PROXY_URL
        ),
        f"BoxScorePlayerTrackV3 {game_id}",
    )
    if trk_ep is not None:
        try:
            trk_df = trk_ep.player_stats.get_data_frame()
            for _, row in trk_df.iterrows():
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
            row = {"game_id": game_id, "player_id": pid}
            row.update(advanced_by_player.get(pid, {}))
            row.update(tracking_by_player.get(pid, {}))
            tracking_rows.append(row)

        # Seed any players that appear in advanced/tracking but were DNP in the
        # traditional box score and therefore never passed through _seed_players.
        # This prevents FK violations on player_tracking_stats.
        _seed_players(tracking_rows, engine)

        upsert(pd.DataFrame(tracking_rows), engine,
               "nba", "player_tracking_stats", ["game_id", "player_id"])
        log.info(f"    Tracking: {len(tracking_rows)} rows")

    # ------------------------------------------------------------------
    # 5d. BoxScoreHustleV2  ->  nba.player_box_score_hustle
    # Accessor: .player_stats.get_data_frame(). All camelCase fields.
    # ------------------------------------------------------------------
    hustle_ep = api_call(
        lambda: boxscorehustlev2.BoxScoreHustleV2(
            game_id=game_id, proxy=PROXY_URL
        ),
        f"BoxScoreHustleV2 {game_id}",
    )
    if hustle_ep is not None:
        try:
            hustle_df = hustle_ep.player_stats.get_data_frame()
            hustle_rows = []
            for _, row in hustle_df.iterrows():
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
                upsert(pd.DataFrame(hustle_rows), engine,
                       "nba", "player_box_score_hustle", ["game_id", "player_id"])
                log.info(f"    Hustle: {len(hustle_rows)} rows")
        except Exception as exc:
            log.warning(f"  BoxScoreHustleV2 parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 5e. BoxScoreMatchupsV3  ->  nba.player_box_score_matchups
    # Accessor: .player_stats.get_data_frame(). camelCase with Off/Def suffixes.
    # ------------------------------------------------------------------
    matchups_ep = api_call(
        lambda: boxscorematchupsv3.BoxScoreMatchupsV3(
            game_id=game_id, proxy=PROXY_URL
        ),
        f"BoxScoreMatchupsV3 {game_id}",
    )
    if matchups_ep is not None:
        try:
            matchups_df = matchups_ep.player_stats.get_data_frame()
            matchup_rows = []
            for _, row in matchups_df.iterrows():
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
                       "nba", "player_box_score_matchups",
                       ["game_id", "person_id_off", "person_id_def"])
                log.info(f"    Matchups: {len(matchup_rows)} rows")
        except Exception as exc:
            log.warning(f"  BoxScoreMatchupsV3 parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 5f. GameRotation  ->  nba.game_rotation
    # Accessors: .away_team and .home_team. Legacy UPPER_CASE field names.
    # ------------------------------------------------------------------
    rotation_ep = api_call(
        lambda: gamerotation.GameRotation(game_id=game_id, proxy=PROXY_URL),
        f"GameRotation {game_id}",
    )
    if rotation_ep is not None:
        try:
            away_df = rotation_ep.away_team.get_data_frame()
            home_df = rotation_ep.home_team.get_data_frame()
            rotation_df = pd.concat([away_df, home_df], ignore_index=True)
            rotation_rows = []
            for _, row in rotation_df.iterrows():
                pid     = safe_int(row.get("PERSON_ID"))
                tid     = safe_int(row.get("TEAM_ID"))
                in_time = safe_int(row.get("IN_TIME_REAL"))
                if pid is None or tid is None or in_time is None:
                    continue
                rotation_rows.append({
                    "game_id":       game_id,
                    "team_id":       tid,
                    "team_city":     safe_str(row.get("TEAM_CITY")),
                    "team_name":     safe_str(row.get("TEAM_NAME")),
                    "person_id":     pid,
                    "player_first":  safe_str(row.get("PLAYER_FIRST")),
                    "player_last":   safe_str(row.get("PLAYER_LAST")),
                    "in_time_real":  in_time,
                    "out_time_real": safe_int(row.get("OUT_TIME_REAL")),
                    "player_pts":    safe_int(row.get("PLAYER_PTS")),
                    "pt_diff":       safe_int(row.get("PT_DIFF")),
                    "usg_pct":       safe_float(row.get("USG_PCT")),
                })
            if rotation_rows:
                upsert(pd.DataFrame(rotation_rows), engine,
                       "nba", "game_rotation",
                       ["game_id", "team_id", "person_id", "in_time_real"])
                log.info(f"    Rotation: {len(rotation_rows)} rows")
        except Exception as exc:
            log.warning(f"  GameRotation parse failed for {game_id}: {exc}")

    # ------------------------------------------------------------------
    # 5g. Matchup position aggregation (in-memory, no extra API call)
    # Derived from quarter rows already written. Uses Q1-Q4 rows only
    # (not OT) to represent regulation position defense; this can be
    # changed to include OT rows by using all_player_rows instead.
    # ------------------------------------------------------------------
    if all_player_rows:
        _aggregate_matchup(all_player_rows, game_id, game_date, engine)


# ---------------------------------------------------------------------------
# Helpers called from process_game
# ---------------------------------------------------------------------------
def _seed_players(player_rows, engine):
    """
    INSERT-only MERGE. Adds new nba.players rows so FK constraints on
    player_box_score_stats and child tables are satisfied before insert.
    Does not overwrite existing rows (upsert_players does that at end of batch).
    """
    seed_sql = """
        MERGE nba.players AS tgt
        USING (VALUES (:nba_player_id, :player_name, :nba_team, :position))
              AS src (nba_player_id, player_name, nba_team, position)
        ON tgt.nba_player_id = src.nba_player_id
        WHEN NOT MATCHED THEN INSERT
            (nba_player_id, player_name, nba_team, position, created_at)
        VALUES
            (src.nba_player_id, src.player_name,
             src.nba_team, src.position, GETUTCDATE());
    """
    seed_rows = [
        {
            "nba_player_id": r["player_id"],
            "player_name": (
                ((r.get("first_name") or "") + " " + (r.get("last_name") or "")).strip()
                or str(r["player_id"])
            ),
            "nba_team": r.get("team_abbreviation") or None,
            "position": r.get("position") or None,
        }
        for r in player_rows
        if r.get("player_id") is not None
    ]
    if not seed_rows:
        return
    with engine.begin() as conn:
        conn.execute(text(seed_sql), seed_rows)


def _aggregate_matchup(player_rows, game_id, game_date, engine):
    """
    For every player row, the defending team is the other team in the game.
    Groups by defending_team_id + position_group and sums counting stats.
    """
    df = pd.DataFrame(player_rows)
    if df.empty or "team_id" not in df.columns:
        return

    team_ids = df["team_id"].dropna().unique().tolist()
    if len(team_ids) != 2:
        return

    team_abbr_map = (
        df[["team_id", "team_abbreviation"]]
        .drop_duplicates("team_id")
        .set_index("team_id")["team_abbreviation"]
        .to_dict()
    )

    numeric_cols = ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta",
                    "oreb", "dreb", "reb", "ast", "stl", "blk", "tov", "pf", "pts"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    matchup_rows = []
    for _, row in df.iterrows():
        att_team = row["team_id"]
        if att_team not in team_ids:
            continue
        def_team = team_ids[0] if att_team == team_ids[1] else team_ids[1]
        pos = str(row.get("position") or "").strip() or "UNKNOWN"
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

    agg_df = pd.DataFrame(matchup_rows)
    grouped = (
        agg_df.groupby(["game_id", "game_date", "defending_team_id",
                        "defending_team_abbr", "position_group"])[numeric_cols]
        .sum()
        .reset_index()
    )
    counts = (
        agg_df.groupby(["game_id", "game_date", "defending_team_id",
                        "defending_team_abbr", "position_group"])
        .size()
        .reset_index(name="player_count")
    )
    final_df = grouped.merge(
        counts,
        on=["game_id", "game_date", "defending_team_id",
            "defending_team_abbr", "position_group"],
    )
    final_df = final_df.rename(columns={c: f"total_{c}" for c in numeric_cols})

    upsert(final_df, engine, "nba", "matchup_position_stats",
           ["game_id", "defending_team_id", "position_group"])
    log.info(f"    Matchup positions: {len(final_df)} rows")


# ---------------------------------------------------------------------------
# Once-per-run: full player reference update from box score data
# ---------------------------------------------------------------------------
def upsert_players(engine):
    """
    Updates nba.players with the latest name/team/position from box score data.
    Sources from the most recent game row per player.
    """
    log.info("Upserting nba.players from box score data")
    sql = """
        MERGE nba.players AS tgt
        USING (
            SELECT
                player_id                                       AS nba_player_id,
                LTRIM(RTRIM(ISNULL(first_name,'') + ' '
                      + ISNULL(last_name,'')))                  AS player_name,
                team_abbreviation                               AS nba_team,
                position
            FROM (
                SELECT
                    player_id, first_name, last_name,
                    team_abbreviation, position,
                    ROW_NUMBER() OVER (
                        PARTITION BY player_id
                        ORDER BY game_id DESC
                    ) AS rn
                FROM nba.player_box_score_stats
                WHERE player_id IS NOT NULL
            ) x
            WHERE rn = 1
        ) AS src
        ON tgt.nba_player_id = src.nba_player_id
        WHEN MATCHED THEN UPDATE SET
            tgt.player_name = src.player_name,
            tgt.nba_team    = src.nba_team,
            tgt.position    = src.position
        WHEN NOT MATCHED THEN INSERT
            (nba_player_id, player_name, nba_team, position, created_at)
        VALUES
            (src.nba_player_id, src.player_name,
             src.nba_team, src.position, GETUTCDATE());
    """
    try:
        with engine.begin() as conn:
            result = conn.execute(text(sql))
        log.info(f"  Players merge: {result.rowcount} rows affected")
    except Exception as exc:
        log.warning(f"  Players merge failed: {exc}")


# ---------------------------------------------------------------------------
# Once-per-run: LeagueDashPlayerStats  ->  nba.player_season_stats
# ---------------------------------------------------------------------------
def load_player_season_stats(engine, season, season_type="Regular Season"):
    log.info(f"Loading nba.player_season_stats for {season}")
    ep = api_call(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="PerGame",
            proxy=PROXY_URL,
        ),
        "LeagueDashPlayerStats",
    )
    if ep is None:
        return
    try:
        df = ep.league_dash_player_stats.get_data_frame()
        rows = []
        for _, row in df.iterrows():
            pid = safe_int(row.get("PLAYER_ID"))
            if pid is None:
                continue
            rows.append({
                "player_id":         pid,
                "season":            season,
                "season_type":       season_type,
                "player_name":       safe_str(row.get("PLAYER_NAME")),
                "team_id":           safe_int(row.get("TEAM_ID")),
                "team_abbreviation": safe_str(row.get("TEAM_ABBREVIATION")),
                "age":               safe_float(row.get("AGE")),
                "gp":                safe_int(row.get("GP")),
                "w":                 safe_int(row.get("W")),
                "l":                 safe_int(row.get("L")),
                "w_pct":             safe_float(row.get("W_PCT")),
                "min":               safe_float(row.get("MIN")),
                "fgm":               safe_float(row.get("FGM")),
                "fga":               safe_float(row.get("FGA")),
                "fg_pct":            safe_float(row.get("FG_PCT")),
                "fg3m":              safe_float(row.get("FG3M")),
                "fg3a":              safe_float(row.get("FG3A")),
                "fg3_pct":           safe_float(row.get("FG3_PCT")),
                "ftm":               safe_float(row.get("FTM")),
                "fta":               safe_float(row.get("FTA")),
                "ft_pct":            safe_float(row.get("FT_PCT")),
                "oreb":              safe_float(row.get("OREB")),
                "dreb":              safe_float(row.get("DREB")),
                "reb":               safe_float(row.get("REB")),
                "ast":               safe_float(row.get("AST")),
                "tov":               safe_float(row.get("TOV")),
                "stl":               safe_float(row.get("STL")),
                "blk":               safe_float(row.get("BLK")),
                "blka":              safe_float(row.get("BLKA")),
                "pf":                safe_float(row.get("PF")),
                "pfd":               safe_float(row.get("PFD")),
                "pts":               safe_float(row.get("PTS")),
                "plus_minus":        safe_float(row.get("PLUS_MINUS")),
                "nba_fantasy_pts":   safe_float(row.get("NBA_FANTASY_PTS")),
                "dd2":               safe_int(row.get("DD2")),
                "td3":               safe_int(row.get("TD3")),
            })
        if rows:
            upsert(pd.DataFrame(rows), engine,
                   "nba", "player_season_stats",
                   ["player_id", "season", "season_type"])
            log.info(f"  {len(rows)} player season stat rows upserted")
    except Exception as exc:
        log.warning(f"  LeagueDashPlayerStats parse failed: {exc}")


# ---------------------------------------------------------------------------
# Once-per-run: LeagueDashLineups  ->  nba.lineup_stats
# ---------------------------------------------------------------------------
def load_lineup_stats(engine, season, season_type="Regular Season"):
    log.info(f"Loading nba.lineup_stats for {season}")
    ep = api_call(
        lambda: leaguedashlineups.LeagueDashLineups(
            group_quantity=5,
            season=season,
            season_type_all_star=season_type,
            per_mode_detailed="PerGame",
            proxy=PROXY_URL,
        ),
        "LeagueDashLineups",
    )
    if ep is None:
        return
    try:
        df = ep.lineups.get_data_frame()
        rows = []
        for _, row in df.iterrows():
            gid = safe_str(row.get("GROUP_ID"))
            if not gid:
                continue
            rows.append({
                "group_id":          gid,
                "season":            season,
                "season_type":       season_type,
                "group_name":        safe_str(row.get("GROUP_NAME")),
                "team_id":           safe_int(row.get("TEAM_ID")),
                "team_abbreviation": safe_str(row.get("TEAM_ABBREVIATION")),
                "gp":                safe_int(row.get("GP")),
                "w":                 safe_int(row.get("W")),
                "l":                 safe_int(row.get("L")),
                "w_pct":             safe_float(row.get("W_PCT")),
                "min":               safe_float(row.get("MIN")),
                "fgm":               safe_float(row.get("FGM")),
                "fga":               safe_float(row.get("FGA")),
                "fg_pct":            safe_float(row.get("FG_PCT")),
                "fg3m":              safe_float(row.get("FG3M")),
                "fg3a":              safe_float(row.get("FG3A")),
                "fg3_pct":           safe_float(row.get("FG3_PCT")),
                "ftm":               safe_float(row.get("FTM")),
                "fta":               safe_float(row.get("FTA")),
                "ft_pct":            safe_float(row.get("FT_PCT")),
                "oreb":              safe_float(row.get("OREB")),
                "dreb":              safe_float(row.get("DREB")),
                "reb":               safe_float(row.get("REB")),
                "ast":               safe_float(row.get("AST")),
                "tov":               safe_float(row.get("TOV")),
                "stl":               safe_float(row.get("STL")),
                "blk":               safe_float(row.get("BLK")),
                "blka":              safe_float(row.get("BLKA")),
                "pf":                safe_float(row.get("PF")),
                "pfd":               safe_float(row.get("PFD")),
                "pts":               safe_float(row.get("PTS")),
                "plus_minus":        safe_float(row.get("PLUS_MINUS")),
            })
        if rows:
            upsert(pd.DataFrame(rows), engine,
                   "nba", "lineup_stats",
                   ["group_id", "season", "season_type"])
            log.info(f"  {len(rows)} lineup stat rows upserted")
    except Exception as exc:
        log.warning(f"  LeagueDashLineups parse failed: {exc}")


# ---------------------------------------------------------------------------
# Once-per-run: GravityLeaders  ->  nba.gravity_leaders
# ---------------------------------------------------------------------------
def load_gravity_leaders(engine, season, season_type="Regular Season"):
    log.info(f"Loading nba.gravity_leaders for {season}")
    ep = api_call(
        lambda: gravityleaders.GravityLeaders(
            season=season,
            season_type_all_star=season_type,
            proxy=PROXY_URL,
        ),
        "GravityLeaders",
    )
    if ep is None:
        return
    try:
        df = ep.leaders.get_data_frame()
        rows = []
        for _, row in df.iterrows():
            pid = safe_int(row.get("PLAYERID"))
            if pid is None:
                continue
            rows.append({
                "player_id":                        pid,
                "season":                           season,
                "season_type":                      season_type,
                "first_name":                       safe_str(row.get("FIRSTNAME")),
                "last_name":                        safe_str(row.get("LASTNAME")),
                "team_id":                          safe_int(row.get("TEAMID")),
                "team_abbreviation":                safe_str(row.get("TEAMABBREVIATION")),
                "team_name":                        safe_str(row.get("TEAMNAME")),
                "team_city":                        safe_str(row.get("TEAMCITY")),
                "frames":                           safe_int(row.get("FRAMES")),
                "gravity_score":                    safe_float(row.get("GRAVITYSCORE")),
                "avg_gravity_score":                safe_float(row.get("AVGGRAVITYSCORE")),
                "on_ball_perimeter_frames":         safe_int(row.get("ONBALLPERIMETERFRAMES")),
                "on_ball_perimeter_gravity_score":  safe_float(row.get("ONBALLPERIMETERGRAVITYSCORE")),
                "avg_on_ball_perimeter_gravity":    safe_float(row.get("AVGONBALLPERIMETERGRAVITYSCORE")),
                "off_ball_perimeter_frames":        safe_int(row.get("OFFBALLPERIMETERFRAMES")),
                "off_ball_perimeter_gravity_score": safe_float(row.get("OFFBALLPERIMETERGRAVITYSCORE")),
                "avg_off_ball_perimeter_gravity":   safe_float(row.get("AVGOFFBALLPERIMETERGRAVITYSCORE")),
                "on_ball_interior_frames":          safe_int(row.get("ONBALLINTERIORFRAMES")),
                "on_ball_interior_gravity_score":   safe_float(row.get("ONBALLINTERIORGRAVITYSCORE")),
                "avg_on_ball_interior_gravity":     safe_float(row.get("AVGONBALLINTERIORGRAVITYSCORE")),
                "off_ball_interior_frames":         safe_int(row.get("OFFBALLINTERIORFRAMES")),
                "off_ball_interior_gravity_score":  safe_float(row.get("OFFBALLINTERIORGRAVITYSCORE")),
                "avg_off_ball_interior_gravity":    safe_float(row.get("AVGOFFBALLINTERIORGRAVITYSCORE")),
                "games_played":                     safe_int(row.get("GAMESPLAYED")),
                "minutes":                          safe_float(row.get("MINUTES")),
                "pts":                              safe_float(row.get("PTS")),
                "reb":                              safe_float(row.get("REB")),
                "ast":                              safe_float(row.get("AST")),
            })
        if rows:
            upsert(pd.DataFrame(rows), engine,
                   "nba", "gravity_leaders",
                   ["player_id", "season", "season_type"])
            log.info(f"  {len(rows)} gravity leader rows upserted")
    except Exception as exc:
        log.warning(f"  GravityLeaders parse failed: {exc}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="NBA ETL for the sports modeling database"
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=DEFAULT_BATCH,
        help=f"Number of unloaded games to process per run (default: {DEFAULT_BATCH})",
    )
    parser.add_argument(
        "--season",
        type=str,
        default="2024-25",
        help="NBA season in YYYY-YY format (default: 2024-25)",
    )
    args = parser.parse_args()

    if PROXY_URL:
        log.info(f"Proxy active: {PROXY_URL.split('@')[-1]}")
    else:
        log.warning(
            "NBA_PROXY_URL not set. stats.nba.com requests will likely be blocked."
        )

    engine = get_engine()

    ensure_tables(engine)
    load_teams(engine)

    all_pairs      = get_all_season_game_ids(args.season)
    unloaded_pairs = get_unloaded_games(all_pairs, engine)
    batch_pairs    = unloaded_pairs[:args.batch]

    if not batch_pairs:
        log.info("No unloaded games found. Running season-aggregate updates only.")
    else:
        remaining_after = len(unloaded_pairs) - len(batch_pairs)
        log.info(
            f"Batch: processing {len(batch_pairs)} game(s). "
            f"{remaining_after} game(s) will remain after this run."
        )

        target_dates = list({gdate for _, gdate in batch_pairs})
        game_meta    = fetch_scoreboard_metadata(target_dates, args.season)

        for game_id, game_date in batch_pairs:
            process_game(game_id, game_date, game_meta, engine)

        upsert_players(engine)

    # Season-aggregate calls run every time, even when batch is empty,
    # so the season totals stay current on nights with no new games.
    load_player_season_stats(engine, args.season)
    load_lineup_stats(engine, args.season)
    load_gravity_leaders(engine, args.season)

    log.info("NBA ETL complete.")


if __name__ == "__main__":
    main()
