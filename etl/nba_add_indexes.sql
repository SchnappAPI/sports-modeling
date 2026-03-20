/*
nba_add_indexes.sql

Adds clustered indexes on game_date DESC to NBA fact tables so that
the most recent data appears first in table scans and previews.

Strategy
--------
SQL Server allows only one clustered index per table. By default the
PRIMARY KEY constraint creates a clustered index. To add a clustered
index on game_date we must:
  1. Drop FK constraints that reference the PKs being modified.
  2. Drop the existing clustered PK constraints.
  3. Re-add the PKs as NONCLUSTERED.
  4. Create new CLUSTERED indexes on game_date DESC.
  5. Restore all FK constraints.

Run this in SSMS against the sports-modeling database.
Safe to re-run; every step checks before acting.

Tables covered
  nba.player_box_score_stats
  nba.player_passing_stats
  nba.player_rebound_chances
  nba.daily_lineups
  nba.games
  nba.schedule
*/

-- ============================================================
-- STEP 1: Drop all FK constraints that reference PKs we are
-- about to modify (required by SQL Server before altering PKs)
-- ============================================================

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_pbss_game')
    ALTER TABLE nba.player_box_score_stats DROP CONSTRAINT fk_nba_pbss_game;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_pbss_player')
    ALTER TABLE nba.player_box_score_stats DROP CONSTRAINT fk_nba_pbss_player;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_pps_player')
    ALTER TABLE nba.player_passing_stats DROP CONSTRAINT fk_nba_pps_player;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_prc_player')
    ALTER TABLE nba.player_rebound_chances DROP CONSTRAINT fk_nba_prc_player;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_tbss_game')
    ALTER TABLE nba.team_box_score_stats DROP CONSTRAINT fk_nba_tbss_game;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_tbss_team')
    ALTER TABLE nba.team_box_score_stats DROP CONSTRAINT fk_nba_tbss_team;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_games_home')
    ALTER TABLE nba.games DROP CONSTRAINT fk_nba_games_home;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_games_away')
    ALTER TABLE nba.games DROP CONSTRAINT fk_nba_games_away;

IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_players_team')
    ALTER TABLE nba.players DROP CONSTRAINT fk_nba_players_team;

-- ============================================================
-- nba.player_box_score_stats
-- PK: (game_id, player_id, period)
-- Clustered on: game_date DESC, game_id, player_id, period
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_box_score_stats')
    AND name = 'pk_nba_pbss' AND is_primary_key = 1
)
    ALTER TABLE nba.player_box_score_stats DROP CONSTRAINT pk_nba_pbss;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_box_score_stats')
    AND name = 'cx_nba_pbss_date'
)
    DROP INDEX cx_nba_pbss_date ON nba.player_box_score_stats;

ALTER TABLE nba.player_box_score_stats
    ADD CONSTRAINT pk_nba_pbss
    PRIMARY KEY NONCLUSTERED (game_id, player_id, period);

CREATE CLUSTERED INDEX cx_nba_pbss_date
    ON nba.player_box_score_stats (game_date DESC, game_id, player_id, period);

-- ============================================================
-- nba.player_passing_stats
-- PK: (player_id, game_date)
-- Clustered on: game_date DESC, player_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_passing_stats')
    AND name = 'pk_nba_pps' AND is_primary_key = 1
)
    ALTER TABLE nba.player_passing_stats DROP CONSTRAINT pk_nba_pps;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_passing_stats')
    AND name = 'cx_nba_pps_date'
)
    DROP INDEX cx_nba_pps_date ON nba.player_passing_stats;

ALTER TABLE nba.player_passing_stats
    ADD CONSTRAINT pk_nba_pps
    PRIMARY KEY NONCLUSTERED (player_id, game_date);

CREATE CLUSTERED INDEX cx_nba_pps_date
    ON nba.player_passing_stats (game_date DESC, player_id);

-- ============================================================
-- nba.player_rebound_chances
-- PK: (player_id, game_date)
-- Clustered on: game_date DESC, player_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_rebound_chances')
    AND name = 'pk_nba_prc' AND is_primary_key = 1
)
    ALTER TABLE nba.player_rebound_chances DROP CONSTRAINT pk_nba_prc;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_rebound_chances')
    AND name = 'cx_nba_prc_date'
)
    DROP INDEX cx_nba_prc_date ON nba.player_rebound_chances;

ALTER TABLE nba.player_rebound_chances
    ADD CONSTRAINT pk_nba_prc
    PRIMARY KEY NONCLUSTERED (player_id, game_date);

CREATE CLUSTERED INDEX cx_nba_prc_date
    ON nba.player_rebound_chances (game_date DESC, player_id);

-- ============================================================
-- nba.daily_lineups
-- PK: (game_id, team_tricode, player_name)
-- Clustered on: game_date DESC, game_id, team_tricode, player_name
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.daily_lineups')
    AND name = 'pk_nba_lineups' AND is_primary_key = 1
)
    ALTER TABLE nba.daily_lineups DROP CONSTRAINT pk_nba_lineups;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.daily_lineups')
    AND name = 'cx_nba_lineups_date'
)
    DROP INDEX cx_nba_lineups_date ON nba.daily_lineups;

ALTER TABLE nba.daily_lineups
    ADD CONSTRAINT pk_nba_lineups
    PRIMARY KEY NONCLUSTERED (game_id, team_tricode, player_name);

CREATE CLUSTERED INDEX cx_nba_lineups_date
    ON nba.daily_lineups (game_date DESC, game_id, team_tricode, player_name);

-- ============================================================
-- nba.games
-- PK: (game_id)
-- Clustered on: game_date DESC, game_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.games')
    AND name = 'pk_nba_games' AND is_primary_key = 1
)
    ALTER TABLE nba.games DROP CONSTRAINT pk_nba_games;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.games')
    AND name = 'cx_nba_games_date'
)
    DROP INDEX cx_nba_games_date ON nba.games;

ALTER TABLE nba.games
    ADD CONSTRAINT pk_nba_games
    PRIMARY KEY NONCLUSTERED (game_id);

CREATE CLUSTERED INDEX cx_nba_games_date
    ON nba.games (game_date DESC, game_id);

-- ============================================================
-- nba.schedule
-- PK: (game_id)
-- Clustered on: game_date DESC, game_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.schedule')
    AND name = 'pk_nba_schedule' AND is_primary_key = 1
)
    ALTER TABLE nba.schedule DROP CONSTRAINT pk_nba_schedule;

IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.schedule')
    AND name = 'cx_nba_schedule_date'
)
    DROP INDEX cx_nba_schedule_date ON nba.schedule;

ALTER TABLE nba.schedule
    ADD CONSTRAINT pk_nba_schedule
    PRIMARY KEY NONCLUSTERED (game_id);

CREATE CLUSTERED INDEX cx_nba_schedule_date
    ON nba.schedule (game_date DESC, game_id);

-- ============================================================
-- STEP 2: Restore all FK constraints
-- ============================================================

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_players_team')
    ALTER TABLE nba.players
        ADD CONSTRAINT fk_nba_players_team FOREIGN KEY (team_id)
            REFERENCES nba.teams (team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_games_home')
    ALTER TABLE nba.games
        ADD CONSTRAINT fk_nba_games_home FOREIGN KEY (home_team_id)
            REFERENCES nba.teams (team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_games_away')
    ALTER TABLE nba.games
        ADD CONSTRAINT fk_nba_games_away FOREIGN KEY (away_team_id)
            REFERENCES nba.teams (team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_pbss_game')
    ALTER TABLE nba.player_box_score_stats
        ADD CONSTRAINT fk_nba_pbss_game FOREIGN KEY (game_id)
            REFERENCES nba.games (game_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_pbss_player')
    ALTER TABLE nba.player_box_score_stats
        ADD CONSTRAINT fk_nba_pbss_player FOREIGN KEY (player_id)
            REFERENCES nba.players (player_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_pps_player')
    ALTER TABLE nba.player_passing_stats
        ADD CONSTRAINT fk_nba_pps_player FOREIGN KEY (player_id)
            REFERENCES nba.players (player_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_nba_prc_player')
    ALTER TABLE nba.player_rebound_chances
        ADD CONSTRAINT fk_nba_prc_player FOREIGN KEY (player_id)
            REFERENCES nba.players (player_id);

PRINT 'All NBA indexes applied and FK constraints restored successfully.';
