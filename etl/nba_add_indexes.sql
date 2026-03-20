/*
nba_add_indexes.sql

Adds clustered indexes on game_date DESC to NBA fact tables so that
the most recent data appears first in table scans and previews.

Strategy
--------
SQL Server allows only one clustered index per table. By default the
PRIMARY KEY constraint creates a clustered index. To add a clustered
index on game_date we must:
  1. Drop the existing clustered PK constraint.
  2. Re-add the PK as a non-clustered constraint.
  3. Create a new clustered index on game_date DESC (plus the PK
     columns as a tiebreaker to keep rows unique within a date).

Run this in SSMS against the sports-modeling database after clearing
and reloading all data. Safe to re-run; each step is guarded.

Tables covered
  nba.player_box_score_stats
  nba.player_passing_stats
  nba.player_rebound_chances
  nba.daily_lineups
  nba.games
  nba.schedule
*/

-- ============================================================
-- nba.player_box_score_stats
-- PK: (game_id, player_id, period)
-- Clustered on: game_date DESC, game_id, player_id, period
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_box_score_stats')
    AND name = 'pk_nba_pbss'
    AND is_primary_key = 1
)
BEGIN
    ALTER TABLE nba.player_box_score_stats
        DROP CONSTRAINT pk_nba_pbss;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_box_score_stats')
    AND name = 'pk_nba_pbss'
)
BEGIN
    ALTER TABLE nba.player_box_score_stats
        ADD CONSTRAINT pk_nba_pbss
        PRIMARY KEY NONCLUSTERED (game_id, player_id, period);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_box_score_stats')
    AND name = 'cx_nba_pbss_date'
)
BEGIN
    CREATE CLUSTERED INDEX cx_nba_pbss_date
        ON nba.player_box_score_stats (game_date DESC, game_id, player_id, period);
END

-- ============================================================
-- nba.player_passing_stats
-- PK: (player_id, game_date)
-- Clustered on: game_date DESC, player_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_passing_stats')
    AND name = 'pk_nba_pps'
    AND is_primary_key = 1
)
BEGIN
    ALTER TABLE nba.player_passing_stats
        DROP CONSTRAINT pk_nba_pps;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_passing_stats')
    AND name = 'pk_nba_pps'
)
BEGIN
    ALTER TABLE nba.player_passing_stats
        ADD CONSTRAINT pk_nba_pps
        PRIMARY KEY NONCLUSTERED (player_id, game_date);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_passing_stats')
    AND name = 'cx_nba_pps_date'
)
BEGIN
    CREATE CLUSTERED INDEX cx_nba_pps_date
        ON nba.player_passing_stats (game_date DESC, player_id);
END

-- ============================================================
-- nba.player_rebound_chances
-- PK: (player_id, game_date)
-- Clustered on: game_date DESC, player_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_rebound_chances')
    AND name = 'pk_nba_prc'
    AND is_primary_key = 1
)
BEGIN
    ALTER TABLE nba.player_rebound_chances
        DROP CONSTRAINT pk_nba_prc;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_rebound_chances')
    AND name = 'pk_nba_prc'
)
BEGIN
    ALTER TABLE nba.player_rebound_chances
        ADD CONSTRAINT pk_nba_prc
        PRIMARY KEY NONCLUSTERED (player_id, game_date);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.player_rebound_chances')
    AND name = 'cx_nba_prc_date'
)
BEGIN
    CREATE CLUSTERED INDEX cx_nba_prc_date
        ON nba.player_rebound_chances (game_date DESC, player_id);
END

-- ============================================================
-- nba.daily_lineups
-- PK: (game_id, team_tricode, player_name)
-- Clustered on: game_date DESC, game_id, team_tricode, player_name
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.daily_lineups')
    AND name = 'pk_nba_lineups'
    AND is_primary_key = 1
)
BEGIN
    ALTER TABLE nba.daily_lineups
        DROP CONSTRAINT pk_nba_lineups;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.daily_lineups')
    AND name = 'pk_nba_lineups'
)
BEGIN
    ALTER TABLE nba.daily_lineups
        ADD CONSTRAINT pk_nba_lineups
        PRIMARY KEY NONCLUSTERED (game_id, team_tricode, player_name);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.daily_lineups')
    AND name = 'cx_nba_lineups_date'
)
BEGIN
    CREATE CLUSTERED INDEX cx_nba_lineups_date
        ON nba.daily_lineups (game_date DESC, game_id, team_tricode, player_name);
END

-- ============================================================
-- nba.games
-- PK: (game_id)
-- Clustered on: game_date DESC, game_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.games')
    AND name = 'pk_nba_games'
    AND is_primary_key = 1
)
BEGIN
    ALTER TABLE nba.games
        DROP CONSTRAINT pk_nba_games;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.games')
    AND name = 'pk_nba_games'
)
BEGIN
    ALTER TABLE nba.games
        ADD CONSTRAINT pk_nba_games
        PRIMARY KEY NONCLUSTERED (game_id);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.games')
    AND name = 'cx_nba_games_date'
)
BEGIN
    CREATE CLUSTERED INDEX cx_nba_games_date
        ON nba.games (game_date DESC, game_id);
END

-- ============================================================
-- nba.schedule
-- PK: (game_id)
-- Clustered on: game_date DESC, game_id
-- ============================================================
IF EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.schedule')
    AND name = 'pk_nba_schedule'
    AND is_primary_key = 1
)
BEGIN
    ALTER TABLE nba.schedule
        DROP CONSTRAINT pk_nba_schedule;
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.schedule')
    AND name = 'pk_nba_schedule'
)
BEGIN
    ALTER TABLE nba.schedule
        ADD CONSTRAINT pk_nba_schedule
        PRIMARY KEY NONCLUSTERED (game_id);
END

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID('nba.schedule')
    AND name = 'cx_nba_schedule_date'
)
BEGIN
    CREATE CLUSTERED INDEX cx_nba_schedule_date
        ON nba.schedule (game_date DESC, game_id);
END

PRINT 'All NBA indexes applied successfully.';
