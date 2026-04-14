-- mlb_batting_stats_migration.sql
--
-- Adds columns to mlb.batting_stats that were missing from the original table definition.
-- Safe to run multiple times: each ALTER uses IF NOT EXISTS guard via INFORMATION_SCHEMA check.

-- intentional_walks
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'intentional_walks'
)
    ALTER TABLE mlb.batting_stats ADD intentional_walks INT NULL;

-- hit_by_pitch
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'hit_by_pitch'
)
    ALTER TABLE mlb.batting_stats ADD hit_by_pitch INT NULL;

-- sac_bunts
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'sac_bunts'
)
    ALTER TABLE mlb.batting_stats ADD sac_bunts INT NULL;

-- sac_flies
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'sac_flies'
)
    ALTER TABLE mlb.batting_stats ADD sac_flies INT NULL;

-- fly_outs
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'fly_outs'
)
    ALTER TABLE mlb.batting_stats ADD fly_outs INT NULL;

-- ground_outs
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'ground_outs'
)
    ALTER TABLE mlb.batting_stats ADD ground_outs INT NULL;

-- air_outs
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'air_outs'
)
    ALTER TABLE mlb.batting_stats ADD air_outs INT NULL;

-- pop_outs
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'pop_outs'
)
    ALTER TABLE mlb.batting_stats ADD pop_outs INT NULL;

-- line_outs
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'line_outs'
)
    ALTER TABLE mlb.batting_stats ADD line_outs INT NULL;

-- total_bases
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'total_bases'
)
    ALTER TABLE mlb.batting_stats ADD total_bases INT NULL;

-- games_played
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'games_played'
)
    ALTER TABLE mlb.batting_stats ADD games_played INT NULL;

-- plate_appearances
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'batting_stats' AND COLUMN_NAME = 'plate_appearances'
)
    ALTER TABLE mlb.batting_stats ADD plate_appearances INT NULL;

-- Widen game_status to accommodate full status strings like 'Scheduled', 'In Progress'
-- Original column was too narrow (likely VARCHAR(1) storing only 'F')
ALTER TABLE mlb.games ALTER COLUMN game_status VARCHAR(20) NULL;
