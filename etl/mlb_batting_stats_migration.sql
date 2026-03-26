-- mlb_batting_stats_migration.sql
--
-- Adds enhanced hitting box score columns to mlb.batting_stats.
-- Sourced from liveData.boxscore.teams.{side}.players in the /withMetrics endpoint.
-- Safe to run multiple times: each ALTER uses IF NOT EXISTS guard via INFORMATION_SCHEMA check.
-- Run once manually via a GitHub Actions dispatch before deploying the updated mlb_etl.py.
--
-- Columns added (matching fnGetHittingBoxScore.pq fields not in the original schema):
--   fly_outs, ground_outs, air_outs, pop_outs, line_outs,
--   total_bases, games_played, plate_appearances

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
