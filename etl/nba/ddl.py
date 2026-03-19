# etl/nba/ddl.py
from sqlalchemy import text

def create_tables(engine):
    ddl = """
    /* =====================================================
       SCHEMA
       ===================================================== */
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'nba')
        EXEC('CREATE SCHEMA nba');


    /* =====================================================
       TEAM FRANCHISES (STATIC REFERENCE)
       ===================================================== */
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'teams'
          AND s.name = 'nba'
    )
    BEGIN
        CREATE TABLE nba.teams (
            team_id            INT           NOT NULL,
            team_city          NVARCHAR(50)  NOT NULL,
            team_name          NVARCHAR(50)  NOT NULL,
            team_abbreviation  NVARCHAR(5)   NOT NULL,
            conference         NVARCHAR(10),
            division           NVARCHAR(20),
            created_at         DATETIME2     DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_nba_teams PRIMARY KEY (team_id)
        );
    END;


    /* =====================================================
       PLAYERS (ACTIVE ROSTER SNAPSHOT)
       ===================================================== */
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'players'
          AND s.name = 'nba'
    )
    BEGIN
        CREATE TABLE nba.players (
            player_id     INT           NOT NULL,
            player_name   NVARCHAR(100) NOT NULL,
            team_id       INT,
            created_at    DATETIME2     DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_nba_players PRIMARY KEY (player_id)
        );
    END;


    /* =====================================================
       GAMES (SCHEDULE / METADATA)
       ===================================================== */
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'games'
          AND s.name = 'nba'
    )
    BEGIN
        CREATE TABLE nba.games (
            game_id              NVARCHAR(20)  NOT NULL,
            game_code            NVARCHAR(30),
            game_date            DATE          NOT NULL,
            game_status          INT,
            game_status_text     NVARCHAR(50),

            game_datetime_est    DATETIME2,
            game_datetime_utc    DATETIME2,

            home_team_id         INT,
            home_team_tricode    NVARCHAR(5),
            away_team_id         INT,
            away_team_tricode    NVARCHAR(5),

            arena_name           NVARCHAR(100),
            arena_city           NVARCHAR(50),
            arena_state          NVARCHAR(50),

            created_at           DATETIME2     DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_nba_games PRIMARY KEY (game_id)
        );

        CREATE INDEX ix_nba_games_game_date
            ON nba.games (game_date);
    END;


    /* =====================================================
       REBOUND CHANCES (FACT)
       ===================================================== */
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'rebound_chances'
          AND s.name = 'nba'
    )
    BEGIN
        CREATE TABLE nba.rebound_chances (
            game_date        DATE          NOT NULL,
            player_id        INT           NOT NULL,
            player_name      NVARCHAR(100),
            team_id          INT,
            team_tricode     NVARCHAR(10),

            oreb              INT,
            dreb              INT,
            reb               INT,

            created_at        DATETIME2     DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_nba_rebound_chances
                PRIMARY KEY (game_date, player_id)
        );

        CREATE INDEX ix_nba_rebch_game_date
            ON nba.rebound_chances (game_date);
    END;


    /* =====================================================
       POTENTIAL ASSISTS (FACT)
       ===================================================== */
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'potential_assists'
          AND s.name = 'nba'
    )
    BEGIN
        CREATE TABLE nba.potential_assists (
            game_date        DATE          NOT NULL,
            player_id        INT           NOT NULL,
            player_name      NVARCHAR(100),
            team_id          INT,
            team_tricode     NVARCHAR(10),

            passes_made      INT,
            potential_ast    INT,

            created_at       DATETIME2     DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_nba_potential_assists
                PRIMARY KEY (game_date, player_id)
        );

        CREATE INDEX ix_nba_potast_game_date
            ON nba.potential_assists (game_date);
    END;


    /* =====================================================
       BOX SCORES (PERIOD-LEVEL FACT)
       ===================================================== */
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'box_scores'
          AND s.name = 'nba'
    )
    BEGIN
        CREATE TABLE nba.box_scores (
            game_id        NVARCHAR(20) NOT NULL,
            game_date      DATE         NOT NULL,
            player_id      INT          NOT NULL,
            period         NVARCHAR(5)  NOT NULL,

            minutes        NVARCHAR(10),
            pts            INT,
            reb            INT,
            ast            INT,

            created_at     DATETIME2    DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_nba_box_scores
                PRIMARY KEY (game_id, player_id, period)
        );

        CREATE INDEX ix_nba_box_scores_game_date
            ON nba.box_scores (game_date);
    END;
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
