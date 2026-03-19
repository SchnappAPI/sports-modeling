# etl/nba/ddl.py
from sqlalchemy import text

def create_tables(engine):
    ddl = """
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'nba')
        EXEC('CREATE SCHEMA nba');

    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'rebound_chances' AND s.name = 'nba'
    )
    CREATE TABLE nba.rebound_chances (
        game_date DATE NOT NULL,
        player_id INT NOT NULL,
        player_name NVARCHAR(100),
        team_id INT,
        team_tricode NVARCHAR(10),

        oreb INT,
        dreb INT,
        reb INT,

        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_rebound_chances PRIMARY KEY (game_date, player_id)
    );

    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'potential_ast' AND s.name = 'nba'
    )
    CREATE TABLE nba.potential_ast (
        game_date DATE NOT NULL,
        player_id INT NOT NULL,
        player_name NVARCHAR(100),
        team_id INT,
        team_tricode NVARCHAR(10),

        passes_made INT,
        potential_ast INT,

        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_potential_ast PRIMARY KEY (game_date, player_id)
    );

    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = 'box_scores' AND s.name = 'nba'
    )
    CREATE TABLE nba.box_scores (
        game_id NVARCHAR(20),
        game_date DATE,
        player_id INT,
        period NVARCHAR(5),

        pts INT,
        reb INT,
        ast INT,
        minutes NVARCHAR(10),

        created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
        CONSTRAINT pk_box_scores PRIMARY KEY (game_id, player_id, period)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
