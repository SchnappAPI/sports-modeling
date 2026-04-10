import os, sys
from sqlalchemy import create_engine, text

conn_str = (
    f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
    f"{os.environ['AZURE_SQL_PASSWORD']}@"
    f"{os.environ['AZURE_SQL_SERVER']}/"
    f"{os.environ['AZURE_SQL_DATABASE']}"
    "?driver=ODBC+Driver+18+for+SQL+Server"
    "&Encrypt=yes&TrustServerCertificate=no&Connection+Timeout=90"
)
engine = create_engine(conn_str, fast_executemany=False)

with engine.connect() as conn:
    r = conn.execute(text("""
        SELECT
            COUNT(*)                                                       AS total_rows,
            SUM(CASE WHEN p_hit_after_hit  IS NOT NULL THEN 1 ELSE 0 END) AS has_hit_prob,
            SUM(CASE WHEN p_hit_after_miss IS NOT NULL THEN 1 ELSE 0 END) AS has_miss_prob,
            SUM(is_momentum_player)                                        AS momentum_players,
            SUM(is_reversion_player)                                       AS reversion_players,
            SUM(is_bouncy_player)                                          AS bouncy_players,
            SUM(CASE WHEN pattern_strength >= 0.20 THEN 1 ELSE 0 END)     AS strong_patterns,
            AVG(CAST(n AS FLOAT))                                          AS avg_n,
            MAX(n)                                                         AS max_n
        FROM common.player_line_patterns
    """))
    for row in r:
        print(f"Total rows:          {row[0]:,}")
        print(f"Has hit prob:        {row[1]:,}")
        print(f"Has miss prob:       {row[2]:,}")
        print(f"Momentum players:    {row[3]:,}")
        print(f"Reversion players:   {row[4]:,}")
        print(f"Bouncy players:      {row[5]:,}")
        print(f"Strong patterns:     {row[6]:,}")
        print(f"Avg games per combo: {row[7]:.1f}")
        print(f"Max games:           {row[8]}")

    r2 = conn.execute(text("""
        SELECT TOP 10
            p.player_id,
            pl.full_name,
            p.market_key,
            p.line_value,
            p.n,
            p.hr_overall,
            p.p_hit_after_hit,
            p.p_hit_after_miss,
            p.pattern_strength
        FROM common.player_line_patterns p
        LEFT JOIN nba.players pl ON pl.player_id = p.player_id
        WHERE p.pattern_strength >= 0.30 AND p.n >= 15
        ORDER BY p.pattern_strength DESC
    """))
    print("\nTop 10 strongest patterns (strength>=0.30, n>=15):")
    print(f"  {'Player':<22} {'Market':<32} {'Line':>5} {'N':>3} {'HR':>5} {'AftHit':>7} {'AftMiss':>8} {'Str':>5}")
    for row in r2:
        name = (row[1] or f"pid={row[0]}")[:22]
        print(f"  {name:<22} {row[2]:<32} {row[3]:>5.1f} {row[4]:>3} "
              f"{row[5]:>5.1%} {(row[6] or 0):>7.1%} {(row[7] or 0):>8.1%} {(row[8] or 0):>5.2f}")

print("\nDone.")
