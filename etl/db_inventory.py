import os, subprocess, sys

out = "/tmp/inv_out.txt"
with open(out, "w") as f:
    subprocess.run([sys.executable, "-c", """
import os
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
    r = conn.execute(text(\"\"\"
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN p_hit_after_hit  IS NOT NULL THEN 1 ELSE 0 END) AS has_hit,
            SUM(CASE WHEN p_hit_after_miss IS NOT NULL THEN 1 ELSE 0 END) AS has_miss,
            SUM(CAST(is_momentum_player  AS INT)) AS momentum,
            SUM(CAST(is_reversion_player AS INT)) AS reversion,
            SUM(CAST(is_bouncy_player    AS INT)) AS bouncy,
            SUM(CASE WHEN pattern_strength >= 0.20 THEN 1 ELSE 0 END) AS strong,
            AVG(CAST(n AS FLOAT)) AS avg_n, MAX(n) AS max_n
        FROM common.player_line_patterns
    \"\"\"))
    for row in r:
        print(f"Total rows:       {row[0]:,}")
        print(f"Has hit prob:     {row[1]:,}   Has miss prob: {row[2]:,}")
        print(f"Momentum:         {row[3]:,}   Reversion: {row[4]:,}   Bouncy: {row[5]:,}")
        print(f"Strong patterns:  {row[6]:,}   Avg n: {row[7]:.1f}   Max n: {row[8]}")

    r2 = conn.execute(text(\"\"\"
        SELECT TOP 10 p.player_id, pl.full_name, p.market_key, p.line_value,
               p.n, p.hr_overall, p.p_hit_after_hit, p.p_hit_after_miss, p.pattern_strength,
               CAST(p.is_momentum_player AS INT) AS mom,
               CAST(p.is_bouncy_player AS INT) AS bnc
        FROM common.player_line_patterns p
        LEFT JOIN nba.players pl ON pl.player_id = p.player_id
        WHERE p.pattern_strength >= 0.25 AND p.n >= 15
        ORDER BY p.pattern_strength DESC
    \"\"\"))
    print("\\nTop 10 strongest patterns (strength>=0.25, n>=15):")
    print(f"  {'Player':<22} {'Market':<30} {'Line':>5} N   HR   AftHit  AftMiss  Str")
    for row in r2:
        name = (row[1] or f"pid={row[0]}")[:22]
        ptype = "MOM" if row[9] else ("BNC" if row[10] else "   ")
        print(f"  {name:<22} {row[2]:<30} {row[3]:>5.1f} {row[4]:>3} "
              f"{row[5]:>5.0%} {row[6] or 0:>7.0%} {row[7] or 0:>8.0%}  {row[8] or 0:.2f} {ptype}")
print("Done.")
"""], stdout=f, stderr=subprocess.STDOUT)

with open(out) as f:
    print(f.read())
