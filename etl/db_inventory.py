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
            SUM(is_momentum_player)  AS momentum,
            SUM(is_reversion_player) AS reversion,
            SUM(is_bouncy_player)    AS bouncy,
            SUM(CASE WHEN pattern_strength >= 0.20 THEN 1 ELSE 0 END) AS strong,
            AVG(CAST(n AS FLOAT)) AS avg_n, MAX(n) AS max_n
        FROM common.player_line_patterns
    \"\"\"))
    for row in r:
        print(f"Total:    {row[0]:,}")
        print(f"Has hit:  {row[1]:,}  Has miss: {row[2]:,}")
        print(f"Momentum: {row[3]:,}  Reversion: {row[4]:,}  Bouncy: {row[5]:,}")
        print(f"Strong:   {row[6]:,}  AvgN: {row[7]:.1f}  MaxN: {row[8]}")

    r2 = conn.execute(text(\"\"\"
        SELECT TOP 8 p.player_id, pl.full_name, p.market_key, p.line_value,
               p.n, p.hr_overall, p.p_hit_after_hit, p.p_hit_after_miss, p.pattern_strength
        FROM common.player_line_patterns p
        LEFT JOIN nba.players pl ON pl.player_id = p.player_id
        WHERE p.pattern_strength >= 0.25 AND p.n >= 15
        ORDER BY p.pattern_strength DESC
    \"\"\"))
    print("\\nTop patterns:")
    for row in r2:
        print(f"  {(row[1] or 'pid='+str(row[0]))[:20]:<20} {row[2]:<30} lv={row[3]:.1f} n={row[4]} "
              f"hr={row[5]:.0%} hit={row[6] or 0:.0%} miss={row[7] or 0:.0%} str={row[8] or 0:.2f}")
print("Done.")
"""], stdout=f, stderr=subprocess.STDOUT)

with open(out) as f:
    print(f.read())
