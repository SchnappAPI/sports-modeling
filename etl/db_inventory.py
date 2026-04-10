"""Check per-player-line sample sizes and autocorrelation feasibility."""
import os, subprocess, sys

script = """
import os
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np

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
    # Per player-line sample size distribution (resolved outcomes only)
    df = pd.read_sql(text(\"\"\"
        SELECT player_id, market_key, line_value,
               COUNT(*) AS n,
               SUM(CASE WHEN outcome='Won' THEN 1.0 ELSE 0 END)/COUNT(*) AS hr
        FROM common.daily_grades
        WHERE outcome_name='Over' AND outcome IS NOT NULL
        GROUP BY player_id, market_key, line_value
    \"\"\"), engine)

print(f"Total player-line combinations with outcomes: {len(df):,}")
print(f"\\nSample size distribution:")
for cutoff in [5, 10, 15, 20, 30]:
    n = (df['n'] >= cutoff).sum()
    print(f"  >= {cutoff} games: {n:,} ({n/len(df):.1%})")

# How many games does the average player have across ALL their lines?
player_games = df.groupby('player_id')['n'].max()
print(f"\\nMax games per player (across best-sampled line):")
for cutoff in [10, 15, 20, 30, 40, 50]:
    n = (player_games >= cutoff).sum()
    print(f"  >= {cutoff}: {n:,} players")

# For players with 15+ games on a line, compute lag-1 autocorrelation
# This tells us: across players, how variable is autocorrelation?
df2 = pd.read_sql(text(\"\"\"
    SELECT player_id, market_key, line_value, grade_date,
           CASE WHEN outcome='Won' THEN 1 ELSE 0 END AS won
    FROM common.daily_grades
    WHERE outcome_name='Over' AND outcome IS NOT NULL
    ORDER BY player_id, market_key, line_value, grade_date
\"\"\"), engine)

print(f"\\nComputing per-player-line autocorrelation for combos with >= 10 games...")
results = []
for (pid, mkt, lv), grp in df2.groupby(['player_id','market_key','line_value']):
    if len(grp) < 10:
        continue
    hits = grp.sort_values('grade_date')['won'].tolist()
    n = len(hits)
    hr = sum(hits) / n

    # Lag-1: given previous result, what's P(hit)?
    p_hit_after_hit  = np.mean([hits[i] for i in range(1,n) if hits[i-1]==1]) if sum(hits[:-1]) > 0 else None
    p_hit_after_miss = np.mean([hits[i] for i in range(1,n) if hits[i-1]==0]) if (n-1-sum(hits[:-1])) > 0 else None

    if p_hit_after_hit is None or p_hit_after_miss is None:
        continue

    # Autocorrelation strength: how much does prior result change next probability vs baseline?
    hit_lift  = p_hit_after_hit  - hr
    miss_lift = p_hit_after_miss - hr

    results.append({
        'player_id': pid, 'market_key': mkt, 'line_value': lv,
        'n': n, 'hr': hr,
        'p_hit_after_hit': p_hit_after_hit,
        'p_hit_after_miss': p_hit_after_miss,
        'hit_lift': hit_lift,   # positive = momentum player
        'miss_lift': miss_lift, # negative = momentum player (missing lowers next prob)
    })

rdf = pd.DataFrame(results)
print(f"Computed autocorrelation for {len(rdf):,} player-line combos")

print(f"\\nAutocorrelation distribution (hit_lift = P(hit|prev hit) - baseline):")
for bucket, label in [
    (rdf['hit_lift'] > 0.15,  "Strong momentum (>+15pp after hit)"),
    ((rdf['hit_lift'] > 0.05) & (rdf['hit_lift'] <= 0.15), "Mild momentum (+5-15pp)"),
    ((rdf['hit_lift'] >= -0.05) & (rdf['hit_lift'] <= 0.05), "Random (within 5pp)"),
    ((rdf['hit_lift'] < -0.05) & (rdf['hit_lift'] >= -0.15), "Mild reversion (-5-15pp)"),
    (rdf['hit_lift'] < -0.15, "Strong reversion (>-15pp after hit)"),
]:
    n = bucket.sum()
    print(f"  {label:<45} {n:>6,} ({n/len(rdf):.1%})")

print(f"\\nSame for miss_lift (P(hit|prev miss) - baseline):")
for bucket, label in [
    (rdf['miss_lift'] > 0.15,  "Bounce-back after miss (>+15pp)"),
    ((rdf['miss_lift'] > 0.05) & (rdf['miss_lift'] <= 0.15), "Mild bounce-back (+5-15pp)"),
    ((rdf['miss_lift'] >= -0.05) & (rdf['miss_lift'] <= 0.05), "Random (within 5pp)"),
    ((rdf['miss_lift'] < -0.05) & (rdf['miss_lift'] >= -0.15), "Mild momentum after miss (-5-15pp)"),
    (rdf['miss_lift'] < -0.15, "Strong miss momentum (>-15pp)"),
]:
    n = bucket.sum()
    print(f"  {label:<45} {n:>6,} ({n/len(rdf):.1%})")

# Show some concrete examples — strong pattern players
print(f"\\nTop 15 strongest momentum patterns (hit_lift, n>=15):")
top = rdf[rdf['n']>=15].nlargest(15, 'hit_lift')[
    ['player_id','market_key','line_value','n','hr','p_hit_after_hit','p_hit_after_miss','hit_lift']]
for _, r in top.iterrows():
    print(f"  pid={int(r.player_id)} {r.market_key:<35} lv={r.line_value:.1f} n={int(r.n):>3} "
          f"hr={r.hr:.1%} after_hit={r.p_hit_after_hit:.1%} lift={r.hit_lift:+.1%}")

print(f"\\nTop 15 strongest bounce-back patterns (miss_lift, n>=15):")
top2 = rdf[rdf['n']>=15].nlargest(15, 'miss_lift')[
    ['player_id','market_key','line_value','n','hr','p_hit_after_hit','p_hit_after_miss','miss_lift']]
for _, r in top2.iterrows():
    print(f"  pid={int(r.player_id)} {r.market_key:<35} lv={r.line_value:.1f} n={int(r.n):>3} "
          f"hr={r.hr:.1%} after_miss={r.p_hit_after_miss:.1%} miss_lift={r.miss_lift:+.1%}")

print("Done.")
"""

out_path = "/tmp/autocorr_output.txt"
with open(out_path, "w") as f:
    import subprocess, sys
    result = subprocess.run([sys.executable, "-c", script], stdout=f, stderr=subprocess.STDOUT)
with open(out_path) as f:
    print(f.read())
