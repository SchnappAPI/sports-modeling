"""
Analyzes streak continuation rates from historical graded data.
For each streak length (1-10+), computes:
  - How often did the next game continue the streak vs reverse?
  - Does this differ for hit streaks vs miss streaks?
  - Does hit_rate_60 modulate this? (high hr60 = hit streaks more likely to continue)
  - What does momentum_grade actually predict?
"""
import os, math
import pandas as pd
import numpy as np
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
    # Pull all resolved rows ordered by player/market/line/date
    # We need consecutive game ordering to compute streaks
    df = pd.read_sql(text("""
        SELECT
            dg.player_id,
            dg.market_key,
            dg.line_value,
            dg.grade_date,
            dg.outcome,
            dg.momentum_grade,
            dg.pattern_grade,
            dg.hit_rate_60,
            dg.composite_grade
        FROM common.daily_grades dg
        WHERE dg.outcome_name = 'Over'
          AND dg.outcome      IS NOT NULL
          AND dg.over_price   IS NOT NULL
          AND dg.momentum_grade IS NOT NULL
        ORDER BY dg.player_id, dg.market_key, dg.line_value, dg.grade_date
    """), engine)
    print(f"Loaded {len(df):,} rows")

df["won"] = (df["outcome"] == "Won").astype(int)

# -------------------------------------------------------------------------
# 1. Momentum grade vs actual win rate (is the score directionally correct?)
# -------------------------------------------------------------------------
print("\n=== Momentum grade vs win rate ===")
print(f"{'Momentum bucket':<25} {'N':>6}  {'Win%':>7}  {'Expected direction'}")
bins = [0, 20, 30, 40, 50, 60, 70, 80, 101]
labels = ["0-20","20-30","30-40","40-50","50-60","60-70","70-80","80-100"]
df["mom_bucket"] = pd.cut(df["momentum_grade"], bins=bins, labels=labels, right=False)
for b in labels:
    sub = df[df["mom_bucket"] == b]
    n = len(sub)
    if n < 20: continue
    wr = sub["won"].mean()
    direction = "GOOD (low mom = miss)" if b in ["0-20","20-30"] else \
                "GOOD (high mom = hit)" if b in ["70-80","80-100"] else "neutral"
    print(f"  {b:<25} {n:>6,}  {wr:>7.1%}  {direction}")

# -------------------------------------------------------------------------
# 2. Pattern grade vs actual win rate
# -------------------------------------------------------------------------
print("\n=== Pattern grade vs win rate ===")
pat_df = df[df["pattern_grade"].notna()]
print(f"Rows with pattern_grade: {len(pat_df):,} of {len(df):,} ({len(pat_df)/len(df):.1%})")
if len(pat_df) > 100:
    pat_df["pat_bucket"] = pd.cut(pat_df["pattern_grade"], bins=bins, labels=labels, right=False)
    for b in labels:
        sub = pat_df[pat_df["pat_bucket"] == b]
        n = len(sub)
        if n < 10: continue
        wr = sub["won"].mean()
        print(f"  {b:<25} {n:>6,}  {wr:>7.1%}")

# -------------------------------------------------------------------------
# 3. Streak continuation analysis
# For each (player, market, line) sequence, compute the streak going into
# each game and whether the next game continued or reversed.
# -------------------------------------------------------------------------
print("\n=== Streak continuation by streak length ===")

records = []
for (pid, mkt, lv), grp in df.groupby(["player_id", "market_key", "line_value"]):
    grp = grp.sort_values("grade_date").reset_index(drop=True)
    outcomes = grp["won"].tolist()
    hr60s    = grp["hit_rate_60"].tolist()
    
    for i in range(1, len(outcomes)):
        # Compute streak length going into game i
        streak_val = outcomes[i-1]
        streak_len = 0
        for j in range(i-1, -1, -1):
            if outcomes[j] == streak_val:
                streak_len += 1
            else:
                break
        
        continued = 1 if outcomes[i] == streak_val else 0
        hr60 = hr60s[i] if hr60s[i] is not None and not (isinstance(hr60s[i], float) and math.isnan(hr60s[i])) else None
        
        records.append({
            "streak_type":  "hit" if streak_val == 1 else "miss",
            "streak_len":   min(streak_len, 10),  # cap at 10+
            "continued":    continued,
            "hr60":         hr60,
        })

streak_df = pd.DataFrame(records)
print(f"\nTotal transition events: {len(streak_df):,}")
print(f"\n{'Type':<6} {'Len':>4}  {'N':>6}  {'Cont%':>7}  {'Interpretation'}")
print("-" * 65)
for stype in ["hit", "miss"]:
    sub = streak_df[streak_df["streak_type"] == stype]
    for slen in range(1, 11):
        grp = sub[sub["streak_len"] == slen]
        n = len(grp)
        if n < 20: continue
        cont_rate = grp["continued"].mean()
        
        if stype == "hit":
            interp = "momentum continues" if cont_rate > 0.55 else \
                     "reversion likely" if cont_rate < 0.45 else "neutral"
        else:
            interp = "slump continues" if cont_rate > 0.55 else \
                     "bounce-back likely" if cont_rate < 0.45 else "neutral"
        
        label = f"{slen}+" if slen == 10 else str(slen)
        print(f"  {stype:<6} {label:>4}  {n:>6,}  {cont_rate:>7.1%}  {interp}")
    print()

# -------------------------------------------------------------------------
# 4. Does hr60 modulate streak continuation?
# Split by whether player normally hits the line frequently or not
# -------------------------------------------------------------------------
print("\n=== Streak continuation modulated by hit_rate_60 ===")
streak_df2 = streak_df[streak_df["hr60"].notna()].copy()
streak_df2["hr60_group"] = pd.cut(streak_df2["hr60"],
    bins=[0, 0.25, 0.45, 0.65, 1.01],
    labels=["low (0-25%)", "mid (25-45%)", "good (45-65%)", "high (65%+)"])

print(f"\nHit streak continuation by player's base hit rate:")
print(f"{'HR60 group':<18} {'Len':>4}  {'N':>6}  {'Cont%':>7}")
hit_sub = streak_df2[streak_df2["streak_type"] == "hit"]
for hgrp in ["low (0-25%)", "mid (25-45%)", "good (45-65%)", "high (65%+)"]:
    for slen in [1, 2, 3, 4, 5]:
        grp = hit_sub[(hit_sub["hr60_group"] == hgrp) & (hit_sub["streak_len"] == slen)]
        n = len(grp)
        if n < 20: continue
        print(f"  {hgrp:<18} {slen:>4}  {n:>6,}  {grp['continued'].mean():>7.1%}")
    print()

print("\nMiss streak continuation by player's base hit rate:")
miss_sub = streak_df2[streak_df2["streak_type"] == "miss"]
for hgrp in ["low (0-25%)", "mid (25-45%)", "good (45-65%)", "high (65%+)"]:
    for slen in [1, 2, 3, 4, 5]:
        grp = miss_sub[(miss_sub["hr60_group"] == hgrp) & (miss_sub["streak_len"] == slen)]
        n = len(grp)
        if n < 20: continue
        print(f"  {hgrp:<18} {slen:>4}  {n:>6,}  {grp['continued'].mean():>7.1%}")
    print()

print("Done.")
