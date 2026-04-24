from etl.db import get_engine
from sqlalchemy import text

eng = get_engine()
with eng.connect() as conn:
    # Calibration buckets
    try:
        rows = conn.execute(text("SELECT bucket_min, bucket_max, sample_size, empirical_hit_rate, isotonic_hit_rate FROM common.grade_calibration ORDER BY bucket_min")).fetchall()
        print(f"Calibration buckets ({len(rows)}):")
        for r in rows:
            print(f"  [{r.bucket_min:.2f}, {r.bucket_max:.2f}): n={r.sample_size}, empirical={r.empirical_hit_rate:.3f}, isotonic={r.isotonic_hit_rate:.3f}")
    except Exception as e:
        print(f"common.grade_calibration query error (table may not exist if no data fit): {e}")

    # Today's tier rows by grade_date
    rows = conn.execute(text("""
        SELECT TOP 5
               grade_date, player_name, market_key, composite_grade,
               safe_line, safe_prob, safe_price, safe_hits_all, safe_games_all, safe_hits_20, safe_games_20,
               value_line, value_prob,
               highrisk_line, highrisk_prob, highrisk_price, highrisk_hits_all, highrisk_games_all,
               lotto_line, lotto_prob, lotto_price, lotto_hits_all, lotto_games_all,
               recent_minutes_20, recent_opportunity, historical_opportunity
        FROM common.player_tier_lines
        WHERE grade_date = (SELECT MAX(grade_date) FROM common.player_tier_lines)
        ORDER BY composite_grade DESC
    """)).fetchall()
    print()
    print(f"Top 5 tier rows from most recent grade_date:")
    for r in rows:
        print(f"\n  {r.player_name} / {r.market_key} (composite={r.composite_grade})")
        print(f"    Safe:     line={r.safe_line} prob={r.safe_prob} price={r.safe_price}  hits={r.safe_hits_all}/{r.safe_games_all} (last20: {r.safe_hits_20}/{r.safe_games_20})")
        print(f"    Value:    line={r.value_line} prob={r.value_prob}")
        print(f"    HighRisk: line={r.highrisk_line} prob={r.highrisk_prob} price={r.highrisk_price}  hits={r.highrisk_hits_all}/{r.highrisk_games_all}")
        print(f"    Lotto:    line={r.lotto_line} prob={r.lotto_prob} price={r.lotto_price}  hits={r.lotto_hits_all}/{r.lotto_games_all}")
        print(f"    recent_minutes_20={r.recent_minutes_20}  recent_opp={r.recent_opportunity}  hist_opp={r.historical_opportunity}")

    # Summary: how many rows got the new columns populated
    summary = conn.execute(text("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN safe_hits_all IS NOT NULL THEN 1 ELSE 0 END) AS safe_hits_populated,
            SUM(CASE WHEN recent_minutes_20 IS NOT NULL THEN 1 ELSE 0 END) AS mins_populated,
            SUM(CASE WHEN recent_opportunity IS NOT NULL THEN 1 ELSE 0 END) AS opp_populated,
            SUM(CASE WHEN safe_price IS NOT NULL THEN 1 ELSE 0 END) AS safe_price_populated
        FROM common.player_tier_lines
        WHERE grade_date = (SELECT MAX(grade_date) FROM common.player_tier_lines)
    """)).fetchone()
    print()
    print(f"Coverage on latest grade_date (n={summary.total}):")
    print(f"  safe_hits_all populated:       {summary.safe_hits_populated}")
    print(f"  recent_minutes_20 populated:   {summary.mins_populated}")
    print(f"  recent_opportunity populated:  {summary.opp_populated}")
    print(f"  safe_price populated:          {summary.safe_price_populated}")
