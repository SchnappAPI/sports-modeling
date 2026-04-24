"""
Isotonic calibration of tier-line probabilities.

Per ADR-20260424-5 control 4: replace raw KDE probabilities with empirically-
calibrated probabilities derived from historical grade -> outcome data.

Entry points:
    fit_calibrator(engine, as_of_date=None) -> (callable, DataFrame)
        Returns (prob -> calibrated_prob) callable and a bucket inspection
        DataFrame. Uses pool-adjacent-violators (PAV) for isotonic regression,
        no sklearn dependency. When as_of_date is provided, only resolved
        tier_lines/grades from grade_date < as_of_date are used (walk-forward,
        no leakage).

    publish_calibration_buckets(engine, bucket_stats)
        Writes common.grade_calibration for human inspection. Idempotent.

Called once per grading run from grade_props.py. Identity fallback when
historical sample is insufficient (< 100 resolved outcomes).
"""

from typing import Callable, Tuple
import numpy as np
import pandas as pd
from sqlalchemy import text

IDENTITY: Callable[[float], float] = lambda p: p


def fit_calibrator(engine, min_bucket_size: int = 20, bucket_width: float = 0.05,
                   as_of_date=None) -> Tuple[Callable[[float], float], pd.DataFrame]:
    """Fit an isotonic calibrator on historical tier_prob -> outcome pairs.

    Joins every tier prob in common.player_tier_lines with the corresponding
    resolved outcome in common.daily_grades (Over side). Bins raw prob into
    buckets of width bucket_width, computes empirical hit rate per bucket,
    enforces non-decreasing monotonicity via PAV, interpolates between bucket
    midpoints at inference time.

    When as_of_date is supplied, only rows with grade_date strictly before
    as_of_date are considered. This prevents leakage during walk-forward
    backfill: each grade_date sees only the calibration evidence available
    on or before the prior day.
    """
    as_of_filter = ""
    params = {}
    if as_of_date is not None:
        as_of_filter = " AND tp.grade_date < :as_of_date AND dg.grade_date < :as_of_date"
        params["as_of_date"] = as_of_date

    query = text(f"""
        SELECT tp.raw_prob, tp.line,
               CASE WHEN dg.outcome = 'Won' THEN 1.0 ELSE 0.0 END AS hit
        FROM (
            SELECT grade_date, game_id, player_id, market_key,
                   safe_line AS line, safe_prob AS raw_prob
            FROM common.player_tier_lines WHERE safe_line IS NOT NULL AND safe_prob IS NOT NULL
            UNION ALL
            SELECT grade_date, game_id, player_id, market_key,
                   value_line, value_prob
            FROM common.player_tier_lines WHERE value_line IS NOT NULL AND value_prob IS NOT NULL
            UNION ALL
            SELECT grade_date, game_id, player_id, market_key,
                   highrisk_line, highrisk_prob
            FROM common.player_tier_lines WHERE highrisk_line IS NOT NULL AND highrisk_prob IS NOT NULL
            UNION ALL
            SELECT grade_date, game_id, player_id, market_key,
                   lotto_line, lotto_prob
            FROM common.player_tier_lines WHERE lotto_line IS NOT NULL AND lotto_prob IS NOT NULL
        ) tp
        INNER JOIN common.daily_grades dg
            ON dg.grade_date = tp.grade_date
           AND dg.game_id = tp.game_id
           AND dg.player_id = tp.player_id
           AND dg.market_key = tp.market_key
           AND dg.line_value = tp.line
           AND dg.outcome_name = 'Over'
        WHERE dg.outcome IN ('Won', 'Lost'){as_of_filter}
    """)

    df = pd.read_sql(query, engine, params=params)
    if len(df) < 100:
        return IDENTITY, pd.DataFrame(columns=["bucket_min", "bucket_max", "n", "empirical_hit_rate", "isotonic_hit_rate"])

    df["bucket"] = (df["raw_prob"] // bucket_width) * bucket_width
    stats = (df.groupby("bucket")
               .agg(n=("hit", "size"), hit_rate=("hit", "mean"))
               .reset_index()
               .sort_values("bucket"))
    stats = stats[stats["n"] >= min_bucket_size].reset_index(drop=True)
    if len(stats) < 3:
        return IDENTITY, stats.rename(columns={"hit_rate": "empirical_hit_rate"})

    stats["iso_hit_rate"] = _pav_isotonic(stats["hit_rate"].values, stats["n"].values)

    # Interpolation mids and rates
    mids = (stats["bucket"].values + bucket_width / 2.0).astype(float)
    rates = stats["iso_hit_rate"].values.astype(float)

    def calibrator(raw_prob):
        if raw_prob is None or (isinstance(raw_prob, float) and np.isnan(raw_prob)):
            return None
        p = float(raw_prob)
        if p <= mids[0]:
            return float(rates[0])
        if p >= mids[-1]:
            return float(rates[-1])
        return float(np.interp(p, mids, rates))

    publish_df = pd.DataFrame({
        "bucket_min": stats["bucket"].astype(float).values,
        "bucket_max": (stats["bucket"] + bucket_width).astype(float).values,
        "n": stats["n"].astype(int).values,
        "empirical_hit_rate": stats["hit_rate"].astype(float).values,
        "isotonic_hit_rate": stats["iso_hit_rate"].astype(float).values,
    })
    return calibrator, publish_df


def _pav_isotonic(y, w):
    """Pool-adjacent-violators for weighted non-decreasing isotonic regression."""
    n = len(y)
    sums = list(y * w)
    counts = list(w.astype(float))
    ends = list(range(n))
    stack_start = []
    # Simpler: iterative left-to-right merging
    blocks = [[float(y[i]) * float(w[i]), float(w[i]), 1] for i in range(n)]
    # each block: [sum, weight, span]
    i = 0
    while i < len(blocks) - 1:
        left_mean = blocks[i][0] / blocks[i][1]
        right_mean = blocks[i + 1][0] / blocks[i + 1][1]
        if left_mean > right_mean:
            merged = [blocks[i][0] + blocks[i + 1][0],
                      blocks[i][1] + blocks[i + 1][1],
                      blocks[i][2] + blocks[i + 1][2]]
            blocks[i:i + 2] = [merged]
            if i > 0:
                i -= 1
        else:
            i += 1
    out = []
    for b in blocks:
        m = b[0] / b[1]
        out.extend([m] * b[2])
    return np.array(out)


def publish_calibration_buckets(engine, bucket_stats: pd.DataFrame) -> None:
    """Write common.grade_calibration for inspection. Idempotent (truncate + insert)."""
    if bucket_stats.empty:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT 1 FROM sys.objects
                           WHERE object_id = OBJECT_ID('common.grade_calibration') AND type = 'U')
            BEGIN
                CREATE TABLE common.grade_calibration (
                    bucket_min          FLOAT NOT NULL,
                    bucket_max          FLOAT NOT NULL,
                    sample_size         INT   NOT NULL,
                    empirical_hit_rate  FLOAT NOT NULL,
                    isotonic_hit_rate   FLOAT NOT NULL,
                    last_updated        DATETIME2 NOT NULL DEFAULT GETUTCDATE(),
                    CONSTRAINT pk_grade_calibration PRIMARY KEY (bucket_min)
                )
            END
        """))
        conn.execute(text("DELETE FROM common.grade_calibration"))
        for _, row in bucket_stats.iterrows():
            conn.execute(text("""
                INSERT INTO common.grade_calibration
                    (bucket_min, bucket_max, sample_size, empirical_hit_rate, isotonic_hit_rate)
                VALUES (:bmin, :bmax, :n, :ehr, :ihr)
            """), {"bmin": float(row["bucket_min"]),
                   "bmax": float(row["bucket_max"]),
                   "n": int(row["n"]),
                   "ehr": float(row["empirical_hit_rate"]),
                   "ihr": float(row["isotonic_hit_rate"])})
