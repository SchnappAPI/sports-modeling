"""
weekly_calibration.py

Recalibrates the prop grading model on a rolling window of resolved
tier-line outcomes. Writes a snapshot to common.grade_calibration_history
for the transparency page trend chart, then replaces common.grade_calibration
so daily grading uses the fresh calibrator.

Per ADR-20260425-3: stop regrading historical data on every code change.
Each daily run uses the latest calibrator from common.grade_calibration.
This script (run weekly via Sunday 06:00 UTC cron) is the only writer of
that table going forward.

Threshold for the safety cap: n >= 30. See sim_v2.py and
threshold_test.py for the analysis behind this choice.

Window: rolling 30 days by default. Configurable via --window-days.
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Same threshold as the static calibrator in calibrate_grades.py.
WELL_SAMPLED_THRESHOLD = 30
DEFAULT_WINDOW_DAYS = 30
BUCKET_WIDTH = 0.05
MIN_BUCKET_SIZE = 20  # bucket must have this many samples to participate at all


def get_engine():
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
        "&Connection+Timeout=90"
    )
    return create_engine(conn_str, fast_executemany=False)


def fetch_resolved_corpus(engine, window_days):
    """Pull every resolved tier-line outcome inside the rolling window.

    Reads the immutable corpus: each row is one (tier_line, outcome) pair
    where the outcome is from common.daily_grades. Joins on the natural key.
    """
    sql = text(f"""
        SELECT tp.raw_prob,
               tp.line,
               CASE WHEN dg.outcome = 'Won' THEN 1.0 ELSE 0.0 END AS hit
          FROM (
              SELECT grade_date, game_id, player_id, market_key,
                     safe_line AS line, safe_prob AS raw_prob
                FROM common.player_tier_lines
               WHERE safe_line IS NOT NULL AND safe_prob IS NOT NULL
              UNION ALL
              SELECT grade_date, game_id, player_id, market_key,
                     value_line, value_prob
                FROM common.player_tier_lines
               WHERE value_line IS NOT NULL AND value_prob IS NOT NULL
              UNION ALL
              SELECT grade_date, game_id, player_id, market_key,
                     highrisk_line, highrisk_prob
                FROM common.player_tier_lines
               WHERE highrisk_line IS NOT NULL AND highrisk_prob IS NOT NULL
              UNION ALL
              SELECT grade_date, game_id, player_id, market_key,
                     lotto_line, lotto_prob
                FROM common.player_tier_lines
               WHERE lotto_line IS NOT NULL AND lotto_prob IS NOT NULL
          ) tp
         INNER JOIN common.daily_grades dg
                ON dg.grade_date = tp.grade_date
               AND dg.game_id    = tp.game_id
               AND dg.player_id  = tp.player_id
               AND dg.market_key = tp.market_key
               AND dg.line_value = tp.line
               AND dg.outcome_name = 'Over'
         WHERE dg.outcome IN ('Won', 'Lost')
           AND tp.grade_date >= DATEADD(day, -:window, CAST(GETUTCDATE() AS DATE))
    """)
    df = pd.read_sql(sql, engine, params={"window": int(window_days)})
    return df


def pav_isotonic(y, w):
    """Pool-adjacent-violators for non-decreasing isotonic regression.

    Identical algorithm to calibrate_grades._pav_isotonic. Inlined here
    so weekly_calibration.py has no cross-module dependency on grading
    code (it can run standalone in a thin workflow).
    """
    blocks = [[float(y[i]) * float(w[i]), float(w[i]), 1] for i in range(len(y))]
    i = 0
    while i < len(blocks) - 1:
        left_mean = blocks[i][0] / blocks[i][1]
        right_mean = blocks[i + 1][0] / blocks[i + 1][1]
        if left_mean > right_mean:
            merged = [
                blocks[i][0] + blocks[i + 1][0],
                blocks[i][1] + blocks[i + 1][1],
                blocks[i][2] + blocks[i + 1][2],
            ]
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


def fit_buckets(df):
    """Bucket the corpus into bucket_width slices and compute empirical +
    isotonic hit rates. Returns a tuple (DataFrame, max_well_sampled_rate).

    DataFrame columns: bucket_min, bucket_max, n, empirical_hit_rate,
    isotonic_hit_rate.

    max_well_sampled_rate is max(empirical_hit_rate) where n >= WELL_SAMPLED_THRESHOLD.
    Falls back to None when no buckets clear the threshold.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame(), None

    df = df.copy()
    df["bucket"] = (df["raw_prob"] // BUCKET_WIDTH) * BUCKET_WIDTH
    stats = (
        df.groupby("bucket")
          .agg(n=("hit", "size"), hit_rate=("hit", "mean"))
          .reset_index()
          .sort_values("bucket")
    )
    stats = stats[stats["n"] >= MIN_BUCKET_SIZE].reset_index(drop=True)
    if len(stats) < 3:
        return pd.DataFrame(), None

    stats["iso_hit_rate"] = pav_isotonic(stats["hit_rate"].values, stats["n"].values)

    well = stats[stats["n"] >= WELL_SAMPLED_THRESHOLD]
    if len(well) > 0:
        cap = float(well["hit_rate"].max())
    else:
        cap = float(stats["iso_hit_rate"].max())

    out = pd.DataFrame({
        "bucket_min": stats["bucket"].astype(float).values,
        "bucket_max": (stats["bucket"] + BUCKET_WIDTH).astype(float).values,
        "n": stats["n"].astype(int).values,
        "empirical_hit_rate": stats["hit_rate"].astype(float).values,
        "isotonic_hit_rate": stats["iso_hit_rate"].astype(float).values,
    })
    return out, cap


def write_snapshot(engine, buckets, cap, window_days, sport="nba", model_version=None):
    """Append one snapshot row per bucket to common.grade_calibration_history.

    Idempotent on (snapshot_date, sport, bucket_min): re-runs on the same
    day overwrite via DELETE + INSERT.
    """
    if buckets.empty:
        log.warning("No bucket data; skipping snapshot.")
        return 0
    snapshot_date = datetime.now(timezone.utc).date()
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM common.grade_calibration_history "
                "WHERE snapshot_date = :snap AND sport = :sport"
            ),
            {"snap": snapshot_date, "sport": sport},
        )
        for _, row in buckets.iterrows():
            conn.execute(
                text("""
                    INSERT INTO common.grade_calibration_history
                        (snapshot_date, sport, bucket_min, bucket_max,
                         sample_size, empirical_hit_rate, isotonic_hit_rate,
                         max_well_sampled_rate, window_days, model_version)
                    VALUES (:snap, :sport, :bmin, :bmax, :n, :ehr, :ihr,
                            :cap, :wd, :mv)
                """),
                {
                    "snap": snapshot_date,
                    "sport": sport,
                    "bmin": float(row["bucket_min"]),
                    "bmax": float(row["bucket_max"]),
                    "n": int(row["n"]),
                    "ehr": float(row["empirical_hit_rate"]),
                    "ihr": float(row["isotonic_hit_rate"]),
                    "cap": float(cap) if cap is not None else None,
                    "wd": int(window_days),
                    "mv": model_version,
                },
            )
    log.info(f"Wrote {len(buckets)} bucket rows to grade_calibration_history "
             f"for snapshot_date={snapshot_date}, sport={sport}.")
    return len(buckets)


def replace_active_calibration(engine, buckets, cap):
    """Replace common.grade_calibration with the latest fit. The daily grading
    run reads from this table, so this is what makes a new calibrator active.

    Idempotent (DELETE + INSERT). Schema columns are added on demand if missing.
    """
    if buckets.empty:
        log.warning("No buckets to publish; leaving common.grade_calibration unchanged.")
        return 0
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
        conn.execute(text("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='common' AND TABLE_NAME='grade_calibration'
                  AND COLUMN_NAME='max_well_sampled_rate'
            )
            ALTER TABLE common.grade_calibration ADD max_well_sampled_rate FLOAT NULL
        """))
        conn.execute(text("DELETE FROM common.grade_calibration"))
        for _, row in buckets.iterrows():
            conn.execute(
                text("""
                    INSERT INTO common.grade_calibration
                        (bucket_min, bucket_max, sample_size, empirical_hit_rate,
                         isotonic_hit_rate, max_well_sampled_rate)
                    VALUES (:bmin, :bmax, :n, :ehr, :ihr, :cap)
                """),
                {
                    "bmin": float(row["bucket_min"]),
                    "bmax": float(row["bucket_max"]),
                    "n": int(row["n"]),
                    "ehr": float(row["empirical_hit_rate"]),
                    "ihr": float(row["isotonic_hit_rate"]),
                    "cap": float(cap) if cap is not None else None,
                },
            )
    log.info(f"Replaced common.grade_calibration with {len(buckets)} bucket rows. "
             f"Cap = {cap:.4f}." if cap else "No cap.")
    return len(buckets)


def main():
    parser = argparse.ArgumentParser(description="Weekly recalibration of the grading model")
    parser.add_argument("--window-days", type=int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--sport", type=str, default="nba")
    parser.add_argument("--model-version", type=str, default=None,
                        help="Model version stamp to record on the snapshot. "
                             "Defaults to looking up the most recent on common.daily_grades.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute the calibrator but do not write to DB.")
    args = parser.parse_args()

    engine = get_engine()
    log.info(f"Window: {args.window_days} days. Sport: {args.sport}.")

    # Determine model_version stamp from latest stamped row, if not provided.
    model_version = args.model_version
    if model_version is None:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT TOP 1 model_version
                  FROM common.daily_grades
                 WHERE model_version IS NOT NULL
              ORDER BY grade_date DESC, grade_id DESC
            """)).fetchone()
            model_version = row[0] if row else None
    log.info(f"Model version stamp: {model_version}")

    df = fetch_resolved_corpus(engine, args.window_days)
    log.info(f"Resolved corpus: {len(df)} rows in last {args.window_days} days.")

    if len(df) < 100:
        log.warning(f"Only {len(df)} rows; need >= 100 for a meaningful calibrator.")
        log.warning("Not writing anything. Will retry next week.")
        return 0

    buckets, cap = fit_buckets(df)
    log.info(f"Fit produced {len(buckets)} qualifying buckets. Cap = {cap}")

    if args.dry_run:
        log.info("Dry run; not writing anything. Bucket preview:")
        print(buckets.to_string(index=False))
        return 0

    write_snapshot(engine, buckets, cap, args.window_days,
                   sport=args.sport, model_version=model_version)
    replace_active_calibration(engine, buckets, cap)
    log.info("Weekly calibration done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
