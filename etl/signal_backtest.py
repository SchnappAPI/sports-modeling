"""
signal_backtest.py

Evaluates historical signal predictiveness and odds calibration
against resolved outcomes in common.daily_grades.

Outputs two analyses:
  1. Signal lift table — for each signal condition, win rate vs baseline
  2. Odds calibration table — actual win rate vs implied probability by bucket

Run modes:
  --mode signals   : signal evaluation only
  --mode odds      : odds calibration only
  --mode all       : both (default)
  --days N         : lookback window in days (default 90)
"""

import argparse
import os
import time
import logging
from datetime import datetime, timezone, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_engine(max_retries=3, retry_wait=60):
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        f"&Encrypt=yes&TrustServerCertificate={os.environ.get('AZURE_SQL_TRUST_CERT', 'no')}"
        "&Connection+Timeout=90"
    )
    engine = create_engine(conn_str, fast_executemany=False)
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            log.info("Database connection established.")
            return engine
        except Exception as exc:
            log.warning(f"DB connection attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                log.info(f"Waiting {retry_wait}s...")
                time.sleep(retry_wait)
    raise RuntimeError("Could not connect to Azure SQL after retries.")


def fetch_resolved(engine, days: int) -> pd.DataFrame:
    """
    Pull all resolved Over rows with full grade components and price.
    Only rows where every component grade is present are included so
    signal conditions can be evaluated cleanly.
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    sql = text("""
        SELECT
            dg.grade_date,
            dg.player_id,
            dg.player_name,
            dg.market_key,
            dg.line_value,
            dg.over_price,
            dg.outcome,                          -- 'Won' / 'Lost'
            dg.composite_grade,
            dg.grade             AS hr_grade,     -- weighted hit-rate grade
            dg.trend_grade,
            dg.regression_grade,
            dg.momentum_grade,
            dg.matchup_grade,
            dg.hit_rate_20,
            dg.hit_rate_60,
            dg.hit_rate_opp,
            dg.sample_size_20,
            dg.sample_size_60
        FROM common.daily_grades dg
        WHERE dg.outcome_name = 'Over'
          AND dg.outcome      IS NOT NULL
          AND dg.over_price   IS NOT NULL
          AND dg.grade_date   >= :cutoff
        ORDER BY dg.grade_date, dg.player_name
    """)
    df = pd.read_sql(sql, engine, params={"cutoff": cutoff})
    log.info(f"Fetched {len(df):,} resolved Over rows since {cutoff}.")
    return df


def implied_prob(price: float) -> float:
    """Convert American odds to implied probability (raw, not vig-adjusted)."""
    if price < 0:
        return abs(price) / (abs(price) + 100)
    else:
        return 100 / (price + 100)


def kelly_ev(win_rate: float, price: float) -> float:
    """
    Expected value per unit staked at these odds given estimated win rate.
    EV > 0 means positive expected value.
    """
    if price >= 0:
        payout = price / 100
    else:
        payout = 100 / abs(price)
    return win_rate * payout - (1 - win_rate)


# ---------------------------------------------------------------------------
# Signal definitions — mirrors the frontend logic so results are comparable
# ---------------------------------------------------------------------------

def apply_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Add boolean columns for each signal condition."""
    df = df.copy()

    # Player-level signals (trend/regression)
    df["sig_HOT"]  = df["trend_grade"].notna()      & (df["trend_grade"]      > 72)
    df["sig_COLD"] = df["trend_grade"].notna()      & (df["trend_grade"]      < 28)
    df["sig_DUE"]  = df["regression_grade"].notna() & (df["regression_grade"] > 72)
    df["sig_FADE"] = df["regression_grade"].notna() & (df["regression_grade"] < 28)

    # Line-level signals (momentum), gated on hit rate
    hr60 = df["hit_rate_60"]
    mom  = df["momentum_grade"]
    df["sig_STREAK"] = (
        mom.notna() & (mom > 75) &
        (hr60.isna() | ((hr60 >= 0.25) & (hr60 <= 0.80)))
    )
    df["sig_SLUMP"] = (
        mom.notna() & (mom < 25) &
        (hr60.isna() | (hr60 >= 0.30))
    )

    # Value signal
    df["sig_LONGSHOT"] = (
        (df["over_price"] > 250) &
        df["hit_rate_20"].notna() & (df["hit_rate_20"] > 0) &
        hr60.notna() & (hr60 >= 0.20)
    )

    # Composite grade buckets (for reference)
    df["grade_bucket"] = pd.cut(
        df["composite_grade"].fillna(0),
        bins=[-1, 40, 50, 55, 60, 65, 70, 101],
        labels=["<40", "40-50", "50-55", "55-60", "60-65", "65-70", "70+"]
    )

    return df


def run_signal_analysis(df: pd.DataFrame) -> None:
    """
    For each signal condition, compute:
      - How many rows it fired on
      - Actual win rate
      - Baseline win rate (same odds range, signal not fired)
      - Lift vs baseline
      - Average EV at those odds given actual win rate
      - P-value proxy (z-test on proportions) to gauge statistical significance
    """
    import math

    df["won"] = (df["outcome"] == "Won").astype(int)
    baseline_wr = df["won"].mean()
    baseline_n  = len(df)

    log.info(f"\n{'='*70}")
    log.info(f"SIGNAL ANALYSIS  (n={baseline_n:,}  baseline win rate={baseline_wr:.1%})")
    log.info(f"{'='*70}")
    log.info(f"\n{'Signal':<12} {'N':>6} {'Win%':>7} {'Baseline':>9} {'Lift':>7} {'AvgEV':>7} {'Z':>6}  {'Verdict'}")
    log.info(f"{'-'*80}")

    signals = ["HOT", "COLD", "DUE", "FADE", "STREAK", "SLUMP", "LONGSHOT"]

    for sig in signals:
        col = f"sig_{sig}"
        if col not in df.columns:
            continue
        fired = df[df[col] == True]
        n = len(fired)
        if n < 20:
            log.info(f"  {sig:<12} {'<20 samples — skip':>60}")
            continue

        wr = fired["won"].mean()

        # Baseline: rows where this signal did NOT fire (same dataset)
        not_fired = df[df[col] == False]
        bl_wr = not_fired["won"].mean() if len(not_fired) > 0 else baseline_wr

        lift = wr - bl_wr

        # Average EV at the actual win rate
        avg_ev = fired.apply(lambda r: kelly_ev(wr, r["over_price"]), axis=1).mean()

        # Z-score for difference in proportions
        p_pool = (fired["won"].sum() + not_fired["won"].sum()) / (n + len(not_fired))
        se = math.sqrt(p_pool * (1 - p_pool) * (1/n + 1/len(not_fired))) if n > 0 and len(not_fired) > 0 else 1
        z = lift / se if se > 0 else 0

        # Verdict
        if abs(z) < 1.5:
            verdict = "NOISE (not significant)"
        elif lift > 0.04 and z > 1.5:
            verdict = "USEFUL — positive lift"
        elif lift < -0.04 and z < -1.5:
            verdict = "CONTRARIAN — negative lift"
        elif 0 < lift <= 0.04:
            verdict = "WEAK positive"
        else:
            verdict = "WEAK negative"

        log.info(f"  {sig:<12} {n:>6,} {wr:>7.1%} {bl_wr:>9.1%} {lift:>+7.1%} {avg_ev:>+7.3f} {z:>6.2f}  {verdict}")

    # Also evaluate composite grade buckets
    log.info(f"\n--- Composite grade buckets ---")
    log.info(f"{'Grade':<10} {'N':>6} {'Win%':>7} {'Baseline':>9} {'Lift':>7} {'AvgEV':>7}")
    for bucket in ["<40", "40-50", "50-55", "55-60", "60-65", "65-70", "70+"]:
        sub = df[df["grade_bucket"] == bucket]
        n = len(sub)
        if n < 10:
            continue
        wr = sub["won"].mean()
        lift = wr - baseline_wr
        avg_ev = sub.apply(lambda r: kelly_ev(wr, r["over_price"]), axis=1).mean()
        log.info(f"  {bucket:<10} {n:>6,} {wr:>7.1%} {baseline_wr:>9.1%} {lift:>+7.1%} {avg_ev:>+7.3f}")


def run_odds_analysis(df: pd.DataFrame) -> None:
    """
    Odds calibration: how does actual win rate compare to implied probability
    across price buckets, and within each market type?
    """
    df = df.copy()
    df["won"]     = (df["outcome"] == "Won").astype(int)
    df["imp_prob"] = df["over_price"].apply(implied_prob)
    df["edge"]     = df["won"] - df["imp_prob"]   # positive = you beat the vig

    # Global calibration
    log.info(f"\n{'='*70}")
    log.info(f"ODDS CALIBRATION ANALYSIS  (n={len(df):,})")
    log.info(f"{'='*70}")

    buckets = [
        ("1: <=-400",       df["over_price"] <= -400),
        ("2: -399 to -200", (df["over_price"] > -400) & (df["over_price"] <= -200)),
        ("3: -199 to -110", (df["over_price"] > -200) & (df["over_price"] <= -110)),
        ("4: -109 to +100", (df["over_price"] > -110) & (df["over_price"] <= 100)),
        ("5: +101 to +200", (df["over_price"] > 100)  & (df["over_price"] <= 200)),
        ("6: +201 to +400", (df["over_price"] > 200)  & (df["over_price"] <= 400)),
        ("7: +401 to +700", (df["over_price"] > 400)  & (df["over_price"] <= 700)),
        ("8: >+700",        df["over_price"] > 700),
    ]

    log.info(f"\n{'Odds bucket':<22} {'N':>6} {'ActWin%':>8} {'Imp%':>8} {'Edge':>7} {'AvgEV':>7}  {'Assessment'}")
    log.info(f"{'-'*80}")

    for label, mask in buckets:
        sub = df[mask]
        n = len(sub)
        if n < 10:
            continue
        act_wr = sub["won"].mean()
        imp    = sub["imp_prob"].mean()
        edge   = act_wr - imp
        avg_ev = sub.apply(lambda r: kelly_ev(act_wr, r["over_price"]), axis=1).mean()

        # Assessment
        if abs(edge) < 0.02:
            assess = "Well calibrated"
        elif edge > 0.04:
            assess = "UNDERPRICED — better than odds suggest"
        elif edge > 0.02:
            assess = "Slightly underpriced"
        elif edge < -0.04:
            assess = "OVERPRICED — worse than odds suggest"
        else:
            assess = "Slightly overpriced"

        log.info(f"  {label:<22} {n:>6,} {act_wr:>8.1%} {imp:>8.1%} {edge:>+7.1%} {avg_ev:>+7.3f}  {assess}")

    # By market
    log.info(f"\n--- By market (sorted by edge) ---")
    log.info(f"{'Market':<45} {'N':>6} {'ActWin%':>8} {'Imp%':>8} {'Edge':>7}")
    log.info(f"{'-'*75}")
    mkt_stats = (
        df.groupby("market_key")
          .apply(lambda g: pd.Series({
              "n":      len(g),
              "act_wr": g["won"].mean(),
              "imp":    g["imp_prob"].mean(),
          }), include_groups=False)
          .reset_index()
    )
    mkt_stats["edge"] = mkt_stats["act_wr"] - mkt_stats["imp"]
    mkt_stats = mkt_stats[mkt_stats["n"] >= 30].sort_values("edge", ascending=False)
    for _, row in mkt_stats.iterrows():
        log.info(f"  {row['market_key']:<45} {row['n']:>6,} {row['act_wr']:>8.1%} {row['imp']:>8.1%} {row['edge']:>+7.1%}")

    # Within each odds bucket — do higher-graded props outperform?
    log.info(f"\n--- Grade vs win rate by odds bucket ---")
    log.info(f"{'Odds bucket':<22} {'Grade':<10} {'N':>6} {'Win%':>8} {'Imp%':>8} {'Edge':>7}")
    log.info(f"{'-'*75}")
    grade_cuts = [("Low (<50)",  df["composite_grade"] < 50),
                  ("Mid (50-65)", (df["composite_grade"] >= 50) & (df["composite_grade"] < 65)),
                  ("High (65+)",  df["composite_grade"] >= 65)]

    for blabel, bmask in [
        ("Fav (<=-110)",   df["over_price"] <= -110),
        ("Pick/Dog (>-110)", df["over_price"] > -110),
    ]:
        for glabel, gmask in grade_cuts:
            sub = df[bmask & gmask]
            n = len(sub)
            if n < 15:
                continue
            act_wr = sub["won"].mean()
            imp    = sub["imp_prob"].mean()
            edge   = act_wr - imp
            log.info(f"  {blabel:<22} {glabel:<10} {n:>6,} {act_wr:>8.1%} {imp:>8.1%} {edge:>+7.1%}")

    # Odds-based tendency: do players hit more when they're a bigger favorite?
    log.info(f"\n--- Odds tendency: does being a heavy favorite correlate with hitting? ---")
    log.info(f"(Controls for market by normalizing within market)")
    df["imp_bucket"] = pd.cut(df["imp_prob"],
                               bins=[0, 0.3, 0.45, 0.55, 0.65, 0.75, 1.0],
                               labels=["0-30%", "30-45%", "45-55%", "55-65%", "65-75%", "75%+"])
    log.info(f"{'Implied prob bucket':<22} {'N':>6} {'ActWin%':>8} {'Edge':>7}")
    for bucket in ["0-30%", "30-45%", "45-55%", "55-65%", "65-75%", "75%+"]:
        sub = df[df["imp_bucket"] == bucket]
        n = len(sub)
        if n < 10:
            continue
        act_wr = sub["won"].mean()
        imp    = sub["imp_prob"].mean()
        log.info(f"  {bucket:<22} {n:>6,} {act_wr:>8.1%} {act_wr-imp:>+7.1%}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["signals", "odds", "all"], default="all")
    parser.add_argument("--days", type=int, default=90)
    args = parser.parse_args()

    engine = get_engine()
    df = fetch_resolved(engine, args.days)

    if len(df) < 50:
        log.warning(f"Only {len(df)} resolved rows — not enough for reliable analysis.")
        return

    df = apply_signals(df)

    if args.mode in ("signals", "all"):
        run_signal_analysis(df)

    if args.mode in ("odds", "all"):
        run_odds_analysis(df)

    log.info("\nDone.")


if __name__ == "__main__":
    main()
