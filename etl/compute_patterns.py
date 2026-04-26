"""
compute_patterns.py

Computes per-player-line autocorrelation patterns from resolved historical
outcomes and stores them in common.player_line_patterns.

This runs nightly after outcomes are resolved. The grading job then reads
this table to look up each player's personal transition probabilities rather
than using aggregate population averages.

Schema:
  common.player_line_patterns (
    player_id         BIGINT,
    market_key        VARCHAR(100),
    line_value        DECIMAL(6,1),
    n                 INT,           -- total resolved observations
    hr_overall        FLOAT,         -- unconditional hit rate
    p_hit_after_hit   FLOAT,         -- P(hit | previous was hit), NULL if <3 obs
    p_hit_after_miss  FLOAT,         -- P(hit | previous was miss), NULL if <3 obs
    hit_momentum      FLOAT,         -- p_hit_after_hit - hr_overall
    miss_momentum     FLOAT,         -- p_hit_after_miss - hr_overall
    pattern_strength  FLOAT,         -- max(abs(hit_momentum), abs(miss_momentum))
    is_momentum_player  BIT,         -- hit_momentum > 0.10
    is_reversion_player BIT,         -- hit_momentum < -0.10
    is_bouncy_player    BIT,         -- miss_momentum > 0.10 (bounce-back after miss)
    last_updated      DATETIME2
  )

Usage:
  python etl/compute_patterns.py [--min-games N]  default: 10
"""

import argparse
import os
import time
import logging
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

MIN_GAMES_DEFAULT = 10
MIN_TRANSITION_OBS = 3  # minimum observations in each state to trust the probability


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
            log.warning(f"DB attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                time.sleep(retry_wait)
    raise RuntimeError("Could not connect after retries.")


def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("""
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='common' AND TABLE_NAME='player_line_patterns'
)
CREATE TABLE common.player_line_patterns (
    player_id            BIGINT        NOT NULL,
    market_key           VARCHAR(100)  NOT NULL,
    line_value           DECIMAL(6,1)  NOT NULL,
    n                    INT           NOT NULL,
    hr_overall           FLOAT         NOT NULL,
    p_hit_after_hit      FLOAT         NULL,
    p_hit_after_miss     FLOAT         NULL,
    hit_momentum         FLOAT         NULL,
    miss_momentum        FLOAT         NULL,
    pattern_strength     FLOAT         NULL,
    is_momentum_player   BIT           NOT NULL DEFAULT 0,
    is_reversion_player  BIT           NOT NULL DEFAULT 0,
    is_bouncy_player     BIT           NOT NULL DEFAULT 0,
    last_updated         DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_player_line_patterns
        PRIMARY KEY (player_id, market_key, line_value)
)
"""))
    log.info("Schema ready.")


def fetch_resolved_outcomes(engine) -> pd.DataFrame:
    """Pull all resolved Over outcomes ordered by player/market/line/date."""
    df = pd.read_sql(text("""
        SELECT
            dg.player_id,
            dg.market_key,
            dg.line_value,
            dg.grade_date,
            CASE WHEN dg.outcome = 'Won' THEN 1 ELSE 0 END AS hit
        FROM common.daily_grades dg
        WHERE dg.outcome_name = 'Over'
          AND dg.outcome IS NOT NULL
        ORDER BY dg.player_id, dg.market_key, dg.line_value, dg.grade_date
    """), engine)
    log.info(f"Loaded {len(df):,} resolved outcomes.")
    return df


def compute_patterns(df: pd.DataFrame, min_games: int) -> pd.DataFrame:
    """
    For each (player_id, market_key, line_value):
    1. Order results chronologically
    2. Compute lag-1 transition probabilities:
       - P(hit | previous hit)
       - P(hit | previous miss)
    3. Compute momentum/reversion metrics
    """
    records = []

    for (pid, mkt, lv), grp in df.groupby(["player_id", "market_key", "line_value"]):
        hits = grp.sort_values("grade_date")["hit"].tolist()
        n = len(hits)

        if n < min_games:
            continue

        hr = float(np.mean(hits))

        # Collect transitions: (prev_result, next_result) pairs
        transitions = [(hits[i-1], hits[i]) for i in range(1, n)]

        after_hit  = [t[1] for t in transitions if t[0] == 1]
        after_miss = [t[1] for t in transitions if t[0] == 0]

        p_hit_after_hit  = float(np.mean(after_hit))  if len(after_hit)  >= MIN_TRANSITION_OBS else None
        p_hit_after_miss = float(np.mean(after_miss)) if len(after_miss) >= MIN_TRANSITION_OBS else None

        hit_momentum  = (p_hit_after_hit  - hr) if p_hit_after_hit  is not None else None
        miss_momentum = (p_hit_after_miss - hr) if p_hit_after_miss is not None else None

        strengths = [abs(v) for v in [hit_momentum, miss_momentum] if v is not None]
        pattern_strength = float(max(strengths)) if strengths else None

        records.append({
            "player_id":            int(pid),
            "market_key":           mkt,
            "line_value":           float(lv),
            "n":                    n,
            "hr_overall":           hr,
            "p_hit_after_hit":      p_hit_after_hit,
            "p_hit_after_miss":     p_hit_after_miss,
            "hit_momentum":         hit_momentum,
            "miss_momentum":        miss_momentum,
            "pattern_strength":     pattern_strength,
            "is_momentum_player":   int(hit_momentum is not None and hit_momentum >  0.10),
            "is_reversion_player":  int(hit_momentum is not None and hit_momentum < -0.10),
            "is_bouncy_player":     int(miss_momentum is not None and miss_momentum > 0.10),
        })

    result = pd.DataFrame(records)
    log.info(f"Computed patterns for {len(result):,} player-line combos (min_games={min_games}).")
    return result


def upsert_patterns(engine, df: pd.DataFrame):
    """Merge patterns into common.player_line_patterns."""
    if df.empty:
        log.info("No patterns to upsert.")
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    with engine.begin() as conn:
        # Stage
        conn.execute(text("""
IF OBJECT_ID('tempdb..#stage_patterns') IS NOT NULL
    DROP TABLE #stage_patterns
CREATE TABLE #stage_patterns (
    player_id BIGINT, market_key VARCHAR(100), line_value DECIMAL(6,1),
    n INT, hr_overall FLOAT,
    p_hit_after_hit FLOAT, p_hit_after_miss FLOAT,
    hit_momentum FLOAT, miss_momentum FLOAT, pattern_strength FLOAT,
    is_momentum_player BIT, is_reversion_player BIT, is_bouncy_player BIT
)
"""))

        # Bulk insert via executemany — much faster than row-by-row
        batch = [
            (
                int(r.player_id), r.market_key, float(r.line_value),
                int(r.n), float(r.hr_overall),
                float(r.p_hit_after_hit)  if r.p_hit_after_hit  is not None and not pd.isna(r.p_hit_after_hit)  else None,
                float(r.p_hit_after_miss) if r.p_hit_after_miss is not None and not pd.isna(r.p_hit_after_miss) else None,
                float(r.hit_momentum)     if r.hit_momentum     is not None and not pd.isna(r.hit_momentum)     else None,
                float(r.miss_momentum)    if r.miss_momentum    is not None and not pd.isna(r.miss_momentum)    else None,
                float(r.pattern_strength) if r.pattern_strength is not None and not pd.isna(r.pattern_strength) else None,
                int(r.is_momentum_player), int(r.is_reversion_player), int(r.is_bouncy_player),
            )
            for _, r in df.iterrows()
        ]
        conn.exec_driver_sql(
            "INSERT INTO #stage_patterns VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch
        )

        conn.execute(text(f"""
MERGE common.player_line_patterns AS t
USING #stage_patterns AS s
ON (t.player_id = s.player_id AND t.market_key = s.market_key AND t.line_value = s.line_value)
WHEN MATCHED THEN UPDATE SET
    t.n                   = s.n,
    t.hr_overall          = s.hr_overall,
    t.p_hit_after_hit     = s.p_hit_after_hit,
    t.p_hit_after_miss    = s.p_hit_after_miss,
    t.hit_momentum        = s.hit_momentum,
    t.miss_momentum       = s.miss_momentum,
    t.pattern_strength    = s.pattern_strength,
    t.is_momentum_player  = s.is_momentum_player,
    t.is_reversion_player = s.is_reversion_player,
    t.is_bouncy_player    = s.is_bouncy_player,
    t.last_updated        = '{now}'
WHEN NOT MATCHED THEN INSERT (
    player_id, market_key, line_value, n, hr_overall,
    p_hit_after_hit, p_hit_after_miss, hit_momentum, miss_momentum,
    pattern_strength, is_momentum_player, is_reversion_player, is_bouncy_player,
    last_updated
) VALUES (
    s.player_id, s.market_key, s.line_value, s.n, s.hr_overall,
    s.p_hit_after_hit, s.p_hit_after_miss, s.hit_momentum, s.miss_momentum,
    s.pattern_strength, s.is_momentum_player, s.is_reversion_player, s.is_bouncy_player,
    '{now}'
);
"""))

    log.info(f"Upserted {len(df):,} pattern rows.")


def print_summary(df: pd.DataFrame):
    if df.empty:
        return
    log.info(f"\n--- Pattern Summary ---")
    log.info(f"Total combos:          {len(df):,}")
    log.info(f"Momentum players:      {df['is_momentum_player'].sum():,} ({df['is_momentum_player'].mean():.1%})")
    log.info(f"Reversion players:     {df['is_reversion_player'].sum():,} ({df['is_reversion_player'].mean():.1%})")
    log.info(f"Bouncy players:        {df['is_bouncy_player'].sum():,} ({df['is_bouncy_player'].mean():.1%})")
    log.info(f"No clear pattern:      {(~(df['is_momentum_player'] | df['is_reversion_player'] | df['is_bouncy_player']).astype(bool)).sum():,}")
    strong = df[df['pattern_strength'].notna() & (df['pattern_strength'] >= 0.20)]
    log.info(f"Strong patterns (>=20pp): {len(strong):,}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-games", type=int, default=MIN_GAMES_DEFAULT)
    args = parser.parse_args()

    engine = get_engine()
    ensure_schema(engine)

    df_outcomes = fetch_resolved_outcomes(engine)
    df_patterns = compute_patterns(df_outcomes, min_games=args.min_games)
    print_summary(df_patterns)
    upsert_patterns(engine, df_patterns)

    log.info("Done.")


if __name__ == "__main__":
    main()
