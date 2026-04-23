"""
grade_props.py

NBA prop grading model.

Composite formula (ADR-20260423-1, 2026-04-23):
  composite_grade = 0.40 * momentum_grade
                  + 0.40 * (hit_rate_60 * 100)
                  + 0.20 * pattern_grade
  All other components (matchup, regression, trend, opportunity) are stored
  as context columns but NOT included in the composite mean. Calibration
  showed they add no predictive lift.

Tier lines (compute_kde_tier_lines):
  KDE fitted on grade-weighted game log window. Tier cutoffs derived from
  model probability: safe>=80%, value>=58%, high_risk>=28% (+150 market),
  lotto>=7% (+400 market). Blowout dampening applied for pts/combo markets
  when spread>=10.5 and player is on projected losing team.
"""

import argparse
import math
import os
import time
import logging
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde, norm
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def today_et() -> str:
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-4))).strftime("%Y-%m-%d")


BOOKMAKER = "fanduel"
LOOKBACK_LONG  = 60
LOOKBACK_SHORT = 20
LOOKBACK_OPP   = 200
WEIGHT_SHORT = 0.60
WEIGHT_LONG  = 0.40
MIN_SAMPLE = 5
SEASON_START = "2024-10-01"
SEASON_MIN = 10
RECENT_WINDOW = 10
TREND_SHORT = 10
TREND_LONG  = 30
TREND_MIN   = 3
PATTERN_MIN = 3
BRACKET_STEPS     = 5
BRACKET_INCREMENT = 1.0
BATCH_DEFAULT     = 10

# KDE tier computation knobs.
# Lookback window selected by composite grade to weight recency vs stability.
KDE_WINDOW_HOT    = 15   # composite >= 80: use last 15 games (player is peaking)
KDE_WINDOW_MID    = 30   # composite 50-79: use last 30 games (balanced)
KDE_WINDOW_COLD   = 82   # composite < 50: full season (recent form is uninformative)
KDE_MIN_GAMES     = 10   # fall back to normal distribution below this
KDE_REFLECT_AT    = 0.0  # reflect distribution at 0 to prevent negative-stat probability

# Tier probability thresholds (calibrated from historical data, section 5 analysis).
TIER_SAFE_PROB      = 0.80
TIER_VALUE_PROB     = 0.58
TIER_HIGHRISK_PROB  = 0.28
TIER_LOTTO_PROB     = 0.07

# Minimum market price (American odds) required to surface high-risk / lotto tiers.
# High risk: at least +150 (implied ~40%). Lotto: at least +400 (implied ~20%).
TIER_HIGHRISK_MIN_PRICE = 150
TIER_LOTTO_MIN_PRICE    = 400

# Blowout dampening: applied when pre-game spread >= this threshold and
# player is on projected losing team. Points/combo markets only.
BLOWOUT_SPREAD_THRESHOLD = 10.5
# Markets where blowout dampening applies (stat production drops in blowout losses).
BLOWOUT_DAMPEN_MARKETS = {
    "player_points", "player_points_alternate",
    "player_points_rebounds_assists", "player_points_rebounds_assists_alternate",
    "player_points_rebounds", "player_points_rebounds_alternate",
    "player_points_assists", "player_points_assists_alternate",
}

OPP_TREND_SHORT = 10
OPP_TREND_LONG  = 30
OPP_TREND_MIN   = 3
OPP_STREAK_MIN  = 2

STANDARD_MARKETS = {
    "player_points", "player_rebounds", "player_assists", "player_threes",
    "player_blocks", "player_steals",
    "player_points_rebounds_assists", "player_points_rebounds",
    "player_points_assists", "player_rebounds_assists",
    "player_double_double", "player_triple_double", "player_first_basket",
}

ALTERNATE_MARKETS = {
    "player_points_alternate", "player_rebounds_alternate",
    "player_assists_alternate", "player_threes_alternate",
    "player_blocks_alternate", "player_steals_alternate",
    "player_points_assists_alternate", "player_points_rebounds_alternate",
    "player_rebounds_assists_alternate",
    "player_points_rebounds_assists_alternate",
}

PLAYER_MARKETS = STANDARD_MARKETS | ALTERNATE_MARKETS

ALT_GRIDS = {
    "pts":  [4.5, 9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5],
    "reb":  [3.5, 5.5, 7.5, 9.5, 11.5, 13.5, 15.5],
    "ast":  [1.5, 3.5, 5.5, 7.5, 9.5, 11.5, 13.5],
    "fg3m": [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5],
    "blk":  [0.5, 1.5, 2.5, 3.5],
    "stl":  [0.5, 1.5, 2.5, 3.5],
    "pra":  [9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5, 54.5, 59.5],
    "pr":   [9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5],
    "pa":   [9.5, 14.5, 19.5, 24.5, 29.5, 34.5, 39.5, 44.5, 49.5],
    "ra":   [4.5, 9.5, 14.5, 19.5, 24.5],
}

MARKET_STAT_MAP = {
    "player_points":                            "SUM(pts)",
    "player_points_alternate":                  "SUM(pts)",
    "player_rebounds":                          "SUM(reb)",
    "player_rebounds_alternate":                "SUM(reb)",
    "player_assists":                           "SUM(ast)",
    "player_assists_alternate":                 "SUM(ast)",
    "player_threes":                            "SUM(fg3m)",
    "player_threes_alternate":                  "SUM(fg3m)",
    "player_blocks":                            "SUM(blk)",
    "player_blocks_alternate":                  "SUM(blk)",
    "player_steals":                            "SUM(stl)",
    "player_steals_alternate":                  "SUM(stl)",
    "player_points_rebounds_assists":           "SUM(pts) + SUM(reb) + SUM(ast)",
    "player_points_rebounds_assists_alternate": "SUM(pts) + SUM(reb) + SUM(ast)",
    "player_points_rebounds":                   "SUM(pts) + SUM(reb)",
    "player_points_rebounds_alternate":         "SUM(pts) + SUM(reb)",
    "player_points_assists":                    "SUM(pts) + SUM(ast)",
    "player_points_assists_alternate":          "SUM(pts) + SUM(ast)",
    "player_rebounds_assists":                  "SUM(reb) + SUM(ast)",
    "player_rebounds_assists_alternate":        "SUM(reb) + SUM(ast)",
}

MARKET_STAT_COL = {
    "player_points":                            "pts",
    "player_points_alternate":                  "pts",
    "player_rebounds":                          "reb",
    "player_rebounds_alternate":                "reb",
    "player_assists":                           "ast",
    "player_assists_alternate":                 "ast",
    "player_threes":                            "fg3m",
    "player_threes_alternate":                  "fg3m",
    "player_blocks":                            "blk",
    "player_blocks_alternate":                  "blk",
    "player_steals":                            "stl",
    "player_steals_alternate":                  "stl",
    "player_points_rebounds_assists":           "pra",
    "player_points_rebounds_assists_alternate": "pra",
    "player_points_rebounds":                   "pr",
    "player_points_rebounds_alternate":         "pr",
    "player_points_assists":                    "pa",
    "player_points_assists_alternate":          "pa",
    "player_rebounds_assists":                  "ra",
    "player_rebounds_assists_alternate":        "ra",
}

MARKET_DEF_RANK = {
    "player_points":             "rank_pts",
    "player_points_alternate":   "rank_pts",
    "player_rebounds":           "rank_reb",
    "player_rebounds_alternate": "rank_reb",
    "player_assists":            "rank_ast",
    "player_assists_alternate":  "rank_ast",
    "player_threes":             "rank_fg3m",
    "player_threes_alternate":   "rank_fg3m",
    "player_blocks":             "rank_blk",
    "player_blocks_alternate":   "rank_blk",
    "player_steals":             "rank_stl",
    "player_steals_alternate":   "rank_stl",
}

MARKET_TO_ACTUAL_COL = {
    "player_points":                            "pts",
    "player_points_alternate":                  "pts",
    "player_rebounds":                          "reb",
    "player_rebounds_alternate":                "reb",
    "player_assists":                           "ast",
    "player_assists_alternate":                 "ast",
    "player_threes":                            "fg3m",
    "player_threes_alternate":                  "fg3m",
    "player_blocks":                            "blk",
    "player_blocks_alternate":                  "blk",
    "player_steals":                            "stl",
    "player_steals_alternate":                  "stl",
    "player_points_rebounds_assists":           "pra",
    "player_points_rebounds_assists_alternate": "pra",
    "player_points_rebounds":                   "pr",
    "player_points_rebounds_alternate":         "pr",
    "player_points_assists":                    "pa",
    "player_points_assists_alternate":          "pa",
    "player_rebounds_assists":                  "ra",
    "player_rebounds_assists_alternate":        "ra",
}

MARKET_OPP_COMPONENTS = {
    "player_points":                            ("pts",),
    "player_points_alternate":                  ("pts",),
    "player_rebounds":                          ("reb",),
    "player_rebounds_alternate":                ("reb",),
    "player_assists":                           ("ast",),
    "player_assists_alternate":                 ("ast",),
    "player_threes":                            ("fg3m",),
    "player_threes_alternate":                  ("fg3m",),
    "player_points_rebounds_assists":           ("pts", "reb", "ast"),
    "player_points_rebounds_assists_alternate": ("pts", "reb", "ast"),
    "player_points_rebounds":                   ("pts", "reb"),
    "player_points_rebounds_alternate":         ("pts", "reb"),
    "player_points_assists":                    ("pts", "ast"),
    "player_points_assists_alternate":          ("pts", "ast"),
    "player_rebounds_assists":                  ("reb", "ast"),
    "player_rebounds_assists_alternate":        ("reb", "ast"),
}

OPP_MATCHUP_RANK = {
    "pts":  "rank_opp_pts",
    "reb":  "rank_reb_chances",
    "ast":  "rank_potential_ast",
    "fg3m": "rank_opp_fg3m",
}

_ARCHIVE_COLS = (
    "grade_id, grade_date, event_id, game_id, player_id, player_name, "
    "market_key, bookmaker_key, line_value, outcome_name, over_price, "
    "hit_rate_60, hit_rate_20, sample_size_60, sample_size_20, "
    "weighted_hit_rate, grade, trend_grade, momentum_grade, pattern_grade, "
    "matchup_grade, regression_grade, composite_grade, hit_rate_opp, "
    "sample_size_opp, outcome, created_at, "
    "opportunity_short_grade, opportunity_long_grade, "
    "opportunity_matchup_grade, opportunity_streak_grade, "
    "opportunity_volume_grade, opportunity_expected_grade"
)

_ARCHIVE_COLS_DG = ", ".join(
    f"dg.{c.strip()}" for c in _ARCHIVE_COLS.split(",")
)


def get_engine(max_retries=3, retry_wait=60):
    conn_str = (
        f"mssql+pyodbc://{os.environ['AZURE_SQL_USERNAME']}:"
        f"{os.environ['AZURE_SQL_PASSWORD']}@"
        f"{os.environ['AZURE_SQL_SERVER']}/"
        f"{os.environ['AZURE_SQL_DATABASE']}"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
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


def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(
            "IF OBJECT_ID('common.grade_thresholds','U') IS NOT NULL "
            "DROP TABLE common.grade_thresholds"
        ))
        conn.execute(text(
            "IF NOT EXISTS(SELECT 1 FROM sys.schemas WHERE name='common') "
            "EXEC('CREATE SCHEMA common')"
        ))
        conn.execute(text("""
IF NOT EXISTS(
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades'
)
CREATE TABLE common.daily_grades(
    grade_id          INT IDENTITY(1,1) NOT NULL,
    grade_date        DATE          NOT NULL,
    event_id          VARCHAR(50)   NOT NULL,
    game_id           VARCHAR(15)   NULL,
    player_id         BIGINT        NULL,
    player_name       NVARCHAR(100) NOT NULL,
    market_key        VARCHAR(100)  NOT NULL,
    bookmaker_key     VARCHAR(50)   NOT NULL,
    line_value        DECIMAL(6,1)  NOT NULL,
    outcome_name      VARCHAR(5)    NOT NULL DEFAULT 'Over',
    over_price        INT           NULL,
    hit_rate_60       FLOAT         NULL,
    hit_rate_20       FLOAT         NULL,
    sample_size_60    INT           NULL,
    sample_size_20    INT           NULL,
    weighted_hit_rate FLOAT         NULL,
    grade             FLOAT         NULL,
    trend_grade       FLOAT         NULL,
    momentum_grade    FLOAT         NULL,
    pattern_grade     FLOAT         NULL,
    matchup_grade     FLOAT         NULL,
    regression_grade  FLOAT         NULL,
    composite_grade   FLOAT         NULL,
    hit_rate_opp      FLOAT         NULL,
    sample_size_opp   INT           NULL,
    outcome           VARCHAR(5)    NULL,
    created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_daily_grades PRIMARY KEY (grade_id),
    CONSTRAINT uq_daily_grades_v3 UNIQUE (
        grade_date, event_id, player_id,
        market_key, bookmaker_key, line_value, outcome_name
    )
)
"""))
        for col, dtype in [
            ("over_price",      "INT"),
            ("trend_grade",     "FLOAT"),
            ("momentum_grade",  "FLOAT"),
            ("pattern_grade",   "FLOAT"),
            ("matchup_grade",   "FLOAT"),
            ("regression_grade","FLOAT"),
            ("composite_grade", "FLOAT"),
            ("hit_rate_opp",    "FLOAT"),
            ("sample_size_opp", "INT"),
            ("outcome_name",    "VARCHAR(5)"),
            ("outcome",         "VARCHAR(5)"),
            ("opportunity_short_grade",    "FLOAT"),
            ("opportunity_long_grade",     "FLOAT"),
            ("opportunity_matchup_grade",  "FLOAT"),
            ("opportunity_streak_grade",   "FLOAT"),
            ("opportunity_volume_grade",   "FLOAT"),
            ("opportunity_expected_grade", "FLOAT"),
        ]:
            conn.execute(text(
                f"IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades' AND COLUMN_NAME='{col}') "
                f"ALTER TABLE common.daily_grades ADD {col} {dtype} NULL"
            ))

        conn.execute(text("""
IF NOT EXISTS(
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive'
)
CREATE TABLE common.daily_grades_archive(
    grade_id          INT           NOT NULL,
    grade_date        DATE          NOT NULL,
    event_id          VARCHAR(50)   NOT NULL,
    game_id           VARCHAR(15)   NULL,
    player_id         BIGINT        NULL,
    player_name       NVARCHAR(100) NOT NULL,
    market_key        VARCHAR(100)  NOT NULL,
    bookmaker_key     VARCHAR(50)   NOT NULL,
    line_value        DECIMAL(6,1)  NOT NULL,
    outcome_name      VARCHAR(5)    NULL,
    over_price        INT           NULL,
    hit_rate_60       FLOAT         NULL,
    hit_rate_20       FLOAT         NULL,
    sample_size_60    INT           NULL,
    sample_size_20    INT           NULL,
    weighted_hit_rate FLOAT         NULL,
    grade             FLOAT         NULL,
    trend_grade       FLOAT         NULL,
    momentum_grade    FLOAT         NULL,
    pattern_grade     FLOAT         NULL,
    matchup_grade     FLOAT         NULL,
    regression_grade  FLOAT         NULL,
    composite_grade   FLOAT         NULL,
    hit_rate_opp      FLOAT         NULL,
    sample_size_opp   INT           NULL,
    outcome           VARCHAR(5)    NULL,
    created_at        DATETIME2     NULL,
    archived_at       DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
)
"""))
        for col, dtype in [
            ("outcome_name",    "VARCHAR(5)"),
            ("over_price",      "INT"),
            ("trend_grade",     "FLOAT"),
            ("momentum_grade",  "FLOAT"),
            ("pattern_grade",   "FLOAT"),
            ("matchup_grade",   "FLOAT"),
            ("regression_grade","FLOAT"),
            ("composite_grade", "FLOAT"),
            ("hit_rate_opp",    "FLOAT"),
            ("sample_size_opp", "INT"),
            ("outcome",         "VARCHAR(5)"),
            ("archived_at",     "DATETIME2"),
            ("opportunity_short_grade",    "FLOAT"),
            ("opportunity_long_grade",     "FLOAT"),
            ("opportunity_matchup_grade",  "FLOAT"),
            ("opportunity_streak_grade",   "FLOAT"),
            ("opportunity_volume_grade",   "FLOAT"),
            ("opportunity_expected_grade", "FLOAT"),
        ]:
            conn.execute(text(
                f"IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive' AND COLUMN_NAME='{col}') "
                f"ALTER TABLE common.daily_grades_archive ADD {col} {dtype} NULL"
            ))

        # player_tier_lines: one row per (player, market, game, grade_date).
        # Stores model-derived tier line values and probabilities independently
        # of which lines the market has posted. High-risk and lotto are NULL
        # when no qualifying market price exists or no signal is present.
        conn.execute(text("""
IF NOT EXISTS(
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA='common' AND TABLE_NAME='player_tier_lines'
)
CREATE TABLE common.player_tier_lines(
    tier_id           INT IDENTITY(1,1) NOT NULL,
    grade_date        DATE          NOT NULL,
    game_id           VARCHAR(15)   NOT NULL,
    player_id         BIGINT        NOT NULL,
    player_name       NVARCHAR(100) NOT NULL,
    market_key        VARCHAR(100)  NOT NULL,
    composite_grade   FLOAT         NULL,
    kde_window        INT           NULL,
    blowout_dampened  BIT           NOT NULL DEFAULT 0,
    safe_line         DECIMAL(6,1)  NULL,
    safe_prob         FLOAT         NULL,
    value_line        DECIMAL(6,1)  NULL,
    value_prob        FLOAT         NULL,
    highrisk_line     DECIMAL(6,1)  NULL,
    highrisk_prob     FLOAT         NULL,
    highrisk_price    INT           NULL,
    lotto_line        DECIMAL(6,1)  NULL,
    lotto_prob        FLOAT         NULL,
    lotto_price       INT           NULL,
    created_at        DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT pk_player_tier_lines PRIMARY KEY (tier_id),
    CONSTRAINT uq_player_tier_lines UNIQUE (
        grade_date, game_id, player_id, market_key
    )
)
"""))

    log.info("Schema verified.")


# ---------------------------------------------------------------------------
# KDE tier line computation
# ---------------------------------------------------------------------------

def _kde_prob_above(values: np.ndarray, line: float) -> float:
    """
    P(stat > line) from KDE with reflection boundary at 0.

    Reflection prevents the KDE from assigning probability mass below 0
    (e.g. a player averaging 3 rebounds won't have 15% probability of
    negative rebounds). Technique: mirror the sample around 0, fit KDE
    on the combined original+mirrored values, then double the upper-tail
    integral to correct for the mirror mass.

    Falls back to normal distribution when n < KDE_MIN_GAMES.
    """
    n = len(values)
    if n < KDE_MIN_GAMES:
        mu = float(np.mean(values))
        sigma = max(float(np.std(values)), 0.5)
        return float(1 - norm.cdf(line, loc=mu, scale=sigma))

    try:
        # Reflection boundary at 0
        reflected = np.concatenate([values, -values])
        kde = gaussian_kde(reflected, bw_method="scott")
        # Integrate from line to line+500 (captures full upper tail for any sport stat)
        prob = kde.integrate_box_1d(line, line + 500) * 2.0
        return float(np.clip(prob, 1e-6, 1 - 1e-6))
    except Exception:
        mu = float(np.mean(values))
        sigma = max(float(np.std(values)), 0.5)
        return float(1 - norm.cdf(line, loc=mu, scale=sigma))


def _select_kde_window(composite_grade) -> int:
    """Select game-log lookback window based on composite grade."""
    if composite_grade is None:
        return KDE_WINDOW_MID
    g = float(composite_grade)
    if g >= 80:
        return KDE_WINDOW_HOT
    if g >= 50:
        return KDE_WINDOW_MID
    return KDE_WINDOW_COLD


def compute_kde_tier_lines(
    stat_values: np.ndarray,
    composite_grade,
    available_lines: list,
    price_lookup: dict,
    blowout_delta: float = 0.0,
) -> dict:
    """
    Compute tier line values from the player's stat distribution.

    Args:
        stat_values:      Full season game log for this stat (sorted ASC by date).
        composite_grade:  New composite grade (0-100) for this player-market.
        available_lines:  Sorted list of line values available in the market
                          (standard + alternate). Can be empty.
        price_lookup:     Dict of {line_value: american_odds} for Over prices.
                          Used to find qualifying prices for high_risk/lotto.
        blowout_delta:    Points to subtract from each stat value before fitting
                          KDE (only for pts/combo markets when blowout risk is
                          high and player is on projected losing team).
                          Derived from player's historical blowout loss profile.

    Returns dict with keys:
        safe_line, safe_prob,
        value_line, value_prob,
        highrisk_line, highrisk_prob, highrisk_price,
        lotto_line, lotto_prob, lotto_price,
        kde_window, blowout_dampened
    """
    result = {
        "safe_line": None, "safe_prob": None,
        "value_line": None, "value_prob": None,
        "highrisk_line": None, "highrisk_prob": None, "highrisk_price": None,
        "lotto_line": None, "lotto_prob": None, "lotto_price": None,
        "kde_window": None, "blowout_dampened": False,
    }

    if stat_values is None or len(stat_values) < 3:
        return result

    window = _select_kde_window(composite_grade)
    result["kde_window"] = window

    # Apply grade-weighted window
    values = stat_values[-window:] if len(stat_values) > window else stat_values
    values = values.astype(float)

    # Apply blowout dampening: shift the distribution left by delta
    if blowout_delta != 0.0:
        values = np.maximum(values - abs(blowout_delta), 0.0)
        result["blowout_dampened"] = True

    # Build candidate lines: union of available market lines and a dense grid
    # across the player's plausible stat range. This ensures we always find
    # tier cutoffs even when the market hasn't posted lines in that range.
    p5  = float(np.percentile(values, 5))
    p95 = float(np.percentile(values, 95))
    step = 0.5
    grid_min = max(0.0, p5 - 5.0)
    grid_max = p95 + 15.0
    dense_grid = list(np.arange(grid_min, grid_max + step, step))

    posted_lines = sorted(set(dense_grid + [float(l) for l in available_lines]))

    # Compute P(stat > line) for every candidate line
    probs = [(line, _kde_prob_above(values, line)) for line in posted_lines]

    # Safe: highest line where P >= TIER_SAFE_PROB
    safe_candidates = [(l, p) for l, p in probs if p >= TIER_SAFE_PROB]
    if safe_candidates:
        result["safe_line"] = safe_candidates[-1][0]
        result["safe_prob"] = round(safe_candidates[-1][1], 4)

    # Value: highest line where P >= TIER_VALUE_PROB (must be above safe line)
    safe_floor = result["safe_line"] or 0.0
    value_candidates = [(l, p) for l, p in probs if p >= TIER_VALUE_PROB and l > safe_floor]
    if value_candidates:
        result["value_line"] = value_candidates[-1][0]
        result["value_prob"] = round(value_candidates[-1][1], 4)

    # High risk: highest line where P >= TIER_HIGHRISK_PROB AND market offers +150 or better.
    # Only populate if a qualifying price exists near that line (within 0.5).
    value_floor = result["value_line"] or safe_floor
    for line, prob in reversed(probs):
        if prob < TIER_HIGHRISK_PROB:
            continue
        if line <= value_floor:
            continue
        # Find closest posted price
        closest_price = None
        for candidate_line, price in price_lookup.items():
            if abs(candidate_line - line) <= 0.5 and price is not None:
                if price >= TIER_HIGHRISK_MIN_PRICE:
                    if closest_price is None or price > closest_price:
                        closest_price = price
        if closest_price is not None:
            result["highrisk_line"] = line
            result["highrisk_prob"] = round(prob, 4)
            result["highrisk_price"] = closest_price
            break

    # Lotto: highest line where P >= TIER_LOTTO_PROB AND market offers +400 or better
    # AND composite_grade is above 50 (need a signal, not just a rare event).
    highrisk_floor = result["highrisk_line"] or value_floor
    if composite_grade is not None and float(composite_grade) >= 50:
        for line, prob in reversed(probs):
            if prob < TIER_LOTTO_PROB:
                continue
            if line <= highrisk_floor:
                continue
            closest_price = None
            for candidate_line, price in price_lookup.items():
                if abs(candidate_line - line) <= 0.5 and price is not None:
                    if price >= TIER_LOTTO_MIN_PRICE:
                        if closest_price is None or price > closest_price:
                            closest_price = price
            if closest_price is not None:
                result["lotto_line"] = line
                result["lotto_prob"] = round(prob, 4)
                result["lotto_price"] = closest_price
                break

    return result


# ---------------------------------------------------------------------------
# New data fetchers: spreads and blowout profiles
# ---------------------------------------------------------------------------

def fetch_game_spreads(engine, game_ids: list) -> dict:
    """
    Return opening spread (favorite's absolute point spread) per game_id.
    Uses earliest snap_ts per event as the opening line.
    Returns dict: {game_id: float_spread}
    """
    if not game_ids:
        return {}
    gid_list = ", ".join(f"'{g}'" for g in game_ids)
    df = pd.read_sql(text(f"""
        SELECT egm.game_id,
               MIN(ABS(CAST(gl.outcome_point AS FLOAT))) AS open_spread
        FROM odds.game_lines gl
        JOIN odds.event_game_map egm ON egm.event_id = gl.event_id
        WHERE gl.market_key = 'spreads'
        AND gl.sport_key = 'basketball_nba'
        AND egm.game_id IN ({gid_list})
        GROUP BY egm.game_id
    """), engine)
    return {row["game_id"]: float(row["open_spread"]) for _, row in df.iterrows()}


def fetch_upcoming_game_spreads(engine, game_ids: list) -> dict:
    """
    Same as fetch_game_spreads but reads from odds.upcoming_game_lines
    for today's games (not yet in historical table).
    Falls back to fetch_game_spreads if nothing found.
    """
    if not game_ids:
        return {}
    gid_list = ", ".join(f"'{g}'" for g in game_ids)
    df = pd.read_sql(text(f"""
        SELECT egm.game_id,
               MIN(ABS(CAST(gl.outcome_point AS FLOAT))) AS open_spread
        FROM odds.upcoming_game_lines gl
        JOIN odds.event_game_map egm ON egm.event_id = gl.event_id
        WHERE gl.market_key = 'spreads'
        AND gl.sport_key = 'basketball_nba'
        AND egm.game_id IN ({gid_list})
        GROUP BY egm.game_id
    """), engine)
    result = {row["game_id"]: float(row["open_spread"]) for _, row in df.iterrows()}
    # Fill any misses from historical table (in case upcoming hasn't populated)
    missing = [g for g in game_ids if g not in result]
    if missing:
        result.update(fetch_game_spreads(engine, missing))
    return result


def fetch_player_blowout_profiles(engine, player_ids: list) -> dict:
    """
    Per-player historical pts delta in blowout losses vs close games.
    Returns dict: {player_id: float_delta}
    delta is negative for players who score less in blowout losses (most stars).
    delta is positive for garbage-time beneficiaries.

    Only meaningful for players with >= 5 blowout loss games and >= 8 close games.
    """
    if not player_ids:
        return {}
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    df = pd.read_sql(text(f"""
        WITH game_mins AS (
            SELECT player_id, game_id, team_tricode,
                SUM(CAST(pts AS FLOAT)) as pts,
                SUM(CAST(minutes AS FLOAT)) as mins
            FROM nba.player_box_score_stats
            WHERE player_id IN ({pid_list})
            GROUP BY player_id, game_id, team_tricode
            HAVING SUM(CAST(minutes AS FLOAT)) >= 10
        ),
        game_context AS (
            SELECT gm.player_id, gm.pts,
                ABS(g.home_score - g.away_score) as margin,
                CASE
                    WHEN (gm.team_tricode=g.home_team_tricode AND g.home_score>g.away_score)
                      OR (gm.team_tricode=g.away_team_tricode AND g.away_score>g.home_score)
                    THEN 'W' ELSE 'L'
                END as wl
            FROM game_mins gm
            JOIN nba.games g ON g.game_id = gm.game_id
            WHERE g.home_score IS NOT NULL
        )
        SELECT player_id,
            AVG(CASE WHEN margin <= 10 THEN pts END) as pts_close,
            AVG(CASE WHEN margin > 20 AND wl='L' THEN pts END) as pts_blowout_l,
            SUM(CASE WHEN margin <= 10 THEN 1 ELSE 0 END) as n_close,
            SUM(CASE WHEN margin > 20 AND wl='L' THEN 1 ELSE 0 END) as n_blowout_l
        FROM game_context
        GROUP BY player_id
        HAVING SUM(CASE WHEN margin <= 10 THEN 1 ELSE 0 END) >= 8
        AND SUM(CASE WHEN margin > 20 AND wl='L' THEN 1 ELSE 0 END) >= 5
    """), engine)
    result = {}
    for _, row in df.iterrows():
        if pd.notna(row["pts_close"]) and pd.notna(row["pts_blowout_l"]):
            delta = float(row["pts_blowout_l"]) - float(row["pts_close"])
            result[int(row["player_id"])] = round(delta, 1)
    log.info(f"  Blowout profiles: {len(result)} players with sufficient history.")
    return result


def upsert_tier_lines(engine, rows: list) -> int:
    """
    Upsert rows into common.player_tier_lines.
    One row per (grade_date, game_id, player_id, market_key).
    """
    if not rows:
        return 0
    seen = {}
    for r in rows:
        k = (r["grade_date"], r["game_id"], r["player_id"], r["market_key"])
        seen[k] = r
    rows = list(seen.values())

    with engine.begin() as conn:
        conn.execute(text(
            "IF OBJECT_ID('tempdb..#stage_tiers') IS NOT NULL DROP TABLE #stage_tiers"
        ))
        conn.execute(text("""
CREATE TABLE #stage_tiers(
    grade_date DATE, game_id VARCHAR(15), player_id BIGINT, player_name NVARCHAR(100),
    market_key VARCHAR(100), composite_grade FLOAT, kde_window INT, blowout_dampened BIT,
    safe_line DECIMAL(6,1), safe_prob FLOAT,
    value_line DECIMAL(6,1), value_prob FLOAT,
    highrisk_line DECIMAL(6,1), highrisk_prob FLOAT, highrisk_price INT,
    lotto_line DECIMAL(6,1), lotto_prob FLOAT, lotto_price INT
)"""))
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            conn.exec_driver_sql(
                "INSERT INTO #stage_tiers VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(r["grade_date"], r["game_id"], r["player_id"], r["player_name"],
                  r["market_key"], r["composite_grade"], r["kde_window"],
                  1 if r["blowout_dampened"] else 0,
                  r["safe_line"], r["safe_prob"],
                  r["value_line"], r["value_prob"],
                  r["highrisk_line"], r["highrisk_prob"], r["highrisk_price"],
                  r["lotto_line"], r["lotto_prob"], r["lotto_price"])
                 for r in chunk]
            )
        conn.execute(text("""
MERGE common.player_tier_lines AS t USING #stage_tiers AS s
ON (t.grade_date=s.grade_date AND t.game_id=s.game_id
    AND t.player_id=s.player_id AND t.market_key=s.market_key)
WHEN MATCHED THEN UPDATE SET
    t.player_name=s.player_name, t.composite_grade=s.composite_grade,
    t.kde_window=s.kde_window, t.blowout_dampened=s.blowout_dampened,
    t.safe_line=s.safe_line, t.safe_prob=s.safe_prob,
    t.value_line=s.value_line, t.value_prob=s.value_prob,
    t.highrisk_line=s.highrisk_line, t.highrisk_prob=s.highrisk_prob,
    t.highrisk_price=s.highrisk_price,
    t.lotto_line=s.lotto_line, t.lotto_prob=s.lotto_prob,
    t.lotto_price=s.lotto_price
WHEN NOT MATCHED THEN INSERT(
    grade_date, game_id, player_id, player_name, market_key,
    composite_grade, kde_window, blowout_dampened,
    safe_line, safe_prob, value_line, value_prob,
    highrisk_line, highrisk_prob, highrisk_price,
    lotto_line, lotto_prob, lotto_price
) VALUES(
    s.grade_date, s.game_id, s.player_id, s.player_name, s.market_key,
    s.composite_grade, s.kde_window, s.blowout_dampened,
    s.safe_line, s.safe_prob, s.value_line, s.value_prob,
    s.highrisk_line, s.highrisk_prob, s.highrisk_price,
    s.lotto_line, s.lotto_prob, s.lotto_price
);"""))
    return len(rows)


def fetch_history(engine, player_ids, market_keys, as_of_date, lookback=None):
    if lookback is None:
        lookback = LOOKBACK_LONG
    gradeable = [m for m in market_keys if m in MARKET_STAT_MAP]
    if not player_ids or not gradeable:
        return pd.DataFrame()
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    expr_to_mkts: dict = {}
    for m in gradeable:
        expr_to_mkts.setdefault(MARKET_STAT_MAP[m], []).append(m)
    branches = []
    for expr, mkts in expr_to_mkts.items():
        mkt_vals = ", ".join(f"('{m}')" for m in mkts)
        branches.append(
            f"SELECT b.player_id, m.market_key, b.game_date, b.game_id,"
            f" {expr} AS stat_value,"
            f" CASE WHEN b.team_id=s.home_team_id THEN s.away_team_id ELSE s.home_team_id END AS opp_team_id"
            f" FROM nba.player_box_score_stats b"
            f" JOIN nba.schedule s ON s.game_id=b.game_id"
            f" CROSS JOIN (SELECT market_key FROM (VALUES {mkt_vals}) AS t(market_key)) m"
            f" WHERE b.player_id IN ({pid_list})"
            f" AND b.game_date < :aod AND b.game_date >= DATEADD(day,-:lb_long,:aod)"
            f" GROUP BY b.player_id,b.game_id,b.game_date,b.team_id,s.home_team_id,s.away_team_id,m.market_key"
        )
    union_sql = " UNION ALL ".join(branches)
    sql = text(
        f"SELECT player_id,market_key,game_date,stat_value,opp_team_id,"
        f" CASE WHEN game_date>=DATEADD(day,-:lb_short,:aod) THEN 1 ELSE 0 END AS in_short_window"
        f" FROM ({union_sql}) AS combined WHERE stat_value IS NOT NULL"
    )
    df = pd.read_sql(sql, engine, params={
        "aod": str(as_of_date), "lb_long": lookback, "lb_short": LOOKBACK_SHORT,
    })
    log.info(f"  Hit-rate history (lookback={lookback}d): {len(df)} rows.")
    return df


def fetch_season_history(engine, player_ids, as_of_date):
    if not player_ids:
        return pd.DataFrame()
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    sql = text(
        f"SELECT b.player_id,b.game_date,b.game_id,"
        f" SUM(b.pts) AS pts,SUM(b.reb) AS reb,SUM(b.ast) AS ast,"
        f" SUM(b.stl) AS stl,SUM(b.blk) AS blk,SUM(b.fg3m) AS fg3m,SUM(b.tov) AS tov"
        f" FROM nba.player_box_score_stats b"
        f" WHERE b.player_id IN ({pid_list}) AND b.game_date>=:ss AND b.game_date<:aod"
        f" GROUP BY b.player_id,b.game_id,b.game_date"
    )
    df = pd.read_sql(sql, engine, params={"ss": SEASON_START, "aod": str(as_of_date)})
    df["pra"] = df["pts"] + df["reb"] + df["ast"]
    df["pr"]  = df["pts"] + df["reb"]
    df["pa"]  = df["pts"] + df["ast"]
    df["ra"]  = df["reb"] + df["ast"]
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["player_id", "game_date"])
    log.info(f"  Season history: {len(df)} rows.")
    return df


def fetch_opportunity_history(engine, player_ids, as_of_date):
    if not player_ids:
        return pd.DataFrame()
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    sql = text(f"""
        SELECT b.player_id, b.game_date, b.game_id, b.team_id,
               CASE WHEN b.team_id = s.home_team_id THEN s.away_team_id ELSE s.home_team_id END AS opp_team_id,
               SUM(CAST(b.fga  AS INT)) AS fga,
               SUM(CAST(b.fg3a AS INT)) AS fg3a,
               SUM(CAST(b.fta  AS INT)) AS fta,
               SUM(CAST(b.fgm  AS INT)) AS fgm,
               SUM(CAST(b.fg3m AS INT)) AS fg3m,
               SUM(CAST(b.ftm  AS INT)) AS ftm,
               MAX(pa.potential_ast)    AS potential_ast,
               MAX(rc.reb_chances)      AS reb_chances
          FROM nba.player_box_score_stats b
          JOIN nba.schedule s ON s.game_id = b.game_id
          LEFT JOIN nba.player_passing_stats pa
                 ON pa.player_id = b.player_id AND pa.game_date = b.game_date
          LEFT JOIN nba.player_rebound_chances rc
                 ON rc.player_id = b.player_id AND rc.game_date = b.game_date
         WHERE b.player_id IN ({pid_list})
           AND b.game_date >= :ss
           AND b.game_date <  :aod
         GROUP BY b.player_id, b.game_id, b.game_date, b.team_id, s.home_team_id, s.away_team_id
    """)
    df = pd.read_sql(sql, engine, params={"ss": SEASON_START, "aod": str(as_of_date)})
    if df.empty:
        log.info("  Opportunity history: 0 rows.")
        return df
    df["game_date"] = pd.to_datetime(df["game_date"])
    df = df.sort_values(["player_id", "game_date"]).reset_index(drop=True)

    def _pct(made, att):
        a = att.replace(0, np.nan)
        return (made / a).astype(float)

    df["pct2_game"]  = _pct(df["fgm"] - df["fg3m"], df["fga"] - df["fg3a"])
    df["pct3_game"]  = _pct(df["fg3m"], df["fg3a"])
    df["pctft_game"] = _pct(df["ftm"], df["fta"])

    grouped = df.groupby("player_id")
    for src_col, out in [("pct2_game", "r2"), ("pct3_game", "r3"), ("pctft_game", "rft")]:
        long_mean  = grouped[src_col].transform(lambda s: s.shift(1).expanding(min_periods=1).mean())
        short_mean = grouped[src_col].transform(lambda s: s.shift(1).rolling(window=10, min_periods=3).mean())
        df[out] = short_mean.where(short_mean.notna(), long_mean)

    pa2 = (df["fga"].fillna(0) - df["fg3a"].fillna(0)).clip(lower=0)
    df["opp_pts"] = (
        pa2 * df["r2"].fillna(0.48) * 2.0
        + df["fg3a"].fillna(0) * df["r3"].fillna(0.36) * 3.0
        + df["fta"].fillna(0)  * df["rft"].fillna(0.77) * 1.0
    )
    df["opp_reb"] = df["reb_chances"].astype(float)
    df["opp_ast"] = df["potential_ast"].astype(float)
    df["opp_fg3m_expected"] = df["fg3a"].fillna(0) * df["r3"].fillna(0.36)
    df["opp_fg3m_volume"]   = df["fg3a"].astype(float)

    log.info(
        f"  Opportunity history: {len(df)} rows for {df['player_id'].nunique()} players; "
        f"reb_chances coverage={df['reb_chances'].notna().sum()}, "
        f"potential_ast coverage={df['potential_ast'].notna().sum()}."
    )
    return df


def fetch_opp_info(engine, player_ids, grade_date_str):
    if not player_ids:
        return {}
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    sql = text(
        f"SELECT p.player_id,p.position,"
        f" CASE WHEN p.team_id=s.home_team_id THEN s.away_team_id ELSE s.home_team_id END AS opp_team_id"
        f" FROM nba.players p"
        f" JOIN nba.schedule s ON (s.home_team_id=p.team_id OR s.away_team_id=p.team_id)"
        f" AND CAST(s.game_date AS DATE)=:gd"
        f" WHERE p.player_id IN ({pid_list})"
    )
    df = pd.read_sql(sql, engine, params={"gd": grade_date_str})
    return {
        int(row["player_id"]): {
            "position":    row["position"] or "",
            "opp_team_id": int(row["opp_team_id"]) if pd.notna(row["opp_team_id"]) else None,
        }
        for _, row in df.iterrows()
    }


def fetch_matchup_defense(engine, opp_player_pairs):
    unique = list(set((tid, pg) for tid, pg in opp_player_pairs if tid is not None and pg is not None))
    if not unique:
        return {}
    values_rows = ", ".join(f"({tid}, '{pg}')" for tid, pg in unique)
    sql = text(f"""
WITH ss AS (
    SELECT CAST(CAST(
        CASE WHEN MONTH(GETUTCDATE())<10 THEN YEAR(GETUTCDATE())-1 ELSE YEAR(GETUTCDATE()) END
    AS VARCHAR(4))+'-10-01' AS DATE) AS dt
),
gt AS (
    SELECT pbs.player_id,pbs.game_id,pbs.game_date,
           CASE WHEN pbs.team_id=s.home_team_id THEN s.away_team_id ELSE s.home_team_id END AS opp_team_id,
           SUM(pbs.pts) AS pts,SUM(pbs.reb) AS reb,SUM(pbs.ast) AS ast,
           SUM(pbs.stl) AS stl,SUM(pbs.blk) AS blk,SUM(pbs.fg3m) AS fg3m,SUM(pbs.tov) AS tov,
           SUM(CAST(pbs.fga  AS INT)) AS fga,
           SUM(CAST(pbs.fg3a AS INT)) AS fg3a,
           SUM(CAST(pbs.fta  AS INT)) AS fta
    FROM nba.player_box_score_stats pbs
    JOIN nba.schedule s ON s.game_id=pbs.game_id
    WHERE s.game_date>=(SELECT dt FROM ss)
    GROUP BY pbs.player_id,pbs.game_id,pbs.game_date,pbs.team_id,s.home_team_id,s.away_team_id
),
pf AS (
    SELECT gt.*,LEFT(p.position,1) AS pos_group FROM gt
    JOIN nba.players p ON p.player_id=gt.player_id
    WHERE LEFT(p.position,1) IN ('G','F','C')
),
pf2 AS (
    SELECT pf.*, pa.potential_ast, rc.reb_chances
      FROM pf
      LEFT JOIN nba.player_passing_stats   pa ON pa.player_id=pf.player_id AND pa.game_date=pf.game_date
      LEFT JOIN nba.player_rebound_chances rc ON rc.player_id=pf.player_id AND rc.game_date=pf.game_date
),
tp AS (SELECT opp_team_id,pos_group FROM (VALUES {values_rows}) AS t(opp_team_id,pos_group)),
td AS (
    SELECT pf2.opp_team_id,pf2.pos_group,COUNT(*) AS games_defended,
           AVG(CAST(pf2.pts AS FLOAT)) AS avg_pts,AVG(CAST(pf2.reb AS FLOAT)) AS avg_reb,
           AVG(CAST(pf2.ast AS FLOAT)) AS avg_ast,AVG(CAST(pf2.stl AS FLOAT)) AS avg_stl,
           AVG(CAST(pf2.blk AS FLOAT)) AS avg_blk,AVG(CAST(pf2.fg3m AS FLOAT)) AS avg_fg3m,
           AVG(CAST(pf2.tov AS FLOAT)) AS avg_tov,
           AVG(CAST(
               ((pf2.fga - pf2.fg3a) * 0.48 * 2.0)
             + (pf2.fg3a * 0.36 * 3.0)
             + (pf2.fta * 0.77)
           AS FLOAT)) AS avg_opp_pts,
           AVG(CAST(pf2.fg3a AS FLOAT))            AS avg_opp_fg3a,
           AVG(CAST(pf2.fg3a * 0.36 AS FLOAT))     AS avg_opp_fg3m,
           AVG(CAST(pf2.reb_chances  AS FLOAT))    AS avg_reb_chances,
           AVG(CAST(pf2.potential_ast AS FLOAT))   AS avg_potential_ast
    FROM pf2 JOIN tp ON tp.opp_team_id=pf2.opp_team_id AND tp.pos_group=pf2.pos_group
    GROUP BY pf2.opp_team_id,pf2.pos_group
)
SELECT pos_group,opp_team_id,games_defended,
       avg_pts,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_pts  DESC) AS rank_pts,
       avg_reb,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_reb  DESC) AS rank_reb,
       avg_ast,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_ast  DESC) AS rank_ast,
       avg_stl,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_stl  DESC) AS rank_stl,
       avg_blk,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_blk  DESC) AS rank_blk,
       avg_fg3m, RANK() OVER (PARTITION BY pos_group ORDER BY avg_fg3m DESC) AS rank_fg3m,
       avg_tov,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_tov  DESC) AS rank_tov,
       avg_opp_pts,
       RANK() OVER (PARTITION BY pos_group ORDER BY avg_opp_pts      DESC) AS rank_opp_pts,
       avg_opp_fg3a,
       RANK() OVER (PARTITION BY pos_group ORDER BY avg_opp_fg3a     DESC) AS rank_opp_fg3a,
       avg_opp_fg3m,
       RANK() OVER (PARTITION BY pos_group ORDER BY avg_opp_fg3m     DESC) AS rank_opp_fg3m,
       avg_reb_chances,
       RANK() OVER (PARTITION BY pos_group ORDER BY avg_reb_chances  DESC) AS rank_reb_chances,
       avg_potential_ast,
       RANK() OVER (PARTITION BY pos_group ORDER BY avg_potential_ast DESC) AS rank_potential_ast
FROM td
""")
    df = pd.read_sql(sql, engine)
    result = {(int(row["opp_team_id"]), str(row["pos_group"])): row.to_dict() for _, row in df.iterrows()}
    log.info(f"  Matchup defense: {len(result)} team-position pairs.")
    return result


def fetch_player_patterns(engine, player_ids: list) -> dict:
    if not player_ids:
        return {}
    pid_list = ", ".join(str(int(p)) for p in player_ids)
    df = pd.read_sql(text(f"""
        SELECT player_id, market_key, line_value,
               hr_overall, p_hit_after_hit, p_hit_after_miss,
               hit_momentum, miss_momentum, pattern_strength, n
        FROM common.player_line_patterns
        WHERE player_id IN ({pid_list})
    """), engine)
    result = {}
    for _, row in df.iterrows():
        key = (int(row["player_id"]), row["market_key"], float(row["line_value"]))
        result[key] = {
            "hr_overall":        row["hr_overall"],
            "p_hit_after_hit":   row["p_hit_after_hit"]  if pd.notna(row["p_hit_after_hit"])  else None,
            "p_hit_after_miss":  row["p_hit_after_miss"] if pd.notna(row["p_hit_after_miss"]) else None,
            "hit_momentum":      row["hit_momentum"]     if pd.notna(row["hit_momentum"])      else None,
            "miss_momentum":     row["miss_momentum"]    if pd.notna(row["miss_momentum"])     else None,
            "pattern_strength":  row["pattern_strength"] if pd.notna(row["pattern_strength"])  else None,
            "n":                 int(row["n"]),
        }
    log.info(f"  Player patterns: {len(result)} player-line combos loaded.")
    return result


def fetch_under_prices(engine, table="odds.upcoming_player_props", date_filter="", params=None):
    std_mkt_list = ", ".join(f"'{m}'" for m in STANDARD_MARKETS)
    sql = text(
        f"SELECT pm.player_id,pp.market_key,pp.outcome_point AS line_value,pp.outcome_price AS under_price"
        f" FROM {table} pp"
        f" JOIN odds.event_game_map egm ON egm.event_id=pp.event_id AND egm.sport_key='basketball_nba' AND egm.game_id IS NOT NULL"
        f" JOIN odds.player_map pm ON pm.odds_player_name=pp.player_name AND pm.sport_key=pp.sport_key AND pm.player_id IS NOT NULL"
        f" WHERE pp.sport_key='basketball_nba' AND pp.bookmaker_key=:bk AND pp.outcome_name='Under'"
        f" AND pp.outcome_point IS NOT NULL AND pp.market_key IN ({std_mkt_list}) {date_filter}"
    )
    df = pd.read_sql(sql, engine, params={**(params or {}), "bk": BOOKMAKER})
    result = {}
    for _, row in df.iterrows():
        if pd.notna(row["player_id"]) and pd.notna(row["under_price"]):
            result[(int(row["player_id"]), row["market_key"], float(row["line_value"]))] = int(row["under_price"])
    log.info(f"  Under prices: {len(result)} lines.")
    return result


MARKET_LIST_SQL = ", ".join(f"'{m}'" for m in PLAYER_MARKETS)
BASE_PROPS_SELECT = (
    "SELECT DISTINCT pp.event_id,pm.player_id,pp.player_name,pp.market_key,"
    "pp.bookmaker_key,pp.outcome_point AS line_value,egm.game_id,pp.outcome_price AS over_price"
    " FROM {props_table} pp"
    " JOIN odds.event_game_map egm ON egm.event_id=pp.event_id AND egm.sport_key='basketball_nba' AND egm.game_id IS NOT NULL"
    " JOIN odds.player_map pm ON pm.odds_player_name=pp.player_name AND pm.sport_key=pp.sport_key AND pm.player_id IS NOT NULL"
    " WHERE pp.sport_key='basketball_nba' AND pp.bookmaker_key=:bk AND pp.outcome_name='Over'"
    " AND pp.outcome_point IS NOT NULL AND pp.market_key IN ({mkt_list}) {date_filter}"
)


def fetch_posted_props(engine, table="odds.upcoming_player_props", date_filter="", params=None):
    sql = text(BASE_PROPS_SELECT.format(props_table=table, mkt_list=MARKET_LIST_SQL, date_filter=date_filter))
    return pd.read_sql(sql, engine, params={**(params or {}), "bk": BOOKMAKER})


def build_standard_props(posted_df, under_prices=None):
    std = posted_df[posted_df["market_key"].isin(STANDARD_MARKETS)].copy()
    if std.empty:
        return pd.DataFrame()
    std = std.drop_duplicates(subset=["player_id", "market_key"])
    rows = []
    for _, r in std.iterrows():
        rows.append({
            "event_id":      r["event_id"],
            "player_id":     r["player_id"],
            "player_name":   r["player_name"],
            "market_key":    r["market_key"],
            "bookmaker_key": r["bookmaker_key"],
            "line_value":    float(r["line_value"]),
            "game_id":       r["game_id"],
            "over_price":    int(r["over_price"]) if pd.notna(r.get("over_price")) else None,
            "outcome_name":  "Over",
        })
    return pd.DataFrame(rows).drop_duplicates(subset=["player_id", "market_key", "line_value"])


def build_under_props(posted_df, under_prices):
    if not under_prices:
        return pd.DataFrame()
    std = posted_df[posted_df["market_key"].isin(STANDARD_MARKETS)].drop_duplicates(subset=["player_id", "market_key"]).copy()
    if std.empty:
        return pd.DataFrame()
    rows = []
    for _, r in std.iterrows():
        pid = int(r["player_id"]); mkt = r["market_key"]; lv = float(r["line_value"])
        price = under_prices.get((pid, mkt, lv))
        if price is None:
            continue
        rows.append({
            "event_id":      r["event_id"],
            "player_id":     pid,
            "player_name":   r["player_name"],
            "market_key":    mkt,
            "bookmaker_key": r["bookmaker_key"],
            "line_value":    lv,
            "game_id":       r["game_id"],
            "over_price":    price,
            "outcome_name":  "Under",
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def build_alt_props(posted_df, active_players_df, event_map):
    alt_posted = posted_df[posted_df["market_key"].isin(ALTERNATE_MARKETS)].copy()
    if alt_posted.empty:
        return pd.DataFrame()
    posted_set = set(zip(alt_posted["player_id"].astype(int), alt_posted["market_key"], alt_posted["line_value"].astype(float)))
    price_lookup = {
        (int(r["player_id"]), r["market_key"], float(r["line_value"])): int(r["over_price"]) if pd.notna(r.get("over_price")) else None
        for _, r in alt_posted.iterrows()
    }
    rows = []
    for _, p in active_players_df.iterrows():
        pid = int(p["player_id"]); pname = p["player_name"]; team_id = int(p["team_id"])
        ev_info = event_map.get(team_id)
        if ev_info is None:
            continue
        event_id, game_id = ev_info
        for mkt in ALTERNATE_MARKETS:
            stat_col = MARKET_STAT_COL.get(mkt)
            if stat_col is None:
                continue
            for lv in ALT_GRIDS.get(stat_col, []):
                if (pid, mkt, float(lv)) not in posted_set:
                    continue
                rows.append({
                    "event_id":      event_id,
                    "player_id":     pid,
                    "player_name":   pname,
                    "market_key":    mkt,
                    "bookmaker_key": BOOKMAKER,
                    "line_value":    float(lv),
                    "game_id":       game_id,
                    "over_price":    price_lookup.get((pid, mkt, float(lv))),
                    "outcome_name":  "Over",
                })
    return pd.DataFrame(rows).drop_duplicates(subset=["player_id", "market_key", "line_value"]) if rows else pd.DataFrame()


def drop_bracket_lines_covered_by_alts(std_df, alt_df):
    if std_df.empty or alt_df.empty:
        return std_df
    alt_df = alt_df.copy()
    alt_df["stat_col"] = alt_df["market_key"].map(MARKET_STAT_COL)
    alt_covered = set(zip(alt_df["player_id"].astype(int), alt_df["stat_col"], alt_df["line_value"].astype(float)))
    std_df = std_df.copy()
    std_df["stat_col"] = std_df["market_key"].map(MARKET_STAT_COL)
    mask = std_df.apply(lambda r: (int(r["player_id"]), r["stat_col"], float(r["line_value"])) in alt_covered, axis=1)
    dropped = mask.sum()
    if dropped:
        log.info(f"  Dropped {dropped} standard bracket lines superseded by alternate lines.")
    return std_df[~mask].drop(columns=["stat_col"])


def fetch_active_players_today(engine, grade_date_str):
    return pd.read_sql(text(
        "SELECT p.player_id,p.player_name,p.team_id FROM nba.players p"
        " JOIN nba.schedule s ON (s.home_team_id=p.team_id OR s.away_team_id=p.team_id)"
        " AND CAST(s.game_date AS DATE)=:gd WHERE p.roster_status=1"
    ), engine, params={"gd": grade_date_str})


def fetch_event_map_today(engine, grade_date_str):
    df = pd.read_sql(text(
        "SELECT egm.event_id,egm.game_id,s.home_team_id,s.away_team_id"
        " FROM nba.schedule s"
        " JOIN odds.event_game_map egm ON egm.game_id=s.game_id"
        " JOIN odds.upcoming_events ue ON ue.event_id=egm.event_id"
        " WHERE CAST(s.game_date AS DATE)=:gd AND egm.game_id IS NOT NULL"
    ), engine, params={"gd": grade_date_str})
    result = {}
    for _, row in df.iterrows():
        ev = (row["event_id"], row["game_id"])
        result[int(row["home_team_id"])] = ev
        result[int(row["away_team_id"])] = ev
    return result


def compute_all_hit_rates(props_df, history_df, opp_info, direction="over", opp_history_df=None):
    result = props_df.copy()
    all_grade_cols = (
        "hit_rate_60", "sample_size_60", "hit_rate_20", "sample_size_20",
        "weighted_hit_rate", "grade", "hit_rate_opp", "sample_size_opp",
    )
    if history_df.empty:
        for col in all_grade_cols:
            result[col] = None
        return result

    history = history_df.copy()
    history["stat_value"] = history["stat_value"].astype(float)
    result["line_value"] = result["line_value"].astype(float)

    lines = result[["player_id", "market_key", "line_value"]].drop_duplicates()
    merged = history.merge(lines, on=["player_id", "market_key"], how="inner")

    if direction == "under":
        merged["hit"] = (merged["stat_value"] < merged["line_value"]).astype(int)
    else:
        merged["hit"] = (merged["stat_value"] > merged["line_value"]).astype(int)

    g60 = merged.groupby(["player_id", "market_key", "line_value"]).agg(
        sample_size_60=("hit", "count"), hits_60=("hit", "sum")
    ).reset_index()
    g60["hit_rate_60"] = g60["hits_60"] / g60["sample_size_60"]

    g20 = merged[merged["in_short_window"] == 1].groupby(["player_id", "market_key", "line_value"]).agg(
        sample_size_20=("hit", "count"), hits_20=("hit", "sum")
    ).reset_index()
    g20["hit_rate_20"] = g20["hits_20"] / g20["sample_size_20"]

    result = result.merge(g60[["player_id", "market_key", "line_value", "hit_rate_60", "sample_size_60"]], on=["player_id", "market_key", "line_value"], how="left")
    result = result.merge(g20[["player_id", "market_key", "line_value", "hit_rate_20", "sample_size_20"]], on=["player_id", "market_key", "line_value"], how="left")
    result["sample_size_60"] = result["sample_size_60"].fillna(0).astype(int)
    result["sample_size_20"] = result["sample_size_20"].fillna(0).astype(int)

    use_blend = (result["sample_size_20"] >= MIN_SAMPLE) & result["hit_rate_20"].notna()
    result["weighted_hit_rate"] = result["hit_rate_60"]
    result.loc[use_blend, "weighted_hit_rate"] = (
        WEIGHT_SHORT * result.loc[use_blend, "hit_rate_20"]
        + WEIGHT_LONG  * result.loc[use_blend, "hit_rate_60"]
    )
    result["grade"] = result["weighted_hit_rate"].apply(lambda x: round(x * 100, 1) if pd.notna(x) else None)
    for col in ("weighted_hit_rate", "hit_rate_60", "hit_rate_20"):
        result[col] = result[col].apply(lambda x: round(x, 4) if pd.notna(x) else None)

    opp_src = opp_history_df if (opp_history_df is not None and not opp_history_df.empty) else history_df
    player_opp = {
        pid: int(info["opp_team_id"])
        for pid, info in opp_info.items()
        if info.get("opp_team_id") is not None
    }

    if player_opp and "opp_team_id" in opp_src.columns:
        opp_src_copy = opp_src.copy()
        opp_src_copy["stat_value"] = opp_src_copy["stat_value"].astype(float)
        merged_opp = opp_src_copy.merge(lines, on=["player_id", "market_key"], how="inner")
        if direction == "under":
            merged_opp["hit"] = (merged_opp["stat_value"] < merged_opp["line_value"]).astype(int)
        else:
            merged_opp["hit"] = (merged_opp["stat_value"] > merged_opp["line_value"]).astype(int)

        merged_opp["today_opp"] = merged_opp["player_id"].map(player_opp)
        opp_rows = merged_opp[
            merged_opp["today_opp"].notna()
            & (merged_opp["opp_team_id"] == merged_opp["today_opp"])
        ]
        if not opp_rows.empty:
            g_opp = opp_rows.groupby(["player_id", "market_key", "line_value"]).agg(
                sample_size_opp=("hit", "count"), hits_opp=("hit", "sum")
            ).reset_index()
            g_opp["hit_rate_opp"] = (g_opp["hits_opp"] / g_opp["sample_size_opp"]).apply(
                lambda x: round(x, 4) if pd.notna(x) else None
            )
            result = result.merge(
                g_opp[["player_id", "market_key", "line_value", "hit_rate_opp", "sample_size_opp"]],
                on=["player_id", "market_key", "line_value"],
                how="left",
            )
            result["sample_size_opp"] = result["sample_size_opp"].fillna(0).astype(int)
        else:
            result["hit_rate_opp"] = None
            result["sample_size_opp"] = 0
    else:
        result["hit_rate_opp"] = None
        result["sample_size_opp"] = 0

    if "sample_size_opp" in result.columns:
        result.loc[result["sample_size_opp"] == 0, "hit_rate_opp"]    = None
        result.loc[result["sample_size_opp"] == 0, "sample_size_opp"] = None

    return result


def _safe(v):
    if v is None:
        return None
    try:
        if math.isnan(v) or math.isinf(v):
            return None
        return round(float(v), 1)
    except Exception:
        return None


def _invert(v):
    return None if v is None else _safe(100.0 - float(v))


def precompute_player_market_grades(season_df, props_df):
    combos = props_df[["player_id", "market_key"]].drop_duplicates()
    result = {}
    player_groups = {pid: grp.sort_values("game_date") for pid, grp in season_df.groupby("player_id")}
    for _, row in combos.iterrows():
        pid = int(row["player_id"]); mkt = row["market_key"]
        stat_col = MARKET_STAT_COL.get(mkt); pdf = player_groups.get(pid)
        if stat_col is None or pdf is None or pdf.empty or stat_col not in pdf.columns:
            result[(pid, mkt)] = {"trend_grade": None, "regression_grade": None}
            continue
        vals = pdf[stat_col].dropna().values
        trend = None
        if len(vals) >= TREND_MIN:
            short = vals[-TREND_SHORT:] if len(vals) >= TREND_SHORT else vals
            long  = vals[-TREND_LONG:]  if len(vals) >= TREND_LONG  else vals
            if len(short) >= TREND_MIN:
                sm, lm = float(np.mean(short)), float(np.mean(long))
                if lm != 0:
                    trend = _safe(max(0.0, min(100.0, 50.0 + (sm - lm) / lm * 150.0)))
        regression = None
        if len(vals) >= SEASON_MIN:
            recent = vals[-RECENT_WINDOW:] if len(vals) >= RECENT_WINDOW else vals[-max(1, len(vals) // 2):]
            if len(recent) >= 3:
                s_std = float(np.std(vals))
                if s_std >= 0.01:
                    z = (float(np.mean(recent)) - float(np.mean(vals))) / s_std
                    regression = _safe(max(0.0, min(100.0, 50.0 - z * 25.0)))
        result[(pid, mkt)] = {"trend_grade": trend, "regression_grade": regression}
    return result


def _opp_components_for_market(mkt: str):
    return MARKET_OPP_COMPONENTS.get(mkt, ())


def _combine_opp_components(pdf, components):
    import numpy as np
    if not components:
        return pd.Series([], dtype=float)
    col_map = {"pts": "opp_pts", "reb": "opp_reb", "ast": "opp_ast", "fg3m": "opp_fg3m_expected"}
    cols = [col_map[c] for c in components if c in col_map]
    if not cols or not all(c in pdf.columns for c in cols):
        return pd.Series([np.nan] * len(pdf), index=pdf.index)
    return pdf[cols].sum(axis=1, min_count=1)


def precompute_opportunity_grades(opp_df, props_df, opp_info, matchup_cache):
    if opp_df is None or opp_df.empty:
        return {}

    result = {}
    combos = props_df[["player_id", "market_key"]].drop_duplicates()
    player_groups = {pid: grp.sort_values("game_date").reset_index(drop=True)
                     for pid, grp in opp_df.groupby("player_id")}

    for _, row in combos.iterrows():
        pid = int(row["player_id"])
        mkt = row["market_key"]
        key = (pid, mkt)

        components = _opp_components_for_market(mkt)
        pdf = player_groups.get(pid)
        if not components or pdf is None or pdf.empty:
            result[key] = {
                "opportunity_short_grade":    None,
                "opportunity_long_grade":     None,
                "opportunity_matchup_grade":  None,
                "opportunity_streak_grade":   None,
                "opportunity_volume_grade":   None,
                "opportunity_expected_grade": None,
            }
            continue

        series = _combine_opp_components(pdf, components).dropna()
        vals = series.values if len(series) else np.array([])

        short_grade = None
        long_grade  = None
        if len(vals) >= OPP_TREND_MIN:
            short = vals[-OPP_TREND_SHORT:] if len(vals) >= OPP_TREND_SHORT else vals
            long_slice = vals[-OPP_TREND_LONG:] if len(vals) >= OPP_TREND_LONG else vals
            season = vals
            sm = float(np.mean(short))
            lm = float(np.mean(long_slice))
            season_m = float(np.mean(season))
            if lm > 0:
                short_grade = _safe(max(0.0, min(100.0, 50.0 + (sm - lm) / lm * 150.0)))
            if season_m > 0:
                long_grade = _safe(max(0.0, min(100.0, 50.0 + (lm - season_m) / season_m * 150.0)))

        streak_grade = None
        if len(vals) >= OPP_STREAK_MIN:
            baseline = float(np.mean(vals))
            if baseline > 0:
                diffs = np.sign(vals - baseline)
                run = 0
                for d in reversed(diffs):
                    if d == 0:
                        break
                    if run == 0:
                        run = int(d)
                    elif (run > 0 and d > 0) or (run < 0 and d < 0):
                        run = run + int(d)
                    else:
                        break
                if abs(run) >= OPP_STREAK_MIN:
                    delta = max(-30.0, min(30.0, run * 6.0))
                    streak_grade = _safe(50.0 + delta)
                else:
                    streak_grade = 50.0

        matchup_grade = None
        info = opp_info.get(pid, {})
        pos = info.get("position", "") or ""
        opp_team_id = info.get("opp_team_id")
        pg = "G" if pos.startswith("G") else "F" if pos.startswith("F") else "C" if pos.startswith("C") else None
        if opp_team_id is not None and pg is not None:
            defense = matchup_cache.get((int(opp_team_id), pg))
            if defense is not None:
                ranks = []
                for comp in components:
                    rank_col = OPP_MATCHUP_RANK.get(comp)
                    if rank_col is None:
                        continue
                    rk = defense.get(rank_col)
                    if rk is not None and not (isinstance(rk, float) and math.isnan(rk)):
                        ranks.append(int(rk))
                if ranks:
                    avg_rank = float(np.mean(ranks))
                    matchup_grade = _safe(max(0.0, min(100.0, (30 - avg_rank + 1) / 30.0 * 100.0)))

        volume_grade = None
        expected_grade = None
        if mkt in ("player_threes", "player_threes_alternate"):
            vol_vals = pdf["opp_fg3m_volume"].dropna().values
            exp_vals = pdf["opp_fg3m_expected"].dropna().values
            if len(vol_vals) >= OPP_TREND_MIN:
                short = vol_vals[-OPP_TREND_SHORT:] if len(vol_vals) >= OPP_TREND_SHORT else vol_vals
                long_slice = vol_vals[-OPP_TREND_LONG:] if len(vol_vals) >= OPP_TREND_LONG else vol_vals
                sm = float(np.mean(short)); lm = float(np.mean(long_slice))
                if lm > 0:
                    volume_grade = _safe(max(0.0, min(100.0, 50.0 + (sm - lm) / lm * 150.0)))
            if len(exp_vals) >= OPP_TREND_MIN:
                short = exp_vals[-OPP_TREND_SHORT:] if len(exp_vals) >= OPP_TREND_SHORT else exp_vals
                long_slice = exp_vals[-OPP_TREND_LONG:] if len(exp_vals) >= OPP_TREND_LONG else exp_vals
                sm = float(np.mean(short)); lm = float(np.mean(long_slice))
                if lm > 0:
                    expected_grade = _safe(max(0.0, min(100.0, 50.0 + (sm - lm) / lm * 150.0)))

        result[key] = {
            "opportunity_short_grade":    short_grade,
            "opportunity_long_grade":     long_grade,
            "opportunity_matchup_grade":  matchup_grade,
            "opportunity_streak_grade":   streak_grade,
            "opportunity_volume_grade":   volume_grade,
            "opportunity_expected_grade": expected_grade,
        }

    return result


def precompute_line_grades(season_df, props_df, patterns: dict = None):
    combos = props_df[["player_id", "market_key", "line_value"]].drop_duplicates()
    result = {}
    player_groups = {pid: grp.sort_values("game_date")
                     for pid, grp in season_df.groupby("player_id")}
    patterns = patterns or {}

    for _, row in combos.iterrows():
        pid = int(row["player_id"])
        mkt = row["market_key"]
        lv  = float(row["line_value"])
        key = (pid, mkt, lv)

        stat_col = MARKET_STAT_COL.get(mkt)
        pdf      = player_groups.get(pid)

        if stat_col is None or pdf is None or pdf.empty or stat_col not in pdf.columns:
            result[key] = {"momentum_grade": None, "pattern_grade": None}
            continue

        vals = pdf[stat_col].dropna().values
        if len(vals) == 0:
            result[key] = {"momentum_grade": None, "pattern_grade": None}
            continue

        hits = [bool(v > lv) for v in vals]
        is_hit_streak = hits[-1]

        streak = 0
        for h in reversed(hits):
            if h == is_hit_streak:
                streak += 1
            else:
                break

        momentum = None
        pattern  = None

        pat = patterns.get(key)
        if pat is not None and pat["n"] >= 10:
            if is_hit_streak and pat["p_hit_after_hit"] is not None:
                momentum = _safe(pat["p_hit_after_hit"] * 100.0)
            elif not is_hit_streak and pat["p_hit_after_miss"] is not None:
                momentum = _safe(pat["p_hit_after_miss"] * 100.0)

            if pat["pattern_strength"] is not None:
                pattern = _safe(min(100.0, pat["pattern_strength"] * 300.0))
            if pattern is not None and momentum is not None:
                sample_bonus = min(20.0, (pat["n"] - 10) * (20.0 / 20.0))
                pattern = _safe(min(100.0, pattern + sample_bonus))

        if momentum is None and len(hits) >= 5:
            hr60 = float(np.mean(hits))
            if is_hit_streak:
                momentum = _safe(min(100.0, hr60 * 100.0 + streak * 2.0))
            else:
                momentum = _safe(hr60 * 100.0)

        result[key] = {"momentum_grade": momentum, "pattern_grade": pattern}
    return result


def compute_matchup_grade(market_key, opp_team_id, position, matchup_cache):
    if opp_team_id is None or not position: return None
    rank_col = MARKET_DEF_RANK.get(market_key)
    if rank_col is None: return None
    pg = "G" if position.startswith("G") else "F" if position.startswith("F") else "C" if position.startswith("C") else None
    if pg is None: return None
    defense = matchup_cache.get((int(opp_team_id), pg))
    if defense is None: return None
    rank = defense.get(rank_col)
    if rank is None or (isinstance(rank, float) and math.isnan(rank)): return None
    return _safe(max(0.0, min(100.0, (30 - int(rank) + 1) / 30.0 * 100.0)))


def compute_composite(momentum, hit_rate_60, pattern):
    """
    Reweighted composite grade (ADR-20260423-1).

    Weights: 40% momentum_grade + 40% (hit_rate_60 * 100) + 20% pattern_grade.
    Only non-NULL components contribute. If all three are NULL returns None.

    Removed from composite (stored as context only):
      trend_grade, matchup_grade, regression_grade,
      opportunity_short/long/matchup/streak grades.
    Calibration showed these add no predictive lift over the three-component
    formula (Brier score improvement 0.000012, effectively zero).
    """
    parts = []
    weights = []
    if momentum is not None:
        parts.append(float(momentum) * 0.40)
        weights.append(0.40)
    if hit_rate_60 is not None:
        parts.append(float(hit_rate_60) * 100.0 * 0.40)
        weights.append(0.40)
    if pattern is not None:
        parts.append(float(pattern) * 0.20)
        weights.append(0.20)
    if not parts:
        return None
    # Renormalize so partial availability still produces a meaningful 0-100 value
    total_weight = sum(weights)
    return _safe(sum(parts) / total_weight)


def upsert_grades(engine, rows):
    if not rows: return 0
    seen = {}
    for r in rows:
        k = (
            r["grade_date"], r["event_id"], r["player_id"],
            r["market_key"], r["bookmaker_key"], r["line_value"],
            r.get("outcome_name", "Over"),
        )
        seen[k] = r
    rows = list(seen.values())
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('tempdb..#stage_grades') IS NOT NULL DROP TABLE #stage_grades"))
        conn.execute(text("""
CREATE TABLE #stage_grades(
    grade_date DATE,event_id VARCHAR(50),game_id VARCHAR(15),player_id BIGINT,player_name NVARCHAR(100),
    market_key VARCHAR(100),bookmaker_key VARCHAR(50),line_value DECIMAL(6,1),outcome_name VARCHAR(5),over_price INT,
    hit_rate_60 FLOAT,hit_rate_20 FLOAT,sample_size_60 INT,sample_size_20 INT,weighted_hit_rate FLOAT,grade FLOAT,
    trend_grade FLOAT,momentum_grade FLOAT,pattern_grade FLOAT,matchup_grade FLOAT,regression_grade FLOAT,
    composite_grade FLOAT,hit_rate_opp FLOAT,sample_size_opp INT,
    opportunity_short_grade FLOAT,opportunity_long_grade FLOAT,
    opportunity_matchup_grade FLOAT,opportunity_streak_grade FLOAT,
    opportunity_volume_grade FLOAT,opportunity_expected_grade FLOAT
)"""))
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            conn.exec_driver_sql(
                "INSERT INTO #stage_grades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(r["grade_date"], r["event_id"], r["game_id"], r["player_id"], r["player_name"],
                  r["market_key"], r["bookmaker_key"], r["line_value"], r.get("outcome_name", "Over"),
                  r["over_price"], r["hit_rate_60"], r["hit_rate_20"], r["sample_size_60"],
                  r["sample_size_20"], r["weighted_hit_rate"], r["grade"], r["trend_grade"],
                  r["momentum_grade"], r["pattern_grade"], r["matchup_grade"], r["regression_grade"],
                  r["composite_grade"], r["hit_rate_opp"], r["sample_size_opp"],
                  r.get("opportunity_short_grade"), r.get("opportunity_long_grade"),
                  r.get("opportunity_matchup_grade"), r.get("opportunity_streak_grade"),
                  r.get("opportunity_volume_grade"), r.get("opportunity_expected_grade"))
                 for r in chunk]
            )
        conn.execute(text("""
MERGE common.daily_grades AS t USING #stage_grades AS s
ON(t.grade_date=s.grade_date AND t.event_id=s.event_id AND t.player_id=s.player_id
   AND t.market_key=s.market_key AND t.bookmaker_key=s.bookmaker_key
   AND t.line_value=s.line_value AND t.outcome_name=s.outcome_name)
WHEN MATCHED THEN UPDATE SET
    t.game_id=s.game_id,t.over_price=COALESCE(s.over_price,t.over_price),
    t.hit_rate_60=s.hit_rate_60,t.hit_rate_20=s.hit_rate_20,
    t.sample_size_60=s.sample_size_60,t.sample_size_20=s.sample_size_20,
    t.weighted_hit_rate=s.weighted_hit_rate,t.grade=s.grade,
    t.trend_grade=s.trend_grade,t.momentum_grade=s.momentum_grade,
    t.pattern_grade=s.pattern_grade,t.matchup_grade=s.matchup_grade,
    t.regression_grade=s.regression_grade,t.composite_grade=s.composite_grade,
    t.hit_rate_opp=s.hit_rate_opp,t.sample_size_opp=s.sample_size_opp,
    t.opportunity_short_grade=s.opportunity_short_grade,
    t.opportunity_long_grade=s.opportunity_long_grade,
    t.opportunity_matchup_grade=s.opportunity_matchup_grade,
    t.opportunity_streak_grade=s.opportunity_streak_grade,
    t.opportunity_volume_grade=s.opportunity_volume_grade,
    t.opportunity_expected_grade=s.opportunity_expected_grade
WHEN NOT MATCHED THEN INSERT(
    grade_date,event_id,game_id,player_id,player_name,market_key,bookmaker_key,
    line_value,outcome_name,over_price,hit_rate_60,hit_rate_20,sample_size_60,
    sample_size_20,weighted_hit_rate,grade,trend_grade,momentum_grade,pattern_grade,
    matchup_grade,regression_grade,composite_grade,hit_rate_opp,sample_size_opp,
    opportunity_short_grade,opportunity_long_grade,opportunity_matchup_grade,
    opportunity_streak_grade,opportunity_volume_grade,opportunity_expected_grade
) VALUES(
    s.grade_date,s.event_id,s.game_id,s.player_id,s.player_name,s.market_key,
    s.bookmaker_key,s.line_value,s.outcome_name,s.over_price,s.hit_rate_60,
    s.hit_rate_20,s.sample_size_60,s.sample_size_20,s.weighted_hit_rate,s.grade,
    s.trend_grade,s.momentum_grade,s.pattern_grade,s.matchup_grade,s.regression_grade,
    s.composite_grade,s.hit_rate_opp,s.sample_size_opp,
    s.opportunity_short_grade,s.opportunity_long_grade,s.opportunity_matchup_grade,
    s.opportunity_streak_grade,s.opportunity_volume_grade,s.opportunity_expected_grade
);"""))

        if conn.execute(text(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive'"
        )).scalar():
            conn.execute(text(f"""
WITH affected AS (
    SELECT DISTINCT grade_date, event_id, player_id, market_key,
                    bookmaker_key, line_value, outcome_name
      FROM #stage_grades
),
ranked AS (
    SELECT dg.grade_id,
           ROW_NUMBER() OVER (
             PARTITION BY dg.grade_date, dg.event_id, dg.player_id,
                          dg.market_key, dg.bookmaker_key,
                          dg.line_value, dg.outcome_name
             ORDER BY dg.grade_id DESC
           ) AS rn
      FROM common.daily_grades dg
      JOIN affected a
        ON a.grade_date=dg.grade_date AND a.event_id=dg.event_id
       AND a.player_id=dg.player_id  AND a.market_key=dg.market_key
       AND a.bookmaker_key=dg.bookmaker_key AND a.line_value=dg.line_value
       AND a.outcome_name=dg.outcome_name
)
INSERT INTO common.daily_grades_archive ({_ARCHIVE_COLS}, archived_at)
SELECT {_ARCHIVE_COLS_DG}, SYSUTCDATETIME()
  FROM common.daily_grades dg
  JOIN ranked r ON r.grade_id = dg.grade_id
 WHERE r.rn > 1;
"""))
            conn.execute(text("""
WITH ranked AS (
    SELECT dg.grade_id,
           ROW_NUMBER() OVER (
             PARTITION BY dg.grade_date, dg.event_id, dg.player_id,
                          dg.market_key, dg.bookmaker_key,
                          dg.line_value, dg.outcome_name
             ORDER BY dg.grade_id DESC
           ) AS rn
      FROM common.daily_grades dg
      JOIN (SELECT DISTINCT grade_date, event_id, player_id, market_key,
                            bookmaker_key, line_value, outcome_name
              FROM #stage_grades) a
        ON a.grade_date=dg.grade_date AND a.event_id=dg.event_id
       AND a.player_id=dg.player_id  AND a.market_key=dg.market_key
       AND a.bookmaker_key=dg.bookmaker_key AND a.line_value=dg.line_value
       AND a.outcome_name=dg.outcome_name
)
DELETE dg
  FROM common.daily_grades dg
  JOIN ranked r ON r.grade_id = dg.grade_id
 WHERE r.rn > 1;
"""))
    return len(rows)


def grade_props_for_date(
    engine, grade_date_str, props_df, history_df, season_df, opp_info,
    matchup_cache, direction="over", opp_history_df=None, patterns=None,
    opp_df=None, game_spreads=None, blowout_profiles=None,
    props_price_lookup=None,
):
    """
    Grade props for a single date.

    game_spreads:      dict {game_id: spread} for blowout dampening.
    blowout_profiles:  dict {player_id: pts_delta} for blowout dampening.
    props_price_lookup: dict {(player_id, market_key, line_value): price}
                        for all posted lines. Used by tier line computation.
    """
    if props_df.empty:
        return [], []
    props_df = props_df.drop_duplicates(subset=["player_id", "market_key", "line_value"]).copy()
    graded_df   = compute_all_hit_rates(props_df, history_df, opp_info, direction=direction, opp_history_df=opp_history_df)
    pm_grades   = precompute_player_market_grades(season_df, graded_df)
    line_grades = precompute_line_grades(season_df, graded_df, patterns=patterns)
    opp_grades  = precompute_opportunity_grades(opp_df, graded_df, opp_info, matchup_cache) if opp_df is not None else {}

    game_spreads      = game_spreads or {}
    blowout_profiles  = blowout_profiles or {}
    props_price_lookup = props_price_lookup or {}

    is_under = (direction == "under")

    # Build per-(player_id, market_key) lists of all available lines and prices
    # for tier computation. Only Over rows; tier lines always expressed as Over.
    # Build price lookup per (player_id, market_key) for tier computation
    player_market_lines: dict = {}
    for _, r in graded_df.iterrows():
        if r.get("outcome_name", "Over") == "Under":
            continue
        pid_int = int(r["player_id"]) if pd.notna(r["player_id"]) else None
        if pid_int is None:
            continue
        mkt = r["market_key"]
        lv  = float(r["line_value"])
        price = r.get("over_price")
        key = (pid_int, mkt)
        if key not in player_market_lines:
            player_market_lines[key] = {}
        player_market_lines[key][lv] = int(price) if pd.notna(price) and price is not None else None

    # Season stat arrays per player for KDE
    player_stat_arrays: dict = {}
    if not season_df.empty:
        for pid, grp in season_df.groupby("player_id"):
            player_stat_arrays[int(pid)] = grp.sort_values("game_date")

    grade_rows = []
    tier_rows  = []
    seen_tier_keys = set()

    for _, r in graded_df.iterrows():
        pid = r["player_id"]
        if pd.isna(pid):
            continue
        pid_int = int(pid)
        mkt = r["market_key"]
        lv  = float(r["line_value"])
        info     = opp_info.get(pid_int, {})
        position = info.get("position", "")
        opp_id   = info.get("opp_team_id")
        game_id  = r.get("game_id")

        whr = r.get("weighted_hit_rate")
        whr = whr if pd.notna(whr) else None

        pm = pm_grades.get((pid_int, mkt), {})
        lk = line_grades.get((pid_int, mkt, lv), {})
        og = opp_grades.get((pid_int, mkt), {})

        t_r  = pm.get("trend_grade")
        rg_r = pm.get("regression_grade")
        mo_r = lk.get("momentum_grade")
        pa_r = lk.get("pattern_grade")
        ma_r = compute_matchup_grade(mkt, opp_id, position, matchup_cache)

        os_r = og.get("opportunity_short_grade")
        ol_r = og.get("opportunity_long_grade")
        om_r = og.get("opportunity_matchup_grade")
        ok_r = og.get("opportunity_streak_grade")
        ov_r = og.get("opportunity_volume_grade")
        oe_r = og.get("opportunity_expected_grade")

        if is_under:
            trend      = _invert(t_r);  regression = _invert(rg_r)
            momentum   = _invert(mo_r); pattern    = _invert(pa_r)
            matchup    = _invert(ma_r)
            opp_short    = _invert(os_r)
            opp_long     = _invert(ol_r)
            opp_matchup  = _invert(om_r)
            opp_streak   = _invert(ok_r)
            opp_volume   = _invert(ov_r)
            opp_expected = _invert(oe_r)
            hr60_for_composite = (1.0 - float(whr)) if whr is not None else None
        else:
            trend = t_r; regression = rg_r; momentum = mo_r; pattern = pa_r; matchup = ma_r
            opp_short    = os_r
            opp_long     = ol_r
            opp_matchup  = om_r
            opp_streak   = ok_r
            opp_volume   = ov_r
            opp_expected = oe_r
            hr60_for_composite = whr

        # New composite: 40% momentum + 40% hr60*100 + 20% pattern
        composite = compute_composite(momentum, hr60_for_composite, pattern)

        hr_opp = r.get("hit_rate_opp"); hr_opp = hr_opp if pd.notna(hr_opp) else None
        n_opp  = r.get("sample_size_opp"); n_opp = int(n_opp) if pd.notna(n_opp) and n_opp else None
        raw_price = r.get("over_price"); price = int(raw_price) if pd.notna(raw_price) and raw_price is not None else None

        grade_rows.append({
            "grade_date":        grade_date_str,
            "event_id":          r["event_id"],
            "game_id":           game_id,
            "player_id":         pid_int,
            "player_name":       r["player_name"],
            "market_key":        mkt,
            "bookmaker_key":     r["bookmaker_key"],
            "line_value":        lv,
            "outcome_name":      r.get("outcome_name", "Over" if not is_under else "Under"),
            "over_price":        price,
            "hit_rate_60":       r.get("hit_rate_60") if pd.notna(r.get("hit_rate_60")) else None,
            "hit_rate_20":       r.get("hit_rate_20") if pd.notna(r.get("hit_rate_20")) else None,
            "sample_size_60":    int(r["sample_size_60"]) if pd.notna(r.get("sample_size_60")) else 0,
            "sample_size_20":    int(r["sample_size_20"]) if pd.notna(r.get("sample_size_20")) else 0,
            "weighted_hit_rate": whr,
            "grade":             r.get("grade") if pd.notna(r.get("grade")) else None,
            "trend_grade":       trend,
            "momentum_grade":    momentum,
            "pattern_grade":     pattern,
            "matchup_grade":     matchup,
            "regression_grade":  regression,
            "composite_grade":   composite,
            "hit_rate_opp":      hr_opp,
            "sample_size_opp":   n_opp,
            "opportunity_short_grade":    opp_short,
            "opportunity_long_grade":     opp_long,
            "opportunity_matchup_grade":  opp_matchup,
            "opportunity_streak_grade":   opp_streak,
            "opportunity_volume_grade":   opp_volume,
            "opportunity_expected_grade": opp_expected,
        })

        # Tier lines: compute once per (player, market, game), Over only
        if not is_under and game_id and (pid_int, mkt, game_id) not in seen_tier_keys:
            seen_tier_keys.add((pid_int, mkt, game_id))

            stat_col = MARKET_STAT_COL.get(mkt)
            stat_grp = player_stat_arrays.get(pid_int)
            stat_values = None
            if stat_col and stat_grp is not None and stat_col in stat_grp.columns:
                stat_values = stat_grp[stat_col].dropna().values

            if stat_values is not None and len(stat_values) >= 3:
                # Blowout dampening for pts/combo markets
                blowout_delta = 0.0
                if mkt in BLOWOUT_DAMPEN_MARKETS and game_id:
                    spread = game_spreads.get(game_id)
                    if spread is not None and spread >= BLOWOUT_SPREAD_THRESHOLD:
                        # Is this player on the projected losing team?
                        # Losing team = away team when spread shows home team favored,
                        # or home team when away team favored. Use opp_team_id as proxy:
                        # if the spread favorite is the opponent, this player is on the dog.
                        # Simplified: apply dampening whenever spread >= threshold
                        # and player has a documented blowout loss profile.
                        delta = blowout_profiles.get(pid_int)
                        if delta is not None and delta < 0:
                            blowout_delta = abs(delta) * 0.5  # apply 50% of historical delta

                price_lookup_for_player = player_market_lines.get((pid_int, mkt), {})

                tier = compute_kde_tier_lines(
                    stat_values=stat_values,
                    composite_grade=composite,
                    available_lines=list(price_lookup_for_player.keys()),
                    price_lookup=price_lookup_for_player,
                    blowout_delta=blowout_delta,
                )

                tier_rows.append({
                    "grade_date":       grade_date_str,
                    "game_id":          game_id,
                    "player_id":        pid_int,
                    "player_name":      r["player_name"],
                    "market_key":       mkt,
                    "composite_grade":  composite,
                    "kde_window":       tier["kde_window"],
                    "blowout_dampened": tier["blowout_dampened"],
                    "safe_line":        tier["safe_line"],
                    "safe_prob":        tier["safe_prob"],
                    "value_line":       tier["value_line"],
                    "value_prob":       tier["value_prob"],
                    "highrisk_line":    tier["highrisk_line"],
                    "highrisk_prob":    tier["highrisk_prob"],
                    "highrisk_price":   tier["highrisk_price"],
                    "lotto_line":       tier["lotto_line"],
                    "lotto_prob":       tier["lotto_prob"],
                    "lotto_price":      tier["lotto_price"],
                })

    return grade_rows, tier_rows


def _common_grade_data(engine, all_over, under_props, today):
    all_player_ids = list(set(
        all_over["player_id"].dropna().tolist()
        + (under_props["player_id"].dropna().tolist() if not under_props.empty else [])
    ))
    all_market_keys = list(set(
        all_over["market_key"].dropna().tolist()
        + (under_props["market_key"].dropna().tolist() if not under_props.empty else [])
    ))
    history_df     = fetch_history(engine, all_player_ids, all_market_keys, today)
    opp_history_df = fetch_history(engine, all_player_ids, all_market_keys, today, lookback=LOOKBACK_OPP)
    season_df      = fetch_season_history(engine, all_player_ids, today)
    opp_info       = fetch_opp_info(engine, all_player_ids, today)
    matchup_pairs  = []
    for pid, info in opp_info.items():
        pos = info.get("position", "")
        pg = "G" if pos.startswith("G") else "F" if pos.startswith("F") else "C" if pos.startswith("C") else None
        if pg and info.get("opp_team_id"):
            matchup_pairs.append((int(info["opp_team_id"]), pg))
    matchup_cache = fetch_matchup_defense(engine, matchup_pairs)
    player_ids = list(all_over["player_id"].dropna().astype(int).unique())
    patterns   = fetch_player_patterns(engine, player_ids)
    opp_df     = fetch_opportunity_history(engine, all_player_ids, today)
    return history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns, opp_df


def run_upcoming(engine):
    today = today_et()
    log.info(f"Upcoming mode: {today}")
    posted    = fetch_posted_props(engine)
    active    = fetch_active_players_today(engine, today)
    event_map = fetch_event_map_today(engine, today)
    std_props   = build_standard_props(posted)
    alt_props   = build_alt_props(posted, active, event_map)
    std_trimmed = drop_bracket_lines_covered_by_alts(std_props, alt_props)
    all_over = pd.concat([p for p in [std_trimmed, alt_props] if not p.empty], ignore_index=True)
    if all_over.empty:
        log.info("No props to grade."); return
    all_over = all_over.drop_duplicates(subset=["player_id", "market_key", "line_value"])
    log.info(f"  {len(all_over)} over prop rows ({len(std_trimmed)} standard, {len(alt_props)} alternate).")
    under_prices = fetch_under_prices(engine)
    under_props  = build_under_props(posted, under_prices)
    log.info(f"  {len(under_props)} under prop rows.")

    history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns, opp_df = \
        _common_grade_data(engine, all_over, under_props, today)

    game_ids = all_over["game_id"].dropna().unique().tolist()
    game_spreads     = fetch_upcoming_game_spreads(engine, game_ids)
    player_ids       = list(all_over["player_id"].dropna().astype(int).unique())
    blowout_profiles = fetch_player_blowout_profiles(engine, player_ids)

    over_rows, over_tiers   = grade_props_for_date(
        engine, today, all_over, history_df, season_df, opp_info, matchup_cache,
        direction="over", opp_history_df=opp_history_df, patterns=patterns, opp_df=opp_df,
        game_spreads=game_spreads, blowout_profiles=blowout_profiles,
    )
    under_rows, under_tiers = grade_props_for_date(
        engine, today, under_props, history_df, season_df, opp_info, matchup_cache,
        direction="under", opp_history_df=opp_history_df, patterns=patterns, opp_df=opp_df,
    ) if not under_props.empty else ([], [])

    written = upsert_grades(engine, over_rows + under_rows)
    tier_written = upsert_tier_lines(engine, over_tiers)
    log.info(f"  {written} grade rows written ({len(over_rows)} over, {len(under_rows)} under).")
    log.info(f"  {tier_written} tier line rows written.")


def run_intraday(engine):
    today = today_et()
    log.info(f"Intraday mode: {today}")
    posted     = fetch_posted_props(engine)
    std_posted = posted[posted["market_key"].isin(STANDARD_MARKETS)].copy()
    if std_posted.empty:
        log.info("  No standard lines posted."); return
    player_ids = std_posted["player_id"].dropna().unique().tolist()
    if not player_ids: return
    pid_list     = ", ".join(str(int(p)) for p in player_ids)
    std_mkt_list = ", ".join(f"'{m}'" for m in STANDARD_MARKETS)
    last_graded = pd.read_sql(text(
        f"SELECT player_id,market_key,line_value AS last_line FROM("
        f"SELECT player_id,market_key,line_value,"
        f"ROW_NUMBER() OVER(PARTITION BY player_id,market_key ORDER BY grade_id DESC) AS rn"
        f" FROM common.daily_grades"
        f" WHERE grade_date=:gd AND player_id IN({pid_list})"
        f" AND market_key IN({std_mkt_list}) AND bookmaker_key=:bk AND outcome_name='Over'"
        f") ranked WHERE rn=1"
    ), engine, params={"gd": today, "bk": BOOKMAKER})
    current = std_posted[["player_id", "market_key", "line_value"]].rename(columns={"line_value": "current_line"})
    if not last_graded.empty:
        merged = current.merge(last_graded, on=["player_id", "market_key"], how="left")
        moved  = merged[merged["last_line"].isna() | (merged["current_line"].astype(float) != merged["last_line"].astype(float))]
    else:
        moved = current.copy()
    if moved.empty:
        log.info("  No line movement. Nothing to do."); return
    log.info(f"  {len(moved)} player-market pairs with movement.")
    moved_posted = std_posted.merge(moved[["player_id", "market_key"]], on=["player_id", "market_key"], how="inner")
    over_bracket = build_standard_props(moved_posted)
    if over_bracket.empty: return
    under_prices = fetch_under_prices(engine)
    under_props  = build_under_props(moved_posted, under_prices)

    history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns, opp_df = \
        _common_grade_data(engine, over_bracket, under_props, today)

    game_ids         = over_bracket["game_id"].dropna().unique().tolist()
    game_spreads     = fetch_upcoming_game_spreads(engine, game_ids)
    blowout_profiles = fetch_player_blowout_profiles(engine, list(over_bracket["player_id"].dropna().astype(int).unique()))

    over_rows, over_tiers   = grade_props_for_date(
        engine, today, over_bracket, history_df, season_df, opp_info, matchup_cache,
        direction="over", opp_history_df=opp_history_df, patterns=patterns, opp_df=opp_df,
        game_spreads=game_spreads, blowout_profiles=blowout_profiles,
    )
    under_rows, under_tiers = grade_props_for_date(
        engine, today, under_props, history_df, season_df, opp_info, matchup_cache,
        direction="under", opp_history_df=opp_history_df, patterns=patterns, opp_df=opp_df,
    ) if not under_props.empty else ([], [])

    written = upsert_grades(engine, over_rows + under_rows)
    tier_written = upsert_tier_lines(engine, over_tiers)
    log.info(f"  {written} rows written. {tier_written} tier rows written.")


def run_backfill(engine, batch_size, specific_date=None, force=False):
    if specific_date:
        work_dates = [specific_date]
    else:
        skip_clause = "" if force else (
            " AND NOT EXISTS(SELECT 1 FROM common.daily_grades g "
            "WHERE g.grade_date=CAST(egm.game_date AS DATE))"
        )
        df = pd.read_sql(text(
            "SELECT DISTINCT CAST(egm.game_date AS DATE) AS game_date"
            " FROM odds.player_props pp"
            " JOIN odds.event_game_map egm ON egm.event_id=pp.event_id AND egm.game_id IS NOT NULL"
            " WHERE pp.sport_key='basketball_nba' AND pp.bookmaker_key=:bk"
            " AND pp.outcome_name='Over' AND pp.outcome_point IS NOT NULL AND egm.game_date IS NOT NULL"
            f"{skip_clause}"
            " ORDER BY game_date ASC"
        ), engine, params={"bk": BOOKMAKER})
        work_dates = df["game_date"].astype(str).tolist()[:batch_size]
    if not work_dates:
        log.info("Backfill: nothing to do."); return
    log.info(f"Backfill: {len(work_dates)} date(s): {work_dates[0]} to {work_dates[-1]}")
    total = 0
    total_tiers = 0
    for gd in work_dates:
        props = fetch_posted_props(engine, table="odds.player_props", date_filter="AND CAST(egm.game_date AS DATE)=:gd", params={"gd": gd})
        if props.empty: continue
        player_ids  = props["player_id"].dropna().unique().tolist()
        market_keys = props["market_key"].dropna().unique().tolist()
        history_df     = fetch_history(engine, player_ids, market_keys, gd)
        opp_history_df = fetch_history(engine, player_ids, market_keys, gd, lookback=LOOKBACK_OPP)
        season_df      = fetch_season_history(engine, player_ids, gd)
        opp_info       = fetch_opp_info(engine, player_ids, gd)
        matchup_pairs  = []
        for pid, info in opp_info.items():
            pos = info.get("position", "")
            pg = "G" if pos.startswith("G") else "F" if pos.startswith("F") else "C" if pos.startswith("C") else None
            if pg and info.get("opp_team_id"):
                matchup_pairs.append((int(info["opp_team_id"]), pg))
        matchup_cache = fetch_matchup_defense(engine, matchup_pairs)
        patterns = fetch_player_patterns(engine, player_ids)
        opp_df   = fetch_opportunity_history(engine, player_ids, gd)

        game_ids         = props["game_id"].dropna().unique().tolist()
        game_spreads     = fetch_game_spreads(engine, game_ids)
        blowout_profiles = fetch_player_blowout_profiles(engine, player_ids)

        rows, tier_rows = grade_props_for_date(
            engine, gd, props, history_df, season_df, opp_info, matchup_cache,
            direction="over", opp_history_df=opp_history_df, patterns=patterns, opp_df=opp_df,
            game_spreads=game_spreads, blowout_profiles=blowout_profiles,
        )
        total       += upsert_grades(engine, rows)
        total_tiers += upsert_tier_lines(engine, tier_rows)
    log.info(f"Backfill complete. {total} grade rows written, {total_tiers} tier rows written.")


def run_outcomes(engine, specific_date=None):
    date_clause = "AND dg.grade_date = :gd" if specific_date else ""
    params: dict = {}
    if specific_date:
        params["gd"] = specific_date

    count_sql = text(f"""
        SELECT COUNT(*) AS n
        FROM common.daily_grades dg
        JOIN nba.schedule s ON s.game_id = dg.game_id
        WHERE dg.outcome IS NULL
          AND dg.game_id IS NOT NULL
          AND dg.player_id IS NOT NULL
          AND s.game_status = 3
          {date_clause}
    """)
    with engine.connect() as conn:
        n_pending = conn.execute(count_sql, params).scalar()

    if not n_pending:
        log.info("Outcomes: no pending rows to resolve.")
        return 0

    log.info(f"Outcomes: {n_pending} rows to resolve.")

    market_groups = [
        (("player_points", "player_points_alternate"),                   "SUM(b.pts)"),
        (("player_rebounds", "player_rebounds_alternate"),               "SUM(b.reb)"),
        (("player_assists", "player_assists_alternate"),                 "SUM(b.ast)"),
        (("player_threes", "player_threes_alternate"),                   "SUM(b.fg3m)"),
        (("player_blocks", "player_blocks_alternate"),                   "SUM(b.blk)"),
        (("player_steals", "player_steals_alternate"),                   "SUM(b.stl)"),
        (("player_points_rebounds_assists", "player_points_rebounds_assists_alternate"),
         "SUM(b.pts) + SUM(b.reb) + SUM(b.ast)"),
        (("player_points_rebounds", "player_points_rebounds_alternate"), "SUM(b.pts) + SUM(b.reb)"),
        (("player_points_assists", "player_points_assists_alternate"),   "SUM(b.pts) + SUM(b.ast)"),
        (("player_rebounds_assists", "player_rebounds_assists_alternate"), "SUM(b.reb) + SUM(b.ast)"),
    ]

    total_updated = 0
    for market_keys, stat_expr in market_groups:
        mkt_list = ", ".join(f"'{m}'" for m in market_keys)
        update_sql = text(f"""
            UPDATE dg
            SET dg.outcome = CASE
                WHEN dg.outcome_name = 'Over'  AND actual.stat_val > dg.line_value THEN 'Won'
                WHEN dg.outcome_name = 'Over'  AND actual.stat_val <= dg.line_value THEN 'Lost'
                WHEN dg.outcome_name = 'Under' AND actual.stat_val < dg.line_value  THEN 'Won'
                WHEN dg.outcome_name = 'Under' AND actual.stat_val >= dg.line_value THEN 'Lost'
                ELSE NULL
            END
            FROM common.daily_grades dg
            JOIN nba.schedule s ON s.game_id = dg.game_id
            JOIN (
                SELECT b.player_id, b.game_id, {stat_expr} AS stat_val
                FROM nba.player_box_score_stats b
                GROUP BY b.player_id, b.game_id
            ) actual ON actual.player_id = dg.player_id
                     AND actual.game_id   = dg.game_id
            WHERE dg.outcome IS NULL
              AND dg.game_id IS NOT NULL
              AND dg.player_id IS NOT NULL
              AND s.game_status = 3
              AND dg.market_key IN ({mkt_list})
              {date_clause}
        """)
        with engine.begin() as conn:
            result = conn.execute(update_sql, params)
            n = result.rowcount
            total_updated += n
            if n:
                log.info(f"  {market_keys[0]}: {n} rows updated.")

    log.info(f"Outcomes: {total_updated} total rows updated.")
    return total_updated


def main():
    parser = argparse.ArgumentParser(description="NBA prop grading model")
    parser.add_argument("--mode",  choices=["upcoming", "intraday", "backfill", "outcomes"], default="upcoming")
    parser.add_argument("--batch", type=int, default=BATCH_DEFAULT)
    parser.add_argument("--date",  type=str, default=None)
    parser.add_argument("--force", action="store_true",
                        help="Backfill mode only: regrade dates already in daily_grades.")
    args = parser.parse_args()
    engine = get_engine()
    ensure_tables(engine)
    if args.mode == "upcoming":
        run_upcoming(engine)
    elif args.mode == "intraday":
        run_intraday(engine)
    elif args.mode == "backfill":
        run_backfill(engine, batch_size=args.batch, specific_date=args.date, force=args.force)
    else:
        run_outcomes(engine, specific_date=args.date)
    log.info("Done.")


if __name__ == "__main__":
    main()
