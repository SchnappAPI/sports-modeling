"""
grade_props.py

NBA prop grading model.
"""

import argparse
import math
import os
import time
import logging
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
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
LOOKBACK_OPP   = 200   # full-season window for vs-opponent hit rate
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

# Maps market_key to the actual stat column in player_box_score_stats.
# Used by run_outcomes for the SQL CASE expression.
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
    is_standard       BIT           NOT NULL DEFAULT 0,
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
            ("outcome",         "VARCHAR(5)"),   # 'Won' / 'Lost' / NULL
        ]:
            conn.execute(text(
                f"IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades' AND COLUMN_NAME='{col}') "
                f"ALTER TABLE common.daily_grades ADD {col} {dtype} NULL"
            ))
    log.info("Schema verified.")


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
    SELECT pbs.player_id,pbs.game_id,
           CASE WHEN pbs.team_id=s.home_team_id THEN s.away_team_id ELSE s.home_team_id END AS opp_team_id,
           SUM(pbs.pts) AS pts,SUM(pbs.reb) AS reb,SUM(pbs.ast) AS ast,
           SUM(pbs.stl) AS stl,SUM(pbs.blk) AS blk,SUM(pbs.fg3m) AS fg3m,SUM(pbs.tov) AS tov
    FROM nba.player_box_score_stats pbs
    JOIN nba.schedule s ON s.game_id=pbs.game_id
    WHERE s.game_date>=(SELECT dt FROM ss)
    GROUP BY pbs.player_id,pbs.game_id,pbs.team_id,s.home_team_id,s.away_team_id
),
pf AS (
    SELECT gt.*,LEFT(p.position,1) AS pos_group FROM gt
    JOIN nba.players p ON p.player_id=gt.player_id
    WHERE LEFT(p.position,1) IN ('G','F','C')
),
tp AS (SELECT opp_team_id,pos_group FROM (VALUES {values_rows}) AS t(opp_team_id,pos_group)),
td AS (
    SELECT pf.opp_team_id,pf.pos_group,COUNT(*) AS games_defended,
           AVG(CAST(pf.pts AS FLOAT)) AS avg_pts,AVG(CAST(pf.reb AS FLOAT)) AS avg_reb,
           AVG(CAST(pf.ast AS FLOAT)) AS avg_ast,AVG(CAST(pf.stl AS FLOAT)) AS avg_stl,
           AVG(CAST(pf.blk AS FLOAT)) AS avg_blk,AVG(CAST(pf.fg3m AS FLOAT)) AS avg_fg3m,
           AVG(CAST(pf.tov AS FLOAT)) AS avg_tov
    FROM pf JOIN tp ON tp.opp_team_id=pf.opp_team_id AND tp.pos_group=pf.pos_group
    GROUP BY pf.opp_team_id,pf.pos_group
)
SELECT pos_group,opp_team_id,games_defended,
       avg_pts,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_pts  DESC) AS rank_pts,
       avg_reb,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_reb  DESC) AS rank_reb,
       avg_ast,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_ast  DESC) AS rank_ast,
       avg_stl,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_stl  DESC) AS rank_stl,
       avg_blk,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_blk  DESC) AS rank_blk,
       avg_fg3m, RANK() OVER (PARTITION BY pos_group ORDER BY avg_fg3m DESC) AS rank_fg3m,
       avg_tov,  RANK() OVER (PARTITION BY pos_group ORDER BY avg_tov  DESC) AS rank_tov
FROM td
""")
    df = pd.read_sql(sql, engine)
    result = {(int(row["opp_team_id"]), str(row["pos_group"])): row.to_dict() for _, row in df.iterrows()}
    log.info(f"  Matchup defense: {len(result)} team-position pairs.")
    return result


def fetch_player_patterns(engine, player_ids: list) -> dict:
    """
    Load personal autocorrelation patterns from common.player_line_patterns.
    Returns dict keyed by (player_id, market_key, line_value).
    """
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
        center = float(r["line_value"])
        for step in range(-BRACKET_STEPS, BRACKET_STEPS + 1):
            lv = round(center + step * BRACKET_INCREMENT, 1)
            if lv < 0.5:
                continue
            rows.append({
                "event_id":      r["event_id"],
                "player_id":     r["player_id"],
                "player_name":   r["player_name"],
                "market_key":    r["market_key"],
                "bookmaker_key": r["bookmaker_key"],
                "line_value":    lv,
                "game_id":       r["game_id"],
                "over_price":    int(r["over_price"]) if step == 0 and pd.notna(r.get("over_price")) else None,
                "outcome_name":  "Over",
                "is_standard":   1 if step == 0 else 0,
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
            "is_standard":   1,
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
                    "is_standard":   0,
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


def precompute_line_grades(season_df, props_df, patterns: dict = None):
    """
    Compute momentum_grade and pattern_grade per (player, market, line).

    Uses personal autocorrelation patterns from common.player_line_patterns
    when available (patterns dict from fetch_player_patterns). Falls back to
    a population-average approach when no personal pattern exists.

    momentum_grade: estimated probability (0-100) of hitting the Over,
        derived from the player's own P(hit|prev hit) or P(hit|prev miss).
        Score of 80 means 80% personal probability based on their history.

    pattern_strength_grade: how predictable this player is (0-100).
        Derived from pattern_strength in the patterns table — high means
        their history repeats consistently, low means random/noisy.
        This is stored as pattern_grade in the DB.
    """
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

        # Current streak length (for reference, not used directly in scoring)
        streak = 0
        for h in reversed(hits):
            if h == is_hit_streak:
                streak += 1
            else:
                break

        momentum = None
        pattern  = None

        # --- Personal pattern lookup ---
        pat = patterns.get(key)
        if pat is not None and pat["n"] >= 10:
            if is_hit_streak and pat["p_hit_after_hit"] is not None:
                # Player has hit this line. Use their personal P(hit again).
                momentum = _safe(pat["p_hit_after_hit"] * 100.0)
            elif not is_hit_streak and pat["p_hit_after_miss"] is not None:
                # Player has missed this line. Use their personal P(hit next).
                momentum = _safe(pat["p_hit_after_miss"] * 100.0)
            # else: personal transition prob not available (too few obs in that state)

            # Pattern strength grade: how reliable is this player's pattern?
            # 0 = completely random, 100 = very consistent repeating pattern
            # We scale pattern_strength (0.0-1.0 lift) to 0-100
            if pat["pattern_strength"] is not None:
                pattern = _safe(min(100.0, pat["pattern_strength"] * 300.0))
            # Minimum sample size bonus: more games = more confidence
            # Add up to 20 points for sample size (30+ games = full bonus)
            if pattern is not None and momentum is not None:
                sample_bonus = min(20.0, (pat["n"] - 10) * (20.0 / 20.0))
                pattern = _safe(min(100.0, pattern + sample_bonus))

        # --- Fallback: use season hit rate as a simple baseline ---
        if momentum is None and len(hits) >= 5:
            hr60 = float(np.mean(hits))
            # Without personal pattern, score is just the hit rate
            # from the player's perspective (no streak information)
            if is_hit_streak:
                # Slight upward nudge for being on a hit streak vs base rate
                momentum = _safe(min(100.0, hr60 * 100.0 + streak * 2.0))
            else:
                # On a miss streak — use base rate (no pattern to refine)
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


def compute_composite(whr, trend, momentum, pattern, matchup, regression):
    parts = []
    if whr is not None: parts.append(whr * 100.0)
    for v in (trend, momentum, pattern, matchup, regression):
        if v is not None: parts.append(float(v))
    return _safe(sum(parts) / len(parts)) if parts else None


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
    composite_grade FLOAT,hit_rate_opp FLOAT,sample_size_opp INT,is_standard BIT
)"""))
        for i in range(0, len(rows), 500):
            chunk = rows[i:i + 500]
            conn.exec_driver_sql(
                "INSERT INTO #stage_grades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(r["grade_date"], r["event_id"], r["game_id"], r["player_id"], r["player_name"],
                  r["market_key"], r["bookmaker_key"], r["line_value"], r.get("outcome_name", "Over"),
                  r["over_price"], r["hit_rate_60"], r["hit_rate_20"], r["sample_size_60"],
                  r["sample_size_20"], r["weighted_hit_rate"], r["grade"], r["trend_grade"],
                  r["momentum_grade"], r["pattern_grade"], r["matchup_grade"], r["regression_grade"],
                  r["composite_grade"], r["hit_rate_opp"], r["sample_size_opp"],
                  1 if r.get("is_standard") else 0)
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
    t.is_standard=s.is_standard
WHEN NOT MATCHED THEN INSERT(
    grade_date,event_id,game_id,player_id,player_name,market_key,bookmaker_key,
    line_value,outcome_name,over_price,hit_rate_60,hit_rate_20,sample_size_60,
    sample_size_20,weighted_hit_rate,grade,trend_grade,momentum_grade,pattern_grade,
    matchup_grade,regression_grade,composite_grade,hit_rate_opp,sample_size_opp,is_standard
) VALUES(
    s.grade_date,s.event_id,s.game_id,s.player_id,s.player_name,s.market_key,
    s.bookmaker_key,s.line_value,s.outcome_name,s.over_price,s.hit_rate_60,
    s.hit_rate_20,s.sample_size_60,s.sample_size_20,s.weighted_hit_rate,s.grade,
    s.trend_grade,s.momentum_grade,s.pattern_grade,s.matchup_grade,s.regression_grade,
    s.composite_grade,s.hit_rate_opp,s.sample_size_opp,s.is_standard
);"""))
    return len(rows)


def grade_props_for_date(engine, grade_date_str, props_df, history_df, season_df, opp_info, matchup_cache, direction="over", opp_history_df=None, patterns=None):
    if props_df.empty: return []
    props_df = props_df.drop_duplicates(subset=["player_id", "market_key", "line_value"]).copy()
    graded_df   = compute_all_hit_rates(props_df, history_df, opp_info, direction=direction, opp_history_df=opp_history_df)
    pm_grades   = precompute_player_market_grades(season_df, graded_df)
    line_grades = precompute_line_grades(season_df, graded_df, patterns=patterns)
    is_under = (direction == "under")
    rows = []
    for _, r in graded_df.iterrows():
        pid = r["player_id"]
        if pd.isna(pid): continue
        pid_int = int(pid); mkt = r["market_key"]; lv = float(r["line_value"])
        info = opp_info.get(pid_int, {}); position = info.get("position", ""); opp_id = info.get("opp_team_id")
        whr = r.get("weighted_hit_rate"); whr = whr if pd.notna(whr) else None
        pm = pm_grades.get((pid_int, mkt), {}); lk = line_grades.get((pid_int, mkt, lv), {})
        t_r  = pm.get("trend_grade");      rg_r = pm.get("regression_grade")
        mo_r = lk.get("momentum_grade");   pa_r = lk.get("pattern_grade")
        ma_r = compute_matchup_grade(mkt, opp_id, position, matchup_cache)
        if is_under:
            trend = _invert(t_r); regression = _invert(rg_r)
            momentum = _invert(mo_r); pattern = _invert(pa_r); matchup = _invert(ma_r)
        else:
            trend = t_r; regression = rg_r; momentum = mo_r; pattern = pa_r; matchup = ma_r
        composite = compute_composite(whr, trend, momentum, pattern, matchup, regression)
        hr_opp = r.get("hit_rate_opp"); hr_opp = hr_opp if pd.notna(hr_opp) else None
        n_opp  = r.get("sample_size_opp"); n_opp = int(n_opp) if pd.notna(n_opp) and n_opp else None
        raw_price = r.get("over_price"); price = int(raw_price) if pd.notna(raw_price) and raw_price is not None else None
        rows.append({
            "grade_date":        grade_date_str,
            "event_id":          r["event_id"],
            "game_id":           r.get("game_id"),
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
            "is_standard":       int(r["is_standard"]) if pd.notna(r.get("is_standard")) else 0,
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
        })
    return rows


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
    return history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns


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
    history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns = _common_grade_data(engine, all_over, under_props, today)
    over_rows  = grade_props_for_date(engine, today, all_over, history_df, season_df, opp_info, matchup_cache, direction="over", opp_history_df=opp_history_df, patterns=patterns)
    under_rows = grade_props_for_date(engine, today, under_props, history_df, season_df, opp_info, matchup_cache, direction="under", opp_history_df=opp_history_df, patterns=patterns) if not under_props.empty else []
    written = upsert_grades(engine, over_rows + under_rows)
    log.info(f"  {written} total rows written ({len(over_rows)} over, {len(under_rows)} under).")


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
    history_df, season_df, opp_info, matchup_cache, opp_history_df, patterns = _common_grade_data(engine, over_bracket, under_props, today)
    over_rows  = grade_props_for_date(engine, today, over_bracket, history_df, season_df, opp_info, matchup_cache, direction="over", opp_history_df=opp_history_df, patterns=patterns)
    under_rows = grade_props_for_date(engine, today, under_props, history_df, season_df, opp_info, matchup_cache, direction="under", opp_history_df=opp_history_df, patterns=patterns) if not under_props.empty else []
    written = upsert_grades(engine, over_rows + under_rows)
    log.info(f"  {written} rows written ({len(over_rows)} over, {len(under_rows)} under).")


def run_backfill(engine, batch_size, specific_date=None):
    if specific_date:
        work_dates = [specific_date]
    else:
        df = pd.read_sql(text(
            "SELECT DISTINCT CAST(egm.game_date AS DATE) AS game_date"
            " FROM odds.player_props pp"
            " JOIN odds.event_game_map egm ON egm.event_id=pp.event_id AND egm.game_id IS NOT NULL"
            " WHERE pp.sport_key='basketball_nba' AND pp.bookmaker_key=:bk"
            " AND pp.outcome_name='Over' AND pp.outcome_point IS NOT NULL AND egm.game_date IS NOT NULL"
            " AND NOT EXISTS(SELECT 1 FROM common.daily_grades g WHERE g.grade_date=CAST(egm.game_date AS DATE))"
            " ORDER BY game_date ASC"
        ), engine, params={"bk": BOOKMAKER})
        work_dates = df["game_date"].astype(str).tolist()[:batch_size]
    if not work_dates:
        log.info("Backfill: nothing to do."); return
    log.info(f"Backfill: {len(work_dates)} date(s): {work_dates[0]} to {work_dates[-1]}")
    total = 0
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
        rows  = grade_props_for_date(engine, gd, props, history_df, season_df, opp_info, matchup_cache, direction="over", opp_history_df=opp_history_df, patterns=patterns)
        total += upsert_grades(engine, rows)
    log.info(f"Backfill complete. {total} total rows written.")


def run_outcomes(engine, specific_date=None):
    """
    Compute and persist Won/Lost outcomes for all graded props where the game
    has completed (game_status = 3) and outcome is still NULL.

    Uses a pure SQL UPDATE — no pandas, no Python loops over rows.
    Each market_key maps to a specific stat expression computed inline from
    nba.player_box_score_stats. One UPDATE per market group, fast regardless
    of history size.
    """
    date_clause = "AND dg.grade_date = :gd" if specific_date else ""
    params: dict = {}
    if specific_date:
        params["gd"] = specific_date

    # Check how many rows need resolving before doing any work
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

    # Each entry: (market_keys_tuple, stat_sql_expression)
    # stat_sql_expression is evaluated against nba.player_box_score_stats
    # grouped by (player_id, game_id).
    market_groups = [
        (("player_points", "player_points_alternate"),
         "SUM(b.pts)"),
        (("player_rebounds", "player_rebounds_alternate"),
         "SUM(b.reb)"),
        (("player_assists", "player_assists_alternate"),
         "SUM(b.ast)"),
        (("player_threes", "player_threes_alternate"),
         "SUM(b.fg3m)"),
        (("player_blocks", "player_blocks_alternate"),
         "SUM(b.blk)"),
        (("player_steals", "player_steals_alternate"),
         "SUM(b.stl)"),
        (("player_points_rebounds_assists", "player_points_rebounds_assists_alternate"),
         "SUM(b.pts) + SUM(b.reb) + SUM(b.ast)"),
        (("player_points_rebounds", "player_points_rebounds_alternate"),
         "SUM(b.pts) + SUM(b.reb)"),
        (("player_points_assists", "player_points_assists_alternate"),
         "SUM(b.pts) + SUM(b.ast)"),
        (("player_rebounds_assists", "player_rebounds_assists_alternate"),
         "SUM(b.reb) + SUM(b.ast)"),
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
    args = parser.parse_args()
    engine = get_engine()
    ensure_tables(engine)
    if args.mode == "upcoming":
        run_upcoming(engine)
    elif args.mode == "intraday":
        run_intraday(engine)
    elif args.mode == "backfill":
        run_backfill(engine, batch_size=args.batch, specific_date=args.date)
    else:
        run_outcomes(engine, specific_date=args.date)
    log.info("Done.")


if __name__ == "__main__":
    main()
