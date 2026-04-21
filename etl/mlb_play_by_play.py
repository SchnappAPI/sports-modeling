"""
mlb_play_by_play.py

Loads pitch-level play-by-play data for MLB games into mlb.play_by_play, then
in-lockstep materializes derived tables for the same games:
  mlb.player_at_bats          — one row per completed at-bat, IDs only
  mlb.career_batter_vs_pitcher — lifetime counts + rates per (batter, pitcher)

Source: https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics

Three tables written:
  mlb.play_by_play               — one row per play event (pitch, pickoff, baserunning)
  mlb.player_at_bats             — one row per completed at-bat, IDs only (no names)
  mlb.career_batter_vs_pitcher   — one row per (batter_id, pitcher_id) lifetime

Why no denormalized names on player_at_bats:
  mlb.players is truncate-and-reload scoped to the current season, so roughly
  30% of pitcher_ids and 20% of batter_ids across historical PBP would land
  as NULL if we joined at write time. Web routes join mlb.players at read
  time instead — the table has under a thousand rows with a PK on player_id,
  so the read-time join is effectively free.

Write strategies (by table):
  play_by_play:
    Direct INSERT via to_sql(if_exists='append') + fast_executemany=True. The
    pre-diff against existing game_pks guarantees every game is new (ADR-0013).
  player_at_bats:
    Direct INSERT. Separate diff against player_at_bats.game_pk so partial
    runs (PBP wrote, at-bats failed) are self-healing (ADR-0018).
  career_batter_vs_pitcher:
    Staged MERGE. Unlike the other two, a (batter_id, pitcher_id) pair that
    appeared in a flush five runs ago already has a row; the new flush needs
    to update it, not insert a duplicate. For each flush, recompute lifetime
    rows for the (batter_id, pitcher_id) pairs present in the flushed games,
    stage to a temp table, MERGE into the permanent table.

Incremental logic:
  PBP:
    1. Load desired game_pk set from mlb.games (Final regular season games).
    2. Load existing game_pk set from mlb.play_by_play.
    3. Diff: only process games not already loaded.
    4. Process oldest --batch games per run.
  At-bats (always runs after each PBP flush, plus --rebuild-at-bats mode):
    1. Candidate game_pks = games present in mlb.play_by_play.
    2. Existing game_pks = games already in mlb.player_at_bats.
    3. Diff. For each new game, build at-bat rows from PBP and INSERT.
  Career BvP (always runs after each at-bats flush, plus --rebuild-bvp mode):
    1. Determine (batter_id, pitcher_id) pairs affected by the flushed games.
    2. Recompute lifetime counts + rates for those pairs from the full
       mlb.player_at_bats table.
    3. Stage + MERGE into mlb.career_batter_vs_pitcher.

Rebuild modes:
  --rebuild-at-bats: skip PBP fetch; rebuild mlb.player_at_bats from existing
    PBP data. Does NOT delete rows; for a full rebuild, DELETE first.
  --rebuild-bvp: skip PBP fetch; rebuild mlb.career_batter_vs_pitcher from
    the full mlb.player_at_bats table. Chunked by batter_id. Does NOT delete
    rows; for a full rebuild, DELETE first.
  The two flags are independent. Passing both runs at-bats first, then bvp.

Runs exclusively in GitHub Actions. Credentials injected as environment variables.
"""

import sys
import time
import logging

import requests
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import (
    VARCHAR, Integer, Date, SmallInteger, Float, Boolean, NVARCHAR, DATETIME
)

from pathlib import Path
_repo_root = str(Path(__file__).resolve().parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

from etl.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SEASONS = [2025]
DEFAULT_BATCH = 50
API_PAUSE = 0.25
API_BASE  = "https://statsapi.mlb.com/api/v1/game/{game_pk}/withMetrics"
FLUSH_EVERY = 5  # games per DB write; each game ~300 rows = ~3000 rows per flush

# Explicit column types for to_sql. Prevents pandas from inferring VARCHAR(N)
# from batch data, which causes right-truncation when a later row is longer.
INSERT_DTYPES = {
    "play_event_id":          VARCHAR(50),
    "game_date":              Date(),
    "result_event_type":      VARCHAR(50),
    "result_description":     VARCHAR(1000),
    "batter_hand_code":       VARCHAR(1),
    "batter_split":           VARCHAR(30),
    "pitcher_hand_code":      VARCHAR(1),
    "pitcher_split":          VARCHAR(30),
    "play_id":                VARCHAR(50),
    "play_event_type":        VARCHAR(30),
    "pitch_call_code":        VARCHAR(5),
    "pitch_type_code":        VARCHAR(5),
    "play_event_description": VARCHAR(1000),
    "count_balls_strikes":    VARCHAR(5),
    "hit_trajectory":         VARCHAR(30),
    "hit_hardness":           VARCHAR(20),
    "at_bat_end_time":        DATETIME(),
    "play_end_time":          DATETIME(),
    "play_event_end_time":    DATETIME(),
}

AB_INSERT_DTYPES = {
    "at_bat_id":          VARCHAR(30),
    "game_date":          Date(),
    "result_event_type":  VARCHAR(50),
    "result_description": VARCHAR(1000),
    "hit_trajectory":     VARCHAR(30),
    "hit_hardness":       VARCHAR(20),
}

DDL_CREATE = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'play_by_play'
)
CREATE TABLE mlb.play_by_play (
    play_event_id              VARCHAR(50)   NOT NULL PRIMARY KEY,
    game_pk                    INT           NOT NULL,
    game_date                  DATE          NULL,
    at_bat_number              INT           NULL,
    play_event_index           INT           NULL,
    inning                     INT           NULL,
    is_top_inning              BIT           NULL,
    team_id                    INT           NULL,
    vs_team_id                 INT           NULL,
    away_team_id               INT           NULL,
    home_team_id               INT           NULL,
    venue_id                   INT           NULL,
    result_event_type          VARCHAR(50)   NULL,
    result_description         VARCHAR(1000) NULL,
    result_rbi                 INT           NULL,
    result_is_out              BIT           NULL,
    at_bat_is_complete         BIT           NULL,
    at_bat_is_scoring_play     BIT           NULL,
    at_bat_has_out             BIT           NULL,
    at_bat_end_time            DATETIME2     NULL,
    play_end_time              DATETIME2     NULL,
    batter_id                  INT           NULL,
    batter_hand_code           CHAR(1)       NULL,
    batter_split               VARCHAR(30)   NULL,
    pitcher_id                 INT           NULL,
    pitcher_hand_code          CHAR(1)       NULL,
    pitcher_split              VARCHAR(30)   NULL,
    play_id                    VARCHAR(50)   NULL,
    play_event_type            VARCHAR(30)   NULL,
    is_pitch                   BIT           NULL,
    is_base_running_play       BIT           NULL,
    pitch_number               INT           NULL,
    pitch_call_code            VARCHAR(5)    NULL,
    pitch_type_code            VARCHAR(5)    NULL,
    play_event_description     VARCHAR(1000) NULL,
    is_hit_into_play           BIT           NULL,
    is_strike                  BIT           NULL,
    is_ball                    BIT           NULL,
    is_out                     BIT           NULL,
    runner_going               BIT           NULL,
    count_balls_strikes        VARCHAR(5)    NULL,
    count_outs                 INT           NULL,
    is_last_pitch              BIT           NULL,
    is_at_bat                  BIT           NULL,
    is_plate_appearance        BIT           NULL,
    play_event_end_time        DATETIME2     NULL,
    pitch_start_speed          DECIMAL(5,1)  NULL,
    pitch_end_speed            DECIMAL(5,1)  NULL,
    pitch_zone                 INT           NULL,
    strike_zone_top            DECIMAL(5,2)  NULL,
    strike_zone_bottom         DECIMAL(5,2)  NULL,
    hit_launch_speed           DECIMAL(5,1)  NULL,
    hit_launch_angle           INT           NULL,
    hit_total_distance         INT           NULL,
    hit_trajectory             VARCHAR(30)   NULL,
    hit_hardness               VARCHAR(20)   NULL,
    hit_location               INT           NULL,
    hit_probability            DECIMAL(5,2)  NULL,
    hit_bat_speed              DECIMAL(5,1)  NULL,
    home_run_ballparks         INT           NULL,
    created_at                 DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
"""

DDL_ALTER_DESCRIPTIONS = """
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'play_by_play'
      AND COLUMN_NAME = 'result_description'
      AND CHARACTER_MAXIMUM_LENGTH < 1000
)
    ALTER TABLE mlb.play_by_play ALTER COLUMN result_description VARCHAR(1000) NULL;

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'play_by_play'
      AND COLUMN_NAME = 'play_event_description'
      AND CHARACTER_MAXIMUM_LENGTH < 1000
)
    ALTER TABLE mlb.play_by_play ALTER COLUMN play_event_description VARCHAR(1000) NULL;
"""

DDL_CREATE_AT_BATS = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'player_at_bats'
)
CREATE TABLE mlb.player_at_bats (
    at_bat_id           VARCHAR(30)   NOT NULL PRIMARY KEY,
    game_pk             INT           NOT NULL,
    game_date           DATE          NULL,
    at_bat_number       INT           NOT NULL,
    inning              INT           NULL,
    is_top_inning       BIT           NULL,
    batter_id           INT           NULL,
    pitcher_id          INT           NULL,
    result_event_type   VARCHAR(50)   NULL,
    result_description  VARCHAR(1000) NULL,
    result_rbi          INT           NULL,
    hit_launch_speed    DECIMAL(5,1)  NULL,
    hit_launch_angle    INT           NULL,
    hit_total_distance  INT           NULL,
    hit_trajectory      VARCHAR(30)   NULL,
    hit_hardness        VARCHAR(20)   NULL,
    hit_probability     DECIMAL(5,2)  NULL,
    hit_bat_speed       DECIMAL(5,1)  NULL,
    home_run_ballparks  INT           NULL,
    away_team_id        INT           NULL,
    home_team_id        INT           NULL,
    created_at          DATETIME2     NOT NULL DEFAULT GETUTCDATE()
);
"""

# If the table already exists from the initial denormalized design, drop the
# name columns. Idempotent: only runs when the columns are still present.
DDL_DROP_NAME_COLUMNS = """
IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'player_at_bats'
      AND COLUMN_NAME = 'batter_name'
)
    ALTER TABLE mlb.player_at_bats DROP COLUMN batter_name;

IF EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'player_at_bats'
      AND COLUMN_NAME = 'pitcher_name'
)
    ALTER TABLE mlb.player_at_bats DROP COLUMN pitcher_name;
"""

DDL_CREATE_AT_BATS_INDEXES = """
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_player_at_bats_game_pk'
      AND object_id = OBJECT_ID('mlb.player_at_bats')
)
    CREATE NONCLUSTERED INDEX IX_player_at_bats_game_pk
        ON mlb.player_at_bats (game_pk);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_player_at_bats_batter'
      AND object_id = OBJECT_ID('mlb.player_at_bats')
)
    CREATE NONCLUSTERED INDEX IX_player_at_bats_batter
        ON mlb.player_at_bats (batter_id, game_date);
"""

# career_batter_vs_pitcher: lifetime counts + rates per (batter, pitcher).
# Compound PK (batter_id, pitcher_id), clustered. All rate stats stored
# pre-computed so the web can read without re-deriving AVG/OBP/SLG/OPS.
DDL_CREATE_BVP = """
IF NOT EXISTS (
    SELECT 1 FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'mlb' AND TABLE_NAME = 'career_batter_vs_pitcher'
)
CREATE TABLE mlb.career_batter_vs_pitcher (
    batter_id        INT           NOT NULL,
    pitcher_id       INT           NOT NULL,
    plate_appearances INT          NOT NULL,
    at_bats          INT           NOT NULL,
    hits             INT           NOT NULL,
    singles          INT           NOT NULL,
    doubles          INT           NOT NULL,
    triples          INT           NOT NULL,
    home_runs        INT           NOT NULL,
    rbi              INT           NOT NULL,
    walks            INT           NOT NULL,
    strikeouts       INT           NOT NULL,
    hit_by_pitch     INT           NOT NULL,
    sac_flies        INT           NOT NULL,
    total_bases      INT           NOT NULL,
    batting_avg      DECIMAL(5,3)  NULL,
    obp              DECIMAL(5,3)  NULL,
    slg              DECIMAL(5,3)  NULL,
    ops              DECIMAL(5,3)  NULL,
    last_faced_date  DATE          NULL,
    updated_at       DATETIME2     NOT NULL DEFAULT GETUTCDATE(),
    CONSTRAINT PK_career_batter_vs_pitcher PRIMARY KEY CLUSTERED (batter_id, pitcher_id)
);
"""

DDL_CREATE_BVP_INDEXES = """
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_bvp_pitcher'
      AND object_id = OBJECT_ID('mlb.career_batter_vs_pitcher')
)
    CREATE NONCLUSTERED INDEX IX_bvp_pitcher
        ON mlb.career_batter_vs_pitcher (pitcher_id, batter_id);
"""


def ensure_table(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL_CREATE))
        conn.execute(text(DDL_ALTER_DESCRIPTIONS))
        conn.execute(text(DDL_CREATE_AT_BATS))
        conn.execute(text(DDL_DROP_NAME_COLUMNS))
        conn.execute(text(DDL_CREATE_AT_BATS_INDEXES))
        conn.execute(text(DDL_CREATE_BVP))
        conn.execute(text(DDL_CREATE_BVP_INDEXES))
    log.info("mlb.play_by_play, mlb.player_at_bats, and mlb.career_batter_vs_pitcher tables ensured.")


def safe_int(val):
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def safe_float(val):
    try:
        s = str(val).strip()
        return float(s) if s not in ("", "None") else None
    except (ValueError, TypeError):
        return None


def safe_bool(val):
    """
    Return 1/0/None for BIT columns.
    FIX: added float to the isinstance check — the API sometimes returns
    numeric 0.0/1.0 for boolean fields, which SQL Server rejects as an
    invalid cast to BIT when sent as a Python float.
    """
    if val is None:
        return None
    if isinstance(val, (bool, int, float)):
        return 1 if val else 0
    if isinstance(val, str):
        return 1 if val.lower() in ("true", "1", "yes") else 0
    return None


def safe_datetime(val):
    """
    Parse ISO timestamp strings from the API into Python datetime objects.
    Returns None on any parse failure so the column lands as NULL rather
    than an unconverted string being implicitly cast by SQL Server.
    """
    if val is None:
        return None
    try:
        return pd.Timestamp(val).to_pydatetime()
    except Exception:
        return None


def trunc(val, max_len):
    if val is None:
        return None
    s = str(val)
    return s[:max_len] if len(s) > max_len else s


def fetch_game_json(game_pk, retries=3, pause=5):
    url = API_BASE.format(game_pk=game_pk)
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.warning("Fetch failed for game_pk %d (attempt %d/%d): %s",
                        game_pk, attempt, retries, exc)
            if attempt < retries:
                time.sleep(pause)
    return None


def parse_play_by_play(game_json, game_pk, game_date):
    try:
        all_plays = game_json["liveData"]["plays"]["allPlays"]
    except (KeyError, TypeError):
        return []

    game_data = game_json.get("gameData", {})
    away_id   = game_data.get("teams", {}).get("away", {}).get("id")
    home_id   = game_data.get("teams", {}).get("home", {}).get("id")
    venue_id  = game_data.get("venue", {}).get("id")

    rows = []

    for play in all_plays:
        about   = play.get("about", {})
        matchup = play.get("matchup", {})
        result  = play.get("result", {})
        credits = [c.get("credit") for c in play.get("credits", [])]

        is_top     = about.get("isTopInning")
        at_bat_num = safe_int(about.get("atBatIndex", -1)) + 1 if about.get("atBatIndex") is not None else None
        batter_id  = matchup.get("batter", {}).get("id")
        pitcher_id = matchup.get("pitcher", {}).get("id")
        team_id    = away_id if is_top else home_id
        vs_team_id = home_id if is_top else away_id
        is_ab      = 1 if "b_ab" in credits else 0
        is_pa      = 1 if "b_pa" in credits else 0

        play_events = play.get("playEvents", [])
        max_index   = max((e.get("index", -1) for e in play_events), default=-1)

        for event in play_events:
            play_id = event.get("playId")
            if play_id is None:
                continue

            ev_index = event.get("index")
            is_last  = (ev_index == max_index)

            details    = event.get("details", {})
            pitch_data = event.get("pitchData", {})
            hit_data   = event.get("hitData", {})
            ctx        = event.get("contextMetrics", {})
            count      = event.get("count", {})

            rows.append({
                "play_event_id":          f"{game_pk}-{at_bat_num}-{ev_index}",
                "game_pk":                game_pk,
                "game_date":              pd.Timestamp(game_date).date() if game_date else None,
                "at_bat_number":          at_bat_num,
                "play_event_index":       ev_index,
                "inning":                 safe_int(about.get("inning")),
                "is_top_inning":          safe_bool(is_top),
                "team_id":                team_id,
                "vs_team_id":             vs_team_id,
                "away_team_id":           away_id,
                "home_team_id":           home_id,
                "venue_id":               venue_id,
                "result_event_type":      trunc(result.get("eventType"), 50)     if is_last else None,
                "result_description":     trunc(result.get("description"), 1000) if is_last else None,
                "result_rbi":             safe_int(result.get("rbi"))            if is_last else None,
                "result_is_out":          safe_bool(result.get("isOut"))         if is_last else None,
                "at_bat_is_complete":     safe_bool(about.get("isComplete"))     if is_last else None,
                "at_bat_is_scoring_play": safe_bool(about.get("isScoringPlay"))  if is_last else None,
                "at_bat_has_out":         safe_bool(about.get("hasOut"))         if is_last else None,
                "at_bat_end_time":        safe_datetime(about.get("endTime"))    if is_last else None,
                "play_end_time":          safe_datetime(play.get("playEndTime")) if is_last else None,
                "is_at_bat":              is_ab                                  if is_last else None,
                "is_plate_appearance":    is_pa                                  if is_last else None,
                "batter_id":              batter_id,
                "batter_hand_code":       trunc(matchup.get("batSide", {}).get("code"), 1),
                "batter_split":           trunc(matchup.get("splits", {}).get("batter"), 30),
                "pitcher_id":             pitcher_id,
                "pitcher_hand_code":      trunc(matchup.get("pitchHand", {}).get("code"), 1),
                "pitcher_split":          trunc(matchup.get("splits", {}).get("pitcher"), 30),
                "play_id":                trunc(play_id, 50),
                "play_event_type":        trunc(event.get("type"), 30),
                "is_pitch":               safe_bool(event.get("isPitch")),
                "is_base_running_play":   safe_bool(event.get("isBaseRunningPlay")),
                "pitch_number":           safe_int(event.get("pitchNumber")),
                "pitch_call_code":        trunc(details.get("call", {}).get("code") if isinstance(details.get("call"), dict) else None, 5),
                "pitch_type_code":        trunc(details.get("type", {}).get("code") if isinstance(details.get("type"), dict) else None, 5),
                "play_event_description": trunc(details.get("description"), 1000),
                "is_hit_into_play":       safe_bool(details.get("isInPlay")),
                "is_strike":              safe_bool(details.get("isStrike")),
                "is_ball":                safe_bool(details.get("isBall")),
                "is_out":                 safe_bool(details.get("isOut")),
                "runner_going":           safe_bool(details.get("runnerGoing")),
                "count_balls_strikes":    f"{count.get('balls', '')}-{count.get('strikes', '')}" if count else None,
                "count_outs":             safe_int(count.get("outs")),
                "is_last_pitch":          safe_bool(is_last),
                "play_event_end_time":    safe_datetime(event.get("endTime")),
                "pitch_start_speed":      safe_float(pitch_data.get("startSpeed")),
                "pitch_end_speed":        safe_float(pitch_data.get("endSpeed")),
                "pitch_zone":             safe_int(pitch_data.get("zone")),
                "strike_zone_top":        safe_float(pitch_data.get("strikeZoneTop")),
                "strike_zone_bottom":     safe_float(pitch_data.get("strikeZoneBottom")),
                "hit_launch_speed":       safe_float(hit_data.get("launchSpeed")),
                "hit_launch_angle":       safe_int(hit_data.get("launchAngle")),
                "hit_total_distance":     safe_int(hit_data.get("totalDistance")),
                "hit_trajectory":         trunc(hit_data.get("trajectory"), 30),
                "hit_hardness":           trunc(hit_data.get("hardness"), 20),
                "hit_location":           safe_int(hit_data.get("location")),
                "hit_probability":        safe_float(hit_data.get("hitProbability")),
                "hit_bat_speed":          safe_float(hit_data.get("batSpeed")),
                "home_run_ballparks":     safe_int(ctx.get("homeRunBallparks")),
            })

    return rows


def flush(engine, rows):
    """
    Write accumulated rows directly to mlb.play_by_play via INSERT.
    All games in the batch are new (diffed before the loop), so MERGE is
    unnecessary. Direct INSERT with fast_executemany=True is ~10x faster.
    """
    df = pd.DataFrame(rows)
    df = df.astype(object).where(pd.notna(df), other=None)
    df.to_sql(
        "play_by_play",
        engine,
        schema="mlb",
        if_exists="append",
        index=False,
        chunksize=500,
        dtype=INSERT_DTYPES,
    )


def load_player_at_bats_for_games(engine, game_pks):
    """
    Materialize one-row-per-at-bat data from mlb.play_by_play into
    mlb.player_at_bats for the given game_pks.

    Skips any game_pk already present in mlb.player_at_bats so partial
    runs are self-healing. Same filter as the live Exit Velo query:
    is_last_pitch = 1 AND result_event_type IS NOT NULL.

    Batter and pitcher names are NOT stored here. The web layer joins
    mlb.players at read time.
    """
    if not game_pks:
        return

    game_pks = list(set(int(g) for g in game_pks))

    with engine.connect() as conn:
        existing = {
            row[0] for row in conn.execute(
                text("SELECT DISTINCT game_pk FROM mlb.player_at_bats")
            ).fetchall()
        }

    target = [g for g in game_pks if g not in existing]
    if not target:
        log.info("at_bats: all %d games already materialized.", len(game_pks))
        return

    # Pull at-bat rows straight from PBP. IDs only; names joined at read time.
    placeholders = ", ".join(str(g) for g in target)
    query = f"""
        SELECT
            CAST(p.game_pk AS VARCHAR(10)) + '-' + CAST(p.at_bat_number AS VARCHAR(10)) AS at_bat_id,
            p.game_pk,
            p.game_date,
            p.at_bat_number,
            p.inning,
            p.is_top_inning,
            p.batter_id,
            p.pitcher_id,
            p.result_event_type,
            p.result_description,
            p.result_rbi,
            p.hit_launch_speed,
            p.hit_launch_angle,
            p.hit_total_distance,
            p.hit_trajectory,
            p.hit_hardness,
            p.hit_probability,
            p.hit_bat_speed,
            p.home_run_ballparks,
            p.away_team_id,
            p.home_team_id
        FROM mlb.play_by_play p
        WHERE p.game_pk IN ({placeholders})
          AND p.is_last_pitch = 1
          AND p.result_event_type IS NOT NULL
        ORDER BY p.game_pk, p.at_bat_number
    """

    df = pd.read_sql(query, engine)
    if df.empty:
        log.info("at_bats: no completed at-bats found for %d games.", len(target))
        return

    df = df.astype(object).where(pd.notna(df), other=None)
    df.to_sql(
        "player_at_bats",
        engine,
        schema="mlb",
        if_exists="append",
        index=False,
        chunksize=500,
        dtype=AB_INSERT_DTYPES,
    )
    log.info(
        "at_bats: wrote %d rows across %d games (%d skipped as already present).",
        len(df), len(target), len(game_pks) - len(target)
    )


def rebuild_player_at_bats(engine):
    """
    Standalone materializer for --rebuild-at-bats mode. Runs the at-bats
    loader against every game_pk currently in mlb.play_by_play.

    Does NOT delete existing rows. If you want a full rebuild rather than
    a gap fill, manually DELETE FROM mlb.player_at_bats first.
    """
    with engine.connect() as conn:
        pbp_games = [
            row[0] for row in conn.execute(
                text("SELECT DISTINCT game_pk FROM mlb.play_by_play")
            ).fetchall()
        ]

    log.info("rebuild: %d distinct game_pks in mlb.play_by_play.", len(pbp_games))
    if not pbp_games:
        return

    CHUNK = 100
    for start in range(0, len(pbp_games), CHUNK):
        chunk = pbp_games[start:start + CHUNK]
        log.info("rebuild: processing games %d-%d of %d.",
                 start + 1, start + len(chunk), len(pbp_games))
        load_player_at_bats_for_games(engine, chunk)


# SQL expression that classifies a row from mlb.player_at_bats into at-bat
# count buckets. Reused by both the per-flush loader and the full rebuild.
# Event types follow the MLB Stats API contract observed in production data.
# Note: references unqualified "ab" alias; callers supply FROM mlb.player_at_bats AS ab.
BVP_AGGREGATE_SELECT = """
    ab.batter_id,
    ab.pitcher_id,
    COUNT(*) AS plate_appearances,
    SUM(CASE
        WHEN ab.result_event_type IN (
            'walk','intent_walk','hit_by_pitch','sac_fly','sac_fly_double_play',
            'sac_bunt','sac_bunt_double_play','catcher_interf'
        ) THEN 0 ELSE 1
    END) AS at_bats,
    SUM(CASE WHEN ab.result_event_type IN ('single','double','triple','home_run') THEN 1 ELSE 0 END) AS hits,
    SUM(CASE WHEN ab.result_event_type = 'single'   THEN 1 ELSE 0 END) AS singles,
    SUM(CASE WHEN ab.result_event_type = 'double'   THEN 1 ELSE 0 END) AS doubles,
    SUM(CASE WHEN ab.result_event_type = 'triple'   THEN 1 ELSE 0 END) AS triples,
    SUM(CASE WHEN ab.result_event_type = 'home_run' THEN 1 ELSE 0 END) AS home_runs,
    SUM(ISNULL(ab.result_rbi, 0)) AS rbi,
    SUM(CASE WHEN ab.result_event_type IN ('walk','intent_walk') THEN 1 ELSE 0 END) AS walks,
    SUM(CASE WHEN ab.result_event_type IN ('strikeout','strikeout_double_play') THEN 1 ELSE 0 END) AS strikeouts,
    SUM(CASE WHEN ab.result_event_type = 'hit_by_pitch' THEN 1 ELSE 0 END) AS hit_by_pitch,
    SUM(CASE WHEN ab.result_event_type IN ('sac_fly','sac_fly_double_play') THEN 1 ELSE 0 END) AS sac_flies,
    SUM(
        CASE WHEN ab.result_event_type = 'single'   THEN 1
             WHEN ab.result_event_type = 'double'   THEN 2
             WHEN ab.result_event_type = 'triple'   THEN 3
             WHEN ab.result_event_type = 'home_run' THEN 4
             ELSE 0 END
    ) AS total_bases,
    MAX(ab.game_date) AS last_faced_date
"""


def _merge_bvp_from_temp(conn, temp_table):
    """
    MERGE a staging temp table (columns matching the permanent table shape)
    into mlb.career_batter_vs_pitcher. Called by both the incremental loader
    and the full rebuilder once staging is populated.
    """
    conn.execute(text(f"""
        MERGE mlb.career_batter_vs_pitcher AS tgt
        USING {temp_table} AS src
        ON tgt.batter_id = src.batter_id AND tgt.pitcher_id = src.pitcher_id
        WHEN MATCHED THEN UPDATE SET
            plate_appearances = src.plate_appearances,
            at_bats           = src.at_bats,
            hits              = src.hits,
            singles           = src.singles,
            doubles           = src.doubles,
            triples           = src.triples,
            home_runs         = src.home_runs,
            rbi               = src.rbi,
            walks             = src.walks,
            strikeouts        = src.strikeouts,
            hit_by_pitch      = src.hit_by_pitch,
            sac_flies         = src.sac_flies,
            total_bases       = src.total_bases,
            batting_avg       = CASE WHEN src.at_bats > 0
                                     THEN CAST(src.hits AS DECIMAL(10,4)) / src.at_bats
                                     ELSE NULL END,
            obp               = CASE WHEN (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies) > 0
                                     THEN CAST(src.hits + src.walks + src.hit_by_pitch AS DECIMAL(10,4))
                                        / (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies)
                                     ELSE NULL END,
            slg               = CASE WHEN src.at_bats > 0
                                     THEN CAST(src.total_bases AS DECIMAL(10,4)) / src.at_bats
                                     ELSE NULL END,
            ops               = CASE WHEN src.at_bats > 0
                                           AND (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies) > 0
                                     THEN (CAST(src.hits + src.walks + src.hit_by_pitch AS DECIMAL(10,4))
                                            / (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies))
                                        + (CAST(src.total_bases AS DECIMAL(10,4)) / src.at_bats)
                                     ELSE NULL END,
            last_faced_date   = src.last_faced_date,
            updated_at        = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN INSERT (
            batter_id, pitcher_id,
            plate_appearances, at_bats, hits,
            singles, doubles, triples, home_runs,
            rbi, walks, strikeouts, hit_by_pitch, sac_flies, total_bases,
            batting_avg, obp, slg, ops,
            last_faced_date
        ) VALUES (
            src.batter_id, src.pitcher_id,
            src.plate_appearances, src.at_bats, src.hits,
            src.singles, src.doubles, src.triples, src.home_runs,
            src.rbi, src.walks, src.strikeouts, src.hit_by_pitch, src.sac_flies, src.total_bases,
            CASE WHEN src.at_bats > 0
                 THEN CAST(src.hits AS DECIMAL(10,4)) / src.at_bats
                 ELSE NULL END,
            CASE WHEN (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies) > 0
                 THEN CAST(src.hits + src.walks + src.hit_by_pitch AS DECIMAL(10,4))
                    / (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies)
                 ELSE NULL END,
            CASE WHEN src.at_bats > 0
                 THEN CAST(src.total_bases AS DECIMAL(10,4)) / src.at_bats
                 ELSE NULL END,
            CASE WHEN src.at_bats > 0
                       AND (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies) > 0
                 THEN (CAST(src.hits + src.walks + src.hit_by_pitch AS DECIMAL(10,4))
                        / (src.at_bats + src.walks + src.hit_by_pitch + src.sac_flies))
                    + (CAST(src.total_bases AS DECIMAL(10,4)) / src.at_bats)
                 ELSE NULL END,
            src.last_faced_date
        );
    """))


def load_career_bvp_for_games(engine, game_pks):
    """
    Recompute mlb.career_batter_vs_pitcher rows for every (batter_id,
    pitcher_id) pair that appears in the given game_pks.

    Unlike the at-bats materializer, this cannot use pre-diffed INSERT: a
    pair in this flush may already have a row from a previous flush and
    needs an UPDATE rather than an INSERT. Staged MERGE handles both.

    Steps:
      1. Stage the affected (batter, pitcher) pairs from player_at_bats
         where game_pk IN game_pks into #affected_pairs.
      2. Aggregate lifetime counts for those pairs across the full
         player_at_bats table via INNER JOIN to #affected_pairs into #stage_bvp.
      3. MERGE #stage_bvp into mlb.career_batter_vs_pitcher.

    SQL Server does not support tuple-IN syntax, so both staging tables
    carry their own compound PK and the second stage uses a JOIN rather
    than a (batter_id, pitcher_id) IN (...) predicate.
    """
    if not game_pks:
        return

    game_pks = list(set(int(g) for g in game_pks))
    placeholders = ", ".join(str(g) for g in game_pks)

    with engine.begin() as conn:
        conn.execute(text("""
            IF OBJECT_ID('tempdb..#affected_pairs') IS NOT NULL DROP TABLE #affected_pairs;
            IF OBJECT_ID('tempdb..#stage_bvp') IS NOT NULL DROP TABLE #stage_bvp;

            CREATE TABLE #affected_pairs (
                batter_id  INT NOT NULL,
                pitcher_id INT NOT NULL,
                PRIMARY KEY (batter_id, pitcher_id)
            );

            CREATE TABLE #stage_bvp (
                batter_id         INT NOT NULL,
                pitcher_id        INT NOT NULL,
                plate_appearances INT NOT NULL,
                at_bats           INT NOT NULL,
                hits              INT NOT NULL,
                singles           INT NOT NULL,
                doubles           INT NOT NULL,
                triples           INT NOT NULL,
                home_runs         INT NOT NULL,
                rbi               INT NOT NULL,
                walks             INT NOT NULL,
                strikeouts        INT NOT NULL,
                hit_by_pitch      INT NOT NULL,
                sac_flies         INT NOT NULL,
                total_bases       INT NOT NULL,
                last_faced_date   DATE NULL,
                PRIMARY KEY (batter_id, pitcher_id)
            );
        """))

        conn.execute(text(f"""
            INSERT INTO #affected_pairs (batter_id, pitcher_id)
            SELECT DISTINCT batter_id, pitcher_id
            FROM mlb.player_at_bats
            WHERE game_pk IN ({placeholders})
              AND batter_id IS NOT NULL
              AND pitcher_id IS NOT NULL;
        """))

        conn.execute(text(f"""
            INSERT INTO #stage_bvp (
                batter_id, pitcher_id, plate_appearances, at_bats, hits,
                singles, doubles, triples, home_runs,
                rbi, walks, strikeouts, hit_by_pitch, sac_flies, total_bases,
                last_faced_date
            )
            SELECT {BVP_AGGREGATE_SELECT}
            FROM mlb.player_at_bats AS ab
            INNER JOIN #affected_pairs AS ap
                ON ab.batter_id = ap.batter_id AND ab.pitcher_id = ap.pitcher_id
            GROUP BY ab.batter_id, ab.pitcher_id;
        """))

        result = conn.execute(text("SELECT COUNT(*) FROM #stage_bvp")).fetchone()
        staged = result[0] if result else 0
        if staged == 0:
            log.info("career_bvp: no pairs found for %d games.", len(game_pks))
            return

        _merge_bvp_from_temp(conn, "#stage_bvp")
        log.info("career_bvp: merged %d (batter, pitcher) pairs from %d games.",
                 staged, len(game_pks))


def rebuild_career_bvp(engine):
    """
    Standalone rebuilder for --rebuild-bvp mode. Rebuilds
    mlb.career_batter_vs_pitcher from the full mlb.player_at_bats table.

    Chunked by batter_id to keep the staging temp table bounded. Each chunk
    aggregates a slice of batters against all pitchers they've faced, then
    merges.

    Does NOT delete existing rows. Because every chunk MERGEs on
    (batter_id, pitcher_id), stale rows for pairs that no longer appear
    in player_at_bats would remain. That case shouldn't occur in normal
    operation (player_at_bats only grows). For a hard rebuild, DELETE
    FROM mlb.career_batter_vs_pitcher first.
    """
    with engine.connect() as conn:
        batters = [
            row[0] for row in conn.execute(text(
                "SELECT DISTINCT batter_id FROM mlb.player_at_bats "
                "WHERE batter_id IS NOT NULL ORDER BY batter_id"
            )).fetchall()
        ]

    log.info("rebuild-bvp: %d distinct batters in mlb.player_at_bats.", len(batters))
    if not batters:
        return

    CHUNK = 200
    total_pairs = 0
    for start in range(0, len(batters), CHUNK):
        chunk = batters[start:start + CHUNK]
        placeholders = ", ".join(str(b) for b in chunk)

        with engine.begin() as conn:
            conn.execute(text("""
                IF OBJECT_ID('tempdb..#stage_bvp') IS NOT NULL DROP TABLE #stage_bvp;
                CREATE TABLE #stage_bvp (
                    batter_id         INT NOT NULL,
                    pitcher_id        INT NOT NULL,
                    plate_appearances INT NOT NULL,
                    at_bats           INT NOT NULL,
                    hits              INT NOT NULL,
                    singles           INT NOT NULL,
                    doubles           INT NOT NULL,
                    triples           INT NOT NULL,
                    home_runs         INT NOT NULL,
                    rbi               INT NOT NULL,
                    walks             INT NOT NULL,
                    strikeouts        INT NOT NULL,
                    hit_by_pitch      INT NOT NULL,
                    sac_flies         INT NOT NULL,
                    total_bases       INT NOT NULL,
                    last_faced_date   DATE NULL,
                    PRIMARY KEY (batter_id, pitcher_id)
                );
            """))

            conn.execute(text(f"""
                INSERT INTO #stage_bvp (
                    batter_id, pitcher_id, plate_appearances, at_bats, hits,
                    singles, doubles, triples, home_runs,
                    rbi, walks, strikeouts, hit_by_pitch, sac_flies, total_bases,
                    last_faced_date
                )
                SELECT {BVP_AGGREGATE_SELECT}
                FROM mlb.player_at_bats AS ab
                WHERE ab.batter_id IN ({placeholders})
                  AND ab.pitcher_id IS NOT NULL
                GROUP BY ab.batter_id, ab.pitcher_id;
            """))

            result = conn.execute(text("SELECT COUNT(*) FROM #stage_bvp")).fetchone()
            staged = result[0] if result else 0
            total_pairs += staged

            if staged > 0:
                _merge_bvp_from_temp(conn, "#stage_bvp")

        log.info("rebuild-bvp: batters %d-%d of %d (%d pairs merged this chunk).",
                 start + 1, start + len(chunk), len(batters), staged)

    log.info("rebuild-bvp: done. %d total pairs merged.", total_pairs)


def load_play_by_play(engine, seasons, batch_size):
    season_list = ", ".join(str(s) for s in seasons)
    with engine.connect() as conn:
        desired = [
            (row[0], row[1]) for row in conn.execute(text(
                f"""
                SELECT game_pk, game_date
                FROM mlb.games
                WHERE game_status = 'F'
                  AND game_type = 'R'
                  AND YEAR(game_date) IN ({season_list})
                ORDER BY game_date ASC
                """
            )).fetchall()
        ]

    if not desired:
        log.info("No Final regular season games found in mlb.games for seasons %s.", seasons)
        return

    with engine.connect() as conn:
        existing = {
            row[0] for row in conn.execute(
                text("SELECT DISTINCT game_pk FROM mlb.play_by_play")
            ).fetchall()
        }

    new_games = [(pk, gd) for pk, gd in desired if pk not in existing]
    log.info(
        "play_by_play: %d desired, %d existing, %d new. Processing oldest %d.",
        len(desired), len(existing), len(new_games), min(batch_size, len(new_games))
    )

    if not new_games:
        log.info("No new PBP games to process.")
        return

    work        = new_games[:batch_size]
    flush_rows  = []
    flush_games = []

    for i, (game_pk, game_date) in enumerate(work, 1):
        game_json = fetch_game_json(game_pk)
        if game_json is None:
            log.warning("Skipping game_pk %d: no data returned.", game_pk)
            time.sleep(API_PAUSE)
            continue

        rows = parse_play_by_play(game_json, game_pk, game_date)
        if not rows:
            log.warning("game_pk %d: no play events parsed (postponed or no data).", game_pk)
            time.sleep(API_PAUSE)
            continue

        flush_rows.extend(rows)
        flush_games.append(game_pk)
        log.info("game_pk %d: %d events parsed (%d/%d).", game_pk, len(rows), i, len(work))

        if i % FLUSH_EVERY == 0 or i == len(work):
            flush(engine, flush_rows)
            log.info("Wrote %d PBP rows after game %d of %d.", len(flush_rows), i, len(work))
            load_player_at_bats_for_games(engine, flush_games)
            load_career_bvp_for_games(engine, flush_games)
            flush_rows  = []
            flush_games = []

        time.sleep(API_PAUSE)

    log.info("play_by_play load complete.")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch",   type=int, default=DEFAULT_BATCH)
    parser.add_argument("--seasons", type=int, nargs="+", default=None)
    parser.add_argument(
        "--rebuild-at-bats",
        action="store_true",
        help="Skip PBP fetch loop; rebuild mlb.player_at_bats from existing PBP data.",
    )
    parser.add_argument(
        "--rebuild-bvp",
        action="store_true",
        help="Skip PBP fetch loop; rebuild mlb.career_batter_vs_pitcher from existing player_at_bats data.",
    )
    args = parser.parse_args()

    seasons = args.seasons or SEASONS
    log.info("=== MLB Play-by-Play ETL started ===")
    log.info("Seasons: %s  Batch: %d  Rebuild at-bats: %s  Rebuild BvP: %s",
             seasons, args.batch, args.rebuild_at_bats, args.rebuild_bvp)

    engine = get_engine()
    ensure_table(engine)

    rebuild_mode = args.rebuild_at_bats or args.rebuild_bvp

    if rebuild_mode:
        if args.rebuild_at_bats:
            rebuild_player_at_bats(engine)
        if args.rebuild_bvp:
            rebuild_career_bvp(engine)
    else:
        load_play_by_play(engine, seasons, args.batch)

    log.info("=== MLB Play-by-Play ETL complete ===")


if __name__ == "__main__":
    main()
