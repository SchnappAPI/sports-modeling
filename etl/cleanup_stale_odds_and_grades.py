"""
One-shot cleanup: collapse both odds and grades to the single snapshot closest
to tipoff (strictly before commence_time) for each prop.

Rules:
  * odds.upcoming_player_props  : per (event_id, bookmaker_key, market_key,
                                  player_id, outcome_point, outcome_name)
                                  keep row with max snap_ts WHERE snap_ts <
                                  upcoming_events.commence_time. Any row with
                                  snap_ts >= commence_time is archived too.
  * common.daily_grades         : per (grade_date, event_id, player_id,
                                  market_key, bookmaker_key, line_value,
                                  outcome_name) keep row with max grade_id.

Older rows get moved to *_archive tables (not deleted) so this is reversible.

Also performs push-cycle-1 rollback: drops common.daily_grades.is_standard
and the filtered unique index uq_daily_grades_standard. These were the
previous attempt at fixing the same bug and are no longer needed.

NBA-only scope. MLB and NFL daily_grades/odds will be handled in a follow-up
once the NBA flow is verified.

Safe to re-run: every step is idempotent.
"""
import os, time
import pyodbc

NBA_SPORT_KEY = "basketball_nba"


def connect():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ['AZURE_SQL_DATABASE']};"
        f"UID={os.environ['AZURE_SQL_USERNAME']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;",
        autocommit=True,
    )


def main():
    conn = connect()
    cur = conn.cursor()

    # 1) Ensure archive tables exist (same schema as source, no rows yet).
    print("Step 1: ensure archive tables exist")
    cur.execute("""
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA='odds' AND TABLE_NAME='upcoming_player_props_archive')
BEGIN
  SELECT TOP 0 *, CAST(NULL AS DATETIME2) AS archived_at
    INTO odds.upcoming_player_props_archive
    FROM odds.upcoming_player_props;
END
""")
    cur.execute("""
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES
               WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades_archive')
BEGIN
  SELECT TOP 0 *, CAST(NULL AS DATETIME2) AS archived_at
    INTO common.daily_grades_archive
    FROM common.daily_grades;
END
""")
    print("  archives ready")

    # 2) odds.upcoming_player_props: NBA cleanup.
    print("Step 2: clean odds.upcoming_player_props (NBA)")
    t0 = time.time()
    # 2a) Archive rows whose snap_ts is at or after commence_time.
    cur.execute(f"""
WITH post_tip AS (
  SELECT upp.*
    FROM odds.upcoming_player_props upp
    JOIN odds.upcoming_events ev ON ev.event_id = upp.event_id
   WHERE ev.sport_key = '{NBA_SPORT_KEY}'
     AND upp.snap_ts >= ev.commence_time
)
INSERT INTO odds.upcoming_player_props_archive
SELECT *, SYSUTCDATETIME() AS archived_at FROM post_tip
""")
    post_tip_archived = cur.rowcount
    cur.execute(f"""
DELETE upp
  FROM odds.upcoming_player_props upp
  JOIN odds.upcoming_events ev ON ev.event_id = upp.event_id
 WHERE ev.sport_key = '{NBA_SPORT_KEY}'
   AND upp.snap_ts >= ev.commence_time
""")
    post_tip_deleted = cur.rowcount
    print(f"  post-tip rows archived+deleted: {post_tip_archived:,} (delete={post_tip_deleted:,})")

    # 2b) For each (event, market, player, book, outcome_point, outcome_name),
    #     keep only the row with max snap_ts. Archive older rows.
    cur.execute(f"""
WITH ranked AS (
  SELECT upp.event_id, upp.market_key, upp.bookmaker_key, upp.player_id,
         upp.outcome_point, upp.outcome_name, upp.snap_ts,
         ROW_NUMBER() OVER (
           PARTITION BY upp.event_id, upp.market_key, upp.bookmaker_key,
                        upp.player_id, upp.outcome_point, upp.outcome_name
           ORDER BY upp.snap_ts DESC
         ) AS rn
    FROM odds.upcoming_player_props upp
    JOIN odds.upcoming_events ev ON ev.event_id = upp.event_id
   WHERE ev.sport_key = '{NBA_SPORT_KEY}'
)
INSERT INTO odds.upcoming_player_props_archive
SELECT upp.*, SYSUTCDATETIME() AS archived_at
  FROM odds.upcoming_player_props upp
  JOIN ranked r
    ON r.event_id=upp.event_id AND r.market_key=upp.market_key
   AND r.bookmaker_key=upp.bookmaker_key AND r.player_id=upp.player_id
   AND r.outcome_point=upp.outcome_point AND r.outcome_name=upp.outcome_name
   AND r.snap_ts=upp.snap_ts
 WHERE r.rn > 1
""")
    old_snap_archived = cur.rowcount
    cur.execute(f"""
WITH ranked AS (
  SELECT upp.event_id, upp.market_key, upp.bookmaker_key, upp.player_id,
         upp.outcome_point, upp.outcome_name, upp.snap_ts,
         ROW_NUMBER() OVER (
           PARTITION BY upp.event_id, upp.market_key, upp.bookmaker_key,
                        upp.player_id, upp.outcome_point, upp.outcome_name
           ORDER BY upp.snap_ts DESC
         ) AS rn
    FROM odds.upcoming_player_props upp
    JOIN odds.upcoming_events ev ON ev.event_id = upp.event_id
   WHERE ev.sport_key = '{NBA_SPORT_KEY}'
)
DELETE upp
  FROM odds.upcoming_player_props upp
  JOIN ranked r
    ON r.event_id=upp.event_id AND r.market_key=upp.market_key
   AND r.bookmaker_key=upp.bookmaker_key AND r.player_id=upp.player_id
   AND r.outcome_point=upp.outcome_point AND r.outcome_name=upp.outcome_name
   AND r.snap_ts=upp.snap_ts
 WHERE r.rn > 1
""")
    old_snap_deleted = cur.rowcount
    print(f"  older-snap rows archived+deleted: {old_snap_archived:,} (delete={old_snap_deleted:,})")
    print(f"  upcoming_player_props cleanup done in {time.time()-t0:.1f}s")

    # 3) common.daily_grades: NBA cleanup.
    print("Step 3: clean common.daily_grades (NBA)")
    t0 = time.time()

    # Scope rows that come from NBA via their event_id presence in odds.upcoming_events
    # (same scoping as above). This avoids touching MLB/NFL.
    cur.execute(f"""
WITH ranked AS (
  SELECT dg.grade_id,
         ROW_NUMBER() OVER (
           PARTITION BY dg.grade_date, dg.event_id, dg.player_id, dg.market_key,
                        dg.bookmaker_key, dg.line_value, dg.outcome_name
           ORDER BY dg.grade_id DESC
         ) AS rn
    FROM common.daily_grades dg
    JOIN odds.upcoming_events ev ON ev.event_id = dg.event_id
   WHERE ev.sport_key = '{NBA_SPORT_KEY}'
)
INSERT INTO common.daily_grades_archive
SELECT dg.*, SYSUTCDATETIME() AS archived_at
  FROM common.daily_grades dg
  JOIN ranked r ON r.grade_id = dg.grade_id
 WHERE r.rn > 1
""")
    dg_archived = cur.rowcount
    cur.execute(f"""
WITH ranked AS (
  SELECT dg.grade_id,
         ROW_NUMBER() OVER (
           PARTITION BY dg.grade_date, dg.event_id, dg.player_id, dg.market_key,
                        dg.bookmaker_key, dg.line_value, dg.outcome_name
           ORDER BY dg.grade_id DESC
         ) AS rn
    FROM common.daily_grades dg
    JOIN odds.upcoming_events ev ON ev.event_id = dg.event_id
   WHERE ev.sport_key = '{NBA_SPORT_KEY}'
)
DELETE dg
  FROM common.daily_grades dg
  JOIN ranked r ON r.grade_id = dg.grade_id
 WHERE r.rn > 1
""")
    dg_deleted = cur.rowcount
    print(f"  older grade rows archived+deleted: {dg_archived:,} (delete={dg_deleted:,})")
    print(f"  daily_grades cleanup done in {time.time()-t0:.1f}s")

    # 4) Drop is_standard filtered unique index if present (push cycle 1 rollback).
    print("Step 4: drop uq_daily_grades_standard if present")
    cur.execute("""
IF EXISTS (
  SELECT 1 FROM sys.indexes
   WHERE name='uq_daily_grades_standard'
     AND object_id=OBJECT_ID('common.daily_grades')
)
  DROP INDEX uq_daily_grades_standard ON common.daily_grades
""")
    print("  index dropped (or did not exist)")

    # 5) Drop is_standard column (push cycle 1 rollback).
    print("Step 5: drop common.daily_grades.is_standard if present")
    cur.execute("""
IF EXISTS (
  SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
   WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades' AND COLUMN_NAME='is_standard'
)
BEGIN
  IF EXISTS (SELECT 1 FROM sys.default_constraints WHERE name='df_daily_grades_is_standard')
    ALTER TABLE common.daily_grades DROP CONSTRAINT df_daily_grades_is_standard;
  ALTER TABLE common.daily_grades DROP COLUMN is_standard;
END
""")
    print("  column dropped (or did not exist)")

    # 6) Verify.
    print("Step 6: verify")
    cur.execute("""
SELECT COUNT(*) AS upp_rows FROM odds.upcoming_player_props upp
  JOIN odds.upcoming_events ev ON ev.event_id=upp.event_id
 WHERE ev.sport_key = 'basketball_nba'""")
    print(f"  upcoming_player_props NBA rows remaining: {cur.fetchone()[0]:,}")

    cur.execute("""
SELECT COUNT(*) FROM common.daily_grades dg
  JOIN odds.upcoming_events ev ON ev.event_id=dg.event_id
 WHERE ev.sport_key = 'basketball_nba'""")
    print(f"  daily_grades NBA rows remaining: {cur.fetchone()[0]:,}")

    cur.execute("SELECT COUNT(*) FROM odds.upcoming_player_props_archive")
    print(f"  upcoming_player_props_archive: {cur.fetchone()[0]:,}")
    cur.execute("SELECT COUNT(*) FROM common.daily_grades_archive")
    print(f"  daily_grades_archive: {cur.fetchone()[0]:,}")

    # Spot check: Maxey's game 2026-04-19 player_points
    print("Step 7: Maxey player_points 04-19 remaining rows")
    cur.execute("""
SELECT dg.line_value, dg.outcome_name, dg.over_price, dg.composite_grade, dg.grade_id
  FROM common.daily_grades dg
  JOIN nba.players p ON p.player_id = dg.player_id
 WHERE p.player_name LIKE '%Tyrese Maxey%'
   AND dg.bookmaker_key='fanduel' AND dg.market_key='player_points'
   AND dg.grade_date='2026-04-19'
 ORDER BY dg.line_value, dg.outcome_name""")
    for r in cur.fetchall():
        print(f"  {r}")

    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()
