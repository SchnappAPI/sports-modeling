"""
One-shot migration + backfill for common.daily_grades.is_standard.

Adds the column if missing, then sets is_standard=1 on rows that correspond to
the posted standard line. Two-step inference (matches the grader's forward logic):

  1. market_key IN (STANDARD_MARKETS) AND outcome_name='Over' AND over_price IS NOT NULL
     -> the step=0 row in build_standard_props got the real over_price; bracket rows did not.
  2. market_key IN (STANDARD_MARKETS) AND outcome_name='Under'
     -> build_under_props only writes Under rows at the posted center line.

Rows not matching either rule stay is_standard=0 (includes alt market rows, bracket
Over rows without a posted price, and any rare edge cases).

Safe to re-run: UPDATE is idempotent; ALTER TABLE is guarded by INFORMATION_SCHEMA check.
"""
import os, time
import pyodbc

STANDARD_MARKETS = (
    'player_points', 'player_rebounds', 'player_assists', 'player_threes',
    'player_blocks', 'player_steals',
    'player_points_rebounds_assists', 'player_points_rebounds',
    'player_points_assists', 'player_rebounds_assists',
    'player_double_double', 'player_triple_double', 'player_first_basket',
)

def main():
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={os.environ['AZURE_SQL_SERVER']};"
        f"DATABASE={os.environ['AZURE_SQL_DATABASE']};"
        f"UID={os.environ['AZURE_SQL_USERNAME']};"
        f"PWD={os.environ['AZURE_SQL_PASSWORD']};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    conn = pyodbc.connect(conn_str, autocommit=True)
    cur = conn.cursor()

    # 1) Add column if not exists (NOT NULL DEFAULT 0 is safe because SQL Server
    #    will backfill existing rows to 0 at ALTER time).
    print("Step 1: ensure column exists")
    cur.execute("""
IF NOT EXISTS (
  SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA='common' AND TABLE_NAME='daily_grades' AND COLUMN_NAME='is_standard'
)
  ALTER TABLE common.daily_grades ADD is_standard BIT NOT NULL CONSTRAINT df_daily_grades_is_standard DEFAULT 0
""")
    print("  column ensured")

    # 2) Add filtered unique index (one posted-standard Over and one posted-standard
    #    Under per grade_date/event/player/market/book). Guarded so re-runs are safe.
    print("Step 2: ensure filtered unique index")
    cur.execute("""
IF NOT EXISTS (
  SELECT 1 FROM sys.indexes WHERE name='uq_daily_grades_standard'
    AND object_id=OBJECT_ID('common.daily_grades')
)
  CREATE UNIQUE INDEX uq_daily_grades_standard
  ON common.daily_grades (grade_date, event_id, player_id, market_key, bookmaker_key, outcome_name)
  WHERE is_standard = 1
""")
    print("  index ensured")

    mkt_list = ",".join(f"'{m}'" for m in STANDARD_MARKETS)

    # 3) Backfill Over rows where over_price IS NOT NULL
    print("Step 3: backfill Over posted-standard rows (NBA)")
    t0 = time.time()
    sql_over = f"""
UPDATE dg
   SET dg.is_standard = 1
  FROM common.daily_grades dg
 WHERE dg.is_standard = 0
   AND dg.market_key IN ({mkt_list})
   AND dg.outcome_name = 'Over'
   AND dg.over_price IS NOT NULL
"""
    cur.execute(sql_over)
    over_rows = cur.rowcount
    print(f"  Over rows updated: {over_rows} in {time.time()-t0:.1f}s")

    # 4) Backfill Under rows (all Under rows in standard markets are at posted center)
    print("Step 4: backfill Under posted-standard rows (NBA)")
    t0 = time.time()
    sql_under = f"""
UPDATE dg
   SET dg.is_standard = 1
  FROM common.daily_grades dg
 WHERE dg.is_standard = 0
   AND dg.market_key IN ({mkt_list})
   AND dg.outcome_name = 'Under'
"""
    cur.execute(sql_under)
    under_rows = cur.rowcount
    print(f"  Under rows updated: {under_rows} in {time.time()-t0:.1f}s")

    # 5) Summary
    print("Step 5: verify")
    cur.execute("""
SELECT is_standard, COUNT(*) AS rows
  FROM common.daily_grades
 GROUP BY is_standard
 ORDER BY is_standard
""")
    for r in cur.fetchall():
        print(f"  is_standard={r[0]}: {r[1]:,} rows")

    # 6) Spot check: Maxey, should now return exactly one Over and one Under per game/market
    print("Step 6: spot check (Maxey player_points)")
    cur.execute("""
SELECT TOP 20 egm.game_id, dg.grade_date, dg.outcome_name, dg.line_value, dg.over_price
  FROM common.daily_grades dg
  JOIN odds.event_game_map egm ON egm.event_id = dg.event_id
  JOIN nba.players p ON p.player_id = dg.player_id
 WHERE p.player_name LIKE '%Tyrese Maxey%'
   AND dg.bookmaker_key = 'fanduel'
   AND dg.market_key = 'player_points'
   AND dg.is_standard = 1
 ORDER BY egm.game_id DESC, dg.outcome_name
""")
    for r in cur.fetchall():
        print(f"  {r}")

    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()
