"""One-off: add 21 additive columns to common.player_tier_lines per ADR-20260424-5."""
from etl.db import get_engine
from sqlalchemy import text

NEW_COLUMNS = [
    # 16 per-tier hit evidence
    ("safe_hits_all", "INT"),
    ("safe_games_all", "INT"),
    ("safe_hits_20", "INT"),
    ("safe_games_20", "INT"),
    ("value_hits_all", "INT"),
    ("value_games_all", "INT"),
    ("value_hits_20", "INT"),
    ("value_games_20", "INT"),
    ("highrisk_hits_all", "INT"),
    ("highrisk_games_all", "INT"),
    ("highrisk_hits_20", "INT"),
    ("highrisk_games_20", "INT"),
    ("lotto_hits_all", "INT"),
    ("lotto_games_all", "INT"),
    ("lotto_hits_20", "INT"),
    ("lotto_games_20", "INT"),
    # 2 tier prices
    ("safe_price", "INT"),
    ("value_price", "INT"),
    # 3 per-player-market opportunity context
    ("recent_minutes_20", "FLOAT"),
    ("recent_opportunity", "FLOAT"),
    ("historical_opportunity", "FLOAT"),
]


def main():
    eng = get_engine()
    with eng.begin() as conn:
        for col, typ in NEW_COLUMNS:
            sql = (
                "IF NOT EXISTS (SELECT 1 FROM sys.columns "
                "WHERE object_id = OBJECT_ID(\'common.player_tier_lines\') "
                f"AND name = \'{col}\') "
                f"ALTER TABLE common.player_tier_lines ADD {col} {typ} NULL"
            )
            conn.execute(text(sql))
            print(f"  added/verified: {col} {typ}")

        rows = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = \'common\' AND table_name = \'player_tier_lines\' "
            "ORDER BY ordinal_position"
        )).fetchall()
        print()
        print(f"Total columns in common.player_tier_lines: {len(rows)}")
        for col, _ in NEW_COLUMNS:
            present = any(r[0] == col for r in rows)
            status = "OK" if present else "MISSING"
            print(f"  {col}: {status}")
            assert present, f"{col} missing after migration"
        print("All 21 columns present.")


if __name__ == "__main__":
    main()
