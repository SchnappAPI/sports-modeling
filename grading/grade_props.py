    # Build per-(player_id, market_key) lists of all available lines and prices
    # for tier computation. Only Over rows; tier lines always expressed as Over.
    outcome_name_col = graded_df["outcome_name"] if "outcome_name" in graded_df.columns else pd.Series("Over", index=graded_df.index)
    player_market_lines: dict = {}