def fetch_lineups_for_game_date(game_date):
    date_key = game_date.strftime("%Y%m%d")
    url      = f"https://stats.nba.com/js/data/leaders/00_daily_lineups_{date_key}.json"
    data     = _direct_get(url, f"daily_lineups {date_key}", proxies=get_proxies(), timeout=30)
    if data is None:
        return []
    rows = []
    for g in data.get("games", []):
        game_id = safe_str(g.get("gameId"))
        if game_id is None:
            continue
        for side, home_away in (("homeTeam", "Home"), ("awayTeam", "Away")):
            team    = g.get(side, {})
            tricode = safe_str(team.get("teamAbbreviation"))
            for p in team.get("players", []):
                pos    = safe_str(p.get("position"))
                roster = safe_str(p.get("rosterStatus"))
                lineup = safe_str(p.get("lineupStatus")) or ""
                # Mark as Inactive if lineupStatus signals unavailability,
                # regardless of rosterStatus. Active roster players listed as
                # Out/Inactive/Not With Team should not appear as Bench.
                inactive_keywords = ("out", "inactive", "not with team", "gtd")
                if any(kw in lineup.lower() for kw in inactive_keywords):
                    starter = "Inactive"
                elif pos:
                    starter = "Starter"
                elif roster == "Active":
                    starter = "Bench"
                else:
                    starter = "Inactive"
                rows.append({
                    "game_id":        game_id,
                    "game_date":      game_date,
                    "home_away":      home_away,
                    "team_tricode":   tricode,
                    "player_name":    safe_str(p.get("playerName")),
                    "position":       pos,
                    "lineup_status":  lineup or None,
                    "roster_status":  roster,
                    "starter_status": starter,
                })
    return rows
