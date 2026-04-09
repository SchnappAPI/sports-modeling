# Historical Backfill Scripts

One-time backfill scripts for MLB, NBA, and NFL historical data stored as Parquet in Azure Blob Storage.

## Storage

All data lands in `schnappmlbdata` storage account across three containers:

| Container | Sport | Size | Tables |
|---|---|---|---|
| `mlb-backfill` | MLB | ~3.5 GB | statcast_pitches, game_boxscores_batting, game_boxscores_pitching, play_by_play, games, batting_stats_season, pitching_stats_season, players, teams |
| `nba-backfill` | NBA | ~492 MB | play_by_play_nbastats, play_by_play_pbpstats, shot_detail, game_logs, players, teams |
| `nfl-backfill` | NFL | ~147 MB | play_by_play, player_stats, rosters, schedules, players, teams |

## Season Coverage
- MLB: 2015-2026
- NBA: 2015-2025
- NFL: 2015-2025

## Running

All scripts read credentials from environment variables:
```
AZURE_STORAGE_ACCOUNT=schnappmlbdata
AZURE_STORAGE_KEY=<key>
AZURE_STORAGE_CONTAINER=<mlb-backfill|nba-backfill|nfl-backfill>
NBA_PROXY_URL=<webshare proxy url>  # NBA only
```

Scripts are checkpoint-safe: interrupted runs resume from where they left off. Errors are logged to `_errors/` in each container without stopping the run.

## Sources
- MLB Statcast: pybaseball (Baseball Savant)
- MLB Stats API: python-mlb-statsapi
- MLB FanGraphs: pybaseball
- NBA PBP/shots: shufinskiy/nba_data (GitHub pre-built files)
- NBA game logs: nba_api (stats.nba.com via proxy)
- NFL all tables: nflreadpy (nflverse GitHub releases)
