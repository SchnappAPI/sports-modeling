# Glossary

Domain vocabulary for the project. Cross-sport terms come first, then sport-specific sections.

## Cross-sport terms

**At a Glance**: A grid view that surfaces all upcoming player props across all games for a sport, sorted and filterable by signal strength, odds, and other criteria. Designed for quick scanning before placing bets.

**Composite grade**: A 0 to 100 score combining all available grading components (weighted hit rate, trend, momentum, pattern, matchup, regression). Equal-weighted average of non-NULL components. Higher score means stronger over signal; for under bets, components are inverted before averaging.

**Connected visual**: A page-level pattern where multiple visuals on the same page subscribe to a shared selection state (typically a selected player). Tapping a different player updates every visual at once. See `/docs/PRODUCT_BLUEPRINT.md`.

**Demo mode**: A passcode-gated mode that shows the site as it appeared on a fixed historical date so prospective users can explore without seeing live data. Configured in `common.demo_config`.

**Game page**: The hub view for a single matchup. Contains lineups, props, live stats, matchups, and the at-a-glance summary scoped to that game.

**Grade**: A 0 to 100 score on a single prop reflecting predicted strength. Subdivided into component grades that each measure one signal (recent form, momentum, matchup, etc.).

**Grading**: The pipeline that produces grades. Runs after odds ingestion fetches the day's lines.

**Outcome**: Over or Under on a prop line. Each (player, market, line) can have both outcomes graded separately as of grading schema v3.

**Player page**: The drill-down view for a single player. Shows game log, splits, current props, and recent trends.

**Player prop**: A bet on whether a specific player's stat will go over or under a posted line. Distinct from team props (game total, spread).

**Posted line**: The standard line offered by the bookmaker. Distinct from alternate lines (alt lines), which are offered at varied prices for the same market.

**Signal**: A discrete tag attached to a prop indicating a notable pattern. Examples: STREAK (strong recent run), DUE (bounce-back from miss streak), HOT/COLD (player-level form).

**STATUS line**: The first line of every component README, stating one of: live, in development, design phase, planned. Used by sessions to gauge maturity at a glance.

## NBA-specific

**3PM, 3PA, FG, FGM, FGA, FT, FTM, FTA**: Standard basketball stat abbreviations (three-pointers made, three-pointers attempted, field goals, etc.).

**boxscoretraditionalv3, leaguedashptstats, playergamelogs**: NBA Stats API endpoints used by the ETL.

**G/F/C grouping**: Position groups used in the matchup defense view. PG and SG map to G; SF and PF map to F; C is C. Implemented in `posToGroup()`. Do not use `position[0]` for grouping.

**MIN**: Minutes played, shown as `mm:ss` (e.g., `21:49`). Prefix `*` indicates the player started.

**PRA, PR, PA, RA**: Composite scoring stats. PRA = points + rebounds + assists. PR = points + rebounds. PA = points + assists. RA = rebounds + assists. All four are common prop markets.

**Period**: A quarter or overtime segment. Stored as `'1Q'`, `'2Q'`, `'3Q'`, `'4Q'`, `'OT'` in `nba.player_box_score_stats`. The column is VARCHAR(2); do not insert longer values.

## MLB-specific

**At Bat (AB)**: A plate appearance that resulted in a hit, out, or other completed at-bat (excludes walks, HBP, sacrifices). Tracked at the pitch level in Statcast data.

**Barrel**: A batted ball with combination of exit velocity and launch angle that historically produces a high slugging percentage. Tracked as `is_speedangle_barrel` in Statcast.

**BABIP**: Batting average on balls in play. Excludes home runs and strikeouts from both numerator and denominator.

**Batter vs Pitcher (BvP)**: Career stats for a specific batter against a specific pitcher. Pulled from Baseball Savant's matchup endpoint. Cached in the `Matchups` query output (legacy PBI naming; web app naming TBD).

**Box score**: Full per-player stats for a single game. From the MLB Stats API `/withMetrics` endpoint, both game-level (current game) and season-level (year-to-date) versions are included in one response.

**Exit velocity (EV)**: Speed of the ball off the bat in mph. Statcast measurement. Visualized prominently in the EV page.

**Hard hit**: Batted ball with exit velocity 95 mph or higher. Tracked as `is_hit_into_play_hardhit`.

**Hot/Cold zones**: A 13-zone grid representing the strike zone, with each zone showing a player's batting average, OBP, SLG, or xBA. Color-coded hot to cold.

**Plate appearance (PA)**: Any time a batter completes a turn at the plate, including walks, HBP, sacrifices, and at-bats. Superset of "at bat".

**Probable pitcher**: The starting pitcher expected to pitch in an upcoming game. Pulled from MLB Stats API schedule with `hydrate=probablePitcher`.

**Spray chart**: Visual showing where a batter's hits go in the field. Statcast-derived.

**Statcast**: MLB's pitch-tracking system. Provides exit velocity, launch angle, expected stats, swing path, timing metrics, and many other measurements at the pitch level.

**withMetrics endpoint**: `https://statsapi.mlb.com/api/v1/game/{gameID}/withMetrics`. Single endpoint that returns box scores, season stats, play-by-play, and pitch data for a game. Most of the MLB ETL pulls from this one URL.

**xBA, xSLG, xwOBA**: Expected stats based on exit velocity and launch angle, independent of defensive positioning. xBA = expected batting average, xSLG = expected slugging, xwOBA = expected weighted on-base average.

## NFL-specific

To be populated as NFL build progresses. Placeholder for terms like snap count, target share, route participation, red-zone usage, etc.
