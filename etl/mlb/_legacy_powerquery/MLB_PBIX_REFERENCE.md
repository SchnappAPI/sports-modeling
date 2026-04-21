# MLB Power BI File (mlbSavantV3.pbix) - Full Reference

This document is the authoritative catalog of everything in `C:\Users\1stLake\OneDrive - Schnapp\mlbSavantV3.pbix` and the companion documents `mlbStatQueries.docx` and `miscMLBinstructions.docx`. It is the foundation for MLB schema design in Azure SQL and for deciding what to keep, consolidate, or remove in the PBI file.

**Sources processed:**
- `mlbSavantV3.pbix` - extracted via zip, 10 pages, 135 visuals parsed
- `mlbStatQueries.docx` - 4,508 lines of M code across 61 named query sections
- `miscMLBinstructions.docx` - authoring rules and the `fnGet*/all*` consolidation pattern spec

**Scope:** Every page, every visual, every field reference, every M query, every API endpoint, and every dependency chain. Relevant and irrelevant content alike, so you can see the full surface area before making cut/keep decisions.

---

## 1. Architecture at a glance

The PBI file has three tiers:

1. **Staging queries** defined in Power Query (documented in `mlbStatQueries.docx`). These fetch raw JSON from MLB Stats API and Baseball Savant, shape it into flat tables, and sometimes cache results in Excel workbook tables for incremental refresh. 61 distinct query sections in the docx.
2. **Model tables** inside the PBI (not in the M code docx). These are the tables visuals actually bind to. Some are renames or subsets of staging queries; some are calculated tables or DAX model artifacts. 16 tables/aliases referenced by visuals.
3. **Visuals** on 10 pages that read from the model. 8 pages are visible; 2 are duplicates or prototypes.

**Key finding:** the M code file contains only the *ingestion* layer. The visual-facing tables (`BATTER`, `PLAYS`, `PITCHER`, `BOXSCORE_DETAIL`, `pitcherSeasonData`, `pitcherStatsSEASON`, `UpcomingGameData`, `Measure`) and every derived column and measure used in visuals (HR Pattern, L5AB EV, Pattern HitRate, _xH, _HR_prob, vs BA, vs EV, etc.) live *inside the PBI model* as DAX or as transforms applied during model load, not in the Power Query layer that was documented externally. This gap is the single most important thing to resolve during schema design.

---

## 2. Page inventory

10 pages total. Listed in `pageOrder` sequence.

| # | Display Name | Visual Count | Visibility | Functional Role |
|---|---|---|---|---|
| 1 | Game | 5 | Visible | Landing/selector page (pick a game) |
| 2 | New | 19 | Visible | Player Analysis iteration A |
| 3 | Duplicate of New | 19 | Visible | Player Analysis iteration B (full copy) |
| 4 | Extra | 20 | Visible | Player Analysis iteration C |
| 5 | Criteria | 17 | Visible | Player Analysis iteration D |
| 6 | EV | 11 | Visible | Team-wide Exit Velocity pivot |
| 7 | MAIN | 13 | Visible | Player Analysis canonical (cleanest version) |
| 8 | VS | 8 | Visible | Lineup-wide career matchup vs next pitcher |
| 9 | Proj | 6 | Visible | Lineup projection table |
| 10 | Duplicate of Extra | 16 | **HIDDEN** | Pitcher Analysis prototype |

**Pages 2, 3, 4, 5, 7** are five versions of the same Player Analysis concept. Pages 2 and 3 are identical (19 visuals each, same field refs). Page 7 (MAIN) is a cleaner 13-visual reduction of the same concept and is the canonical keeper.

**Page 10 (Duplicate of Extra)** is hidden and is the pitcher-focused counterpart. It references the `PITCHER` table, which no other page uses. Keep the concept for Pitcher Analysis.

---

## 3. Model tables and fields

Every table that any visual references, with every field referenced from that table. Measures are marked `(M)`; columns are unmarked. Aliases `b`/`p`/`m`/`t`/`u`/`p1`/`p2` are short names for the same tables used inside DAX queries and are consolidated here.

### 3.1 BATTER (primary batter-level projection table)
Used on every page except Game. The single most referenced table in the file.

**Columns (16):**
- `player_name` - batter's name (display)
- `team_name` - batter's team
- `gameDisplay` - formatted game label (e.g., "LAD @ SF 7:05p")
- `gameTime` - kickoff time for display
- `vs_pitcher_name` - opposing pitcher's name for that game
- `Side` - batter handedness (L/R/S)
- `battingPosition` - lineup slot (1-9), used as column reference
- `xH` - expected hits (static/pre-calc column form)
- `xHR` - expected home runs (column form)
- `x1B`, `x2B`, `x3B` - expected singles/doubles/triples (column form)
- `xBB` - expected walks (column form)
- `xSO` - expected strikeouts (column form)
- `HR_prob` - home run probability (column form)
- `H_prob` - hit probability (column form)

**Measures (18):** DAX measures prefixed with underscore are the "official" measure equivalents of the column-form fields above; they likely normalize or recompute against current filter context.
- `_xH`, `_xHR`, `_x1B`, `_x2B`, `_x3B`, `_xBB`, `_xK`, `_xR`, `_xRBI`, `_xTB`, `_xXBH`, `_xHRR`
- `_HR_prob`, `_H_prob`
- `_battingPosition`, `_#` (row number variants)
- `#` (row index), `AB#` (at-bat index within game)

### 3.2 PLAYS (pitch/play-by-play fact table, hitting side)
Used on every Player Analysis page and EV.

**Columns (15):**
- `Index` - sort index within a game
- `PitcherGameIndex` - pitcher's game sequence (used on Pitcher Analysis)
- `Result` - play outcome text ("Home Run", "Strikeout", etc.)
- `at_bat_number` - sequence of the at-bat in the game
- `batter_name`, `pitcher_name` - participant names
- `game_date` - date of the game
- `inning` - inning number
- `team_batting`, `team_pitching` - team labels
- `pGID` - player-game ID (concat of playerID + gameID)
- `playedABid` - at-bat identifier within played games
- `playerGameAtBatNumber` - PA index per player-game
- `isNextPitcherHand` - boolean flag: does this play match the next pitcher's hand? (used for platoon split filtering)
- `is_lastpitch` - boolean: is this the last pitch of a PA? (used to dedupe pitch rows to PA-level rows)

**Measures (24):**
- Exit velocity / launch: `Average Exit Velo`, `Max Exit Velo`, `Average Launch Angle`, `Average Hit Dist.`, `isHardHit`, `Exit Velo 95+`, `Avg EV Above Escape Velo`
- Expected stats: `Average xBA`, `Average adjxBA`, `Average xAvg`, `Average wOBA`, `TotalBases play`, `Avg`
- Last-N aggregates: `Last 2 Game Avg EV`, `Last 2 Game HH`, `Last 3 Game Total xBA`, `Last Game Max EV`, `L5AB EV`
- Pattern indicators: `HR Pattern Early`, `HR Pattern Late`, `Pattern HitRate`, `Games Since HR`, `Games Since Last HR`
- Lineup helpers: `batOrder`

### 3.3 PlayEVENTS (pitch-event-level fact table, event flags)
Used alongside PLAYS on every Player Analysis page.

**Measures (10):** all boolean aggregates over pitch events
- `isAtBat`, `isPA`, `isBaseHit`, `isHitIntoPlay`, `isHomeRun`, `isStrikeout`, `isWalk`, `isXBH`
- `BABIP` - batting average on balls in play
- `Hit Into Play Per PA %` - ratio

**No columns referenced** - it's used only via its measures.

### 3.4 BOXSCORE_DETAIL (game outcome measures)
Used on every Player Analysis page.

**Measures (3):**
- `Runs`, `RBIs`, `HRR` (HR rate?)

**No columns referenced.** This is effectively a measure container.

### 3.5 Measure (vs-pitcher matchup measures container)
This is a "measure table" - a table that exists only to hold DAX measures grouped under a common prefix. Used on all Player Analysis pages and VS.

**Measures (22):** all begin with "vs " - career stats of the batter vs the specific vs_pitcher
- Counting: `vs PA`, `vs AB`, `vs H`, `vs HR`, `vs SO`, `vs BB`, `vs 2B`, `vs 3B`, `vs TB`, `vs XBH`, `vs Barrels`
- Rate: `vs BA`, `vs BABIP`, `vs xBA`, `vs xwOBA`, `vs K%`, `vs BB%`, `vs Whiff%`
- Statcast: `vs EV`, `vs LA`, `vs Dist`
- Flag: `HR Hot` (lives here despite the name not starting with "vs")

These measures almost certainly read from the Matchups query output (documented in the M code file).

### 3.6 PITCHER (pitcher-level analog of BATTER)
Only used on Duplicate of Extra (the hidden Pitcher Analysis page).

**Columns (3):** `player_name`, `team_name`, `starter`
**Measures (2):** `_xHA` (expected hits allowed), `_xHRA` (expected HRs allowed)

This table is sparsely defined because Pitcher Analysis was only a prototype.

### 3.7 pitcherSeasonData (pitcher season stats)
Used on all Player Analysis pages + VS + Duplicate of Extra.

**Columns (1):** `player_name`
**Measures (9):** `ERA`, `IP`, `Hits Allowed`, `HRs Allowed`, `Runs Allowed`, `Walks`, `Strikeouts`, `Batters Faced`, `Avg Against`

### 3.8 pitcherStatsSEASON (raw pitcher stats, separate from above)
Used on all Player Analysis pages + VS + Duplicate of Extra.

**Columns (1):** `inningsPitched`

Only one column referenced. Likely redundant with `pitcherSeasonData.IP` - **consolidation candidate**.

### 3.9 UpcomingGameData (today's game list, calculated table)
Used on every page for game selection. Almost certainly a DAX calculated table - does not appear in the M code file.

**Columns (6):**
- `Display Game` - formatted game label
- `gameDisplay` - alternate display
- `gameTime` - time string
- `game_date` - date
- `game_abstractGameState` - game state (Preview/Live/Final)
- `IsToday` - boolean flag

### 3.10 TEAMGAME (one row per team-game)
Defined in M code (staging query). Used as a slicer on every page.

**Columns referenced by visuals (1):** `game_date` - but the staging query produces ~20 columns including `teamID`, `vsTeamID`, `gameID`, `teamGameID`, `vsPitcherID`, etc. Most are consumed by downstream queries, not by visuals.

### 3.11 TEAM (teams reference)
Defined in M code. Used only in one role-playing context.

**Columns referenced (1):** `team_name`. Staging query produces ~3 columns.

### 3.12 Alias tables (not real tables)
`b`, `p`, `m`, `t`, `u`, `p1`, `p2` are query aliases from DAX SUMMARIZECOLUMNS constructs. Each resolves to:
- `b` -> BATTER
- `p`, `p1`, `p2` -> PLAYS
- `m` -> Measure
- `t` -> TEAMGAME
- `u` -> UpcomingGameData

These are not separate entities. They show up in the raw visual JSON because the DAX engine assigns short aliases to source references inside projection expressions.

---

## 4. Page-by-page visual inventory

For each page, every visual is listed with visual type and the exact tables/fields it binds to. Fields marked `*` are measures.

### 4.1 Game (5 visuals) - Selector
1. `slicer` -> TEAMGAME[game_date]
2. `advancedSlicerVisual` -> UpcomingGameData[Display Game, IsToday, gameDisplay, game_abstractGameState]
3. `advancedSlicerVisual` -> PLAYS[isNextPitcherHand]
4. `advancedSlicerVisual` -> UpcomingGameData[Display Game, IsToday, gameDisplay, gameTime, game_abstractGameState]
5. `pageNavigator` (nav only)

### 4.2 New (19 visuals) - Player Analysis A
**Duplicate of MAIN.** See MAIN (4.7) for the canonical field list. New has the same visuals plus: explicit duplicate slicers, extra card visuals for gameDisplay and gameTime separately, and a team-overview pivot with one extra column (`Side`). All visuals in New are reproduced in MAIN or are cosmetic duplicates.

### 4.3 Duplicate of New (19 visuals)
**Byte-for-byte copy of New.** Every visual has identical refs. Only difference is layout coordinates. Pure delete candidate.

### 4.4 Extra (20 visuals) - Player Analysis C
Superset of New by one visual: an extra `card` showing BATTER[vs_pitcher_name] as a standalone field readout, plus a `textbox` for documentation. Otherwise the same core set as MAIN.

### 4.5 Criteria (17 visuals) - Player Analysis D
Reduced variant of Extra; drops redundant cards and one of the two textboxes. Still has the same core analytical visuals as MAIN.

### 4.6 EV (11 visuals) - Exit Velocity team view
1. `pivotTable` -> BATTER[player_name, team_name]; BOXSCORE_DETAIL[RBIs*, Runs*]; Measure[HR Hot*]; PLAYS[Average Exit Velo*, Average xBA*, Avg*, Games Since HR*, Max Exit Velo*, Pattern HitRate*, TotalBases play*, batOrder*, batter_name, isHardHit*, team_batting]; PlayEVENTS[BABIP*, isHomeRun*, isPA*, isStrikeout*, isWalk*, isXBH*]
2. `tableEx` -> BATTER[AB#*]; PLAYS[Average Exit Velo*, Average Hit Dist.*, Average Launch Angle*, Average xBA*, Index, Result, at_bat_number, batter_name, game_date, inning, is_lastpitch, playedABid, team_pitching]
3. `advancedSlicerVisual` -> PLAYS[isNextPitcherHand]
4. `slicer` -> UpcomingGameData[gameDisplay, game_date]
5. `slicer` -> PLAYS[playerGameAtBatNumber]
6. `pageNavigator`
7. `advancedSlicerVisual` -> UpcomingGameData[Display Game, IsToday, gameDisplay, game_abstractGameState]
8. `card` -> BATTER[team_name]
9. `cardVisual` -> (same "HR Hot" card as other pages; see MAIN visual #9)
10. `slicer` -> PLAYS[game_date]
11. `advancedSlicerVisual` -> PLAYS[game_date]

Two distinct visuals here not found on MAIN: the team-level pivot (all 9 batters at once) and the detailed at-bat log with batter_name column.

### 4.7 MAIN (13 visuals) - Player Analysis canonical
This is the reduced, clean version. Use this as the source of truth for Player Analysis.

1. `slicer` -> TEAMGAME[game_date]
2. `pivotTable` **[Pitcher Season Stats]** -> pitcherSeasonData[Avg Against*, Batters Faced*, ERA*, HRs Allowed*, Hits Allowed*, IP*, Runs Allowed*, Strikeouts*, Walks*, player_name]; pitcherStatsSEASON[inningsPitched]
3. `tableEx` **[Predictions detail]** -> BATTER[#*, HR_prob, Side, _#*, _HR_prob*, _H_prob*, _x1B*, _x2B*, _x3B*, _xBB*, _xH*, _xHR*, _xHRR*, _xK*, _xR*, _xRBI*, _xTB*, _xXBH*, player_name, x1B, x2B, x3B, xBB, xH, xSO]; BOXSCORE_DETAIL[RBIs*, Runs*]; Measure[vs EV*, vs HR*]; PLAYS[Average Exit Velo*, Average xBA*, Avg*, L5AB EV*]; PlayEVENTS[BABIP*]
4. `pivotTable` **[Team Overview]** -> BATTER[HR_prob, H_prob, Side, _#*, _HR_prob*, _H_prob*, _xH*, _xHR*, player_name, team_name, xH, xHR]; BOXSCORE_DETAIL[HRR*, RBIs*, Runs*]; Measure[HR Hot*, vs BA*, vs HR*]; PLAYS[Average Exit Velo*, Average adjxBA*, Average wOBA*, Average xAvg*, Average xBA*, Avg EV Above Escape Velo*, Avg*, Exit Velo 95+*, Games Since Last HR*, Last 2 Game Avg EV*, Last 3 Game Total xBA*, Max Exit Velo*, TotalBases play*, ...]
5. `card` -> BATTER[team_name]
6. `tableEx` **[Per-game log]** -> BOXSCORE_DETAIL[HRR*, RBIs*, Runs*]; PLAYS[Average Exit Velo*, Average xBA*, Max Exit Velo*, TotalBases play*, game_date, isHardHit*, pGID, team_pitching]; PlayEVENTS[Hit Into Play Per PA %*, isBaseHit*, isHitIntoPlay*, isHomeRun*, isPA*, isStrikeout*, isWalk*, isXBH*]
7. `pageNavigator`
8. `advancedSlicerVisual` -> PLAYS[isNextPitcherHand]
9. `cardVisual` **[HR Pattern / Hot Card]** -> BATTER[HR_prob, H_prob, battingPosition, xH, xHR]; Measure[HR Hot*]; PLAYS[Average wOBA*, Average xAvg*, Games Since HR*, Games Since Last HR*, HR Pattern Early*, HR Pattern Late*, Pattern HitRate*, batOrder*, batter_name, isPA*, team_batting]; PlayEVENTS[isAtBat*, isHitIntoPlay*]
10. `tableEx` **[Per-at-bat log]** -> BATTER[AB#*]; PLAYS[Average Exit Velo*, Average Hit Dist.*, Average Launch Angle*, Average xBA*, Index, Result, at_bat_number, game_date, inning, is_lastpitch, playedABid, team_pitching]
11. `pivotTable` **[VS Pitcher Career detail]** -> BATTER[vs_pitcher_name]; Measure[vs BA*, vs BABIP*, vs BB*, vs Barrels*, vs Dist*, vs EV*, vs H*, vs HR*, vs LA*, vs PA*, vs SO*, vs TB*, vs Whiff%*, vs XBH*, vs xBA*, vs xwOBA*]
12. `cardVisual` **[VS Pitcher Career summary card]** -> BATTER[#*, player_name]; Measure[vs 2B*, vs 3B*, vs AB*, vs BB%*, vs Barrels*, vs EV*, vs H*, vs HR*, vs K%*, vs PA*, vs SO*, vs xBA*]; PLAYS[L5AB EV*]
13. `advancedSlicerVisual` -> UpcomingGameData[Display Game, IsToday, gameDisplay, game_abstractGameState]

### 4.8 VS (8 visuals) - Lineup career matchup
1. `advancedSlicerVisual` -> BATTER[Side] (split L/R)
2. `pivotTable` **[Pitcher Season Stats - same as MAIN #2]**
3. `pivotTable` **[VS Pitcher Career detail - same as MAIN #11]**
4. `pivotTable` **[VS Lineup-wide]** -> BATTER[#*, Side, player_name]; Measure[all 22 vs measures]; PLAYS[L5AB EV*]
5. `advancedSlicerVisual` -> UpcomingGameData[Display Game, IsToday, gameDisplay, game_abstractGameState]
6. `pageNavigator`
7. `slicer` -> UpcomingGameData[gameDisplay, game_date]
8. `card` -> BATTER[team_name]

Visual #4 is the unique visual on this page - the lineup-wide vs-pitcher pivot showing all 9 batters' career lines against the starting pitcher.

### 4.9 Proj (6 visuals) - Lineup projections
1. `card` -> BATTER[team_name]
2. `pivotTable` **[Lineup projections]** -> BATTER[#*, HR_prob, _#*, _HR_prob*, _H_prob*, _x1B*, _x2B*, _x3B*, _xBB*, _xH*, _xHR*, _xHRR*, _xK*, _xR*, _xRBI*, _xTB*, _xXBH*, player_name, x1B, x2B, x3B, xBB, xH, xSO]; BOXSCORE_DETAIL[RBIs*, Runs*]; Measure[vs EV*, vs HR*]; PLAYS[Average Exit Velo*, Average xBA*, Avg*, L5AB EV*]
3. `advancedSlicerVisual` -> UpcomingGameData[...] (same as MAIN #13)
4. `slicer` -> UpcomingGameData[gameDisplay, game_date]
5. `pageNavigator`
6. `advancedSlicerVisual` -> BATTER[Side]

### 4.10 Duplicate of Extra (16 visuals) - Pitcher Analysis prototype (HIDDEN)
Unique visuals here vs other pages:
- `card` -> PITCHER[player_name]
- `card` -> PITCHER[team_name]
- `pivotTable` using PITCHER[starter] as a filter
- `tableEx` with `PITCHER[_xHA*, _xHRA*]` - pitcher expected hits/HRs allowed
- `tableEx` on PLAYS[PitcherGameIndex] - per-game log from pitcher perspective

Everything else is copy-pasted from Extra/MAIN.

---

## 5. Power Query staging layer (mlbStatQueries.docx)

61 named query sections. Many are duplicates, superseded versions, or parameter scalars. Grouped by data source.

### 5.1 MLB Stats API - game-level endpoint (`statsapi.mlb.com/api/v1/game/{id}/withMetrics`)
This is the single-call-per-game endpoint that `miscMLBinstructions.docx` targets for consolidation. **12 queries** read from it:

| Query | Size | Columns | Purpose |
|---|---|---|---|
| MLB_withMetrics_BoxScoreHitting_Game | 162 lines | 54 | Per-player hitting box score, current game |
| MLB_withMetrics_BoxScoreHitting_Season | 186 | 64 | Per-player season-to-date hitting |
| MLB_withMetrics_BoxScorePitching_Game | 231 | 96 | Per-player pitching box, current game |
| MLB_withMetrics_BoxScorePitching_Season | 276 | 116 | Per-player season-to-date pitching |
| MLB_withMetrics_Runner | 142 | 45 | Baserunner detail per play |
| Query2 | 179 | 47 | Play-by-play with runners (variant 1) |
| Query2_v2 | 98 | 33 | Play-by-play with runners (variant 2, superseded?) |
| MLB_Function | 86 | 13 | Generic play-by-play function |
| MLB_withMetrics_playByPlay | 292 | 54 | Full PBP (primary) |
| MLBplayByPlay | 295 | 54 | Full PBP (near-duplicate of above) |
| MLB_Function_v3 | 87 | 13 | MLB_Function rework |
| withMetrics_function | 194 | 49 | Functional form of MLB_withMetrics |

**The misc instructions doc specifies** a consolidation pattern where one `gameList` query fetches the `withMetrics` JSON once per game, stores it in a `gameJSON` column, and each downstream function (`fnGetHittingBoxScore`, `fnGetPitchingBoxScore`, etc.) takes that record as input. This pattern is described but not fully implemented in the docx - most queries still call `Web.Contents` directly.

### 5.2 MLB Stats API - per-player stats (`statsapi.mlb.com/api/v1/people/{id}/stats`)
**6 queries:**

| Query | `stats=` param | Purpose |
|---|---|---|
| statSplits | statSplits | Batter splits by situation (home/away, day/night, vs LHP/RHP, base states) |
| statSplits_v2 | vsTeam | Splits against a specific opposing team |
| sprayChart | sprayChart | Spray chart zone data |
| vsTeam | vsTeam | Batter career vs team |
| opponentsFaced | opponentsFaced | Full pitcher matchup history |
| hotColdZones | hotColdZones | 13-zone hit rate heatmap |

### 5.3 MLB Stats API - schedule (`/api/v1/schedule`)
**3 queries:**

| Query | Purpose |
|---|---|
| GAME | Season schedule, 145 lines, produces primary games table |
| TEAMGAME | Same endpoint but grouped by team (one row per team-game), 134 lines, produces the slicer-backing table |
| Query1 | Schedule with deeper hydration (stats, person), 179 lines |

### 5.4 MLB Stats API - teams (`/api/v1/teams`)
**4 queries:**

| Query | Purpose |
|---|---|
| TEAM | Teams + next schedule (74 lines) |
| TEAM_v2 | Team roster with hitter stats hydration (21 lines) |
| getTeamIDs | Team ID lookup only (7 lines) |
| TeamNextGame | Teams + next schedule (18 lines) - duplicates TEAM partially |

### 5.5 MLB Stats API - reference/config endpoints
Single-query each, small lookup tables:

| Query | Endpoint | Purpose |
|---|---|---|
| positions | /positions | Position code lookup |
| gameStatus | /gameStatus | Game state enum (with custom statusID mapping) |
| statGroups | /statGroups | Stat group enum |
| statTypes | /statTypes | Stat type enum |
| situationCodes | /situationCodes | Situation code enum |
| baseballStats | /baseballStats + /lookup/values/all | Stat metadata |
| eventTypes | /eventTypes | Play event type enum |
| pitchCodes | /pitchCodes | Pitch code enum |
| pitchTypes | /lookup/values/all | Pitch type enum |
| stats | /stats/search/stats | Stat search config |
| statsTable | /stats/search/stats | Stats search (flat table form) |
| config | /stats/search/config | Search config |
| situationCodesConfig | /stats/search/config | Situation search config |
| parametersConfig | /stats/search/config | Parameters search config |
| stats_v2 | /lookup/values/all | Superseded stats lookup |

Most of these are one-time reference fetches. Many are likely unused by visuals.

### 5.6 Baseball Savant (`baseballsavant.mlb.com`)
**6 queries:**

| Query | Endpoint | Purpose |
|---|---|---|
| Matchups | /statcast_search | Batter-vs-pitcher career Statcast (PA, AB, xBA, xwOBA, EV, LA, HH%, Barrels, BABIP, XBH). Has a `getVsStats` inner function, incremental cache in Excel `Matchups` table, appends only new batter-pitcher pairs per day. |
| Statcast_Date | /statcast_search | Daily pitch-level Statcast pull (183 lines, 63 columns) |
| Statcast_Date_v2 | /statcast_search | Duplicate/alternate of above (182 lines) |
| BoxScore | /gf | Live box score fetcher |
| fetchBoxScore | /gf | Near-duplicate (95 vs 92 lines) |
| BoxScores | (no URL) | Aggregator over fetchBoxScore |

### 5.7 Helper / parameter queries (no URL)
**12 entries:**
- Scalar parameters: `gameID_param`, `gameID_v2`, `teamID_param`, `playerID_param`, `pitcherID_param`, `date_today_param`, `date_param`
- Helpers: `gameList`, `VENUE`, `MLB_Function_v2`, `Invoked_Function`, `PLAYER`

### 5.8 Other data
- `PLAYER` - player reference table from `/sports/1/players` (41 lines, 10 columns)
- `VENUE` - venues reference (11 lines, no external URL - probably an Excel sheet)

---

## 6. Dependency map (staging to visual)

Mapping each visual-facing table back to its most likely M source. This is best-effort reconstruction; canonical confirmation requires opening the PBI and inspecting the M for each model table.

| Visual-facing table | Probable M source(s) | Confidence |
|---|---|---|
| BATTER | Calculated from play-level data + predictions model (not in docx) | Low - not in M code |
| PLAYS | MLB_withMetrics_playByPlay OR MLBplayByPlay OR MLB_Function_v3 (one canonical PBP query) | High |
| PlayEVENTS | Derived table from PLAYS - pitch-event granularity | Medium (likely in PBI, not M docx) |
| PITCHER | Symmetrical to BATTER; probably calculated table | Low - not in M code |
| BOXSCORE_DETAIL | MLB_withMetrics_BoxScoreHitting_Game/Season | Medium |
| pitcherSeasonData | MLB_withMetrics_BoxScorePitching_Season | High |
| pitcherStatsSEASON | Likely a duplicate or alternate of pitcherSeasonData | Medium |
| UpcomingGameData | DAX calculated table filtering TEAMGAME to today | Low - not in M code |
| TEAMGAME | TEAMGAME query | Certain |
| TEAM | TEAM query | Certain |
| Measure (vs*) | Matchups query (Baseball Savant) | High |

**Confirmation needed:** to know for certain, open the PBI in Power Query Editor and match each model table name to its M source. The names `BATTER`, `PLAYS`, `PITCHER`, `BOXSCORE_DETAIL`, `pitcherSeasonData`, `pitcherStatsSEASON`, `UpcomingGameData` either exist as query names you renamed after the docx was exported, or they are DAX calculated tables built from the staging queries.

---

## 7. Derived columns / measures not found in the M code file

These are referenced by visuals but do not appear verbatim in `mlbStatQueries.docx`. They must be defined either as DAX measures in the PBI model or as columns added in Power Query after the documented export:

**Projection measures (BATTER._x*, _H_prob, _HR_prob):** These are the predicted values that feed Proj and Predictions visuals. They could be a manual calculation, a separate model, or a Statcast-derived formula. The docx gives no source.

**HR Pattern measures (PLAYS[HR Pattern Early, HR Pattern Late, Pattern HitRate, Games Since HR, Games Since Last HR]):** Custom pattern analysis over play history. Likely DAX measures computed from the PLAYS table itself (game_date + home run flag windowing).

**L5AB EV (PLAYS[L5AB EV]):** "Last 5 AB Exit Velo" - rolling window measure. Likely DAX over PLAYS ordered by at_bat_number/game_date.

**Last-N game measures (Last 2 Game Avg EV, Last 2 Game HH, Last 3 Game Total xBA, Last Game Max EV):** Rolling window DAX over PLAYS.

**HR Hot (Measure[HR Hot]):** A binary indicator combining pattern measures. Custom DAX.

**vs* measures (Measure table, 22 measures):** Batter-vs-pitcher career stats from Matchups query. These are likely simple SELECTEDVALUE or SUM-over-Matchups DAX measures.

**BATTER column-form fields (xH, xHR, x1B, x2B, x3B, xBB, xSO, HR_prob, H_prob, battingPosition):** Stored columns, not measures. Either computed in Power Query (not visible in the docx) or loaded from an external source like an Excel projection sheet.

**BATTER row-index measures (#, AB#, _#):** DAX ranking measures using RANKX or COUNTROWS over filter context.

**BOXSCORE_DETAIL measures (Runs, RBIs, HRR):** DAX aggregates over box score tables or PLAYS.

**PlayEVENTS measures (BABIP, Hit Into Play Per PA %, isAtBat, isBaseHit, isHitIntoPlay, isHomeRun, isPA, isStrikeout, isWalk, isXBH):** Likely DAX over a pitch-event table derived from PLAYS.

---

## 8. Duplication and cleanup opportunities in the PBIX file

### 8.1 Pages to remove outright
- **Duplicate of New** (page 3): identical to New. Delete.
- **New** (page 2): superseded by MAIN. Delete after confirming MAIN covers every visual you actually use.
- **Extra** (page 4): superseded by MAIN + the two unique visuals from EV. Most content is redundant.
- **Criteria** (page 5): reduced variant of Extra. Redundant with MAIN.

Result: drop 4 pages (New, Duplicate of New, Extra, Criteria), keep MAIN as the single Player Analysis page.

### 8.2 Pages to keep
- **Game** - the selector, small and useful.
- **MAIN** - canonical Player Analysis.
- **EV** - team-wide exit velocity pivot (has 2 unique visuals not on MAIN).
- **VS** - lineup-wide matchup pivot (unique lineup-view visual).
- **Proj** - lineup projection table.
- **Duplicate of Extra** - rename to Pitcher Analysis; finish building out PITCHER table.

Final visible page count: 6 (down from 8 visible).

### 8.3 M queries to remove
Many queries in the docx are superseded versions or reference/lookup tables that likely aren't bound to any visual. High-confidence removal candidates:

| Query | Reason |
|---|---|
| statSplits | No direct visual reference |
| statSplits_v2 | Superseded duplicate |
| sprayChart | No visual uses spray chart data |
| vsTeam | Duplicate of Matchups pattern |
| opponentsFaced | Not referenced by any visual |
| roster_vsTeam | Not referenced |
| hotColdZones | No hot/cold zone visual exists in file |
| positions, gameStatus, statGroups, statTypes, situationCodes, baseballStats, eventTypes, pitchCodes, pitchTypes, stats, statsTable, config, situationCodesConfig, parametersConfig, stats_v2 | Reference/enum lookups - none referenced by visuals |
| Query2, Query2_v2 | Superseded PBP variants |
| MLB_Function, MLB_Function_v2, MLB_Function_v3 | Function iterations - keep only the one actually invoked |
| MLBplayByPlay | Near-duplicate of MLB_withMetrics_playByPlay |
| Statcast_Date_v2 | Duplicate of Statcast_Date |
| fetchBoxScore | Near-duplicate of BoxScore |
| BoxScores | Aggregator - keep if used, delete if orphaned |
| Query1 | Likely superseded by GAME |
| TeamNextGame | Redundant with TEAM |
| getTeamIDs | Helper - keep only if invoked |
| VENUE | No visual references venue data |
| PLAYER | Check if used as lookup for name enrichment elsewhere |
| withMetrics_function, Invoked_Function | Function scaffolding - keep only if invoked |

### 8.4 Suggested consolidation (per miscMLBinstructions.docx)
The consolidation pattern in `miscMLBinstructions.docx` describes a target state:

1. `gameList` (with `statusID = "F"` filter) - one row per finished game, with a `gameJSON` column holding the full withMetrics response.
2. `gameListPlayByPlay` (no filter) - for in-progress games too.
3. `fnGetHittingBoxScore(gameJSON)` - function accepting one gameJSON, returning the hitting box.
4. `fnGetPitchingBoxScore(gameJSON)` - pitching box.
5. `fnGetSeasonHitting(gameJSON)` - season stats.
6. `fnGetSeasonPitching(gameJSON)` - season pitching.
7. `fnGetPlayByPlay(gameJSON)` - PBP (uses gameListPlayByPlay).
8. `allHittingBoxScores`, `allPitchingBoxScores`, `allSeasonHitting`, `allSeasonPitching`, `allPlayByPlay` - invocation queries that iterate `fnGet*` over the appropriate gameList and append to an `existing[QueryName]` Excel cache for incremental loading.

This consolidation, if fully applied, would replace **7+ of the 12 withMetrics queries** with 1 source fetch + 5 transform functions + 5 invocation queries. The docx hints it was specified but not fully implemented.

---

## 9. Schema design hints for Azure SQL

These are direct implications for the MLB schema, to pair with `/database/mlb/README.md` ADR-0004 (all visual stats pre-aggregated).

### 9.1 Fact tables
- **`mlb.plays`** - one row per play (at-bat level). Columns from the PLAYS visual fields plus all pitch-event flags needed for isAtBat, isPA, isHomeRun, etc. Consider whether to store pitch-level (PlayEVENTS) in a separate table or merge with `is_lastpitch` flag.
- **`mlb.play_events`** - pitch-level, if kept separate. Populates all the `is*` flags.
- **`mlb.games`** - one row per game. Replaces GAME / TEAMGAME (which is game duplicated by team side).
- **`mlb.team_games`** - one row per team-game (home + away views of same game). Backs TEAMGAME slicers.

### 9.2 Pre-aggregated entities for web visuals (from ADR-0004)
Mapping the 9 entities to concrete Player Analysis visuals:

1. **Upcoming games** -> UpcomingGameData. Source: TEAMGAME filtered to today + game_abstractGameState.
2. **Batter context per game** -> BATTER (most columns). One row per (player_id, game_id). Holds player_name, team_name, gameDisplay, vs_pitcher_name, Side, battingPosition, gameTime.
3. **Batter projections per game** -> BATTER (the `_x*` and `_H_prob`/`_HR_prob` measures). Same grain as #2. Source: wherever your prediction model runs (currently opaque - probably Excel sheet or external model).
4. **Player game stats** -> BOXSCORE_DETAIL (Runs, RBIs, HRR per player per game). Source: MLB Stats API box scores.
5. **Player at-bat stats** -> PLAYS at at-bat grain. Exit velo per AB, result, xBA, etc. Source: MLB_withMetrics_playByPlay with `is_lastpitch = true`.
6. **Player trend/pattern stats** -> PLAYS measures (HR Pattern Early/Late, Pattern HitRate, L5AB EV, Games Since HR, Last 2 Game Avg EV, Last 3 Game Total xBA). One row per (player, game), computed from rolling window over Player at-bat stats.
7. **Player platoon splits** -> Filter of #5/#6 by `isNextPitcherHand`. Could be a view rather than a table.
8. **Career batter vs pitcher matchup** -> Matchups query output. One row per (player_id, vs_pitcher_id). All `vs*` measures.
9. **Pitcher season stats** -> pitcherSeasonData (ERA, IP, Hits Allowed, HRs Allowed, Runs Allowed, Walks, Strikeouts, Batters Faced, Avg Against) + pitcherStatsSEASON.inningsPitched (redundant - consolidate).

### 9.3 Schema naming suggestions
Adopt the Schnapp convention from NBA: `snake_case` columns, `snake_case` for table names. Propose:
- `mlb.games`, `mlb.team_games`, `mlb.plays`, `mlb.play_events`
- `mlb.player_batter_game_context`, `mlb.player_batter_game_projections`
- `mlb.player_game_hitting_stats`, `mlb.player_at_bat_stats`
- `mlb.player_trend_patterns`, `mlb.player_platoon_splits` (or view)
- `mlb.career_batter_vs_pitcher` (the Matchups output)
- `mlb.pitcher_season_stats`

### 9.4 Things to drop entirely
- `VENUE` concept - no visual uses it.
- Every `/stats/search/*`, `/lookup/values/all`, `/pitchCodes`, `/eventTypes`, etc. reference query - none bound to visuals.
- `opponentsFaced`, `hotColdZones`, `sprayChart`, `statSplits*` - unused.
- `positions`, `gameStatus`, `statGroups`, `statTypes` - unused.

---

## 10. Open questions

These can only be answered by opening the PBI file and inspecting the model directly:

1. **Where do BATTER._x* projections come from?** The predictions values driving every Proj and Predictions visual have no M source in the docx. Likely options: (a) an external Excel sheet loaded as a table, (b) a calculated column using a DAX formula based on Statcast-derived metrics, (c) a model output pasted in.

2. **Is PlayEVENTS a separate table or a calculated view over PLAYS?** Its measures are all pitch-event-level but its column list in visuals is empty. Likely a calculated table like `SUMMARIZECOLUMNS(Plays[...])` or a DAX measure table.

3. **What is the exact definition of HR Pattern Early / Late / HitRate?** Critical for reproducing the pattern analysis in Azure SQL. These names imply early-season vs late-season windowing, or early-inning vs late-inning.

4. **Why two pitcher season tables (pitcherSeasonData and pitcherStatsSEASON)?** One column referenced from each. Likely consolidate to one.

5. **What does the Measure table physically look like?** 22 measures all prefixed `vs ` + `HR Hot`. Is it a disconnected table (classic DAX "measure table" pattern) or is it backed by the Matchups query?

6. **Is the consolidation pattern from miscMLBinstructions.docx actually implemented?** Or are all 12 withMetrics queries still making redundant API calls?

Answering these unlocks full schema design confidence. Recommend: open the PBI, use Performance Analyzer or DAX Studio to capture every measure definition, and extract each model table's backing M expression.

---

## 11. Companion raw artifacts

Full extractions of the source materials are stored alongside this reference for future sessions:

- `etl/mlb/_legacy_powerquery/mlbStatQueries_full.txt` - verbatim extracted M code, 225KB, 4,508 lines
- `etl/mlb/_legacy_powerquery/miscMLBinstructions_full.txt` - M authoring guide and consolidation spec
- `etl/mlb/_legacy_powerquery/m_query_catalog.json` - parsed catalog of the 61 query sections with URLs and column counts
- `etl/mlb/_legacy_powerquery/pbix_visual_catalog.json` - full structured inventory of all 135 visuals across 10 pages, with every field reference
