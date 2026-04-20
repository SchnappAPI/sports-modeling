# Product Blueprint

This document captures the product concept that is constant across all sports. Sport-specific implementation details live in the per-sport READMEs under `/web/`, `/etl/`, and `/database/`. The blueprint exists so any session working on a new sport, or revisiting an existing one, inherits the product intent without slogging through another sport's specifics.

## What the product is

Schnapp.bet is a player prop research platform. Bettors come to the site to see, for an upcoming game, what props are available on each player and what the underlying data suggests about whether to bet over or under each line. The same purpose holds for NBA, MLB, and NFL.

The site is a consumer surface, not a tool for power users. Information has to be presentable and quickly digestible on a phone screen during the half hour before a game starts.

## Information architecture

Every sport surfaces the same conceptual pages. The implementation differs because the data and visuals differ, but the user flow does not.

### Game selector
The entry point. Lists today's upcoming games. Tapping a game opens the game page for that matchup.

### Game page
The hub for a single game. Contains tabs or sections for projected and confirmed lineups with each player's role and matchup info, a props strip showing each player's available markets at a glance, live in-game stats once the game starts, a matchup view comparing both teams or both players where applicable, and an at-a-glance summary surfacing the strongest opportunities.

### Player page
A drill-down for one player. Shows that player's full game log, splits, recent trends, prop history, and current props for the upcoming game. Sport-specific: NBA shows points/rebounds/assists style stats, MLB shows hitting and pitching with much more granular Statcast data, NFL shows position-relevant stats.

### At a glance
A cross-game grid that lets the user scan all upcoming props quickly, sorted and filtered by signal strength, odds range, and other criteria. The fastest path from open-app to bet decision.

## The connected visual pattern

The most important interaction pattern is the connected visual. When a user taps a player in the lineup on the game page, every visual on that page updates to show that player's data. The selected player is page-level state, not component-local. Charts, tables, cards, and props strips all subscribe to the same selection.

This pattern came from the legacy Power BI reports where it worked through PBI's slicer-and-filter context. In the web app the equivalent is React context or lifted state at the page level, with visual components as pure consumers of that context.

The implication for ETL: data feeding any visual must be pre-aggregated per player and queryable in a single round trip. If the user taps three players in five seconds, the page should re-render in under a second each time. Runtime aggregation of large fact tables is not viable. See `/docs/DECISIONS.md` ADR-0004 for the MLB-specific commitment.

## What changes per sport

Three layers of variability:

**Concept layer**: fully shared. Game selector, game page, player page, at a glance. Identical across sports.

**Contract layer**: shared structure with sport-specific fields. A "stats table" exists for every sport but the columns differ. A "props strip" exists for every sport but the markets differ. The shared web shell defines the structure; the sport-specific code fills in the fields.

**Implementation layer**: sport-specific. NBA stat columns (MIN, PTS, 3PM, REB, AST, PRA, PR, PA, RA), MLB Statcast metrics (exit velocity, launch angle, expected stats, hot/cold zones), NFL position-relevant stats (rush yards, completions, targets), live data sources, prop market lists, grading signals. None of this is shared and none should leak into the shared shell.

## Why this exists

Without this document, every new sport build session would either start from scratch on the why, or inherit it implicitly from the most-developed sport at the time (currently NBA). That implicit inheritance causes drift: NBA-specific assumptions get baked into things that should be sport-agnostic, and MLB and NFL builds inherit those assumptions without realizing they should be questioned.

Reading this document at the start of any cross-sport or new-sport session prevents that drift.
