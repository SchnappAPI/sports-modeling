'use client';

import { useState } from 'react';

interface GlossaryItem {
  term: string;
  def: string;
}

interface HelpSection {
  heading: string;
  body?: string;
  glossary?: GlossaryItem[];
}

interface HelpContent {
  title: string;
  sections: HelpSection[];
}

const HELP: Record<string, HelpContent> = {

  'grades-list': {
    title: 'At a Glance \u2014 List View',
    sections: [
      {
        heading: 'What this page shows',
        body: 'Every graded prop for today across all games, sorted by composite grade. Each row is one player and one prop line. Use the filters at the top to narrow by outcome (Over/Under), result status, market, player name, odds range, or signal type.',
      },
      {
        heading: 'Columns',
        glossary: [
          { term: 'Mkt',     def: 'Stat category: PTS, REB, AST, 3PM, PRA, PR, PA, RA, STL, BLK' },
          { term: 'Line',    def: 'The prop threshold. O = Over, U = Under, N+ = alternate line.' },
          { term: 'Odds',    def: 'FanDuel price. Live prices shown in green with an L indicator.' },
          { term: 'Imp%',    def: 'Implied probability derived from the odds.' },
          { term: 'Comp',    def: 'Composite grade \u2014 equal-weighted average of all component grades. Higher is better for Overs.' },
          { term: 'HR%',     def: 'Hit rate grade \u2014 weighted blend of L20 and L60 hit rates (60% weight on recent).' },
          { term: 'L20%',    def: 'Hit rate over the last 20 games.' },
          { term: 'L60%',    def: 'Hit rate over the last 60 games.' },
          { term: 'vs Opp',  def: "Hit rate against today's specific opponent across the full season." },
          { term: 'N20/N60', def: 'Sample size for the L20 and L60 windows.' },
          { term: 'Def',     def: 'Opponent defense rank for this position and stat. 1st = most allowed (favorable matchup).' },
        ],
      },
      {
        heading: 'Signals',
        body: 'Colored chips next to a player name flag meaningful patterns. HOT/COLD/DUE/FADE are player-level. STK/SLP/LS appear in the expanded row only.',
        glossary: [
          { term: 'HOT',  def: 'L10 stat average above L30 average \u2014 trending up.' },
          { term: 'COLD', def: 'L10 stat average below L30 average \u2014 trending down.' },
          { term: 'DUE',  def: 'L10 below full-season average \u2014 bounce-back candidate.' },
          { term: 'FADE', def: 'L10 above full-season average \u2014 regression risk.' },
          { term: 'STK',  def: 'Active hit streak for this exact line (expanded row only).' },
          { term: 'SLP',  def: 'Active miss streak for this exact line (expanded row only).' },
          { term: 'LS',   def: 'Long odds (+250+) but hit recently and in 12%+ of last 60 games (expanded row only).' },
        ],
      },
      {
        heading: 'Expanding a row',
        body: 'Tap any row to expand it. Shows the four component grades individually (Trend, Regression, Momentum, Matchup), live player stats when a game is in progress, and line-specific signals (STK/SLP/LS).',
      },
      {
        heading: 'Games strip',
        body: "Pill buttons below the filter bar show today's games with live scores. Tap one to filter to that game only. Tap again to clear.",
      },
    ],
  },

  'grades-matrix': {
    title: 'At a Glance \u2014 Matrix View',
    sections: [
      {
        heading: 'What this view shows',
        body: 'The same graded props reorganized into a grid. Rows are players, columns are prop thresholds (5+, 10+, 15+, etc.), and cells show the odds for that line. Props are grouped by stat category.',
      },
      {
        heading: 'Reading cells',
        body: 'Odds color indicates composite grade: green is 70+, yellow is 55\u201369, gray is below 55. A dash means no graded line at that threshold. Cells with a link open FanDuel directly.',
      },
      {
        heading: 'Cell dots',
        body: 'Tiny dots after the odds show line-specific signals without cluttering the grid.',
        glossary: [
          { term: 'Green dot',  def: 'Active hit streak for this exact line.' },
          { term: 'Orange dot', def: 'Active miss streak for this exact line.' },
          { term: 'Purple dot', def: 'LONGSHOT: odds +250+ but has hit recently and in 12%+ of last 60 games.' },
        ],
      },
      {
        heading: 'Player signals',
        body: 'HOT, COLD, DUE, FADE chips next to the player name reflect the overall stat trend, not a specific line.',
      },
      {
        heading: 'Player panel',
        body: 'Tap a player name to see their last 20 games for that stat: a hit rate summary across every threshold and a game log with per-line checkmarks.',
      },
      {
        heading: 'Filters',
        body: 'All top-bar filters (game, market, player, odds, signal) feed the matrix. Filtering to one game or one market makes the grid much easier to read.',
      },
    ],
  },

  'player': {
    title: 'Player Page',
    sections: [
      {
        heading: 'What this page shows',
        body: "A full game log and stat breakdown for one player, with today's props overlaid as color coding on historical results. Use the team selector in the header to switch to any player on either team.",
      },
      {
        heading: 'Splits table',
        body: "Season, Last 10, Starter, Bench, and vs Opponent averages. Toggle between compact and All Stats view in the filter bar.",
      },
      {
        heading: 'VS Defense',
        body: "How the opposing team ranks in allowing stats to this player's position group. 1st means they allow the most \u2014 favorable matchup. The highlighted column matches today's prop market.",
      },
      {
        heading: "Today's Props",
        body: "The strip shows each market with the posted line and composite grade. Tap a cell to expand the dot plot \u2014 green dots are games where the player hit the line, red are misses. Alt lines appear below the chart with odds, grade, and hit rates.",
      },
      {
        heading: 'Prop signals',
        body: 'When a signal fires for a market it appears in the row between the section header and the strip. A small dot on each strip cell summarizes the signal direction: green for positive, orange for negative, yellow for mixed.',
      },
      {
        heading: 'Game log',
        body: "Full game-by-game stats. When viewing full-game totals, values are colored against today's graded line \u2014 green if the player would have hit, red if they would have missed. Blue left border = game started.",
      },
      {
        heading: 'Period filter',
        body: "The 1Q/2Q/3Q/4Q buttons filter stats to a single quarter. Prop color coding is off in this mode since it only applies to full-game totals.",
      },
    ],
  },

};

interface HelpPanelProps {
  page: keyof typeof HELP;
}

export function HelpButton({ page }: HelpPanelProps) {
  const [open, setOpen] = useState(false);
  const content = HELP[page];
  if (!content) return null;

  return (
    <>
      <button
        onClick={() => setOpen(true)}
        className="text-gray-600 hover:text-gray-400 text-xs px-1.5 py-0.5 rounded border border-transparent hover:border-gray-700 transition-colors leading-none"
        title="How to use this page"
        aria-label="Help"
      >
        ?
      </button>

      {open && (
        <>
          <div
            className="fixed inset-0 z-40 bg-black/40"
            onClick={() => setOpen(false)}
          />
          <div className="fixed inset-y-0 right-0 z-50 w-full max-w-sm bg-gray-950 border-l border-gray-800 shadow-2xl flex flex-col">
            <div className="flex items-center justify-between px-5 py-3 border-b border-gray-800">
              <span className="text-sm font-semibold text-gray-200">{content.title}</span>
              <button
                onClick={() => setOpen(false)}
                className="text-gray-500 hover:text-gray-200 text-xl leading-none"
              >
                &times;
              </button>
            </div>

            <div className="flex-1 overflow-y-auto px-5 py-4 space-y-5">
              {content.sections.map((section) => (
                <div key={section.heading}>
                  <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-1.5">
                    {section.heading}
                  </h3>
                  {section.body && (
                    <p className="text-xs text-gray-400 leading-relaxed mb-2">
                      {section.body}
                    </p>
                  )}
                  {section.glossary && (
                    <div className="space-y-1.5">
                      {section.glossary.map(({ term, def }) => (
                        <div key={term} className="flex gap-2 text-xs">
                          <span className="text-gray-300 font-medium whitespace-nowrap w-16 flex-none">
                            {term}
                          </span>
                          <span className="text-gray-500 leading-relaxed">{def}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>

            <div className="px-5 py-3 border-t border-gray-800">
              <p className="text-xs text-gray-700">schnapp.bet</p>
            </div>
          </div>
        </>
      )}
    </>
  );
}
