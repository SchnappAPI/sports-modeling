'use client';

import { useState } from 'react';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GlossaryItem {
  term: string;
  def: string;
}

interface HelpSection {
  heading: string;
  body: string;
  glossary?: GlossaryItem[];
}

interface HelpContent {
  title: string;
  sections: HelpSection[];
}

// ---------------------------------------------------------------------------
// Content
// ---------------------------------------------------------------------------

const HELP: Record<string, HelpContent> = {

  'grades-list': {
    title: 'At a Glance — List View',
    sections: [
      {
        heading: 'What this page shows',
        body: 'Every graded prop for today across all games, sorted by composite grade. Each row is one player and one prop line. Use the filters at the top to narrow by outcome (Over/Under), result status, market, player name, or odds range.',
      },
      {
        heading: 'Filters',
        body: 'Over/Under toggles which side you are evaluating. All/Open/Won/Lost filters by game status. The market dropdown limits to a single stat. The odds slider cuts off props below a minimum odds threshold — useful for focusing on value lines rather than heavy favorites. The Signals filter shows only rows that match a specific signal type.',
      },
      {
        heading: 'Columns',
        glossary: [
          { term: 'Mkt',    def: 'Stat category (PTS, REB, AST, 3PM, PRA, etc.)' },
          { term: 'Line',   def: 'The prop threshold. O = Over, U = Under, N+ = alternate line.' },
          { term: 'Odds',   def: 'FanDuel price. Live prices shown in green with an L indicator.' },
          { term: 'Imp%',   def: 'Implied probability derived from the odds.' },
          { term: 'Comp',   def: 'Composite grade — equal-weighted average of all component grades. Higher is better for Overs.' },
          { term: 'HR%',    def: 'Hit rate grade — weighted blend of L20 and L60 hit rates (60% weight on recent).' },
          { term: 'L20%',   def: 'Hit rate over the last 20 games.' },
          { term: 'L60%',   def: 'Hit rate over the last 60 games.' },
          { term: 'vs Opp', def: 'Hit rate against today\'s specific opponent across the full season.' },
          { term: 'N20/N60',def: 'Sample size for the L20 and L60 windows.' },
          { term: 'Def',    def: 'Opponent defense rank for this position and stat. 1st = most allowed (favorable matchup).' },
        ],
      },
      {
        heading: 'Signals',
        body: 'Small colored chips next to a player name indicate a meaningful pattern in the underlying data.',
        glossary: [
          { term: 'HOT',  def: 'L10 stat average is above their L30 average — performing above recent baseline.' },
          { term: 'COLD', def: 'L10 stat average is below their L30 average — performance has dipped recently.' },
          { term: 'DUE',  def: 'L10 average is below their full-season average — bounce-back candidate.' },
          { term: 'FADE', def: 'L10 average is above their full-season average — regression risk.' },
          { term: 'STK',  def: 'Active hit streak on this exact line. Shown in expanded row only.' },
          { term: 'SLP',  def: 'Active miss streak on this exact line. Shown in expanded row only.' },
          { term: 'LS',   def: 'Long odds (+250 or more) but has hit this line recently and in at least 12% of last 60 games.' },
        ],
      },
      {
        heading: 'Expanding a row',
        body: 'Tap any row to expand it. The expanded panel shows the four component grades individually (Trend, Regression, Momentum, Matchup) so you can see what is driving the composite score. If a game is live, current player stats are shown. Line-specific signals (STK/SLP) and the LONGSHOT flag appear here.',
      },
      {
        heading: 'Games strip',
        body: 'The pill buttons below the filter bar are today\'s games. Tap one to filter the entire list to props from that game only. Scores and quarter/status are shown live. Tap again to clear the filter.',
      },
    ],
  },

  'grades-matrix': {
    title: 'At a Glance — Matrix View',
    sections: [
      {
        heading: 'What this view shows',
        body: 'The same graded props as the list, reorganized into a grid. Rows are players, columns are prop thresholds (5+, 10+, 15+, etc.), and the value in each cell is the odds for that line. Props are grouped by stat category (PTS, REB, AST, 3PM, PRA, PR, PA, RA, STL, BLK).',
      },
      {
        heading: 'Reading the cells',
        body: 'Cell color indicates composite grade: green is 70 or above, yellow is 55 to 69, gray is below 55. A dash means no graded line exists at that threshold for that player. Cells with a sportsbook link are tappable and open FanDuel directly.',
      },
      {
        heading: 'Cell dots',
        body: 'Tiny colored dots after the odds indicate line-specific signals without cluttering the grid.',
        glossary: [
          { term: 'Green dot',  def: 'Player is on an active hit streak for this exact line.' },
          { term: 'Orange dot', def: 'Player is on an active miss streak for this exact line.' },
          { term: 'Purple dot', def: 'LONGSHOT: odds are +250 or more but the player has hit this line recently and historically at 12%+ over 60 games.' },
        ],
      },
      {
        heading: 'Player signals',
        body: 'HOT, COLD, DUE, and FADE chips appear next to the player name and reflect the player\'s overall stat trend — not tied to a specific line. These are the same signals as in the list view.',
      },
      {
        heading: 'Player panel',
        body: 'Tap a player name to open a slide-in panel showing their last 20 games for that stat, with a hit rate summary across every threshold and a game log with checkmarks per line.',
      },
      {
        heading: 'All filters apply',
        body: 'The game strip, market dropdown, player search, odds slider, and signal filter in the top bar all feed the matrix. Filtering to a single game or market makes the grid much easier to read.',
      },
    ],
  },

  'player': {
    title: 'Player Page',
    sections: [
      {
        heading: 'What this page shows',
        body: 'A full game log and stat breakdown for one player, with today\'s props overlaid as color coding on historical results. Use the team selector in the header to switch to any player on either team.',
      },
      {
        heading: 'Splits table',
        body: 'Season, Last 10, Starter, Bench, and (when available) vs Opponent splits. Shows averages per game for all tracked stats. Toggle between compact and All Stats view using the button in the filter bar.',
      },
      {
        heading: 'VS Defense',
        body: 'Shows how the opposing team ranks in allowing stats to this player\'s position group. 1st means they allow the most — favorable matchup. Highlighted column matches the prop market for today\'s game.',
      },
      {
        heading: "Today's Props",
        body: "The strip below VS Defense shows each market with the posted line and composite grade. Tap a market cell to expand the dot plot for that stat, showing the last 10/30/50/All games with a line at the posted threshold — green dots are hits, red are misses. Alt lines appear below the chart with odds, grade, and hit rates.",
      },
      {
        heading: 'Signals on props',
        body: 'When a signal fires for a specific market it appears in the signals row between the header and the strip. A small colored dot also appears on each market cell in the strip: green for positive signals (HOT/DUE/STK), orange for negative (COLD/FADE/SLP), yellow for mixed.',
      },
      {
        heading: 'Game log',
        body: 'Full game-by-game stats. When viewing full game totals, stats are color coded against the graded line for today\'s game — green if the player would have hit, red if they would have missed. Rows with a blue left border are games where the player started.',
      },
      {
        heading: 'Period filter',
        body: 'The 1Q/2Q/3Q/4Q buttons at the top of the game log filter stats to a single quarter. Prop color coding turns off in this mode since it only applies to full-game totals.',
      },
    ],
  },

};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

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
        className="text-gray-600 hover:text-gray-400 text-xs px-1.5 py-0.5 rounded border border-transparent hover:border-gray-700 transition-colors"
        title="How to use this page"
        aria-label="Help"
      >
        ?
      </button>

      {open && (
        <>
          {/* Backdrop */}
          <div
            className="fixed inset-0 z-40 bg-black/40"
            onClick={() => setOpen(false)}
          />

          {/* Panel */}
          <div className="fixed inset-y-0 right-0 z-50 w-full max-w-sm bg-gray-950 border-l border-gray-800 shadow-2xl flex flex-col">
            {/* Header */}
            <div className="flex items-center justify-between px-5 py-3 border-b border-gray-800">
              <span className="text-sm font-semibold text-gray-200">{content.title}</span>
              <button
                onClick={() => setOpen(false)}
                className="text-gray-500 hover:text-gray-200 text-xl leading-none"
              >
                &times;
              </button>
            </div>

            {/* Body */}
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
                    <div className="space-y-1">
                      {section.glossary.map(({ term, def }) => (
                        <div key={term} className="flex gap-2 text-xs">
                          <span className="text-gray-300 font-medium whitespace-nowrap w-16 flex-none">
                            {term}
                          </span>
                          <span className="text-gray-500">{def}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>

            {/* Footer */}
            <div className="px-5 py-3 border-t border-gray-800">
              <p className="text-xs text-gray-700">schnapp.bet</p>
            </div>
          </div>
        </>
      )}
    </>
  );
}
