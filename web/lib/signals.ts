// Shared signal definitions used across GradesPageInner, PropMatrix, and PlayerPageInner.

// ---------------------------------------------------------------------------
// Player-level signals (derived from trend/regression — same across all lines)
// ---------------------------------------------------------------------------

export type PlayerSignalType = 'HOT' | 'COLD' | 'DUE' | 'FADE';

// ---------------------------------------------------------------------------
// Line-level signals (derived from momentum — specific to a single line value)
// ---------------------------------------------------------------------------

export type LineSignalType = 'STREAK' | 'SLUMP';

// ---------------------------------------------------------------------------
// Value signals (derived from hit rate + odds — cell-level opportunity flags)
// ---------------------------------------------------------------------------

export type ValueSignalType = 'LONGSHOT';

export type SignalType = PlayerSignalType | LineSignalType | ValueSignalType;

export interface Signal {
  type: SignalType;
  label: string;
  title: string;
  chipClass: string;
}

export const SIGNAL_DEFS: Record<SignalType, Omit<Signal, 'type'>> = {
  HOT:      { label: 'HOT',  chipClass: 'bg-amber-900/50 text-amber-300 border-amber-700/50',       title: 'Performing above recent baseline (L10 vs L30 stat mean)' },
  COLD:     { label: 'COLD', chipClass: 'bg-blue-900/50 text-blue-300 border-blue-700/50',          title: 'Performing below recent baseline (L10 vs L30 stat mean)' },
  DUE:      { label: 'DUE',  chipClass: 'bg-green-900/50 text-green-300 border-green-700/50',       title: 'Below season average — bounce-back candidate' },
  FADE:     { label: 'FADE', chipClass: 'bg-red-900/50 text-red-300 border-red-700/50',             title: 'Above season average — regression risk' },
  STREAK:   { label: 'STK',  chipClass: 'bg-emerald-900/50 text-emerald-300 border-emerald-700/50', title: 'Hit streak likely to continue — player normally hits this line and is on a run' },
  SLUMP:    { label: 'DUE',  chipClass: 'bg-green-900/50 text-green-300 border-green-700/50',       title: 'Miss streak likely to reverse — player normally hits this line and is due' },
  LONGSHOT: { label: 'LS',   chipClass: 'bg-purple-900/50 text-purple-300 border-purple-700/50',   title: 'Long odds but has hit this line recently and historically (~1 in 5 games)' },
};

export interface PlayerSignalInputs {
  trendGrade:      number | null;
  regressionGrade: number | null;
}

export interface LineSignalInputs {
  momentumGrade: number | null;
}

export interface CellValueInputs {
  overPrice:   number | null;
  hitRate20:   number | null;
  hitRate60:   number | null;
}

/**
 * Player-level signals — same for every line this player has.
 * These indicate whether the player's underlying stat is trending
 * up or down relative to their baseline or season average.
 *
 * trendGrade:      L10 vs L30 stat mean. >72 = trending up (HOT), <28 = trending down (COLD).
 * regressionGrade: z-score of L10 vs full season. >72 = below avg, due up (DUE). <28 = above avg (FADE).
 *
 * Conflict resolution:
 *   HOT suppresses FADE — a player who is actively trending up has already
 *   exceeded their season average; showing FADE alongside HOT is contradictory
 *   and undercuts an otherwise strong grade. The regression risk is already
 *   reflected in the composite grade.
 *
 *   DUE suppresses COLD — a bounce-back candidate should not also show COLD.
 *   The two signals point in opposite directions for the same player.
 */
export function getPlayerSignals(row: PlayerSignalInputs): Signal[] {
  const signals: Signal[] = [];
  const { trendGrade, regressionGrade } = row;

  const isHot  = trendGrade      != null && trendGrade      > 72;
  const isCold = trendGrade      != null && trendGrade      < 28;
  const isDue  = regressionGrade != null && regressionGrade > 72;
  const isFade = regressionGrade != null && regressionGrade < 28;

  if (isHot)              signals.push({ type: 'HOT',  ...SIGNAL_DEFS.HOT  });
  if (isCold && !isDue)   signals.push({ type: 'COLD', ...SIGNAL_DEFS.COLD });
  if (isDue)              signals.push({ type: 'DUE',  ...SIGNAL_DEFS.DUE  });
  if (isFade && !isHot)   signals.push({ type: 'FADE', ...SIGNAL_DEFS.FADE });

  return signals;
}

/**
 * Line-level signals derived from the empirical momentum_grade.
 *
 * The grading model now uses actual observed streak continuation rates
 * gated on the player's base hit rate (hr60) for this specific line.
 * A score >70 means the line is likely to hit based on streak context.
 * A score <30 means it is likely to miss.
 *
 * STREAK: momentum_grade > 70 — streak likely to continue because the
 *   player normally hits this line AND is on a hit run. High hr60 + hit
 *   streak drives this. A low-hr60 player on a hit streak scores LOW here
 *   because mean reversion is expected.
 *
 * SLUMP (displayed as DUE): momentum_grade > 65 where the player is on a
 *   miss streak but normally hits this line. The grading model inverts miss
 *   streaks for high-hr60 players into high scores, so this fires when a
 *   normally-reliable player is overdue. Miss streaks for low-hr60 players
 *   score LOW (they normally miss, so no signal).
 *
 * Note: both signals can fire together if the grade is very high.
 * The SLUMP signal is shown with a positive (green) chip because for
 * high-hr60 players a miss streak is actually a buying opportunity.
 */
export function getLineSignals(row: LineSignalInputs, hitRate60: number | null = null): Signal[] {
  const signals: Signal[] = [];
  const { momentumGrade } = row;

  if (momentumGrade == null) return signals;

  // STREAK: high momentum on a hit streak — continuation expected
  // Only fires when grade > 70, meaning the empirical table predicts 70%+
  // probability of hitting again
  if (momentumGrade > 70) {
    signals.push({ type: 'STREAK', ...SIGNAL_DEFS.STREAK });
  }

  // SLUMP shown as DUE: the grade is also high (>65) but comes from a miss
  // streak for a player who normally hits. The grading model scores this
  // high because bounce-back is expected. We only fire this when the player
  // has a meaningful base hit rate (suppress for lines they rarely hit).
  // hitRate60 >= 0.35 ensures the player normally hits this often enough
  // that a miss streak is genuinely anomalous.
  if (
    momentumGrade > 65 &&
    hitRate60 != null && hitRate60 >= 0.35
  ) {
    // Only add SLUMP if STREAK wasn't already fired (they're mutually exclusive
    // in meaning — a high grade either comes from a hit streak OR a miss streak
    // for a reliable player, not both)
    if (signals.length === 0) {
      signals.push({ type: 'SLUMP', ...SIGNAL_DEFS.SLUMP });
    }
  }

  return signals;
}

/**
 * Cell-level value signals — combine odds + hit rates to flag long-odds opportunities.
 *
 * LONGSHOT: odds > +250, hit_rate_20 > 0 (hit it at least once in last 20),
 *           and hit_rate_60 >= 0.12 (hit it ~1-in-8 or better over 60 games).
 *           This says: the odds are long but this player actually does this
 *           with some regularity and has done it recently.
 */
export function getCellValueSignals(row: CellValueInputs): Signal[] {
  const signals: Signal[] = [];
  const { overPrice, hitRate20, hitRate60 } = row;

  if (
    overPrice != null &&
    overPrice > 250 &&
    hitRate20 != null && hitRate20 > 0 &&
    hitRate60 != null && hitRate60 >= 0.20
  ) {
    signals.push({ type: 'LONGSHOT', ...SIGNAL_DEFS.LONGSHOT });
  }

  return signals;
}

/**
 * Combined helper — returns all signals for a full grade row.
 * Used in GradesPageInner list view where one row = one player + one line.
 * Separates player-level from line-level so callers can decide what to show where.
 */
export interface AllSignals {
  player: Signal[];
  line:   Signal[];
  cell:   Signal[];
  all:    Signal[];
}

export interface FullRowSignalInputs extends PlayerSignalInputs, LineSignalInputs, CellValueInputs {}

export function getAllSignals(row: FullRowSignalInputs): AllSignals {
  const player = getPlayerSignals(row);
  const line   = getLineSignals(row, row.hitRate60);
  const cell   = getCellValueSignals(row);
  return { player, line, cell, all: [...player, ...line, ...cell] };
}

// Legacy alias for existing code that calls getSignals() with all three fields.
// Kept for backwards compatibility — new code should use getAllSignals().
export function getSignals(row: FullRowSignalInputs): Signal[] {
  return getAllSignals(row).all;
}
