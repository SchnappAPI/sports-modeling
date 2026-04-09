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
  STREAK:   { label: 'STK',  chipClass: 'bg-emerald-900/50 text-emerald-300 border-emerald-700/50', title: 'Active hit streak for this prop line' },
  SLUMP:    { label: 'SLP',  chipClass: 'bg-orange-900/50 text-orange-300 border-orange-700/50',   title: 'Active miss streak for this prop line' },
  LONGSHOT: { label: 'LS',   chipClass: 'bg-purple-900/50 text-purple-300 border-purple-700/50',   title: 'Long odds but has hit this line recently — worth a look' },
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
 */
export function getPlayerSignals(row: PlayerSignalInputs): Signal[] {
  const signals: Signal[] = [];
  const { trendGrade, regressionGrade } = row;

  if (trendGrade != null) {
    if (trendGrade > 72) signals.push({ type: 'HOT',  ...SIGNAL_DEFS.HOT  });
    if (trendGrade < 28) signals.push({ type: 'COLD', ...SIGNAL_DEFS.COLD });
  }

  if (regressionGrade != null) {
    if (regressionGrade > 72) signals.push({ type: 'DUE',  ...SIGNAL_DEFS.DUE  });
    if (regressionGrade < 28) signals.push({ type: 'FADE', ...SIGNAL_DEFS.FADE });
  }

  return signals;
}

/**
 * Line-level signals — specific to a single line value.
 *
 * Gated on hitRate60 to suppress noise on low-probability lines:
 * - STREAK only fires when hr60 <= 0.80 (not just their normal behavior)
 *   and hr60 >= 0.25 (they hit it often enough that a streak is meaningful)
 * - SLUMP only fires when hr60 >= 0.30 (missing is unexpected, not the norm)
 *
 * Without these gates, nearly every high line shows SLUMP because players
 * naturally miss high thresholds most of the time.
 */
export function getLineSignals(row: LineSignalInputs, hitRate60: number | null = null): Signal[] {
  const signals: Signal[] = [];
  const { momentumGrade } = row;
  const hr = hitRate60;

  if (momentumGrade != null) {
    // STREAK: on a hit run, only meaningful if they don't always hit it (hr <= 0.80)
    // and they hit it often enough for a streak to stand out (hr >= 0.25)
    if (momentumGrade > 75 && (hr === null || (hr >= 0.25 && hr <= 0.80))) {
      signals.push({ type: 'STREAK', ...SIGNAL_DEFS.STREAK });
    }
    // SLUMP: on a miss run, only meaningful if they normally hit it (hr >= 0.30)
    // If they miss it 80% of the time normally, a miss streak is not a signal
    if (momentumGrade < 25 && (hr === null || hr >= 0.30)) {
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
