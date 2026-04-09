// Shared signal definitions used across GradesPageInner, PropMatrix, and PlayerPageInner.

export type SignalType = 'HOT' | 'COLD' | 'DUE' | 'FADE' | 'STREAK' | 'SLUMP';

export interface Signal {
  type: SignalType;
  label: string;
  title: string;
  chipClass: string;
}

export const SIGNAL_DEFS: Record<SignalType, Omit<Signal, 'type'>> = {
  HOT:    { label: 'HOT',  chipClass: 'bg-amber-900/50 text-amber-300 border-amber-700/50',       title: 'Performing above recent baseline (L10 vs L30 stat mean)' },
  COLD:   { label: 'COLD', chipClass: 'bg-blue-900/50 text-blue-300 border-blue-700/50',          title: 'Performing below recent baseline (L10 vs L30 stat mean)' },
  DUE:    { label: 'DUE',  chipClass: 'bg-green-900/50 text-green-300 border-green-700/50',       title: 'Below season average — bounce-back candidate' },
  FADE:   { label: 'FADE', chipClass: 'bg-red-900/50 text-red-300 border-red-700/50',             title: 'Above season average — regression risk' },
  STREAK: { label: 'STK',  chipClass: 'bg-emerald-900/50 text-emerald-300 border-emerald-700/50', title: 'Active hit streak for this prop line' },
  SLUMP:  { label: 'SLP',  chipClass: 'bg-orange-900/50 text-orange-300 border-orange-700/50',   title: 'Active miss streak for this prop line' },
};

export interface SignalInputs {
  trendGrade:      number | null;
  regressionGrade: number | null;
  momentumGrade:   number | null;
}

/**
 * Derive signals from the three grade components.
 *
 * trendGrade:      L10 stat avg vs L30 avg. >72 = trending up (HOT), <28 = trending down (COLD).
 * regressionGrade: z-score of L10 vs full season. >72 = below avg, due up (DUE). <28 = above avg, risk (FADE).
 * momentumGrade:   Active hit/miss streak for this specific line. >75 = hit streak (STREAK), <25 = miss streak (SLUMP).
 */
export function getSignals(row: SignalInputs): Signal[] {
  const signals: Signal[] = [];
  const { trendGrade, regressionGrade, momentumGrade } = row;

  if (trendGrade != null) {
    if (trendGrade > 72) signals.push({ type: 'HOT',  ...SIGNAL_DEFS.HOT  });
    if (trendGrade < 28) signals.push({ type: 'COLD', ...SIGNAL_DEFS.COLD });
  }

  if (regressionGrade != null) {
    if (regressionGrade > 72) signals.push({ type: 'DUE',  ...SIGNAL_DEFS.DUE  });
    if (regressionGrade < 28) signals.push({ type: 'FADE', ...SIGNAL_DEFS.FADE });
  }

  if (momentumGrade != null) {
    if (momentumGrade > 75) signals.push({ type: 'STREAK', ...SIGNAL_DEFS.STREAK });
    if (momentumGrade < 25) signals.push({ type: 'SLUMP',  ...SIGNAL_DEFS.SLUMP  });
  }

  return signals;
}
