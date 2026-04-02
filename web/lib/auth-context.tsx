'use client';

/**
 * auth-context.tsx
 *
 * Provides useAuth() to any client component.
 *
 * mode: 'live' | 'demo'
 *   live  — normal user, full date navigation
 *   demo  — locked to demoDates, no date picker, no Refresh Lines
 *
 * demoDates: { nba: string | null }
 *   The fixed date shown to demo users for each sport (YYYY-MM-DD).
 *   null when the sport has no demo date configured.
 *
 * logout: () => void
 *   Clears localStorage and reloads to force the passcode gate.
 *
 * AuthContext is populated by PasscodeGate, which writes mode and demoDates
 * to localStorage after a successful login or token check. Components that
 * call useAuth() must be descendants of PasscodeGate in the tree.
 */

import { createContext, useContext } from 'react';

export interface DemoDates {
  nba: string | null;
}

export interface AuthState {
  mode: 'live' | 'demo';
  demoDates: DemoDates;
  logout: () => void;
}

const DEFAULT: AuthState = {
  mode: 'live',
  demoDates: { nba: null },
  logout: () => {
    localStorage.removeItem('schnapp_auth_token');
    localStorage.removeItem('schnapp_auth_mode');
    localStorage.removeItem('schnapp_demo_dates');
    window.location.reload();
  },
};

export const AuthContext = createContext<AuthState>(DEFAULT);

export function useAuth(): AuthState {
  return useContext(AuthContext);
}
