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
 * demoDates: { nba?: string; nfl?: string; mlb?: string }
 *   The fixed date shown to demo users per sport (YYYY-MM-DD).
 *   Sourced from common.demo_config via the auth/check route on every
 *   page load, so admin changes to the demo date take effect immediately
 *   without requiring users to log in again.
 *
 * logout: () => void
 *   Clears localStorage and resets to the passcode gate.
 *
 * AuthContext is populated by PasscodeGate. Components that call useAuth()
 * must be descendants of PasscodeGate in the tree.
 */

import { createContext, useContext } from 'react';

export interface DemoDates {
  nba?: string;
  nfl?: string;
  mlb?: string;
}

export interface AuthState {
  mode: 'live' | 'demo';
  demoDates: DemoDates;
  logout: () => void;
}

const DEFAULT: AuthState = {
  mode: 'live',
  demoDates: {},
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
