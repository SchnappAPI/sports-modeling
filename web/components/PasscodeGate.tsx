'use client';

import { useEffect, useState, useCallback } from 'react';
import { AuthContext, type DemoDates } from '@/lib/auth-context';

const TOKEN_KEY      = 'schnapp_auth_token';
const MODE_KEY       = 'schnapp_auth_mode';
const DEMO_DATES_KEY = 'schnapp_demo_dates';

function readDemoDates(): DemoDates {
  try {
    const raw = localStorage.getItem(DEMO_DATES_KEY);
    return raw ? JSON.parse(raw) : { nba: null };
  } catch {
    return { nba: null };
  }
}

function writeDemoDates(d: DemoDates) {
  localStorage.setItem(DEMO_DATES_KEY, JSON.stringify(d));
}

export default function PasscodeGate({ children }: { children: React.ReactNode }) {
  const [status, setStatus]       = useState<'loading' | 'authed' | 'gate'>('loading');
  const [mode, setMode]           = useState<'live' | 'demo'>('live');
  const [demoDates, setDemoDates] = useState<DemoDates>({ nba: null });
  const [code, setCode]           = useState('');
  const [error, setError]         = useState('');
  const [submitting, setSubmitting] = useState(false);

  const verify = useCallback(async () => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (!token) { setStatus('gate'); return; }
    try {
      const res = await fetch('/api/auth/check', {
        headers: { 'x-auth-token': token },
      });
      if (res.ok) {
        const data = await res.json();
        const m: 'live' | 'demo' = data.mode === 'demo' ? 'demo' : 'live';
        const dd: DemoDates = data.demoDates ?? { nba: null };
        localStorage.setItem(MODE_KEY, m);
        writeDemoDates(dd);
        setMode(m);
        setDemoDates(dd);
        setStatus('authed');
      } else {
        localStorage.removeItem(TOKEN_KEY);
        localStorage.removeItem(MODE_KEY);
        localStorage.removeItem(DEMO_DATES_KEY);
        setStatus('gate');
      }
    } catch {
      // Network error — trust cached mode so offline PWA still works.
      const cachedMode = localStorage.getItem(MODE_KEY);
      setMode(cachedMode === 'demo' ? 'demo' : 'live');
      setDemoDates(readDemoDates());
      setStatus('authed');
    }
  }, []);

  useEffect(() => { verify(); }, [verify]);

  async function handleSubmit() {
    if (!code.trim()) return;
    setSubmitting(true);
    setError('');
    try {
      const res = await fetch('/api/auth/validate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: code.trim() }),
      });
      const data = await res.json();
      if (res.ok && data.token) {
        const m: 'live' | 'demo' = data.mode === 'demo' ? 'demo' : 'live';
        const dd: DemoDates = data.demoDates ?? { nba: null };
        localStorage.setItem(TOKEN_KEY, data.token);
        localStorage.setItem(MODE_KEY, m);
        writeDemoDates(dd);
        setMode(m);
        setDemoDates(dd);
        setStatus('authed');
      } else {
        setError(data.error ?? 'Invalid code.');
      }
    } catch {
      setError('Connection error. Try again.');
    } finally {
      setSubmitting(false);
    }
  }

  function logout() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(MODE_KEY);
    localStorage.removeItem(DEMO_DATES_KEY);
    setStatus('gate');
    setCode('');
    setError('');
  }

  if (status === 'loading') {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <div className="w-5 h-5 border-2 border-gray-600 border-t-gray-300 rounded-full animate-spin" />
      </div>
    );
  }

  if (status === 'authed') {
    return (
      <AuthContext.Provider value={{ mode, demoDates, logout }}>
        {children}
      </AuthContext.Provider>
    );
  }

  return (
    <div className="min-h-screen bg-gray-950 flex flex-col items-center justify-center px-6">
      <div className="w-full max-w-sm">
        <div className="mb-8 text-center">
          <div className="w-16 h-16 rounded-2xl bg-gray-800 flex items-center justify-center mx-auto mb-4">
            <span className="text-3xl font-bold text-white">S</span>
          </div>
          <h1 className="text-xl font-semibold text-white">Schnapp</h1>
          <p className="text-sm text-gray-500 mt-1">Enter your access code to continue</p>
        </div>

        <div className="space-y-3">
          <input
            type="text"
            value={code}
            onChange={(e) => setCode(e.target.value.toUpperCase())}
            onKeyDown={(e) => e.key === 'Enter' && handleSubmit()}
            placeholder="SPICY-WALRUS-429"
            autoCapitalize="characters"
            autoCorrect="off"
            autoComplete="off"
            spellCheck={false}
            className="w-full bg-gray-900 border border-gray-700 rounded-xl px-4 py-3 text-white text-center text-lg font-mono tracking-widest placeholder-gray-600 focus:outline-none focus:border-gray-500"
          />
          {error && (
            <p className="text-sm text-red-400 text-center">{error}</p>
          )}
          <button
            onClick={handleSubmit}
            disabled={submitting || !code.trim()}
            className="w-full bg-white text-gray-950 rounded-xl py-3 font-semibold text-sm disabled:opacity-40 active:scale-95 transition-transform"
          >
            {submitting ? 'Checking...' : 'Enter'}
          </button>
        </div>
      </div>
    </div>
  );
}
