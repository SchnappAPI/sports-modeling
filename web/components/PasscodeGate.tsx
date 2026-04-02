'use client';

import { useEffect, useState, useCallback } from 'react';

const BYPASS = false; // Set to true to disable passcode gate

const TOKEN_KEY = 'schnapp_auth_token';

export default function PasscodeGate({ children }: { children: React.ReactNode }) {
  const [status, setStatus] = useState<'loading' | 'authed' | 'gate'>('loading');
  const [code, setCode] = useState('');
  const [error, setError] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const verify = useCallback(async () => {
    if (BYPASS) { setStatus('authed'); return; }
    const token = localStorage.getItem(TOKEN_KEY);
    if (!token) { setStatus('gate'); return; }
    try {
      const res = await fetch('/api/auth/check', {
        headers: { 'x-auth-token': token },
      });
      if (res.ok) {
        setStatus('authed');
      } else {
        localStorage.removeItem(TOKEN_KEY);
        setStatus('gate');
      }
    } catch {
      setStatus('gate');
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
        localStorage.setItem(TOKEN_KEY, data.token);
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

  if (status === 'loading') {
    return (
      <div className="min-h-screen bg-gray-950 flex items-center justify-center">
        <div className="w-5 h-5 border-2 border-gray-600 border-t-gray-300 rounded-full animate-spin" />
      </div>
    );
  }

  if (status === 'authed') {
    return <>{children}</>;
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
