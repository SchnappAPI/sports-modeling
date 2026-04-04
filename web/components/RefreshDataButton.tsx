'use client';

import { useRef, useState } from 'react';

type RefreshState = 'idle' | 'prompt' | 'submitting' | 'running' | 'reloading' | 'done' | 'error';

interface Props {
  /** Called when the refresh completes successfully so the parent can reload its data. */
  onComplete?: () => void;
  /** Optional extra CSS classes for the button wrapper. */
  className?: string;
}

export default function RefreshDataButton({ onComplete, className = '' }: Props) {
  const [state, setState]   = useState<RefreshState>('idle');
  const [code, setCode]     = useState('');
  const [error, setError]   = useState('');
  const pollRef             = useRef<ReturnType<typeof setInterval> | null>(null);
  const inputRef            = useRef<HTMLInputElement>(null);

  const isbusy = state === 'submitting' || state === 'running' || state === 'reloading';

  function openPrompt() {
    if (isbusy) return;
    setCode('');
    setError('');
    setState('prompt');
    setTimeout(() => inputRef.current?.focus(), 50);
  }

  function dismiss() {
    if (isbusy) return;
    setState('idle');
    setCode('');
    setError('');
  }

  async function submit() {
    if (!code.trim()) return;
    setState('submitting');
    setError('');

    try {
      const res  = await fetch('/api/refresh-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ code: code.trim() }),
      });
      const data = await res.json();

      if (!res.ok) {
        setError(data.error ?? 'Error.');
        setState('prompt');
        return;
      }

      const { runId } = data;
      setState('running');

      if (!runId) {
        // No run ID — fall back to fixed wait
        setTimeout(() => {
          setState('reloading');
          onComplete?.();
          setTimeout(() => setState('done'), 1500);
        }, 30000);
        return;
      }

      // Poll for completion
      let attempts = 0;
      pollRef.current = setInterval(async () => {
        attempts++;
        try {
          const sr = await fetch(`/api/refresh-status?runId=${runId}`);
          if (!sr.ok) return;
          const { status, conclusion } = await sr.json();
          if (status === 'completed') {
            if (pollRef.current) clearInterval(pollRef.current);
            if (conclusion === 'success') {
              setState('reloading');
              onComplete?.();
              setTimeout(() => setState('done'), 1500);
            } else {
              setState('error');
              setError('Refresh completed but did not succeed.');
            }
          } else if (attempts >= 36) {
            if (pollRef.current) clearInterval(pollRef.current);
            setState('error');
            setError('Timed out waiting for refresh.');
          }
        } catch { /* transient */ }
      }, 5000);

    } catch {
      setError('Connection error. Try again.');
      setState('prompt');
    }
  }

  function buttonLabel(): string {
    if (state === 'submitting') return 'Starting...';
    if (state === 'running')    return 'Refreshing...';
    if (state === 'reloading')  return 'Loading...';
    if (state === 'done')       return 'Updated';
    if (state === 'error')      return 'Retry';
    return 'Refresh Data';
  }

  function buttonClass(): string {
    const base = 'text-xs px-3 py-1 rounded border transition-colors font-medium flex items-center gap-1.5';
    if (isbusy)              return `${base} border-gray-700 text-gray-600 cursor-not-allowed`;
    if (state === 'done')    return `${base} border-green-800 text-green-600 cursor-pointer`;
    if (state === 'error')   return `${base} border-red-800 text-red-500 hover:border-red-600 cursor-pointer`;
    return `${base} border-gray-600 text-gray-400 hover:border-gray-400 hover:text-gray-200 cursor-pointer`;
  }

  return (
    <>
      {/* Trigger button */}
      <button
        onClick={state === 'idle' || state === 'done' || state === 'error' ? openPrompt : undefined}
        disabled={isbusy}
        className={`${buttonClass()} ${className}`}
      >
        {isbusy && (
          <span className="w-2 h-2 rounded-full bg-current animate-pulse" />
        )}
        {!isbusy && (
          <svg className="w-3 h-3" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5">
            <path strokeLinecap="round" d="M13.5 8A5.5 5.5 0 1 1 8 2.5M13.5 2.5v3h-3" />
          </svg>
        )}
        {buttonLabel()}
      </button>

      {/* Passcode modal */}
      {state === 'prompt' && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center bg-black/60"
          onClick={(e) => { if (e.target === e.currentTarget) dismiss(); }}
        >
          <div className="bg-gray-900 border border-gray-700 rounded-xl p-6 w-72 shadow-xl">
            <p className="text-sm font-semibold text-gray-200 mb-1">Admin refresh</p>
            <p className="text-xs text-gray-500 mb-4">Enter your admin code to refresh all data now.</p>
            <input
              ref={inputRef}
              type="text"
              value={code}
              onChange={(e) => setCode(e.target.value.toUpperCase())}
              onKeyDown={(e) => e.key === 'Enter' && submit()}
              placeholder="ADMIN-CODE"
              autoCapitalize="characters"
              autoCorrect="off"
              autoComplete="off"
              spellCheck={false}
              className="w-full bg-gray-800 border border-gray-600 rounded-lg px-3 py-2 text-white text-center text-sm font-mono tracking-widest placeholder-gray-600 focus:outline-none focus:border-gray-400 mb-2"
            />
            {error && <p className="text-xs text-red-400 text-center mb-2">{error}</p>}
            <div className="flex gap-2">
              <button
                onClick={dismiss}
                className="flex-1 py-2 rounded-lg border border-gray-700 text-gray-400 text-sm hover:border-gray-500 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={submit}
                disabled={!code.trim()}
                className="flex-1 py-2 rounded-lg bg-white text-gray-950 text-sm font-semibold disabled:opacity-40 active:scale-95 transition-transform"
              >
                Refresh
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Error toast (post-modal) */}
      {state === 'error' && error && (
        <p className="text-xs text-red-400 mt-1">{error}</p>
      )}
    </>
  );
}
