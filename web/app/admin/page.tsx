'use client';

import { useState, useEffect, useCallback } from 'react';

const ADMIN_KEY = 'schnapp_admin_token';

interface CodeRow {
  code: string;
  name: string;
  active: boolean;
  activated: boolean;
  activated_at: string | null;
  last_seen_at: string | null;
  created_at: string;
}

function fmt(dt: string | null): string {
  if (!dt) return '—';
  return new Date(dt).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

export default function AdminPage() {
  const [authed, setAuthed] = useState(false);
  const [pin, setPin] = useState('');
  const [pinError, setPinError] = useState('');
  const [codes, setCodes] = useState<CodeRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [newCode, setNewCode] = useState('');
  const [newName, setNewName] = useState('');
  const [adding, setAdding] = useState(false);
  const [addError, setAddError] = useState('');
  const [savedPin, setSavedPin] = useState('');

  const loadCodes = useCallback(async (adminPin: string) => {
    setLoading(true);
    try {
      const res = await fetch('/api/admin/codes', {
        headers: { 'x-admin-token': adminPin },
      });
      if (!res.ok) return;
      const data = await res.json();
      setCodes(data.codes ?? []);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const stored = localStorage.getItem(ADMIN_KEY);
    if (stored) { setSavedPin(stored); setAuthed(true); loadCodes(stored); }
  }, [loadCodes]);

  async function handleLogin() {
    setPinError('');
    const res = await fetch('/api/admin/codes', {
      headers: { 'x-admin-token': pin },
    });
    if (res.ok) {
      localStorage.setItem(ADMIN_KEY, pin);
      setSavedPin(pin);
      setAuthed(true);
      const data = await res.json();
      setCodes(data.codes ?? []);
    } else {
      setPinError('Wrong passcode.');
    }
  }

  async function toggleActive(code: string, current: boolean) {
    await fetch('/api/admin/codes', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json', 'x-admin-token': savedPin },
      body: JSON.stringify({ code, active: !current }),
    });
    setCodes((prev) => prev.map((c) => c.code === code ? { ...c, active: !current } : c));
  }

  async function addCode() {
    if (!newCode.trim() || !newName.trim()) { setAddError('Both fields required.'); return; }
    setAdding(true);
    setAddError('');
    const res = await fetch('/api/admin/codes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-admin-token': savedPin },
      body: JSON.stringify({ code: newCode.trim().toUpperCase(), name: newName.trim() }),
    });
    if (res.ok) {
      setNewCode(''); setNewName('');
      loadCodes(savedPin);
    } else {
      const d = await res.json();
      setAddError(d.error ?? 'Failed to add.');
    }
    setAdding(false);
  }

  if (!authed) {
    return (
      <div className="min-h-screen bg-gray-950 flex flex-col items-center justify-center px-6">
        <div className="w-full max-w-sm space-y-3">
          <h1 className="text-lg font-semibold text-white text-center mb-6">Admin</h1>
          <input
            type="password"
            value={pin}
            onChange={(e) => setPin(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && handleLogin()}
            placeholder="Admin passcode"
            className="w-full bg-gray-900 border border-gray-700 rounded-xl px-4 py-3 text-white focus:outline-none focus:border-gray-500"
          />
          {pinError && <p className="text-sm text-red-400 text-center">{pinError}</p>}
          <button
            onClick={handleLogin}
            className="w-full bg-white text-gray-950 rounded-xl py-3 font-semibold text-sm active:scale-95 transition-transform"
          >
            Enter
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-950 px-4 py-6 max-w-lg mx-auto">
      <div className="flex items-center justify-between mb-6">
        <h1 className="text-lg font-semibold text-white">Access Codes</h1>
        <button
          onClick={() => loadCodes(savedPin)}
          className="text-xs text-gray-500 border border-gray-700 rounded-lg px-3 py-1.5"
        >
          Refresh
        </button>
      </div>

      {/* Add new code */}
      <div className="bg-gray-900 rounded-xl p-4 mb-6 space-y-3">
        <p className="text-xs text-gray-500 font-medium uppercase tracking-wider">Add New Code</p>
        <input
          type="text"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="Person's name"
          className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:outline-none focus:border-gray-500"
        />
        <input
          type="text"
          value={newCode}
          onChange={(e) => setNewCode(e.target.value.toUpperCase())}
          placeholder="SPICY-WALRUS-429"
          className="w-full bg-gray-800 border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:outline-none focus:border-gray-500"
        />
        {addError && <p className="text-xs text-red-400">{addError}</p>}
        <button
          onClick={addCode}
          disabled={adding}
          className="w-full bg-white text-gray-950 rounded-lg py-2 font-semibold text-sm disabled:opacity-40 active:scale-95 transition-transform"
        >
          {adding ? 'Adding...' : 'Add Code'}
        </button>
      </div>

      {/* Code list */}
      {loading ? (
        <div className="text-sm text-gray-500 text-center py-8">Loading...</div>
      ) : (
        <div className="space-y-3">
          {codes.map((c) => (
            <div key={c.code} className={`bg-gray-900 rounded-xl p-4 border ${
              c.active ? 'border-gray-800' : 'border-gray-700 opacity-50'
            }`}>
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="font-semibold text-white text-sm">{c.name}</p>
                  <p className="text-xs font-mono text-gray-400 mt-0.5">{c.code}</p>
                  <p className="text-xs text-gray-600 mt-1">
                    {c.activated
                      ? `Activated ${fmt(c.activated_at)} · Last seen ${fmt(c.last_seen_at)}`
                      : 'Not yet activated'}
                  </p>
                </div>
                <button
                  onClick={() => toggleActive(c.code, c.active)}
                  className={`flex-shrink-0 text-xs px-3 py-1.5 rounded-lg font-medium border transition-colors ${
                    c.active
                      ? 'border-red-800 text-red-400 hover:bg-red-900/30'
                      : 'border-green-800 text-green-400 hover:bg-green-900/30'
                  }`}
                >
                  {c.active ? 'Disable' : 'Enable'}
                </button>
              </div>
            </div>
          ))}
          {codes.length === 0 && (
            <p className="text-sm text-gray-600 text-center py-8">No codes yet.</p>
          )}
        </div>
      )}
    </div>
  );
}
