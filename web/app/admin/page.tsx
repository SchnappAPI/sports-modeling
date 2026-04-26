'use client';

import { useState, useEffect, useCallback } from 'react';

const ADMIN_KEY = 'schnapp_admin_token';
type Tab = 'codes' | 'visibility' | 'tools';

interface CodeRow {
  code: string;
  name: string;
  active: boolean;
  activated: boolean;
  activated_at: string | null;
  last_seen_at: string | null;
  created_at: string;
}

interface FlagRow {
  flag_key: string;
  enabled: boolean;
  updated_at: string;
}

function fmt(dt: string | null): string {
  if (!dt) return '—';
  return new Date(dt).toLocaleDateString('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit',
  });
}

// Pretty labels for known flag keys. Unknown keys render their raw key.
function flagLabel(key: string): string {
  const map: Record<string, string> = {
    'maintenance_mode': 'Maintenance mode',
    'sport.nba':        'NBA',
    'sport.mlb':        'MLB',
    'sport.nfl':        'NFL',
    'page.nba.grades':  'NBA · Grades',
    'page.nba.player':  'NBA · Player',
    'page.mlb.main':    'MLB · Main',
    'page.transparency': 'Transparency',
  };
  return map[key] ?? key;
}

function flagGroup(key: string): 'maintenance' | 'sport' | 'page' | 'other' {
  if (key === 'maintenance_mode') return 'maintenance';
  if (key.startsWith('sport.'))   return 'sport';
  if (key.startsWith('page.'))    return 'page';
  return 'other';
}

export default function AdminPage() {
  const [authed, setAuthed]   = useState(false);
  const [pin, setPin]         = useState('');
  const [pinError, setPinError] = useState('');
  const [savedPin, setSavedPin] = useState('');
  const [tab, setTab]         = useState<Tab>('codes');

  // Codes state
  const [codes, setCodes]     = useState<CodeRow[]>([]);
  const [codesLoading, setCodesLoading] = useState(false);
  const [newCode, setNewCode] = useState('');
  const [newName, setNewName] = useState('');
  const [adding, setAdding]   = useState(false);
  const [addError, setAddError] = useState('');

  // Flags state
  const [flags, setFlags] = useState<FlagRow[]>([]);
  const [flagsLoading, setFlagsLoading] = useState(false);

  // Tools state
  const [refreshState, setRefreshState] = useState<'idle' | 'running' | 'done' | 'error'>('idle');
  const [refreshMsg, setRefreshMsg]     = useState('');
  const [flagsToast, setFlagsToast] = useState<string | null>(null);

  const loadCodes = useCallback(async (adminPin: string) => {
    setCodesLoading(true);
    try {
      const res = await fetch('/api/admin/codes', { headers: { 'x-admin-token': adminPin } });
      if (!res.ok) return;
      const data = await res.json();
      setCodes(data.codes ?? []);
    } finally {
      setCodesLoading(false);
    }
  }, []);

  const loadFlags = useCallback(async (adminPin: string) => {
    setFlagsLoading(true);
    try {
      const res = await fetch('/api/admin/flags', { headers: { 'x-admin-token': adminPin } });
      if (!res.ok) return;
      const data = await res.json();
      setFlags(data.flags ?? []);
    } finally {
      setFlagsLoading(false);
    }
  }, []);

  useEffect(() => {
    const stored = localStorage.getItem(ADMIN_KEY);
    if (stored) {
      setSavedPin(stored);
      setAuthed(true);
      loadCodes(stored);
      loadFlags(stored);
    }
  }, [loadCodes, loadFlags]);

  async function handleLogin() {
    setPinError('');
    const res = await fetch('/api/admin/codes', { headers: { 'x-admin-token': pin } });
    if (res.ok) {
      localStorage.setItem(ADMIN_KEY, pin);
      setSavedPin(pin);
      setAuthed(true);
      const data = await res.json();
      setCodes(data.codes ?? []);
      loadFlags(pin);
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

  async function toggleFlag(key: string, current: boolean) {
    // Optimistic update.
    setFlags((prev) => prev.map((f) => f.flag_key === key ? { ...f, enabled: !current } : f));
    const res = await fetch('/api/admin/flags', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json', 'x-admin-token': savedPin },
      body: JSON.stringify({ flag_key: key, enabled: !current }),
    });
    if (res.ok) {
      setFlagsToast('Saved');
      setTimeout(() => setFlagsToast(null), 1500);
    } else {
      // Roll back on error.
      setFlags((prev) => prev.map((f) => f.flag_key === key ? { ...f, enabled: current } : f));
      setFlagsToast('Save failed');
      setTimeout(() => setFlagsToast(null), 2500);
    }
  }

  async function runRefresh() {
    setRefreshState('running');
    setRefreshMsg('Triggering refresh...');
    try {
      const res = await fetch('/api/refresh-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-admin-token': savedPin },
        body: JSON.stringify({}),
      });
      const data = await res.json();
      if (!res.ok) {
        setRefreshState('error');
        setRefreshMsg(data.error ?? 'Failed.');
        return;
      }
      setRefreshState('done');
      setRefreshMsg(data.runId ? `Run #${data.runId} dispatched.` : 'Dispatched.');
    } catch (e) {
      setRefreshState('error');
      setRefreshMsg(e instanceof Error ? e.message : 'Failed.');
    }
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

  // True when site-wide maintenance is currently on. Surfaced in a banner
  // under the h1 so it is visible regardless of which tab is open. The
  // admin viewer is bypassed via the sb_unlock cookie set on login, so
  // the banner is the only signal that the gate is live.
  const maintenanceOn = flags.some((f) => f.flag_key === 'maintenance_mode' && f.enabled);

  // Group flags for the visibility tab.
  const maintFlags = flags.filter((f) => flagGroup(f.flag_key) === 'maintenance');
  const sportFlags = flags.filter((f) => flagGroup(f.flag_key) === 'sport');
  const pageFlags  = flags.filter((f) => flagGroup(f.flag_key) === 'page');
  const otherFlags = flags.filter((f) => flagGroup(f.flag_key) === 'other');

  return (
    <div className="min-h-screen bg-gray-950 px-4 py-6 max-w-lg mx-auto">
      <h1 className="text-lg font-semibold text-white mb-4">Admin</h1>

      {maintenanceOn && (
        <div className="mb-4 rounded-lg border border-yellow-700 bg-yellow-900/30 px-3 py-2 text-xs text-yellow-200 leading-relaxed">
          <span className="font-semibold">Maintenance mode is ON.</span>{' '}
          Anonymous visitors see the 503 page. You see normal pages because
          your admin cookie bypasses the gate. Open an incognito window to
          verify the gate works.
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 mb-6 bg-gray-900 rounded-xl p-1">
        {(['codes', 'visibility', 'tools'] as Tab[]).map((t) => (
          <button
            key={t}
            onClick={() => setTab(t)}
            className={`flex-1 text-xs py-2 rounded-lg font-medium capitalize transition-colors ${
              tab === t ? 'bg-white text-gray-950' : 'text-gray-400'
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* CODES TAB */}
      {tab === 'codes' && (
        <>
          <div className="flex items-center justify-between mb-4">
            <p className="text-xs text-gray-500 font-medium uppercase tracking-wider">Access Codes</p>
            <button
              onClick={() => loadCodes(savedPin)}
              className="text-xs text-gray-500 border border-gray-700 rounded-lg px-3 py-1.5"
            >
              Refresh
            </button>
          </div>

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

          {codesLoading ? (
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
        </>
      )}

      {/* VISIBILITY TAB */}
      {tab === 'visibility' && (
        <>
          <div className="flex items-center justify-between mb-4">
            <p className="text-xs text-gray-500 font-medium uppercase tracking-wider">Visibility</p>
            <button
              onClick={() => loadFlags(savedPin)}
              className="text-xs text-gray-500 border border-gray-700 rounded-lg px-3 py-1.5"
            >
              Refresh
            </button>
          </div>

          {flagsLoading && flags.length === 0 ? (
            <div className="text-sm text-gray-500 text-center py-8">Loading...</div>
          ) : (
            <div className="space-y-6">
              <FlagSection title="Site" flags={maintFlags} onToggle={toggleFlag} />
              <FlagSection title="Sports" flags={sportFlags} onToggle={toggleFlag} />
              <FlagSection title="Pages" flags={pageFlags}  onToggle={toggleFlag} />
              {otherFlags.length > 0 && (
                <FlagSection title="Other" flags={otherFlags} onToggle={toggleFlag} />
              )}
              <p className="text-xs text-gray-600 leading-relaxed pt-2">
                Changes propagate within ~60s (middleware cache). The admin
                cookie keeps you above all gates regardless of flag state.
              </p>
            </div>
          )}
        </>
      )}

      {/* TOOLS TAB */}
      {tab === 'tools' && (
        <>
          <p className="text-xs text-gray-500 font-medium uppercase tracking-wider mb-4">Tools</p>

          <div className="bg-gray-900 rounded-xl p-4 space-y-3">
            <div>
              <p className="text-sm font-medium text-white">Refresh Data</p>
              <p className="text-xs text-gray-500 mt-1">
                Runs live box score, odds, grading, and lineup poll. Takes ~30s.
              </p>
            </div>
            <button
              onClick={runRefresh}
              disabled={refreshState === 'running'}
              className="w-full bg-white text-gray-950 rounded-lg py-2 font-semibold text-sm disabled:opacity-40 active:scale-95 transition-transform"
            >
              {refreshState === 'running' ? 'Running...' : 'Run refresh'}
            </button>
            {refreshMsg && (
              <p className={`text-xs ${refreshState === 'error' ? 'text-red-400' : 'text-gray-400'}`}>
                {refreshMsg}
              </p>
            )}
          </div>
        </>
      )}
      {flagsToast && (
        <div
          className="fixed left-1/2 -translate-x-1/2 bottom-6 z-50 rounded-lg border border-gray-700 bg-gray-900 px-4 py-2 text-xs text-gray-100 shadow-lg"
          role="status"
          aria-live="polite"
        >
          {flagsToast}
        </div>
      )}
    </div>
  );
}

function FlagSection({
  title, flags, onToggle,
}: {
  title: string;
  flags: FlagRow[];
  onToggle: (key: string, current: boolean) => void;
}) {
  if (flags.length === 0) return null;
  return (
    <div>
      <p className="text-xs text-gray-500 font-medium uppercase tracking-wider mb-2">{title}</p>
      <div className="space-y-2">
        {flags.map((f) => (
          <div key={f.flag_key} className="bg-gray-900 rounded-xl px-4 py-3 flex items-center justify-between gap-3 border border-gray-800">
            <div className="min-w-0">
              <p className="text-sm text-white">{flagLabel(f.flag_key)}</p>
              <p className="text-xs font-mono text-gray-600 mt-0.5">{f.flag_key}</p>
            </div>
            <button
              onClick={() => onToggle(f.flag_key, f.enabled)}
              className={`flex-shrink-0 text-xs px-3 py-1.5 rounded-lg font-medium border transition-colors ${
                f.enabled
                  ? 'border-green-800 text-green-400 hover:bg-green-900/30'
                  : 'border-gray-700 text-gray-500 hover:bg-gray-800'
              }`}
            >
              {f.enabled ? 'On' : 'Off'}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
