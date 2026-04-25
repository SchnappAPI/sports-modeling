'use client';

import { useRef } from 'react';
import { useRouter } from 'next/navigation';

// Triple-tap hidden trigger. A 32x32 fully-transparent fixed-position
// button anchored to the top-left of the viewport. Three taps within
// 800ms navigate to /admin. There is no visible affordance — by design.
//
// If a later page header places clickable content directly under the
// top-left 32x32 region, this trigger will intercept those clicks.
// Adjust the position (or shrink the size) here if that comes up.

const TAP_WINDOW_MS = 800;
const REQUIRED_TAPS = 3;

export default function AdminTrigger() {
  const router = useRouter();
  const taps = useRef<number[]>([]);

  function onTap() {
    const now = Date.now();
    const recent = taps.current.filter((t) => now - t < TAP_WINDOW_MS);
    recent.push(now);
    if (recent.length >= REQUIRED_TAPS) {
      taps.current = [];
      router.push('/admin');
      return;
    }
    taps.current = recent;
  }

  return (
    <button
      type="button"
      onClick={onTap}
      aria-label=""
      tabIndex={-1}
      style={{
        position: 'fixed',
        top: 'env(safe-area-inset-top, 0)',
        left: 0,
        width: 32,
        height: 32,
        background: 'transparent',
        border: 'none',
        padding: 0,
        margin: 0,
        cursor: 'default',
        zIndex: 9999,
        WebkitTapHighlightColor: 'transparent',
      }}
    />
  );
}
