import { Suspense } from 'react';
import ComingSoon from '@/components/ComingSoon';
import { isPageVisible } from '@/lib/feature-flags';
import TransparencyInner from './TransparencyInner';

// /transparency: read-only page showing model calibration accuracy and tier
// hit rates over time. Behind feature flag `page.transparency` (default off).
// The admin cookie (sb_unlock=go) bypasses the flag, so signing into /admin
// makes the page visible without flipping the flag publicly.

export default async function TransparencyPage() {
  if (!(await isPageVisible('page.transparency'))) return <ComingSoon label="Transparency" />;
  return (
    <Suspense fallback={<div className="px-4 py-3 text-sm text-gray-500">Loading...</div>}>
      <TransparencyInner />
    </Suspense>
  );
}
