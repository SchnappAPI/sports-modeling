import { Suspense } from 'react';
import NbaPageInner from './NbaPageInner';
import ComingSoon from '@/components/ComingSoon';
import { isPageVisible } from '@/lib/feature-flags';

export default async function NbaPage() {
  if (!(await isPageVisible('sport.nba'))) return <ComingSoon label="NBA" />;
  return (
    <Suspense fallback={<div className="px-4 py-3 text-sm text-gray-500">Loading...</div>}>
      <NbaPageInner />
    </Suspense>
  );
}
