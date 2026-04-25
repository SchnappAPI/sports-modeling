import { Suspense } from 'react';
import MlbPageInner from './MlbPageInner';
import ComingSoon from '@/components/ComingSoon';
import { isPageVisible } from '@/lib/feature-flags';

export default async function MlbPage() {
  if (!(await isPageVisible('sport.mlb'))) return <ComingSoon label="MLB" />;
  return (
    <Suspense fallback={<div className="px-4 py-3 text-sm text-gray-500">Loading...</div>}>
      <MlbPageInner />
    </Suspense>
  );
}
