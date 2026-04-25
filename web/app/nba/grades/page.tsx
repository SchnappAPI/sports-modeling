import { Suspense } from 'react';
import GradesPageInner from './GradesPageInner';
import ComingSoon from '@/components/ComingSoon';
import { isPageVisible } from '@/lib/feature-flags';

export default async function GradesPage() {
  if (!(await isPageVisible('page.nba.grades'))) return <ComingSoon label="At a Glance" />;
  return (
    <Suspense fallback={<div className="p-4 text-sm text-gray-500">Loading...</div>}>
      <GradesPageInner />
    </Suspense>
  );
}
