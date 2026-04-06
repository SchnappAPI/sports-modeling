import { Suspense } from 'react';
import MlbPageInner from './MlbPageInner';

export default function MlbPage() {
  return (
    <Suspense fallback={<div className="px-4 py-3 text-sm text-gray-500">Loading...</div>}>
      <MlbPageInner />
    </Suspense>
  );
}
