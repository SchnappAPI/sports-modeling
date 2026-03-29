import { Suspense } from 'react';
import NbaPageInner from './NbaPageInner';

export default function NbaPage() {
  return (
    <Suspense fallback={<div className="px-4 py-3 text-sm text-gray-500">Loading...</div>}>
      <NbaPageInner />
    </Suspense>
  );
}
