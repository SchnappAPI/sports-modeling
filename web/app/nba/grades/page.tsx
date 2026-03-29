import { Suspense } from 'react';
import GradesPageInner from './GradesPageInner';

export default function GradesPage() {
  return (
    <Suspense fallback={<div className="p-4 text-sm text-gray-500">Loading...</div>}>
      <GradesPageInner />
    </Suspense>
  );
}
