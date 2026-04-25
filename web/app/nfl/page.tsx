import ComingSoon from '@/components/ComingSoon';
import { isPageVisible } from '@/lib/feature-flags';

export default async function NflPage() {
  if (!(await isPageVisible('sport.nfl'))) return <ComingSoon label="NFL" />;
  // Sport is enabled but no real page exists yet. Render the same
  // placeholder so toggling visibility on does not 404.
  return <ComingSoon label="NFL" />;
}
