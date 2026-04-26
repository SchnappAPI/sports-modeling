import { cookies } from 'next/headers';
import HomeHub from './HomeHub';

// Server component. Checks the admin bypass cookie server-side so the
// "admin" link can be rendered conditionally without exposing it to
// anonymous visitors. The triple-tap AdminTrigger remains as the
// universal escape hatch on every page.
export default async function HomePage() {
  const c = await cookies();
  const showAdminLink = c.get('sb_unlock')?.value === 'go';
  return <HomeHub showAdminLink={showAdminLink} />;
}
