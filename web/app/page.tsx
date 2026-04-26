import HomeHub from './HomeHub';

// Home page is the sport picker. The admin link is always shown; the
// /admin page itself is passcode-gated, so a visible link does not
// expose anything. Earlier versions cookie-gated the link visibility
// (sb_unlock=go), which created a bootstrap problem: a fresh device
// had no discoverable path to admin without the triple-tap trigger.
export default function HomePage() {
  return <HomeHub />;
}
