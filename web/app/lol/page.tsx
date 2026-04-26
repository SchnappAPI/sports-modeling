import { redirect } from 'next/navigation';

// /lol was the original sport-picker route; the picker now lives at /.
// Kept as a redirect so any existing links or bookmarks continue to work.
export default function LolPage() {
  redirect('/');
}
