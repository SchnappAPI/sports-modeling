const CACHE_NAME = 'schnapp-shell-v1';

// App shell assets to cache on install.
// Next.js generates hashed filenames for JS/CSS chunks — we cache the
// navigation routes only. Static chunks are cached on first fetch below.
const SHELL_URLS = [
  '/',
  '/nba',
  '/nba/grades',
];

self.addEventListener('install', (event) => {
  self.skipWaiting();
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => cache.addAll(SHELL_URLS))
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME)
          .map((key) => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Never intercept API calls — always go to network for live data.
  if (url.pathname.startsWith('/api/')) return;

  // Never intercept auth routes.
  if (url.pathname.startsWith('/.auth/')) return;

  // For navigation requests (HTML pages): network first, fall back to cache.
  // This means users always get fresh HTML when online, but the app still
  // loads offline or on a flaky connection.
  if (event.request.mode === 'navigate') {
    event.respondWith(
      fetch(event.request)
        .then((response) => {
          const clone = response.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
          return response;
        })
        .catch(() => caches.match(event.request))
    );
    return;
  }

  // For static assets (JS, CSS, fonts, images): cache first, then network.
  // Next.js hashes these filenames so stale cache is never an issue.
  if (
    url.pathname.startsWith('/_next/static/') ||
    url.pathname.startsWith('/icons/') ||
    url.pathname.endsWith('.png') ||
    url.pathname.endsWith('.ico')
  ) {
    event.respondWith(
      caches.match(event.request).then(
        (cached) =>
          cached ??
          fetch(event.request).then((response) => {
            const clone = response.clone();
            caches.open(CACHE_NAME).then((cache) => cache.put(event.request, clone));
            return response;
          })
      )
    );
    return;
  }
});
