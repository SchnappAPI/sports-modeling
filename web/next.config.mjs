/** @type {import('next').NextConfig} */
const nextConfig = {
  serverExternalPackages: ['mssql'],

  // Cache headers for API routes.
  // These apply at the CDN/SWA edge layer, not the Next.js server.
  // Individual API routes can override with their own cache-control headers.
  async headers() {
    return [
      {
        // Schedule and roster data: cache for 60 seconds at the edge.
        // Stale for 60s, then revalidate in background (stale-while-revalidate).
        source: '/api/games',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, s-maxage=60, stale-while-revalidate=120',
          },
        ],
      },
      {
        source: '/api/roster',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, s-maxage=60, stale-while-revalidate=120',
          },
        ],
      },
      {
        // Grades: cache for 90 seconds. Pre-game refresh runs every 30 min
        // so 90s staleness is imperceptible.
        source: '/api/grades',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, s-maxage=90, stale-while-revalidate=180',
          },
        ],
      },
      {
        source: '/api/game-grades',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, s-maxage=90, stale-while-revalidate=180',
          },
        ],
      },
      {
        // Contextual defense: cache for 10 minutes. Changes only when new
        // games are added to the season — very stable data.
        source: '/api/contextual',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, s-maxage=600, stale-while-revalidate=1200',
          },
        ],
      },
      {
        // Player data: cache for 60 seconds.
        source: '/api/player',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, s-maxage=60, stale-while-revalidate=120',
          },
        ],
      },
      {
        // Box score: no cache. This is live data polled every 30 seconds.
        source: '/api/boxscore',
        headers: [
          {
            key: 'Cache-Control',
            value: 'no-cache, no-store, must-revalidate',
          },
        ],
      },
      {
        // Ping: no cache. Used for keep-alive.
        source: '/api/ping',
        headers: [
          {
            key: 'Cache-Control',
            value: 'no-cache, no-store, must-revalidate',
          },
        ],
      },
    ];
  },
};

export default nextConfig;
