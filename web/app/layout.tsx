import type { Metadata, Viewport } from 'next';
import './globals.css';
import PasscodeGate from '@/components/PasscodeGate';
import AdminTrigger from '@/components/AdminTrigger';

export const metadata: Metadata = {
  title: 'Schnapp',
  description: 'NBA, NFL, and MLB prop betting research',
  manifest: '/manifest.json',
  appleWebApp: {
    capable: true,
    statusBarStyle: 'black-translucent',
    title: 'Schnapp',
  },
};

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  maximumScale: 1,
  userScalable: false,
  themeColor: '#030712',
  viewportFit: 'cover',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <head>
        <link rel="apple-touch-icon" href="/icon.svg" />
        <link rel="icon" type="image/svg+xml" href="/icon.svg" />
        <meta name="mobile-web-app-capable" content="yes" />
        <script
          dangerouslySetInnerHTML={{
            __html: `
              if ('serviceWorker' in navigator) {
                window.addEventListener('load', function() {
                  navigator.serviceWorker.register('/sw.js');
                });
              }
            `,
          }}
        />
      </head>
      <body
        className="bg-gray-950 text-gray-100 min-h-screen"
        style={{ paddingTop: 'env(safe-area-inset-top)', paddingBottom: 'env(safe-area-inset-bottom)' }}
      >
        <PasscodeGate>
          {children}
        </PasscodeGate>
        <AdminTrigger />
      </body>
    </html>
  );
}
