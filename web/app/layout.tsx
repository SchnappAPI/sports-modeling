export const metadata = {
  title: 'Sports Modeling',
  description: 'NBA, MLB, NFL prop research',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
