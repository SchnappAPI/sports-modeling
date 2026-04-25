// Server-component placeholder rendered when a page's visibility flag is
// off. Intentionally minimal so it works without client JS and matches
// the maintenance-page aesthetic.

export default function ComingSoon({ label }: { label: string }) {
  return (
    <main className="min-h-[60vh] flex flex-col items-center justify-center text-center px-6 py-16">
      <h1 className="text-base font-medium text-gray-300 mb-2">{label}</h1>
      <p className="text-sm text-gray-500 max-w-sm leading-relaxed">
        Coming soon. We are still putting this together.
      </p>
    </main>
  );
}
