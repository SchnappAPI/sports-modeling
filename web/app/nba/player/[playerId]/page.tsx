type Props = { params: Promise<{ playerId: string }> };

export default async function PlayerPage({ params }: Props) {
  const { playerId } = await params;
  return (
    <main className="p-4">
      <h1 className="text-xl font-semibold">Player {playerId}</h1>
      <p className="text-sm text-gray-500 mt-1">Player detail coming in step 8.</p>
    </main>
  );
}
