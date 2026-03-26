interface Props {
  params: { playerId: string };
}

export default function PlayerPage({ params }: Props) {
  return (
    <main className="p-4">
      <h1 className="text-xl font-semibold">Player {params.playerId}</h1>
      <p className="text-sm text-gray-500 mt-1">Player detail coming in step 8.</p>
    </main>
  );
}
