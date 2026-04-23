      fetch(`/api/player-grades?playerId=${playerId}`)
        .then((r) => r.ok ? r.json() : { grades: [] })
        .catch(() => ({ grades: [] })),
