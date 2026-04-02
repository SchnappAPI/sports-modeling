import mssql from 'mssql';

let pool: mssql.ConnectionPool | null = null;

const MAX_ATTEMPTS = 3;
const RETRY_DELAY_MS = 8000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function getPool(): Promise<mssql.ConnectionPool> {
  if (pool && pool.connected) {
    return pool;
  }

  const connectionString = process.env.AZURE_SQL_CONNECTION_STRING;
  if (!connectionString) {
    throw new Error('AZURE_SQL_CONNECTION_STRING is not set');
  }

  let lastError: unknown;
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      pool = await mssql.connect(connectionString);
      return pool;
    } catch (err) {
      lastError = err;
      pool = null;
      if (attempt < MAX_ATTEMPTS) {
        await sleep(RETRY_DELAY_MS);
      }
    }
  }

  throw lastError;
}
