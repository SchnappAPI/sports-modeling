import mssql from 'mssql';

let pool: mssql.ConnectionPool | null = null;

export async function getPool(): Promise<mssql.ConnectionPool> {
  if (pool && pool.connected) {
    return pool;
  }

  const connectionString = process.env.AZURE_SQL_CONNECTION_STRING;
  if (!connectionString) {
    throw new Error('AZURE_SQL_CONNECTION_STRING is not set');
  }

  pool = await mssql.connect(connectionString);
  return pool;
}
