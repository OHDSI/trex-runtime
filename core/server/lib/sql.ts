export function escapeSql(s: string): string {
  return s.replace(/'/g, "''");
}

export function escapeSqlIdentifier(s: string): string {
  return s.replace(/"/g, '""');
}

export function validateInt(value: unknown, name: string): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) throw new Error(`Invalid ${name}`);
  return n;
}
