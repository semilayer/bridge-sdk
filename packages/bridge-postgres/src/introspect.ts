import type pg from 'pg'

export interface ColumnInfo {
  name: string
  type: string
  nullable: boolean
  primaryKey: boolean
}

export interface TableInfo {
  name: string
  schema: string
  columns: ColumnInfo[]
  rowCount: number
}

export async function introspect(
  pool: pg.Pool,
  table: string,
  schema = 'public',
): Promise<TableInfo> {
  const colResult = await pool.query(
    `SELECT
       c.column_name,
       c.data_type,
       c.is_nullable,
       CASE WHEN kcu.column_name IS NOT NULL THEN true ELSE false END AS is_pk
     FROM information_schema.columns c
     LEFT JOIN information_schema.table_constraints tc
       ON tc.table_name = c.table_name
       AND tc.table_schema = c.table_schema
       AND tc.constraint_type = 'PRIMARY KEY'
     LEFT JOIN information_schema.key_column_usage kcu
       ON kcu.constraint_name = tc.constraint_name
       AND kcu.table_schema = tc.table_schema
       AND kcu.column_name = c.column_name
     WHERE c.table_name = $1
       AND c.table_schema = $2
     ORDER BY c.ordinal_position`,
    [table, schema],
  )

  const columns: ColumnInfo[] = (
    colResult.rows as Array<{
      column_name: string
      data_type: string
      is_nullable: string
      is_pk: boolean
    }>
  ).map((row) => ({
    name: row.column_name,
    type: row.data_type,
    nullable: row.is_nullable === 'YES',
    primaryKey: row.is_pk,
  }))

  const countResult = await pool.query(
    `SELECT count(*)::int AS total FROM "${schema}"."${table}"`,
  )
  const rowCount = (countResult.rows as Array<{ total: number }>)[0]!.total

  return { name: table, schema, columns, rowCount }
}

export async function listTables(
  pool: pg.Pool,
  schema = 'public',
): Promise<string[]> {
  const result = await pool.query(
    `SELECT table_name
     FROM information_schema.tables
     WHERE table_schema = $1
       AND table_type = 'BASE TABLE'
     ORDER BY table_name`,
    [schema],
  )
  return (result.rows as Array<{ table_name: string }>).map(
    (r) => r.table_name,
  )
}
