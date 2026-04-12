import duckdb from 'duckdb'
import type {
  Bridge,
  BridgeManifest,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  TargetSchema,
} from '@semilayer/core'

// ---------------------------------------------------------------------------
// Promisified DuckDB helpers
// ---------------------------------------------------------------------------

function openDb(path: string): Promise<duckdb.Database> {
  return new Promise((resolve, reject) => {
    const db = new duckdb.Database(path, (err) => (err ? reject(err) : resolve(db)))
  })
}

function closeDb(db: duckdb.Database): Promise<void> {
  return new Promise((resolve, reject) => {
    db.close((err) => (err ? reject(err) : resolve()))
  })
}

function allRows(
  conn: duckdb.Connection,
  sql: string,
  params: unknown[] = [],
): Promise<duckdb.RowData[]> {
  return new Promise((resolve, reject) => {
    conn.all(sql, ...params, (err, rows) => (err ? reject(err) : resolve(rows ?? [])))
  })
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface DuckdbBridgeConfig {
  /** File path to the DuckDB database, or ':memory:' for an in-memory database. */
  path: string
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function quote(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

// PRAGMA table_info row shape
interface PragmaColumn {
  cid: number
  name: string
  type: string
  notnull: number
  dflt_value: unknown
  pk: number
}

// ---------------------------------------------------------------------------
// Bridge implementation
// ---------------------------------------------------------------------------

export class DuckdbBridge implements Bridge {
  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-duckdb',
    displayName: 'DuckDB',
    icon: 'duckdb',
    supportsUrl: false,
    fields: [
      {
        key: 'path',
        label: 'Database Path',
        type: 'string',
        required: true,
        placeholder: '/data/analytics.duckdb',
        hint: 'File path or :memory: for in-memory database',
      },
    ],
  }

  private db: duckdb.Database | null = null
  private conn: duckdb.Connection | null = null
  private pkCache = new Map<string, string>()
  private readonly config: DuckdbBridgeConfig

  constructor(config: Record<string, unknown>) {
    this.config = {
      path: typeof config['path'] === 'string' ? config['path'] : ':memory:',
    }
  }

  async connect(): Promise<void> {
    const path = this.config.path
    this.db = await openDb(path)
    this.conn = this.db.connect()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const conn = this.assertConn()

    const fields = options?.fields
    const selectClause = fields ? fields.map(quote).join(', ') : '*'
    const limit = options?.limit ?? 1000

    // Decode cursor as numeric OFFSET
    const offset = options?.cursor ? parseInt(options.cursor, 10) : 0

    const conditions: string[] = []
    const params: unknown[] = []

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(target, col)
      if (hasCol) {
        conditions.push(`${quote(col)} > ?`)
        params.push(options.changedSince.toISOString())
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 to detect whether there is a next page
    const sql = `SELECT ${selectClause} FROM ${quote(target)} ${whereClause} LIMIT ? OFFSET ?`
    const queryParams = [...params, limit + 1, offset]

    const allRowsResult = (await allRows(conn, sql, queryParams)) as BridgeRow[]

    const hasMore = allRowsResult.length > limit
    const rows = hasMore ? allRowsResult.slice(0, limit) : allRowsResult
    const nextCursor = hasMore ? String(offset + limit) : undefined

    const countSql = `SELECT count(*) as total FROM ${quote(target)} ${whereClause}`
    const countRows = await allRows(conn, countSql, params)
    const total = Number((countRows[0] as Record<string, unknown>)?.['total'] ?? 0)

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const conn = this.assertConn()
    const rows = await allRows(conn, `SELECT count(*) as total FROM ${quote(target)}`)
    return Number((rows[0] as Record<string, unknown>)?.['total'] ?? 0)
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const conn = this.assertConn()

    const selectClause = options.select ? options.select.map(quote).join(', ') : '*'

    const params: unknown[] = []
    const conditions: string[] = []

    // WHERE
    if (options.where) {
      for (const [key, value] of Object.entries(options.where)) {
        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, opVal] of Object.entries(ops)) {
            switch (op) {
              case '$eq':
                conditions.push(`${quote(key)} = ?`)
                params.push(opVal)
                break
              case '$gt':
                conditions.push(`${quote(key)} > ?`)
                params.push(opVal)
                break
              case '$gte':
                conditions.push(`${quote(key)} >= ?`)
                params.push(opVal)
                break
              case '$lt':
                conditions.push(`${quote(key)} < ?`)
                params.push(opVal)
                break
              case '$lte':
                conditions.push(`${quote(key)} <= ?`)
                params.push(opVal)
                break
              case '$in':
                if (Array.isArray(opVal) && opVal.length > 0) {
                  const placeholders = opVal.map(() => '?').join(', ')
                  conditions.push(`${quote(key)} IN (${placeholders})`)
                  params.push(...(opVal as unknown[]))
                }
                break
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${quote(key)} = ?`)
          params.push(value)
        }
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // ORDER BY
    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj['field'] === 'string') {
          parts.push(
            `${quote(obj['field'])} ${obj['dir'] === 'desc' ? 'DESC' : 'ASC'}`,
          )
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`${quote(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // Build SQL parts
    const parts = [
      `SELECT ${selectClause} FROM ${quote(target)}`,
      whereClause,
      orderByClause,
    ].filter(Boolean)

    const dataParams = [...params]

    if (options.limit != null) {
      parts.push(`LIMIT ?`)
      dataParams.push(options.limit)
    }
    if (options.offset != null) {
      parts.push(`OFFSET ?`)
      dataParams.push(options.offset)
    }

    const sql = parts.join(' ')
    const countSql = `SELECT count(*) as total FROM ${quote(target)} ${whereClause}`

    const [dataRows, countRows] = await Promise.all([
      allRows(conn, sql, dataParams),
      allRows(conn, countSql, params),
    ])

    const total = Number((countRows[0] as Record<string, unknown>)?.['total'] ?? 0)

    return {
      rows: dataRows as BridgeRow[],
      total,
    }
  }

  async listTargets(): Promise<string[]> {
    const conn = this.assertConn()
    const rows = await allRows(conn, 'SHOW TABLES')
    return (rows as Array<Record<string, unknown>>).map((r) => String(r['name'] ?? ''))
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const conn = this.assertConn()

    const [pragmaRows, countRows] = await Promise.all([
      allRows(conn, `PRAGMA table_info(${quote(target)})`),
      allRows(conn, `SELECT count(*) as total FROM ${quote(target)}`),
    ])

    const columns = (pragmaRows as unknown as PragmaColumn[]).map((row) => ({
      name: row.name,
      type: row.type,
      nullable: row.notnull === 0,
      primaryKey: row.pk > 0,
    }))

    const rowCount = Number(
      (countRows[0] as Record<string, unknown>)?.['total'] ?? 0,
    )

    return { name: target, columns, rowCount }
  }

  async disconnect(): Promise<void> {
    if (this.db) {
      await closeDb(this.db)
      this.db = null
      this.conn = null
    }
    this.pkCache.clear()
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private assertConn(): duckdb.Connection {
    if (!this.conn) throw new Error('DuckdbBridge is not connected')
    return this.conn
  }

  private async getPrimaryKey(target: string): Promise<string> {
    const cached = this.pkCache.get(target)
    if (cached) return cached

    const conn = this.assertConn()
    const rows = (await allRows(
      conn,
      `PRAGMA table_info(${quote(target)})`,
    )) as unknown as PragmaColumn[]

    const pkCol = rows.find((r) => r.pk > 0)
    if (!pkCol) {
      // Fall back to first column if no explicit PK
      const first = rows[0]
      if (!first) throw new Error(`Table "${target}" has no columns`)
      this.pkCache.set(target, first.name)
      return first.name
    }
    this.pkCache.set(target, pkCol.name)
    return pkCol.name
  }

  private async hasColumn(target: string, column: string): Promise<boolean> {
    const conn = this.assertConn()
    const rows = (await allRows(
      conn,
      `PRAGMA table_info(${quote(target)})`,
    )) as unknown as PragmaColumn[]
    return rows.some((r) => r.name === column)
  }
}
