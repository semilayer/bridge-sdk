// Note: 'duckdb' is NOT statically imported here. DuckDB ships a native C++
// addon (duckdb.node) that is loaded at require() time. Eagerly importing it
// at module scope would crash any process on a platform where the prebuilt
// binary is missing. We lazy-load it inside connect() so that bridge-resolver
// can register DuckdbBridge on every platform — the error is deferred until
// the bridge is actually instantiated and connected.
import type DuckDBTypes from 'duckdb'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  CountOptions,
  QueryOptions,
  QueryResult,
  ReadOptions,
  ReadResult,
  TargetSchema,
} from '@semilayer/core'
import {
  buildAggregateSql,
  buildWhereSql,
  executeAggregateQueries,
  DUCKDB_DIALECT,
  DUCKDB_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

// DuckDB where dialect — double-quoted identifiers, `?` positional binds,
// native `ILIKE`. DuckDB's LIKE supports `ESCAPE '\'` so the helper default
// works without any overrides.
const DUCK_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: () => '?',
}

const DUCK_LOGICAL_OPS = ['or', 'and', 'not'] as const
const DUCK_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const DUCK_BRIDGE_NAME = '@semilayer/bridge-duckdb'

type DuckDB = typeof DuckDBTypes

// ---------------------------------------------------------------------------
// Promisified DuckDB helpers (duckdb module passed as arg — loaded lazily)
// ---------------------------------------------------------------------------

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function openDb(ddb: DuckDB, path: string): Promise<any> {
  return new Promise((resolve, reject) => {
    // Use a let + two-step assign so the variable is in scope when the callback
    // fires. Real DuckDB always calls back asynchronously (after the DB opens),
    // so `db` is always assigned before resolve(db) runs in production.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let db: any
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    db = new (ddb as any).Database(path, (err: Error | null) =>
      err ? reject(err) : resolve(db),
    )
  })
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function closeDb(db: any): Promise<void> {
  return new Promise((resolve, reject) => {
    db.close((err: Error | null) => (err ? reject(err) : resolve()))
  })
}

function allRows(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  conn: any,
  sql: string,
  params: unknown[] = [],
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
): Promise<any[]> {
  return new Promise((resolve, reject) => {
    conn.all(sql, ...params, (err: Error | null, rows: unknown[]) =>
      err ? reject(err) : resolve(rows ?? []),
    )
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
  readonly capabilities: Partial<BridgeCapabilities> = {
    batchRead: true,
    wherePushdown: true,
    orderByPushdown: true,
    limitPushdown: true,
    selectProjection: true,
    nativeJoin: false,
    cursor: true,
    changedSince: true,
    perKeyLimit: false,
    whereLogicalOps: DUCK_LOGICAL_OPS,
    whereStringOps: DUCK_STRING_OPS,
    exactCount: true,
  }

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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private db: any = null
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private conn: any = null
  private duckdbNs: DuckDB | null = null
  private pkCache = new Map<string, string>()
  private readonly config: DuckdbBridgeConfig

  constructor(config: Record<string, unknown>) {
    this.config = {
      path: typeof config['path'] === 'string' ? config['path'] : ':memory:',
    }
  }

  // -------------------------------------------------------------------------
  // Lazy native-module loader
  // -------------------------------------------------------------------------

  private async loadDuckdb(): Promise<DuckDB> {
    if (!this.duckdbNs) {
      try {
        const mod = await import('duckdb')
        this.duckdbNs = mod.default as DuckDB
      } catch (e: unknown) {
        const cause = e instanceof Error ? e.message : String(e)
        throw new Error(
          `DuckDB native module failed to load on ${process.platform}/${process.arch} ` +
            `(Node ${process.version}). ` +
            `Run "npm rebuild duckdb" or check supported platforms at ` +
            `https://duckdb.org/docs/api/nodejs/overview\n\nCause: ${cause}`,
        )
      }
    }
    return this.duckdbNs
  }

  // -------------------------------------------------------------------------
  // Bridge interface
  // -------------------------------------------------------------------------

  async connect(): Promise<void> {
    const ddb = await this.loadDuckdb()
    this.db = await openDb(ddb, this.config.path)
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

  async count(target: string, options?: CountOptions): Promise<number> {
    const conn = this.assertConn()
    const built = buildWhereSql(options?.where, DUCK_WHERE_DIALECT, {
      logicalOps: DUCK_LOGICAL_OPS,
      stringOps: DUCK_STRING_OPS,
      bridge: DUCK_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const rows = await allRows(
      conn,
      `SELECT count(*) as total FROM ${quote(target)} ${whereClause}`,
      built.params,
    )
    return Number((rows[0] as Record<string, unknown>)?.['total'] ?? 0)
  }

  async batchRead(
    target: string,
    options: BatchReadOptions,
  ): Promise<BridgeRow[]> {
    const result = await this.query(target, {
      where: options.where,
      select: options.select && options.select !== '*' ? options.select : undefined,
      orderBy: options.orderBy,
      limit: options.limit,
    })
    return result.rows
  }

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return DUCKDB_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const conn = this.assertConn()
    return executeAggregateQueries(
      buildAggregateSql(opts, DUCKDB_DIALECT),
      async (sql, params) => {
        const rows = await allRows(conn, sql, params as unknown[])
        return rows as Array<Record<string, unknown>>
      },
    )
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const conn = this.assertConn()

    const selectClause = options.select ? options.select.map(quote).join(', ') : '*'

    const built = buildWhereSql(options.where, DUCK_WHERE_DIALECT, {
      logicalOps: DUCK_LOGICAL_OPS,
      stringOps: DUCK_STRING_OPS,
      bridge: DUCK_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const params: unknown[] = [...built.params]

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
    const sqlParts = [
      `SELECT ${selectClause} FROM ${quote(target)}`,
      whereClause,
      orderByClause,
    ].filter(Boolean)

    const dataParams = [...params]

    if (options.limit != null) {
      sqlParts.push(`LIMIT ?`)
      dataParams.push(options.limit)
    }
    if (options.offset != null) {
      sqlParts.push(`OFFSET ?`)
      dataParams.push(options.offset)
    }

    const sql = sqlParts.join(' ')
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

  private assertConn() {
    if (!this.conn) throw new Error('DuckdbBridge is not connected')
    return this.conn
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
