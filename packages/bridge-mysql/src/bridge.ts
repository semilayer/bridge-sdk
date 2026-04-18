import mysql from 'mysql2/promise'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  QueryOptions,
  QueryResult,
  ReadOptions,
  ReadResult,
  TargetColumnInfo,
  TargetSchema,
} from '@semilayer/core'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

// mysql2 execute() expects a concrete ExecuteValues type, not unknown[].
// We use this alias to cast at call sites where we know the values are safe.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type SqlParams = any[]

export interface MysqlBridgeConfig {
  url?: string
  host?: string
  port?: number
  user?: string
  password?: string
  database?: string
  pool?: { min?: number; max?: number }
}

export class MysqlBridge implements Bridge {
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
  }

  private pool: mysql.Pool | null = null
  private config: MysqlBridgeConfig
  private pkCache = new Map<string, string>()

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-mysql',
    displayName: 'MySQL',
    icon: 'mysql',
    supportsUrl: true,
    urlPlaceholder: 'mysql://user:pass@host:3306/dbname',
    fields: [
      { key: 'host', label: 'Host', type: 'string', required: true, placeholder: 'localhost' },
      { key: 'port', label: 'Port', type: 'number', required: false, default: 3306 },
      { key: 'database', label: 'Database', type: 'string', required: true },
      { key: 'user', label: 'Username', type: 'string', required: true, placeholder: 'Username' },
      { key: 'password', label: 'Password', type: 'password', required: true },
      { key: 'ssl', label: 'SSL', type: 'boolean', required: false, default: false, group: 'advanced' },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    const host = config['host'] as string | undefined
    const port = config['port'] as number | undefined
    const user = (config['user'] ?? config['username']) as string | undefined
    const password = (config['password'] ?? config['pass']) as string | undefined
    const database = (config['database'] ?? config['db']) as string | undefined

    if (!url && !host && !database) {
      throw new Error(
        'MysqlBridge requires either a "url" or ("host" + "database") config',
      )
    }

    this.config = {
      url,
      host,
      port,
      user,
      password,
      database,
      pool: config['pool'] as MysqlBridgeConfig['pool'],
    }
  }

  async connect(): Promise<void> {
    const max = this.config.pool?.max ?? 3

    if (this.config.url) {
      this.pool = mysql.createPool({
        uri: this.config.url,
        waitForConnections: true,
        connectionLimit: max,
      })
    } else {
      this.pool = mysql.createPool({
        host: this.config.host,
        port: this.config.port,
        user: this.config.user,
        password: this.config.password,
        database: this.config.database,
        waitForConnections: true,
        connectionLimit: max,
      })
    }

    const conn = await this.pool.getConnection()
    try {
      await conn.query('SELECT 1')
    } finally {
      conn.release()
    }
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const pk = await this.getPrimaryKey(table)
    const fields = options?.fields
    const selectClause = fields
      ? fields.map(backtickQuote).join(', ')
      : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []

    if (options?.cursor) {
      conditions.push(`${backtickQuote(pk)} > ?`)
      params.push(options.cursor)
    }

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(table, col)
      if (hasCol) {
        conditions.push(`${backtickQuote(col)} > ?`)
        params.push(options.changedSince)
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 to detect whether there is a next page
    const fetchLimit = limit + 1
    params.push(fetchLimit)

    const sql = `SELECT ${selectClause} FROM ${backtickQuote(table)} ${whereClause} ORDER BY ${backtickQuote(pk)} ASC LIMIT ?`

    const [allRowsRaw] = await pool.execute(sql, params as SqlParams)
    const allRows = allRowsRaw as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore
      ? String(rows[rows.length - 1]![pk])
      : undefined

    const [countRaw] = await pool.execute(
      `SELECT COUNT(*) as total FROM ${backtickQuote(table)}`,
    )
    const total = (countRaw as Array<{ total: number }>)[0]!.total

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const [rows] = await pool.execute(
      `SELECT COUNT(*) as total FROM ${backtickQuote(table)}`,
    )
    return (rows as Array<{ total: number }>)[0]!.total
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.end()
      this.pool = null
    }
    this.pkCache.clear()
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

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const selectClause = options.select
      ? options.select.map(backtickQuote).join(', ')
      : '*'

    const params: unknown[] = []

    // WHERE
    const conditions: string[] = []
    if (options.where) {
      for (const [key, value] of Object.entries(options.where)) {
        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, opVal] of Object.entries(ops)) {
            switch (op) {
              case '$eq':
                conditions.push(`${backtickQuote(key)} = ?`)
                params.push(opVal)
                break
              case '$gt':
                conditions.push(`${backtickQuote(key)} > ?`)
                params.push(opVal)
                break
              case '$gte':
                conditions.push(`${backtickQuote(key)} >= ?`)
                params.push(opVal)
                break
              case '$lt':
                conditions.push(`${backtickQuote(key)} < ?`)
                params.push(opVal)
                break
              case '$lte':
                conditions.push(`${backtickQuote(key)} <= ?`)
                params.push(opVal)
                break
              case '$in':
                // mysql2 handles array expansion for IN clauses
                conditions.push(`${backtickQuote(key)} IN (?)`)
                params.push(opVal)
                break
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${backtickQuote(key)} = ?`)
          params.push(value)
        }
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // ORDER BY
    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy)
        ? options.orderBy
        : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          parts.push(
            `${backtickQuote(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`,
          )
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(
                `${backtickQuote(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`,
              )
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // Keep a snapshot of where-params for the count query
    const whereParamCount = params.length

    // LIMIT / OFFSET
    let limitClause = ''
    if (options.limit != null) {
      limitClause = `LIMIT ?`
      params.push(options.limit)
    }

    let offsetClause = ''
    if (options.offset != null) {
      offsetClause = `OFFSET ?`
      params.push(options.offset)
    }

    const querySql = [
      `SELECT ${selectClause} FROM ${backtickQuote(table)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${backtickQuote(table)} ${whereClause}`
    const countParams = params.slice(0, whereParamCount)

    const [[dataRows], [countRows]] = await Promise.all([
      pool.execute(querySql, params as SqlParams),
      pool.execute(countSql, countParams as SqlParams),
    ])

    return {
      rows: dataRows as BridgeRow[],
      total: (countRows as Array<{ total: number }>)[0]!.total,
    }
  }

  // -------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------

  async listTargets(): Promise<string[]> {
    const pool = this.assertPool()
    const [rows] = await pool.execute(
      `SELECT TABLE_NAME as table_name FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`,
    )
    return (rows as Array<{ table_name: string }>).map((r) => r.table_name)
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const pool = this.assertPool()
    assertTableName(target)

    const [colRows] = await pool.execute(
      `SELECT
         c.COLUMN_NAME as column_name,
         c.DATA_TYPE as data_type,
         c.IS_NULLABLE as is_nullable,
         CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk
       FROM information_schema.COLUMNS c
       LEFT JOIN information_schema.TABLE_CONSTRAINTS tc
         ON tc.TABLE_NAME = c.TABLE_NAME
         AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
         AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
       LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu
         ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
         AND kcu.COLUMN_NAME = c.COLUMN_NAME
       WHERE c.TABLE_NAME = ? AND c.TABLE_SCHEMA = DATABASE()
       ORDER BY c.ORDINAL_POSITION`,
      [target],
    )

    const columns: TargetColumnInfo[] = (
      colRows as Array<{
        column_name: string
        data_type: string
        is_nullable: string
        is_pk: number
      }>
    ).map((row) => ({
      name: row.column_name,
      type: row.data_type,
      nullable: row.is_nullable === 'YES',
      primaryKey: row.is_pk === 1,
    }))

    const [countRows] = await pool.execute(
      `SELECT COUNT(*) as total FROM ${backtickQuote(target)}`,
    )
    const rowCount = (countRows as Array<{ total: number }>)[0]!.total

    return { name: target, columns, rowCount }
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertPool(): mysql.Pool {
    if (!this.pool) throw new Error('MysqlBridge is not connected')
    return this.pool
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const pool = this.assertPool()
    const [rows] = await pool.execute(
      `SELECT column_name FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY'
       ORDER BY ORDINAL_POSITION LIMIT 1`,
      [table],
    )

    const row = (rows as Array<{ column_name: string }>)[0]
    if (!row) {
      throw new Error(`Could not detect primary key for table "${table}"`)
    }
    this.pkCache.set(table, row.column_name)
    return row.column_name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const pool = this.assertPool()
    const [rows] = await pool.execute(
      `SELECT 1 FROM information_schema.COLUMNS
       WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE() LIMIT 1`,
      [table, column],
    )
    return (rows as unknown[]).length > 0
  }
}

function backtickQuote(identifier: string): string {
  return `\`${identifier.replace(/`/g, '``')}\``
}

function assertTableName(table: string): void {
  if (!TABLE_NAME_RE.test(table)) {
    throw new Error(`Invalid table name: "${table}"`)
  }
}
