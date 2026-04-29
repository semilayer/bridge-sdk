import mysql from 'mysql2/promise'
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
  TargetColumnInfo,
  TargetSchema,
} from '@semilayer/core'
import {
  buildAggregateSql,
  buildWhereSql,
  executeAggregateQueries,
  MYSQL_DIALECT,
  MYSQL_FAMILY_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

// MySQL where dialect — backtick-quoted identifiers, `?` placeholders, and
// `LOWER(col) LIKE LOWER(?)` for `$ilike` since MySQL has no native ILIKE.
// `$in` falls back to per-element `?` placeholders (no array param form).
const MYSQL_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '``') + '`',
  placeholder: () => '?',
  ilike: (col, p) => `LOWER(${col}) LIKE LOWER(${p})`,
}

const MYSQL_LOGICAL_OPS = ['or', 'and', 'not'] as const
const MYSQL_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const MYSQL_BRIDGE_NAME = '@semilayer/bridge-mysql'

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
    whereLogicalOps: MYSQL_LOGICAL_OPS,
    whereStringOps: MYSQL_STRING_OPS,
    exactCount: true,
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

    // pool.query (not pool.execute) — mysql2's server-side prepared-statement
    // path rejects JS numbers for LIMIT ? with "Incorrect arguments to
    // mysqld_stmt_execute". See node-mysql2#1239.
    const [allRowsRaw] = await pool.query(sql, params as SqlParams)
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

  async count(target: string, options?: CountOptions): Promise<number> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const built = buildWhereSql(options?.where, MYSQL_WHERE_DIALECT, {
      logicalOps: MYSQL_LOGICAL_OPS,
      stringOps: MYSQL_STRING_OPS,
      bridge: MYSQL_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    // pool.query (not pool.execute) — see read() above for the
    // mysql2 prepared-statement footgun.
    const [rows] = await pool.query(
      `SELECT COUNT(*) as total FROM ${backtickQuote(table)} ${whereClause}`,
      built.params as SqlParams,
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

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return MYSQL_FAMILY_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const pool = this.assertPool()
    return executeAggregateQueries(
      buildAggregateSql(opts, MYSQL_DIALECT),
      async (sql, params) => {
        const [rows] = await pool.query(sql, params as SqlParams)
        return rows as Array<Record<string, unknown>>
      },
    )
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

    const built = buildWhereSql(options.where, MYSQL_WHERE_DIALECT, {
      logicalOps: MYSQL_LOGICAL_OPS,
      stringOps: MYSQL_STRING_OPS,
      bridge: MYSQL_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const params: unknown[] = [...built.params]

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

    // LIMIT / OFFSET — appended after the WHERE params. MySQL uses `?` for
    // every slot so `built.nextSlot` is irrelevant; we just push the values.
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

    // pool.query (not pool.execute) — see read() above for rationale.
    const [[dataRows], [countRows]] = await Promise.all([
      pool.query(querySql, params as SqlParams),
      pool.query(countSql, built.params as SqlParams),
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
    // MySQL 8 returns information_schema column names in UPPERCASE
    // (e.g. `COLUMN_NAME`) regardless of how the SELECT is written, so we
    // alias explicitly to keep the row shape stable across versions.
    const [rows] = await pool.execute(
      `SELECT column_name AS column_name FROM information_schema.KEY_COLUMN_USAGE
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
