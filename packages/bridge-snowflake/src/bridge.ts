import snowflake from 'snowflake-sdk'
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
  TargetSchema,
} from '@semilayer/core'
import {
  buildAggregateSql,
  executeAggregateQueries,
  SNOWFLAKE_DIALECT,
  SNOWFLAKE_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

// Suppress noisy SDK log output
snowflake.configure({ logLevel: 'ERROR' })

// ---------------------------------------------------------------------------
// Promisified Snowflake helpers
// ---------------------------------------------------------------------------

function connectSnowflake(conn: snowflake.Connection): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.connect((err) => (err ? reject(err) : resolve()))
  })
}

function destroySnowflake(conn: snowflake.Connection): Promise<void> {
  return new Promise((resolve, reject) => {
    conn.destroy((err) => (err ? reject(err) : resolve()))
  })
}

function executeQuery(
  conn: snowflake.Connection,
  sql: string,
  binds: snowflake.Bind[] = [],
): Promise<Record<string, unknown>[]> {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: sql,
      binds,
      complete: (err, _stmt, rows) => {
        if (err) reject(err)
        else resolve((rows ?? []) as Record<string, unknown>[])
      },
    })
  })
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

export interface SnowflakeBridgeConfig {
  /** Snowflake account identifier (e.g. 'orgname-accountname'). */
  account: string
  username: string
  password: string
  database: string
  /** Compute warehouse to use. */
  warehouse?: string
  /** Schema to use. Defaults to PUBLIC. */
  schema?: string
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function quote(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

// ---------------------------------------------------------------------------
// Bridge implementation
// ---------------------------------------------------------------------------

export class SnowflakeBridge implements Bridge {
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

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-snowflake',
    displayName: 'Snowflake',
    icon: 'snowflake',
    supportsUrl: false,
    fields: [
      {
        key: 'account',
        label: 'Account',
        type: 'string',
        required: true,
        placeholder: 'orgname-accountname',
        hint: 'Your Snowflake account identifier',
      },
      {
        key: 'username',
        label: 'Username',
        type: 'string',
        required: true,
      },
      {
        key: 'password',
        label: 'Password',
        type: 'password',
        required: true,
      },
      {
        key: 'database',
        label: 'Database',
        type: 'string',
        required: true,
      },
      {
        key: 'warehouse',
        label: 'Warehouse',
        type: 'string',
        required: false,
        hint: 'Compute warehouse to use',
        group: 'advanced',
      },
      {
        key: 'schema',
        label: 'Schema',
        type: 'string',
        required: false,
        default: 'PUBLIC',
        group: 'advanced',
      },
    ],
  }

  private connection: snowflake.Connection | null = null
  private pkCache = new Map<string, string>()
  private readonly config: SnowflakeBridgeConfig

  constructor(config: Record<string, unknown>) {
    this.config = {
      account: String(config['account'] ?? ''),
      username: String(config['username'] ?? ''),
      password: String(config['password'] ?? ''),
      database: String(config['database'] ?? ''),
      warehouse: config['warehouse'] ? String(config['warehouse']) : undefined,
      schema: config['schema'] ? String(config['schema']) : 'PUBLIC',
    }
  }

  async connect(): Promise<void> {
    const connectionOptions: snowflake.ConnectionOptions = {
      account: this.config.account,
      username: this.config.username,
      password: this.config.password,
      database: this.config.database,
      schema: this.config.schema ?? 'PUBLIC',
    }
    if (this.config.warehouse) {
      connectionOptions.warehouse = this.config.warehouse
    }

    const conn = snowflake.createConnection(connectionOptions)
    await connectSnowflake(conn)
    this.connection = conn
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const conn = this.assertConn()

    const fields = options?.fields
    const selectClause = fields ? fields.map(quote).join(', ') : '*'
    const limit = options?.limit ?? 1000

    // Decode cursor as numeric OFFSET
    const offset = options?.cursor ? parseInt(options.cursor, 10) : 0

    const conditions: string[] = []
    const binds: snowflake.Bind[] = []

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(target, col)
      if (hasCol) {
        conditions.push(`${quote(col)} > ?`)
        binds.push(options.changedSince.toISOString())
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 to detect whether there is a next page
    const sql = `SELECT ${selectClause} FROM ${quote(target)} ${whereClause} LIMIT ? OFFSET ?`
    const queryBinds: snowflake.Bind[] = [...binds, limit + 1, offset]

    const allRowsResult = await executeQuery(conn, sql, queryBinds)

    const hasMore = allRowsResult.length > limit
    const rows = (hasMore ? allRowsResult.slice(0, limit) : allRowsResult) as BridgeRow[]
    const nextCursor = hasMore ? String(offset + limit) : undefined

    const countSql = `SELECT COUNT(*) as TOTAL FROM ${quote(target)} ${whereClause}`
    const countRows = await executeQuery(conn, countSql, binds)
    const total = Number(countRows[0]?.['TOTAL'] ?? 0)

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const conn = this.assertConn()
    const rows = await executeQuery(
      conn,
      `SELECT COUNT(*) as TOTAL FROM ${quote(target)}`,
    )
    return Number(rows[0]?.['TOTAL'] ?? 0)
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
    return SNOWFLAKE_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const conn = this.assertConn()
    return executeAggregateQueries(
      buildAggregateSql(opts, SNOWFLAKE_DIALECT),
      async (sql, params) => {
        const rows = await executeQuery(conn, sql, params as snowflake.Bind[])
        return rows
      },
    )
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const conn = this.assertConn()

    const selectClause = options.select ? options.select.map(quote).join(', ') : '*'

    const binds: snowflake.Bind[] = []
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
                binds.push(opVal as snowflake.Bind)
                break
              case '$gt':
                conditions.push(`${quote(key)} > ?`)
                binds.push(opVal as snowflake.Bind)
                break
              case '$gte':
                conditions.push(`${quote(key)} >= ?`)
                binds.push(opVal as snowflake.Bind)
                break
              case '$lt':
                conditions.push(`${quote(key)} < ?`)
                binds.push(opVal as snowflake.Bind)
                break
              case '$lte':
                conditions.push(`${quote(key)} <= ?`)
                binds.push(opVal as snowflake.Bind)
                break
              case '$in':
                if (Array.isArray(opVal) && opVal.length > 0) {
                  const placeholders = opVal.map(() => '?').join(', ')
                  conditions.push(`${quote(key)} IN (${placeholders})`)
                  for (const v of opVal) {
                    binds.push(v as snowflake.Bind)
                  }
                }
                break
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${quote(key)} = ?`)
          binds.push(value as snowflake.Bind)
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

    const sqlParts = [
      `SELECT ${selectClause} FROM ${quote(target)}`,
      whereClause,
      orderByClause,
    ].filter(Boolean)

    const dataBinds: snowflake.Bind[] = [...binds]

    if (options.limit != null) {
      sqlParts.push(`LIMIT ?`)
      dataBinds.push(options.limit)
    }
    if (options.offset != null) {
      sqlParts.push(`OFFSET ?`)
      dataBinds.push(options.offset)
    }

    const sql = sqlParts.join(' ')
    const countSql = `SELECT COUNT(*) as TOTAL FROM ${quote(target)} ${whereClause}`

    const [dataRows, countRows] = await Promise.all([
      executeQuery(conn, sql, dataBinds),
      executeQuery(conn, countSql, binds),
    ])

    const total = Number(countRows[0]?.['TOTAL'] ?? 0)

    return {
      rows: dataRows as BridgeRow[],
      total,
    }
  }

  async listTargets(): Promise<string[]> {
    const conn = this.assertConn()
    const rows = await executeQuery(conn, 'SHOW TABLES')
    return rows.map((r) => String(r['name'] ?? ''))
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const conn = this.assertConn()

    const [describeRows, countRows] = await Promise.all([
      executeQuery(conn, `DESCRIBE TABLE ${quote(target)}`),
      executeQuery(conn, `SELECT COUNT(*) as TOTAL FROM ${quote(target)}`),
    ])

    const columns = describeRows.map((row) => ({
      name: String(row['name'] ?? ''),
      type: String(row['type'] ?? ''),
      nullable: String(row['null?'] ?? 'Y') === 'Y',
      primaryKey: String(row['primary key'] ?? 'N') === 'Y',
    }))

    const rowCount = Number(countRows[0]?.['TOTAL'] ?? 0)

    return { name: target, columns, rowCount }
  }

  async disconnect(): Promise<void> {
    if (this.connection) {
      await destroySnowflake(this.connection)
      this.connection = null
    }
    this.pkCache.clear()
  }

  // ---------------------------------------------------------------------------
  // Internal helpers
  // ---------------------------------------------------------------------------

  private assertConn(): snowflake.Connection {
    if (!this.connection) throw new Error('SnowflakeBridge is not connected')
    return this.connection
  }

  private async hasColumn(target: string, column: string): Promise<boolean> {
    const conn = this.assertConn()
    const rows = await executeQuery(conn, `DESCRIBE TABLE ${quote(target)}`)
    return rows.some((r) => String(r['name'] ?? '') === column)
  }
}
