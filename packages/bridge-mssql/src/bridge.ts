import mssqlLib from 'mssql'
import type { ConnectionPool, IResult, config as MssqlConfig } from 'mssql'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  TargetSchema,
  TargetColumnInfo,
} from '@semilayer/core'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export interface MssqlBridgeConfig {
  url?: string
  server?: string
  port?: number
  user?: string
  password?: string
  database?: string
  encrypt?: boolean
  trustServerCertificate?: boolean
  pool?: { min?: number; max?: number }
}

export class MssqlBridge implements Bridge {
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

  private pool: ConnectionPool | null = null
  private config: MssqlBridgeConfig
  private pkCache = new Map<string, string>()

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-mssql',
    displayName: 'SQL Server',
    icon: 'mssql',
    supportsUrl: true,
    urlPlaceholder: 'mssql://user:pass@server:1433/database',
    fields: [
      { key: 'server', label: 'Server', type: 'string', required: true, placeholder: 'localhost' },
      { key: 'port', label: 'Port', type: 'number', required: false, default: 1433 },
      { key: 'database', label: 'Database', type: 'string', required: true },
      { key: 'user', label: 'Username', type: 'string', required: true, placeholder: 'Username' },
      { key: 'password', label: 'Password', type: 'password', required: true },
      { key: 'encrypt', label: 'Encrypt', type: 'boolean', required: false, default: true, group: 'advanced' },
      { key: 'trustServerCertificate', label: 'Trust Server Certificate', type: 'boolean', required: false, default: false, group: 'advanced' },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    const server = config['server'] as string | undefined
    const user = (config['user'] ?? config['username']) as string | undefined
    const password = (config['password'] ?? config['pass']) as string | undefined
    const database = (config['database'] ?? config['db']) as string | undefined

    if (!url && !server) {
      throw new Error(
        'MssqlBridge requires either a "url" or a "server" config',
      )
    }

    this.config = {
      url,
      server,
      port: config['port'] as number | undefined,
      user,
      password,
      database,
      encrypt: config['encrypt'] as boolean | undefined,
      trustServerCertificate: config['trustServerCertificate'] as boolean | undefined,
      pool: config['pool'] as MssqlBridgeConfig['pool'],
    }
  }

  async connect(): Promise<void> {
    let mssqlConfig: MssqlConfig

    if (this.config.url) {
      const parsed = new URL(this.config.url)
      mssqlConfig = {
        server: parsed.hostname,
        port: parsed.port ? parseInt(parsed.port, 10) : 1433,
        user: parsed.username || undefined,
        password: parsed.password || undefined,
        database: parsed.pathname.slice(1) || undefined,
        options: {
          encrypt: this.config.encrypt ?? false,
          trustServerCertificate: this.config.trustServerCertificate ?? false,
        },
        pool: {
          min: this.config.pool?.min ?? 0,
          max: this.config.pool?.max ?? 3,
        },
      }
    } else {
      mssqlConfig = {
        server: this.config.server!,
        port: this.config.port ?? 1433,
        user: this.config.user,
        password: this.config.password,
        database: this.config.database,
        options: {
          encrypt: this.config.encrypt ?? false,
          trustServerCertificate: this.config.trustServerCertificate ?? false,
        },
        pool: {
          min: this.config.pool?.min ?? 0,
          max: this.config.pool?.max ?? 3,
        },
      }
    }

    this.pool = await mssqlLib.connect(mssqlConfig)
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    this.assertPool()
    const table = target
    assertTableName(table)

    const pk = await this.getPrimaryKey(table)
    const fields = options?.fields
    const selectClause = fields ? fields.map(bracket).join(', ') : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []
    let paramIdx = 1

    if (options?.cursor) {
      conditions.push(`${bracket(pk)} > @p${paramIdx}`)
      params.push(options.cursor)
      paramIdx++
    }

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(table, col)
      if (hasCol) {
        conditions.push(`${bracket(col)} > @p${paramIdx}`)
        params.push(options.changedSince)
        paramIdx++
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Use TOP N+1 for pagination (MSSQL style)
    const fetchLimit = limit + 1
    const sqlText = `SELECT TOP ${fetchLimit} ${selectClause} FROM ${bracket(table)} ${whereClause} ORDER BY ${bracket(pk)} ASC`

    const dataResult = await this.runQuery(sqlText, params)
    const allRows = dataResult.recordset as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore
      ? String(rows[rows.length - 1]![pk])
      : undefined

    const countResult = await this.runQuery(
      `SELECT COUNT(*) as total FROM ${bracket(table)}`,
      [],
    )
    const total = (countResult.recordset as Array<{ total: number }>)[0]!.total

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    this.assertPool()
    const table = target
    assertTableName(table)

    const result = await this.runQuery(
      `SELECT COUNT(*) as total FROM ${bracket(table)}`,
      [],
    )
    return (result.recordset as Array<{ total: number }>)[0]!.total
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.close()
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
    this.assertPool()
    const table = target
    assertTableName(table)

    const selectClause = options.select
      ? options.select.map(bracket).join(', ')
      : '*'

    const params: unknown[] = []
    let paramIdx = 1

    // WHERE
    const conditions: string[] = []
    if (options.where) {
      for (const [key, value] of Object.entries(options.where)) {
        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, opVal] of Object.entries(ops)) {
            switch (op) {
              case '$eq':
                conditions.push(`${bracket(key)} = @p${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gt':
                conditions.push(`${bracket(key)} > @p${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gte':
                conditions.push(`${bracket(key)} >= @p${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lt':
                conditions.push(`${bracket(key)} < @p${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lte':
                conditions.push(`${bracket(key)} <= @p${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$in': {
                const vals = opVal as unknown[]
                conditions.push(
                  bracket(key) +
                    ' IN (' +
                    vals.map((_, i) => '@p' + (paramIdx + i)).join(',') +
                    ')',
                )
                for (const v of vals) params.push(v)
                paramIdx += vals.length
                break
              }
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${bracket(key)} = @p${paramIdx}`)
          params.push(value)
          paramIdx++
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
            `${bracket(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`,
          )
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`${bracket(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // Track where params length for count query
    const whereParamCount = params.length

    // LIMIT / OFFSET (MSSQL uses OFFSET ... FETCH NEXT ... ROWS ONLY)
    let paginationClause = ''
    if (options.limit != null || options.offset != null) {
      const offset = options.offset ?? 0
      paginationClause = `OFFSET @p${paramIdx} ROWS`
      params.push(offset)
      paramIdx++
      if (options.limit != null) {
        paginationClause += ` FETCH NEXT @p${paramIdx} ROWS ONLY`
        params.push(options.limit)
        paramIdx++
      }
      // MSSQL requires ORDER BY when using OFFSET/FETCH
      if (!orderByClause) {
        orderByClause = `ORDER BY (SELECT NULL)`
      }
    }

    const querySql = [
      `SELECT ${selectClause} FROM ${bracket(table)}`,
      whereClause,
      orderByClause,
      paginationClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${bracket(table)} ${whereClause}`
    const countParams = params.slice(0, whereParamCount)

    const [dataResult, countResult] = await Promise.all([
      this.runQuery(querySql, params),
      this.runQuery(countSql, countParams),
    ])

    return {
      rows: dataResult.recordset as BridgeRow[],
      total: (countResult.recordset as Array<{ total: number }>)[0]!.total,
    }
  }

  // -------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------

  async listTargets(): Promise<string[]> {
    this.assertPool()
    const result = await this.runQuery(
      `SELECT TABLE_NAME as table_name FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'`,
      [],
    )
    return (result.recordset as Array<{ table_name: string }>).map(
      (r) => r.table_name,
    )
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    this.assertPool()
    assertTableName(target)

    const colResult = await this.runQuery(
      `SELECT
         c.COLUMN_NAME as column_name,
         c.DATA_TYPE as data_type,
         c.IS_NULLABLE as is_nullable,
         CASE WHEN kcu.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_pk
       FROM INFORMATION_SCHEMA.COLUMNS c
       LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         ON tc.TABLE_NAME = c.TABLE_NAME
         AND tc.TABLE_SCHEMA = c.TABLE_SCHEMA
         AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
       LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
         ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         AND kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
         AND kcu.COLUMN_NAME = c.COLUMN_NAME
       WHERE c.TABLE_NAME = @p1
       ORDER BY c.ORDINAL_POSITION`,
      [target],
    )

    const columns: TargetColumnInfo[] = (
      colResult.recordset as Array<{
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

    const countResult = await this.runQuery(
      `SELECT COUNT(*) as total FROM ${bracket(target)}`,
      [],
    )
    const rowCount = (countResult.recordset as Array<{ total: number }>)[0]!
      .total

    return { name: target, columns, rowCount }
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertPool(): ConnectionPool {
    if (!this.pool) throw new Error('MssqlBridge is not connected')
    return this.pool
  }

  private async runQuery(
    sqlText: string,
    params: unknown[],
  ): Promise<IResult<unknown>> {
    const pool = this.assertPool()
    const req = pool.request()
    for (let i = 0; i < params.length; i++) {
      req.input(`p${i + 1}`, params[i])
    }
    return req.query(sqlText)
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const result = await this.runQuery(
      `SELECT c.COLUMN_NAME as column_name
       FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
       JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS t
         ON c.CONSTRAINT_NAME = t.CONSTRAINT_NAME
         AND c.TABLE_SCHEMA = t.TABLE_SCHEMA
       WHERE t.CONSTRAINT_TYPE = 'PRIMARY KEY'
         AND c.TABLE_NAME = @p1
       ORDER BY c.ORDINAL_POSITION
       OFFSET 0 ROWS FETCH FIRST 1 ROWS ONLY`,
      [table],
    )

    const row = (result.recordset as Array<{ column_name: string }>)[0]
    if (!row) {
      throw new Error(`Could not detect primary key for table "${table}"`)
    }
    this.pkCache.set(table, row.column_name)
    return row.column_name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const result = await this.runQuery(
      `SELECT 1 as found FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @p1 AND COLUMN_NAME = @p2`,
      [table, column],
    )
    return (result.recordset as unknown[]).length > 0
  }
}

function bracket(identifier: string): string {
  return '[' + identifier.replace(/\[/g, '[[').replace(/\]/g, ']]') + ']'
}

function assertTableName(table: string): void {
  if (!TABLE_NAME_RE.test(table)) {
    throw new Error(`Invalid table name: "${table}"`)
  }
}
