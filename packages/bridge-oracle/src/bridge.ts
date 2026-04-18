import oracledb from 'oracledb'
import type { Pool } from 'oracledb'
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

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_$.]*$/

export interface OracleBridgeConfig {
  user: string
  password: string
  connectString: string
  schema?: string
  poolMax?: number
}

export class OracleBridge implements Bridge {
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

  private pool: Pool | null = null
  private config: OracleBridgeConfig
  private pkCache = new Map<string, string>()

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-oracle',
    displayName: 'Oracle Database',
    icon: 'oracle',
    supportsUrl: false,
    fields: [
      { key: 'user', label: 'Username', type: 'string', required: true },
      { key: 'password', label: 'Password', type: 'password', required: true },
      {
        key: 'connectString',
        label: 'Connect String',
        type: 'string',
        required: true,
        placeholder: 'host:1521/service_name',
        hint: 'Host:port/service_name or TNS name',
      },
      {
        key: 'schema',
        label: 'Schema',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Schema/owner to query (defaults to your username)',
      },
      {
        key: 'poolMax',
        label: 'Pool Size',
        type: 'number',
        required: false,
        default: 3,
        group: 'advanced',
      },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const user = config['user'] as string | undefined
    const password = config['password'] as string | undefined
    const connectString = (config['connectString'] ?? config['connectionString']) as string | undefined

    if (!user) throw new Error('OracleBridge requires "user"')
    if (!password) throw new Error('OracleBridge requires "password"')
    if (!connectString) throw new Error('OracleBridge requires "connectString"')

    this.config = {
      user,
      password,
      connectString,
      schema: config['schema'] as string | undefined,
      poolMax: config['poolMax'] as number | undefined,
    }
  }

  async connect(): Promise<void> {
    this.pool = await oracledb.createPool({
      user: this.config.user,
      password: this.config.password,
      connectString: this.config.connectString,
      poolMax: this.config.poolMax ?? 3,
      poolMin: 0,
    })
    // Test connectivity
    const conn = await this.pool.getConnection()
    try {
      await conn.execute('SELECT 1 FROM DUAL')
    } finally {
      await conn.close()
    }
  }

  async disconnect(): Promise<void> {
    await this.pool?.close(0)
    this.pool = null
    this.pkCache.clear()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const pool = this.assertPool()
    assertTableName(target)

    const pk = await this.getPrimaryKey(pool, target)
    const fields = options?.fields
    const selectClause = fields ? fields.map(quoteId).join(', ') : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []
    let paramIdx = 1

    // Cursor is stored as the OFFSET number
    const offset = options?.cursor ? parseInt(options.cursor, 10) : 0

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(pool, target, col)
      if (hasCol) {
        conditions.push(`${quoteId(col)} > :${paramIdx}`)
        params.push(options.changedSince)
        paramIdx++
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 rows to detect whether there's a next page.
    // Oracle 12c+ supports OFFSET n ROWS FETCH NEXT m ROWS ONLY.
    params.push(offset)
    const offsetParamIdx = paramIdx
    paramIdx++

    params.push(limit + 1)
    const fetchParamIdx = paramIdx
    paramIdx++

    const sql =
      `SELECT ${selectClause} FROM ${quoteId(target)} ${whereClause} ` +
      `ORDER BY ${quoteId(pk)} ASC ` +
      `OFFSET :${offsetParamIdx} ROWS FETCH NEXT :${fetchParamIdx} ROWS ONLY`

    const conn = await pool.getConnection()
    let allRows: BridgeRow[]
    let total: number
    try {
      const result = await conn.execute(sql, params, {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
      })
      allRows = (result.rows ?? []) as BridgeRow[]

      const owner = this.resolveOwner()
      const countResult = await conn.execute(
        `SELECT COUNT(*) AS TOTAL FROM ${quoteId(target)}`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const countRows = (countResult.rows ?? []) as Array<Record<string, unknown>>
      total = countRows[0]?.['TOTAL'] as number ?? 0
      void owner
    } finally {
      await conn.close()
    }

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(offset + limit) : undefined

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const pool = this.assertPool()
    assertTableName(target)

    const conn = await pool.getConnection()
    try {
      const result = await conn.execute(
        `SELECT COUNT(*) AS TOTAL FROM ${quoteId(target)}`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const rows = (result.rows ?? []) as Array<Record<string, unknown>>
      return (rows[0]?.['TOTAL'] as number) ?? 0
    } finally {
      await conn.close()
    }
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
    assertTableName(target)

    const selectClause = options.select
      ? options.select.map(quoteId).join(', ')
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
                conditions.push(`${quoteId(key)} = :${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gt':
                conditions.push(`${quoteId(key)} > :${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gte':
                conditions.push(`${quoteId(key)} >= :${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lt':
                conditions.push(`${quoteId(key)} < :${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lte':
                conditions.push(`${quoteId(key)} <= :${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$in': {
                const vals = opVal as unknown[]
                const placeholders = vals
                  .map((_, i) => `:${paramIdx + i}`)
                  .join(', ')
                conditions.push(`${quoteId(key)} IN (${placeholders})`)
                for (const v of vals) params.push(v)
                paramIdx += vals.length
                break
              }
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${quoteId(key)} = :${paramIdx}`)
          params.push(value)
          paramIdx++
        }
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Track where-only param count for the count query
    const whereParamCount = params.length

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
            `${quoteId(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`,
          )
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`${quoteId(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // LIMIT / OFFSET via OFFSET ... ROWS FETCH NEXT ... ROWS ONLY (Oracle 12c+)
    let paginationClause = ''
    if (options.limit != null || options.offset != null) {
      const offsetVal = options.offset ?? 0
      paginationClause = `OFFSET :${paramIdx} ROWS`
      params.push(offsetVal)
      paramIdx++
      if (options.limit != null) {
        paginationClause += ` FETCH NEXT :${paramIdx} ROWS ONLY`
        params.push(options.limit)
        paramIdx++
      }
      // Oracle requires ORDER BY when using OFFSET/FETCH
      if (!orderByClause) {
        orderByClause = 'ORDER BY 1'
      }
    }

    const querySql = [
      `SELECT ${selectClause} FROM ${quoteId(target)}`,
      whereClause,
      orderByClause,
      paginationClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) AS TOTAL FROM ${quoteId(target)} ${whereClause}`
    const countParams = params.slice(0, whereParamCount)

    const conn = await pool.getConnection()
    try {
      const [dataResult, countResult] = await Promise.all([
        conn.execute(querySql, params, { outFormat: oracledb.OUT_FORMAT_OBJECT }),
        conn.execute(countSql, countParams, { outFormat: oracledb.OUT_FORMAT_OBJECT }),
      ])

      const rows = (dataResult.rows ?? []) as BridgeRow[]
      const countRows = (countResult.rows ?? []) as Array<Record<string, unknown>>
      const total = (countRows[0]?.['TOTAL'] as number) ?? 0

      return { rows, total }
    } finally {
      await conn.close()
    }
  }

  // -------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------

  async listTargets(): Promise<string[]> {
    const pool = this.assertPool()
    const owner = this.resolveOwner()

    const conn = await pool.getConnection()
    try {
      const result = await conn.execute(
        `SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name`,
        [owner],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const rows = (result.rows ?? []) as Array<Record<string, unknown>>
      return rows.map((r) => r['TABLE_NAME'] as string)
    } finally {
      await conn.close()
    }
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const pool = this.assertPool()
    assertTableName(target)
    const owner = this.resolveOwner()

    const conn = await pool.getConnection()
    try {
      // Get columns
      const colResult = await conn.execute(
        `SELECT column_name, data_type, nullable
         FROM all_tab_columns
         WHERE owner = :1 AND table_name = :2
         ORDER BY column_id`,
        [owner, target.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const colRows = (colResult.rows ?? []) as Array<Record<string, unknown>>

      // Get primary key columns
      const pkResult = await conn.execute(
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1
           AND cons.table_name = :2`,
        [owner, target.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const pkRows = (pkResult.rows ?? []) as Array<Record<string, unknown>>
      const pkCols = new Set(
        pkRows.map((r) => r['COLUMN_NAME'] as string),
      )

      const columns: TargetColumnInfo[] = colRows.map((row) => ({
        name: row['COLUMN_NAME'] as string,
        type: row['DATA_TYPE'] as string,
        nullable: (row['NULLABLE'] as string) === 'Y',
        primaryKey: pkCols.has(row['COLUMN_NAME'] as string),
      }))

      // Row count
      const countResult = await conn.execute(
        `SELECT COUNT(*) AS TOTAL FROM ${quoteId(target)}`,
        [],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const countRows = (countResult.rows ?? []) as Array<Record<string, unknown>>
      const rowCount = (countRows[0]?.['TOTAL'] as number) ?? 0

      return { name: target, columns, rowCount }
    } finally {
      await conn.close()
    }
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertPool(): Pool {
    if (!this.pool) throw new Error('OracleBridge is not connected')
    return this.pool
  }

  private resolveOwner(): string {
    return (this.config.schema ?? this.config.user).toUpperCase()
  }

  private async getPrimaryKey(pool: Pool, table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const owner = this.resolveOwner()
    const conn = await pool.getConnection()
    try {
      const result = await conn.execute(
        `SELECT cols.column_name
         FROM all_constraints cons
         JOIN all_cons_columns cols
           ON cons.constraint_name = cols.constraint_name
           AND cons.owner = cols.owner
         WHERE cons.constraint_type = 'P'
           AND cons.owner = :1
           AND cons.table_name = :2
           AND ROWNUM = 1`,
        [owner, table.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      const rows = (result.rows ?? []) as Array<Record<string, unknown>>
      const row = rows[0]
      if (!row) {
        throw new Error(`Could not detect primary key for table "${table}"`)
      }
      const pk = row['COLUMN_NAME'] as string
      this.pkCache.set(table, pk)
      return pk
    } finally {
      await conn.close()
    }
  }

  private async hasColumn(
    pool: Pool,
    table: string,
    column: string,
  ): Promise<boolean> {
    const owner = this.resolveOwner()
    const conn = await pool.getConnection()
    try {
      const result = await conn.execute(
        `SELECT 1 FROM all_tab_columns
         WHERE owner = :1 AND table_name = :2 AND column_name = :3
         AND ROWNUM = 1`,
        [owner, table.toUpperCase(), column.toUpperCase()],
        { outFormat: oracledb.OUT_FORMAT_OBJECT },
      )
      return ((result.rows ?? []) as unknown[]).length > 0
    } finally {
      await conn.close()
    }
  }
}

function quoteId(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

function assertTableName(table: string): void {
  if (!TABLE_NAME_RE.test(table)) {
    throw new Error(`Invalid table name: "${table}"`)
  }
}
