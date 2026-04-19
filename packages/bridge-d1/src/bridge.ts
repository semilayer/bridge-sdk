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
} from '@semilayer/core'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export interface D1BridgeConfig {
  accountId: string
  databaseId: string
  apiToken: string
}

export class D1Bridge implements Bridge {
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

  private config: D1BridgeConfig
  private connected = false
  private pkCache = new Map<string, string>()

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-d1',
    displayName: 'Cloudflare D1',
    icon: 'd1',
    supportsUrl: false,
    fields: [
      { key: 'accountId', label: 'Account ID', type: 'string', required: true, hint: 'Cloudflare account ID' },
      { key: 'databaseId', label: 'Database ID', type: 'string', required: true, hint: 'D1 database ID from Cloudflare dashboard' },
      { key: 'apiToken', label: 'API Token', type: 'password', required: true, hint: 'Cloudflare API token with D1 read permissions' },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const accountId = config['accountId'] as string | undefined
    const databaseId = config['databaseId'] as string | undefined
    const apiToken = config['apiToken'] as string | undefined

    if (!accountId || typeof accountId !== 'string') {
      throw new Error('D1Bridge requires an "accountId" config string')
    }
    if (!databaseId || typeof databaseId !== 'string') {
      throw new Error('D1Bridge requires a "databaseId" config string')
    }
    if (!apiToken || typeof apiToken !== 'string') {
      throw new Error('D1Bridge requires an "apiToken" config string')
    }

    this.config = { accountId, databaseId, apiToken }
  }

  private get baseUrl(): string {
    return `https://api.cloudflare.com/client/v4/accounts/${this.config.accountId}/d1/database/${this.config.databaseId}/query`
  }

  private async execute(sql: string, params: unknown[] = []): Promise<BridgeRow[]> {
    const res = await fetch(this.baseUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.config.apiToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ sql, params }),
    })

    const data = (await res.json()) as {
      result: Array<{ results: BridgeRow[]; success: boolean }>
      success: boolean
      errors: Array<{ message: string }>
    }

    if (!data.success || !data.result[0]?.success) {
      throw new Error(data.errors[0]?.message ?? 'D1 query failed')
    }

    return data.result[0]?.results ?? []
  }

  async connect(): Promise<void> {
    await this.execute('SELECT 1')
    this.connected = true
  }

  async disconnect(): Promise<void> {
    this.connected = false
    this.pkCache.clear()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    this.assertConnected()
    assertTableName(target)

    const pk = await this.getPrimaryKey(target)
    const fields = options?.fields
    const selectClause = fields ? fields.map(quote).join(', ') : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []

    if (options?.cursor) {
      conditions.push(`${quote(pk)} > ?`)
      params.push(options.cursor)
    }

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

    params.push(limit + 1)
    const sql = `SELECT ${selectClause} FROM ${quote(target)} ${whereClause} ORDER BY ${quote(pk)} ASC LIMIT ?`

    const allRows = await this.execute(sql, params)

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(rows[rows.length - 1]![pk]) : undefined

    const countRows = await this.execute(
      `SELECT COUNT(*) as total FROM ${quote(target)}`,
    )
    const total = (countRows as Array<{ total: number }>)[0]!.total

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    this.assertConnected()
    assertTableName(target)
    const rows = await this.execute(`SELECT COUNT(*) as total FROM ${quote(target)}`)
    return (rows as Array<{ total: number }>)[0]!.total
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
    this.assertConnected()
    assertTableName(target)

    const selectClause = options.select
      ? options.select.map(quote).join(', ')
      : '*'

    const params: unknown[] = []
    const conditions: string[] = []

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
              case '$in': {
                const vals = opVal as unknown[]
                conditions.push(`${quote(key)} IN (${vals.map(() => '?').join(',')})`)
                for (const v of vals) params.push(v)
                break
              }
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

    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          parts.push(`${quote(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`)
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

    const limitParams: unknown[] = []
    let limitClause = ''
    if (options.limit != null) {
      limitClause = 'LIMIT ?'
      limitParams.push(options.limit)
    }

    let offsetClause = ''
    if (options.offset != null) {
      offsetClause = 'OFFSET ?'
      limitParams.push(options.offset)
    }

    const allParams = [...params, ...limitParams]
    const sql = [
      `SELECT ${selectClause} FROM ${quote(target)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${quote(target)} ${whereClause}`

    const [rows, countRows] = await Promise.all([
      this.execute(sql, allParams),
      this.execute(countSql, params),
    ])

    return {
      rows,
      total: (countRows as Array<{ total: number }>)[0]!.total,
    }
  }

  async listTargets(): Promise<string[]> {
    this.assertConnected()
    const rows = await this.execute(
      `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
    )
    return (rows as Array<{ name: string }>).map(r => r.name)
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertConnected(): void {
    if (!this.connected) throw new Error('D1Bridge is not connected')
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const rows = await this.execute(
      `SELECT name FROM pragma_table_info(?) WHERE pk = 1 ORDER BY pk LIMIT 1`,
      [table],
    )

    const row = (rows as Array<{ name: string }>)[0]
    if (!row) {
      throw new Error(`Could not detect primary key for table "${table}"`)
    }
    this.pkCache.set(table, row.name)
    return row.name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const rows = await this.execute(
      `SELECT name FROM pragma_table_info(?) WHERE name = ?`,
      [table, column],
    )
    return rows.length > 0
  }
}

function quote(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`
}

function assertTableName(table: string): void {
  if (!TABLE_NAME_RE.test(table)) {
    throw new Error(`Invalid table name: "${table}"`)
  }
}
