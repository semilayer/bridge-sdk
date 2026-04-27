import { createClient, type Client, type InValue } from '@libsql/client'
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
import {
  buildAggregateSql,
  executeAggregateQueries,
  SQLITE_DIALECT,
  SQLITE_FAMILY_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export interface TursoBridgeConfig {
  url: string
  authToken?: string
}

export class TursoBridge implements Bridge {
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

  private client: Client | null = null
  private pkCache = new Map<string, string>()
  private config: TursoBridgeConfig

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-turso',
    displayName: 'Turso',
    icon: 'turso',
    supportsUrl: true,
    urlPlaceholder: 'libsql://db-name.turso.io',
    fields: [
      { key: 'authToken', label: 'Auth Token', type: 'password', required: false, hint: 'Authentication token from Turso dashboard' },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    if (!url || typeof url !== 'string') {
      throw new Error('TursoBridge requires a "url" config string')
    }
    this.config = {
      url,
      authToken: config['authToken'] as string | undefined,
    }
  }

  async connect(): Promise<void> {
    this.client = createClient({
      url: this.config.url,
      authToken: this.config.authToken,
    })
    await this.client.execute({ sql: 'SELECT 1', args: [] })
  }

  async disconnect(): Promise<void> {
    this.client?.close()
    this.client = null
    this.pkCache.clear()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
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

    const allRows = await this.run(sql, params)

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(rows[rows.length - 1]![pk]) : undefined

    const countRows = await this.run(`SELECT count(*) as total FROM ${quote(target)}`)
    const total = countRows[0]!['total'] as number

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    assertTableName(target)
    const rows = await this.run(`SELECT count(*) as total FROM ${quote(target)}`)
    return rows[0]!['total'] as number
  }

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return SQLITE_FAMILY_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const client = this.assertClient()
    return executeAggregateQueries(
      buildAggregateSql(opts, SQLITE_DIALECT),
      async (sql, params) => {
        const result = await client.execute({ sql, args: params as InValue[] })
        return result.rows as unknown as Array<Record<string, unknown>>
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
                conditions.push(
                  `${quote(key)} IN (${vals.map(() => '?').join(',')})`,
                )
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

    const countSql = `SELECT count(*) as total FROM ${quote(target)} ${whereClause}`

    const [rows, countRows] = await Promise.all([
      this.run(sql, allParams),
      this.run(countSql, params),
    ])

    return {
      rows,
      total: countRows[0]!['total'] as number,
    }
  }

  async listTargets(): Promise<string[]> {
    const rows = await this.run(
      `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
    )
    return (rows as Array<{ name: string }>).map(r => r.name)
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertClient(): Client {
    if (!this.client) throw new Error('TursoBridge is not connected')
    return this.client
  }

  private async run(sql: string, args: unknown[] = []): Promise<BridgeRow[]> {
    const client = this.assertClient()
    const result = await client.execute({ sql, args: args as InValue[] })
    return result.rows.map(row =>
      Object.fromEntries(result.columns.map((c, i) => [c, (row as unknown as unknown[])[i]])),
    )
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const rows = await this.run(
      `SELECT name FROM pragma_table_info(?) WHERE pk = 1 ORDER BY pk LIMIT 1`,
      [table],
    )

    const row = (rows as Array<{ name: string }>)[0]
    if (!row) {
      throw new Error(`Cannot detect primary key for "${table}"`)
    }
    this.pkCache.set(table, row.name)
    return row.name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const rows = await this.run(
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
