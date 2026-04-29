import { createClient, type Client, type InValue } from '@libsql/client'
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
} from '@semilayer/core'
import {
  buildAggregateSql,
  buildWhereSql,
  executeAggregateQueries,
  SQLITE_DIALECT,
  SQLITE_FAMILY_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

// SQLite where dialect — `?` placeholders, double-quoted identifiers, and
// `LOWER(col) LIKE LOWER(?)` for `$ilike`. SQLite's built-in LIKE is
// ASCII-CI-by-default but unsafe for non-ASCII; the LOWER/LOWER form
// handles unicode CI consistently.
const SQLITE_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: () => '?',
  ilike: (col, p) => `LOWER(${col}) LIKE LOWER(${p})`,
}

const SQLITE_LOGICAL_OPS = ['or', 'and', 'not'] as const
const SQLITE_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const TURSO_BRIDGE_NAME = '@semilayer/bridge-turso'

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
    whereLogicalOps: SQLITE_LOGICAL_OPS,
    whereStringOps: SQLITE_STRING_OPS,
    exactCount: true,
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

  async count(target: string, options?: CountOptions): Promise<number> {
    assertTableName(target)
    const built = buildWhereSql(options?.where, SQLITE_WHERE_DIALECT, {
      logicalOps: SQLITE_LOGICAL_OPS,
      stringOps: SQLITE_STRING_OPS,
      bridge: TURSO_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const rows = await this.run(
      `SELECT count(*) as total FROM ${quote(target)} ${whereClause}`,
      built.params,
    )
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

    const built = buildWhereSql(options.where, SQLITE_WHERE_DIALECT, {
      logicalOps: SQLITE_LOGICAL_OPS,
      stringOps: SQLITE_STRING_OPS,
      bridge: TURSO_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const params: unknown[] = [...built.params]

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

    // LIMIT / OFFSET — SQLite uses `?` for every slot so the helper's
    // `nextSlot` is irrelevant; we just push the values onto `params`.
    let limitClause = ''
    if (options.limit != null) {
      limitClause = 'LIMIT ?'
      params.push(options.limit)
    }

    let offsetClause = ''
    if (options.offset != null) {
      offsetClause = 'OFFSET ?'
      params.push(options.offset)
    }

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
      this.run(sql, params),
      this.run(countSql, built.params),
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
