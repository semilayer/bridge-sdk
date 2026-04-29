import { connect as psConnect, type Connection } from '@planetscale/database'
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
  MYSQL_DIALECT,
  MYSQL_FAMILY_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

// MySQL where dialect — backtick identifiers, `?` placeholders, and
// `LOWER(col) LIKE LOWER(?)` for `$ilike` (PlanetScale = MySQL).
const MYSQL_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '``') + '`',
  placeholder: () => '?',
  ilike: (col, p) => `LOWER(${col}) LIKE LOWER(${p})`,
}

const MYSQL_LOGICAL_OPS = ['or', 'and', 'not'] as const
const MYSQL_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const PLANETSCALE_BRIDGE_NAME = '@semilayer/bridge-planetscale'

export interface PlanetscaleBridgeConfig {
  url?: string
  host?: string
  username?: string
  password?: string
}

export class PlanetscaleBridge implements Bridge {
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

  private conn: Connection | null = null
  private pkCache = new Map<string, string>()
  private config: PlanetscaleBridgeConfig

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-planetscale',
    displayName: 'PlanetScale',
    icon: 'planetscale',
    supportsUrl: true,
    urlPlaceholder: 'mysql://user:pass@host.psdb.cloud/dbname',
    fields: [
      { key: 'host', label: 'Host', type: 'string', required: true, placeholder: 'host.psdb.cloud' },
      { key: 'username', label: 'Username', type: 'string', required: true, placeholder: 'Username' },
      { key: 'password', label: 'Password', type: 'password', required: true },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    const host = config['host'] as string | undefined
    const username = config['username'] as string | undefined
    const password = config['password'] as string | undefined

    if (!url && !(host && username && password)) {
      throw new Error(
        'PlanetscaleBridge requires either a "url" or "host"+"username"+"password" config',
      )
    }
    this.config = { url, host, username, password }
  }

  async connect(): Promise<void> {
    this.conn = this.config.url
      ? psConnect({ url: this.config.url })
      : psConnect({
          host: this.config.host!,
          username: this.config.username!,
          password: this.config.password!,
        })
    await this.conn.execute('SELECT 1', [])
  }

  async disconnect(): Promise<void> {
    // HTTP-based — no persistent socket to close
    this.conn = null
    this.pkCache.clear()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const conn = this.assertConn()
    assertTableName(target)

    const pk = await this.getPrimaryKey(target)
    const fields = options?.fields
    const selectClause = fields ? fields.map(bt).join(', ') : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []

    if (options?.cursor) {
      conditions.push(`${bt(pk)} > ?`)
      params.push(options.cursor)
    }

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(target, col)
      if (hasCol) {
        conditions.push(`${bt(col)} > ?`)
        params.push(options.changedSince)
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    params.push(limit + 1)
    const sql = `SELECT ${selectClause} FROM ${bt(target)} ${whereClause} ORDER BY ${bt(pk)} ASC LIMIT ?`

    const dataResult = await conn.execute(sql, params)
    const allRows: BridgeRow[] = dataResult.rows as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(rows[rows.length - 1]![pk]) : undefined

    const countResult = await conn.execute(
      `SELECT COUNT(*) as total FROM ${bt(target)}`,
      [],
    )
    const total = (countResult.rows as Array<{ total: number }>)[0]!.total

    return { rows, nextCursor, total }
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    const conn = this.assertConn()
    assertTableName(target)
    const built = buildWhereSql(options?.where, MYSQL_WHERE_DIALECT, {
      logicalOps: MYSQL_LOGICAL_OPS,
      stringOps: MYSQL_STRING_OPS,
      bridge: PLANETSCALE_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const result = await conn.execute(
      `SELECT COUNT(*) as total FROM ${bt(target)} ${whereClause}`,
      built.params,
    )
    return (result.rows as Array<{ total: number }>)[0]!.total
  }

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return MYSQL_FAMILY_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const conn = this.assertConn()
    return executeAggregateQueries(
      buildAggregateSql(opts, MYSQL_DIALECT),
      async (sql, params) => {
        const result = await conn.execute(sql, params as unknown[])
        return result.rows as Array<Record<string, unknown>>
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
    const conn = this.assertConn()
    assertTableName(target)

    const selectClause = options.select
      ? options.select.map(bt).join(', ')
      : '*'

    const built = buildWhereSql(options.where, MYSQL_WHERE_DIALECT, {
      logicalOps: MYSQL_LOGICAL_OPS,
      stringOps: MYSQL_STRING_OPS,
      bridge: PLANETSCALE_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''

    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          parts.push(`${bt(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`)
        } else {
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`${bt(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // LIMIT / OFFSET — appended after WHERE params. PlanetScale uses `?`
    // for every slot so `built.nextSlot` is irrelevant.
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

    const allParams = [...built.params, ...limitParams]
    const sql = [
      `SELECT ${selectClause} FROM ${bt(target)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${bt(target)} ${whereClause}`

    const [dataResult, countResult] = await Promise.all([
      conn.execute(sql, allParams),
      conn.execute(countSql, built.params),
    ])

    return {
      rows: dataResult.rows as BridgeRow[],
      total: (countResult.rows as Array<{ total: number }>)[0]!.total,
    }
  }

  async listTargets(): Promise<string[]> {
    const conn = this.assertConn()
    const result = await conn.execute(
      `SELECT TABLE_NAME as table_name FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'`,
      [],
    )
    return (result.rows as Array<{ table_name: string }>).map(r => r.table_name)
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertConn(): Connection {
    if (!this.conn) throw new Error('PlanetscaleBridge is not connected')
    return this.conn
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const conn = this.assertConn()
    const result = await conn.execute(
      `SELECT column_name FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() AND CONSTRAINT_NAME = 'PRIMARY' LIMIT 1`,
      [table],
    )

    const row = (result.rows as Array<{ column_name: string }>)[0]
    if (!row) {
      throw new Error(`Could not detect primary key for table "${table}"`)
    }
    this.pkCache.set(table, row.column_name)
    return row.column_name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const conn = this.assertConn()
    const result = await conn.execute(
      `SELECT 1 FROM information_schema.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ? AND TABLE_SCHEMA = DATABASE() LIMIT 1`,
      [table, column],
    )
    return result.rows.length > 0
  }
}

function bt(identifier: string): string {
  return '`' + identifier.replace(/`/g, '``') + '`'
}

function assertTableName(table: string): void {
  if (!TABLE_NAME_RE.test(table)) {
    throw new Error(`Invalid table name: "${table}"`)
  }
}
