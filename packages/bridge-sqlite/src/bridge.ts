import Database from 'better-sqlite3'
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
const SQLITE_BRIDGE_NAME = '@semilayer/bridge-sqlite'

export interface SqliteBridgeConfig {
  path: string
  pool?: { min?: number; max?: number }
}

export class SqliteBridge implements Bridge {
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

  private db: Database.Database | null = null
  private config: SqliteBridgeConfig
  private pkCache = new Map<string, string>()

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-sqlite',
    displayName: 'SQLite',
    icon: 'sqlite',
    supportsUrl: false,
    fields: [
      { key: 'path', label: 'Database Path', type: 'string', required: true, placeholder: '/data/mydb.sqlite3', hint: 'Path to SQLite database file' },
    ],
  }

  constructor(config: Record<string, unknown>) {
    const path = config['path'] as string | undefined
    if (!path || typeof path !== 'string') {
      throw new Error('SqliteBridge requires a "path" config string')
    }
    this.config = { path, pool: config['pool'] as SqliteBridgeConfig['pool'] }
  }

  async connect(): Promise<void> {
    this.db = new Database(this.config.path)
    // Verify connectivity by running a no-op query
    this.db.prepare('SELECT 1').get()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const db = this.assertDb()
    const table = target
    assertTableName(table)

    const pk = this.getPrimaryKey(db, table)
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
      const hasCol = this.hasColumn(db, table, col)
      if (hasCol) {
        conditions.push(`${quote(col)} > ?`)
        params.push(options.changedSince.toISOString())
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    const fetchLimit = limit + 1
    params.push(fetchLimit)

    const sql = `SELECT ${selectClause} FROM ${quote(table)} ${whereClause} ORDER BY ${quote(pk)} ASC LIMIT ?`

    const allRows = db.prepare(sql).all(...params) as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore
      ? String(rows[rows.length - 1]![pk])
      : undefined

    const countRow = db
      .prepare(`SELECT COUNT(*) as total FROM ${quote(table)}`)
      .get() as { total: number }
    const total = countRow.total

    return Promise.resolve({ rows, nextCursor, total })
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    const db = this.assertDb()
    const table = target
    assertTableName(table)

    const built = buildWhereSql(options?.where, SQLITE_WHERE_DIALECT, {
      logicalOps: SQLITE_LOGICAL_OPS,
      stringOps: SQLITE_STRING_OPS,
      bridge: SQLITE_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const row = db
      .prepare(
        `SELECT COUNT(*) as total FROM ${quote(table)} ${whereClause}`,
      )
      .get(...built.params) as { total: number }
    return Promise.resolve(row.total)
  }

  async disconnect(): Promise<void> {
    if (this.db) {
      this.db.close()
      this.db = null
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

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return SQLITE_FAMILY_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const db = this.assertDb()
    return executeAggregateQueries(
      buildAggregateSql(opts, SQLITE_DIALECT),
      async (sql, params) => {
        const stmt = db.prepare(sql)
        return stmt.all(...(params as unknown[])) as Array<Record<string, unknown>>
      },
    )
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const db = this.assertDb()
    const table = target
    assertTableName(table)

    const selectClause = options.select
      ? options.select.map(quote).join(', ')
      : '*'

    const built = buildWhereSql(options.where, SQLITE_WHERE_DIALECT, {
      logicalOps: SQLITE_LOGICAL_OPS,
      stringOps: SQLITE_STRING_OPS,
      bridge: SQLITE_BRIDGE_NAME,
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
            `${quote(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`,
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

    // LIMIT / OFFSET — SQLite uses `?` for every slot so the helper's
    // `nextSlot` is irrelevant; we just push the values.
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
      `SELECT ${selectClause} FROM ${quote(table)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${quote(table)} ${whereClause}`

    const rows = db.prepare(querySql).all(...params) as BridgeRow[]
    const countRow = db
      .prepare(countSql)
      .get(...built.params) as { total: number }

    return Promise.resolve({ rows, total: countRow.total })
  }

  // -------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------

  async listTargets(): Promise<string[]> {
    const db = this.assertDb()
    const rows = db
      .prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
      )
      .all() as Array<{ name: string }>
    return Promise.resolve(rows.map((r) => r.name))
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const db = this.assertDb()
    assertTableName(target)

    type PragmaRow = { cid: number; name: string; type: string; notnull: number; dflt_value: unknown; pk: number }
    const colRows = db
      .prepare(`SELECT * FROM pragma_table_info(?)`)
      .all(target) as PragmaRow[]

    const columns: TargetColumnInfo[] = colRows.map((row) => ({
      name: row.name,
      type: row.type || 'TEXT',
      nullable: row.notnull === 0,
      primaryKey: row.pk > 0,
    }))

    const countRow = db
      .prepare(`SELECT COUNT(*) as total FROM ${quote(target)}`)
      .get() as { total: number }

    return Promise.resolve({ name: target, columns, rowCount: countRow.total })
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertDb(): Database.Database {
    if (!this.db) throw new Error('SqliteBridge is not connected')
    return this.db
  }

  private getPrimaryKey(db: Database.Database, table: string): string {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const row = db
      .prepare(
        'SELECT name FROM pragma_table_info(?) WHERE pk = 1 ORDER BY pk LIMIT 1',
      )
      .get(table) as { name: string } | undefined

    if (!row) {
      throw new Error(`Could not detect primary key for table "${table}"`)
    }
    this.pkCache.set(table, row.name)
    return row.name
  }

  private hasColumn(db: Database.Database, table: string, col: string): boolean {
    const row = db
      .prepare('SELECT 1 as found FROM pragma_table_info(?) WHERE name = ?')
      .get(table, col)
    return row != null
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
