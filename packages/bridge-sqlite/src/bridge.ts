import Database from 'better-sqlite3'
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

  async count(target: string): Promise<number> {
    const db = this.assertDb()
    const table = target
    assertTableName(table)

    const row = db
      .prepare(`SELECT COUNT(*) as total FROM ${quote(table)}`)
      .get() as { total: number }
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

    // Track where params length for count query
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
      `SELECT ${selectClause} FROM ${quote(table)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT COUNT(*) as total FROM ${quote(table)} ${whereClause}`
    const countParams = params.slice(0, whereParamCount)

    const rows = db.prepare(querySql).all(...params) as BridgeRow[]
    const countRow = db.prepare(countSql).get(...countParams) as { total: number }

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
