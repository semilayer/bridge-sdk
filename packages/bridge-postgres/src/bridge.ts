import pg from 'pg'
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
} from '@semilayer/core'
import { introspect, listTables } from './introspect.js'

const TABLE_NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_.]*$/

export interface PostgresBridgeConfig {
  url: string
  pool?: { min?: number; max?: number }
  /**
   * IP family preference for DNS resolution. Passed to `dns.lookup()`
   * under the hood (via `pg.Pool`'s `family` option).
   *
   * - `4` (default) — **IPv4 only**. Safest for serverless + VPC-egress
   *   setups (Cloud Run Direct VPC, AWS Lambda in-VPC, etc.) where the
   *   platform advertises IPv6 routing but the egress gateway silently
   *   drops IPv6 packets. Happy-eyeballs can't fast-fail through that
   *   blackhole, so an unlucky IPv6 attempt stalls the entire TCP connect
   *   until kernel timeout (~21s per address). IPv4-only sidesteps it
   *   entirely. Every widely-used hosted Postgres (Neon, RDS, Supabase,
   *   Cloud SQL, PlanetScale, Aiven, Railway) returns IPv4 addresses.
   * - `6` — IPv6 only. For clusters that genuinely run IPv6-only PG.
   * - `0` — try both, let Node's happy-eyeballs pick. Legacy behavior.
   *
   * Leave unset unless you know you need `6` or `0`.
   */
  ipFamily?: 4 | 6 | 0
  /**
   * Per-TCP-connect timeout in ms for new pool connections.
   * Default 10s — enough for any healthy DB to ACK a SYN, short enough
   * to fail fast when the target is unreachable. Set to 0 to disable.
   */
  connectionTimeoutMillis?: number
}

export class PostgresBridge implements Bridge {
  private pool: pg.Pool | null = null
  private config: PostgresBridgeConfig
  private pkCache = new Map<string, string>()

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
    packageName: '@semilayer/bridge-postgres',
    displayName: 'PostgreSQL',
    icon: 'postgres',
    supportsUrl: true,
    urlPlaceholder: 'postgresql://user:pass@host:5432/dbname',
    fields: [
      { key: 'host', label: 'Host', type: 'string', required: true, placeholder: 'localhost' },
      { key: 'port', label: 'Port', type: 'number', required: false, default: 5432 },
      { key: 'database', label: 'Database', type: 'string', required: true },
      { key: 'user', label: 'Username', type: 'string', required: true, placeholder: 'Username' },
      { key: 'password', label: 'Password', type: 'password', required: true },
      { key: 'ssl', label: 'SSL', type: 'boolean', required: false, default: false, group: 'advanced' },
      {
        key: 'ipFamily',
        label: 'IP Family',
        type: 'number',
        required: false,
        default: 4,
        group: 'advanced',
      },
      {
        key: 'connectionTimeoutMillis',
        label: 'Connect Timeout (ms)',
        type: 'number',
        required: false,
        default: 10000,
        group: 'advanced',
      },
    ],
  }

  constructor(config: Record<string, unknown>) {
    let url = (config['url'] ?? config['connectionString']) as string | undefined
    if (!url || typeof url !== 'string') {
      const host = config['host'] as string | undefined
      const port = (config['port'] as number | undefined) ?? 5432
      const database = (config['database'] ?? config['db']) as string | undefined
      const user = (config['user'] ?? config['username']) as string | undefined
      const password = config['password'] as string | undefined
      if (host && database) {
        const creds = user
          ? `${encodeURIComponent(user)}${password ? ':' + encodeURIComponent(String(password)) : ''}@`
          : ''
        url = `postgresql://${creds}${host}:${port}/${database}`
      }
    }
    if (!url || typeof url !== 'string') {
      throw new Error('PostgresBridge requires a "url" or ("host" + "database") config')
    }
    // `ipFamily` accepts 4, 6, or 0. Anything else silently falls to the
    // default (4) — saves users from typos mapping to unexpected behavior.
    const rawFamily = config['ipFamily'] as unknown
    const ipFamily: 4 | 6 | 0 =
      rawFamily === 6 ? 6 : rawFamily === 0 ? 0 : 4
    const connectionTimeoutMillis =
      typeof config['connectionTimeoutMillis'] === 'number'
        ? (config['connectionTimeoutMillis'] as number)
        : 10_000
    this.config = {
      url,
      pool: config['pool'] as PostgresBridgeConfig['pool'],
      ipFamily,
      connectionTimeoutMillis,
    }
  }

  async connect(): Promise<void> {
    // `family` is passed through to `dns.lookup()` at connect time.
    // pg-node accepts it but `PoolConfig` doesn't list it — cast through
    // the documented superset that Client/Pool actually honor.
    this.pool = new pg.Pool({
      connectionString: this.config.url,
      min: this.config.pool?.min ?? 0,
      max: this.config.pool?.max ?? 3,
      connectionTimeoutMillis: this.config.connectionTimeoutMillis ?? 10_000,
      // IPv4-only by default — avoids the serverless/VPC-egress
      // happy-eyeballs blackhole where IPv6 packets silently drop and
      // TCP stalls ~21s per IPv6 address before the client falls back.
      // See PostgresBridgeConfig.ipFamily.
      family: this.config.ipFamily ?? 4,
    } as pg.PoolConfig & { family: 4 | 6 | 0 })
    const client = await this.pool.connect()
    try {
      await client.query('SELECT 1')
    } finally {
      client.release()
    }
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const pk = await this.getPrimaryKey(table)
    const fields = options?.fields
    const selectClause = fields ? fields.map(quote).join(', ') : '*'
    const limit = options?.limit ?? 1000

    const conditions: string[] = []
    const params: unknown[] = []
    let paramIdx = 1

    if (options?.cursor) {
      conditions.push(`${quote(pk)} > $${paramIdx}`)
      params.push(options.cursor)
      paramIdx++
    }

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const hasCol = await this.hasColumn(table, col)
      if (hasCol) {
        conditions.push(`${quote(col)} > $${paramIdx}`)
        params.push(options.changedSince)
        paramIdx++
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // Fetch limit+1 to detect whether there's a next page
    params.push(limit + 1)

    const sql = `SELECT ${selectClause} FROM ${quote(table)} ${whereClause} ORDER BY ${quote(pk)} ASC LIMIT $${paramIdx}`

    const result = await pool.query(sql, params)
    const allRows: BridgeRow[] = result.rows as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore
      ? String(rows[rows.length - 1]![pk])
      : undefined

    const countResult = await pool.query(
      `SELECT count(*)::int AS total FROM ${quote(table)}`,
    )
    const total = (countResult.rows as Array<{ total: number }>)[0]!.total

    return { rows, nextCursor, total }
  }

  async count(target: string): Promise<number> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const result = await pool.query(
      `SELECT count(*)::int AS total FROM ${quote(table)}`,
    )
    return (result.rows as Array<{ total: number }>)[0]!.total
  }

  async disconnect(): Promise<void> {
    if (this.pool) {
      await this.pool.end()
      this.pool = null
    }
    this.pkCache.clear()
  }

  async query(
    target: string,
    options: QueryOptions,
  ): Promise<QueryResult<BridgeRow>> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const selectClause = options.select
      ? options.select.map(quote).join(', ')
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
                conditions.push(`${quote(key)} = $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gt':
                conditions.push(`${quote(key)} > $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$gte':
                conditions.push(`${quote(key)} >= $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lt':
                conditions.push(`${quote(key)} < $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$lte':
                conditions.push(`${quote(key)} <= $${paramIdx}`)
                params.push(opVal)
                paramIdx++
                break
              case '$in':
                conditions.push(
                  `${quote(key)} = ANY($${paramIdx})`,
                )
                params.push(opVal)
                paramIdx++
                break
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          conditions.push(`${quote(key)} = $${paramIdx}`)
          params.push(value)
          paramIdx++
        }
      }
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    // ORDER BY
    // Accepts canonical { field, dir? } or record shorthand { col: dir, col2: dir }.
    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          // Canonical: { field: 'id', dir: 'asc' }
          parts.push(`${quote(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`)
        } else {
          // Shorthand: { id: 'asc', name: 'desc' }
          for (const [col, dir] of Object.entries(obj)) {
            if (dir === 'asc' || dir === 'desc') {
              parts.push(`${quote(col)} ${dir === 'desc' ? 'DESC' : 'ASC'}`)
            }
          }
        }
      }
      if (parts.length > 0) orderByClause = `ORDER BY ${parts.join(', ')}`
    }

    // LIMIT / OFFSET
    let limitClause = ''
    if (options.limit != null) {
      limitClause = `LIMIT $${paramIdx}`
      params.push(options.limit)
      paramIdx++
    }

    let offsetClause = ''
    if (options.offset != null) {
      offsetClause = `OFFSET $${paramIdx}`
      params.push(options.offset)
      paramIdx++
    }

    const sql = [
      `SELECT ${selectClause} FROM ${quote(table)}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    // Get total count (with same WHERE, without LIMIT/OFFSET)
    const countSql = `SELECT count(*)::int AS total FROM ${quote(table)} ${whereClause}`
    const countParams = options.where ? params.slice(0, conditions.length) : []

    const [dataResult, countResult] = await Promise.all([
      pool.query(sql, params),
      pool.query(countSql, countParams),
    ])

    return {
      rows: dataResult.rows as BridgeRow[],
      total: (countResult.rows as Array<{ total: number }>)[0]!.total,
    }
  }

  /**
   * Fetch many rows matching a filter — used by the join planner.
   *
   * Reuses the same operator support as `query` but skips the COUNT query,
   * which is pure overhead for join stitching (we're fetching known rows,
   * not paginating).
   */
  async batchRead(
    target: string,
    options: BatchReadOptions,
  ): Promise<BridgeRow[]> {
    const pool = this.assertPool()
    const table = target
    assertTableName(table)

    const selectClause =
      !options.select || options.select === '*'
        ? '*'
        : options.select.map(quote).join(', ')

    const params: unknown[] = []
    let paramIdx = 1

    const conditions: string[] = []
    for (const [key, value] of Object.entries(options.where)) {
      if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
        for (const [op, opVal] of Object.entries(value as Record<string, unknown>)) {
          switch (op) {
            case '$eq':
              conditions.push(`${quote(key)} = $${paramIdx}`)
              params.push(opVal)
              paramIdx++
              break
            case '$gt':
              conditions.push(`${quote(key)} > $${paramIdx}`)
              params.push(opVal)
              paramIdx++
              break
            case '$gte':
              conditions.push(`${quote(key)} >= $${paramIdx}`)
              params.push(opVal)
              paramIdx++
              break
            case '$lt':
              conditions.push(`${quote(key)} < $${paramIdx}`)
              params.push(opVal)
              paramIdx++
              break
            case '$lte':
              conditions.push(`${quote(key)} <= $${paramIdx}`)
              params.push(opVal)
              paramIdx++
              break
            case '$in':
              conditions.push(`${quote(key)} = ANY($${paramIdx})`)
              params.push(opVal)
              paramIdx++
              break
            default:
              throw new Error(`Unknown operator "${op}" on field "${key}"`)
          }
        }
      } else {
        conditions.push(`${quote(key)} = $${paramIdx}`)
        params.push(value)
        paramIdx++
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

    let limitClause = ''
    if (options.limit != null) {
      limitClause = `LIMIT $${paramIdx}`
      params.push(options.limit)
      paramIdx++
    }

    const sql = [
      `SELECT ${selectClause} FROM ${quote(table)}`,
      whereClause,
      orderByClause,
      limitClause,
    ]
      .filter(Boolean)
      .join(' ')

    const result = await pool.query(sql, params)
    return result.rows as BridgeRow[]
  }

  // -------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------

  async listTargets(): Promise<string[]> {
    const pool = this.assertPool()
    return listTables(pool)
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const pool = this.assertPool()
    const info = await introspect(pool, target)
    return {
      name: info.name,
      columns: info.columns,
      rowCount: info.rowCount,
    }
  }

  // -------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------

  private assertPool(): pg.Pool {
    if (!this.pool) throw new Error('PostgresBridge is not connected')
    return this.pool
  }

  private async getPrimaryKey(table: string): Promise<string> {
    const cached = this.pkCache.get(table)
    if (cached) return cached

    const pool = this.assertPool()
    const result = await pool.query(
      `SELECT kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_name = $1
       LIMIT 1`,
      [table],
    )

    const row = (result.rows as Array<{ column_name: string }>)[0]
    if (!row) {
      throw new Error(
        `Could not detect primary key for table "${table}"`,
      )
    }
    this.pkCache.set(table, row.column_name)
    return row.column_name
  }

  private async hasColumn(table: string, column: string): Promise<boolean> {
    const pool = this.assertPool()
    const result = await pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_name = $1 AND column_name = $2
       LIMIT 1`,
      [table, column],
    )
    return result.rowCount != null && result.rowCount > 0
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
