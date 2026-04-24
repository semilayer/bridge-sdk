import pg from 'pg'
import * as dns from 'node:dns/promises'
import { isIP } from 'node:net'
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
   * IP family preference for DNS resolution.
   *
   * We pre-resolve the URL's hostname ourselves (via `dns.lookup(host,
   * {family})`) and substitute the resolved IP literal into the pool
   * config. This is NOT optional plumbing: `pg@8` silently drops the
   * `family` option passed to `pg.Pool` — it calls `net.Socket.connect
   * (port, host)` with no options, so Node's `autoSelectFamily` (happy-
   * eyeballs, default-on in Node 20+) races IPv4 and IPv6 regardless of
   * what we set. In serverless/VPC-egress environments (Cloud Run Direct
   * VPC, AWS Lambda in-VPC) IPv6 packets often blackhole — happy-eyeballs
   * stalls TCP connect until kernel timeout (~21s per v6 address) and
   * the error aggregates ALL addresses even though IPv4 would have
   * succeeded. Passing `host` as a literal IP bypasses DNS entirely and
   * forces the family we actually want.
   *
   * TLS note: we set `ssl.servername` back to the original hostname so
   * SNI-based routing still works after the host swap. Neon, Supabase,
   * and most managed Postgres need this.
   *
   * - `4` (default) — **IPv4 only**. Safest for serverless + VPC-egress
   *   setups. Every widely-used hosted Postgres (Neon, RDS, Supabase,
   *   Cloud SQL, PlanetScale, Aiven, Railway) returns IPv4 addresses.
   * - `6` — IPv6 only. For clusters that genuinely run IPv6-only PG.
   * - `0` — skip pre-resolution entirely, let Node's happy-eyeballs pick.
   *   Legacy behavior; only use if you have a specific reason.
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
    // See PostgresBridgeConfig.ipFamily for the full story. Short version:
    // pg silently ignores its `family` option, so we pre-resolve DNS to
    // the requested family ourselves and feed pg a literal IP as `host`.
    // `ssl.servername` preserves SNI routing for the original hostname.
    const poolConfig = await buildPoolConfig(this.config)
    this.pool = new pg.Pool(poolConfig)
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

type ResolvedSsl =
  | boolean
  | { rejectUnauthorized: boolean; servername: string }

/**
 * Build the `pg.Pool` config from the bridge config, pre-resolving DNS
 * to the requested IP family so pg's happy-eyeballs no-op is bypassed.
 * Exported only for tests.
 */
export async function buildPoolConfig(
  config: PostgresBridgeConfig,
): Promise<pg.PoolConfig> {
  const parsed = parseUrlHostAndSsl(config.url)
  const family = config.ipFamily ?? 4

  let resolvedHost: string | undefined
  let sslServername = parsed.host

  // Skip pre-resolution when the host is a unix socket path, already an
  // IP literal, or the caller explicitly opted out via family=0.
  const shouldResolve =
    family !== 0 &&
    parsed.host.length > 0 &&
    !parsed.host.startsWith('/') &&
    isIP(parsed.host) === 0

  if (shouldResolve) {
    try {
      const { address } = await dns.lookup(parsed.host, { family })
      resolvedHost = address
      sslServername = parsed.host
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      throw new Error(
        `Failed to resolve "${parsed.host}" to IPv${family}: ${msg}`,
      )
    }
  }

  const ssl = buildSslConfig(parsed.sslMode, sslServername)

  const poolConfig: pg.PoolConfig = {
    connectionString: config.url,
    min: config.pool?.min ?? 0,
    max: config.pool?.max ?? 3,
    connectionTimeoutMillis: config.connectionTimeoutMillis ?? 10_000,
  }
  if (resolvedHost !== undefined) {
    poolConfig.host = resolvedHost
  }
  if (ssl !== null) {
    poolConfig.ssl = ssl
  }
  return poolConfig
}

interface ParsedUrl {
  host: string
  sslMode: string | null
}

function parseUrlHostAndSsl(url: string): ParsedUrl {
  try {
    const u = new URL(url)
    return { host: u.hostname, sslMode: u.searchParams.get('sslmode') }
  } catch {
    // Exotic URL shapes pg accepts but WHATWG URL rejects. Fall back to
    // letting pg do whatever it would have done — at least we don't
    // break the connection path.
    return { host: '', sslMode: null }
  }
}

function buildSslConfig(
  sslMode: string | null,
  originalHost: string,
): ResolvedSsl | null {
  if (!sslMode) return null
  if (sslMode === 'disable') return false
  return {
    rejectUnauthorized: sslMode === 'verify-full' || sslMode === 'verify-ca',
    servername: originalHost,
  }
}
