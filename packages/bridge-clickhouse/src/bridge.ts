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
import { createClient, type ClickHouseClient } from '@clickhouse/client'
import {
  buildAggregateSql,
  buildWhereSql,
  executeAggregateQueries,
  CLICKHOUSE_DIALECT,
  CLICKHOUSE_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
  type WhereSqlDialect,
} from '@semilayer/bridge-sdk'

// ClickHouse where dialect — backtick-quoted identifiers, `?` placeholders
// (rebound to `{p0:Type}` at execution time, see `bindParams` below), and
// `LOWER(col) LIKE LOWER(?)` for `$ilike` since ClickHouse has no native
// ILIKE.
const CH_WHERE_DIALECT: WhereSqlDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '``') + '`',
  placeholder: () => '?',
  ilike: (col, p) => `lower(${col}) LIKE lower(${p})`,
}

const CH_LOGICAL_OPS = ['or', 'and', 'not'] as const
const CH_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const CH_BRIDGE_NAME = '@semilayer/bridge-clickhouse'

/**
 * Convert a `?`-placeholder SQL string + positional params array into
 * ClickHouse's named typed-parameter form (`{p0:Type}`, `query_params`).
 *
 * `buildWhereSql` emits `?` for portability across SQL dialects, but
 * `@clickhouse/client` only accepts named typed placeholders over the HTTP
 * interface. We walk each `?` left-to-right, infer the CH type from the
 * runtime JS type of the matching value, and substitute. Bool → UInt8,
 * integer Number → Int64, float Number → Float64, Date → DateTime64(3),
 * everything else → String.
 */
function bindParams(
  sql: string,
  params: readonly unknown[],
): { sql: string; queryParams: Record<string, unknown> } {
  const queryParams: Record<string, unknown> = {}
  let i = 0
  const out = sql.replace(/\?/g, () => {
    const v = params[i]
    const name = `p${i}`
    let chType = 'String'
    if (typeof v === 'number') {
      chType = Number.isInteger(v) ? 'Int64' : 'Float64'
    } else if (typeof v === 'boolean') {
      chType = 'UInt8'
    } else if (v instanceof Date) {
      chType = 'DateTime64(3)'
    }
    queryParams[name] = v instanceof Date ? v.toISOString() : v
    i++
    return `{${name}:${chType}}`
  })
  return { sql: out, queryParams }
}

export interface ClickhouseBridgeConfig {
  host: string
  port?: number
  database?: string
  username?: string
  password?: string
  protocol?: 'http' | 'https'
}

interface ClickHouseTableRow {
  name: string
}

interface ClickHouseDescribeRow {
  name: string
  type: string
  default_type: string
}

interface ClickHouseCountRow {
  total: string
}

function quoteId(id: string): string {
  return '`' + id.replace(/`/g, '``') + '`'
}

export class ClickhouseBridge implements Bridge {
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
    whereLogicalOps: CH_LOGICAL_OPS,
    whereStringOps: CH_STRING_OPS,
    exactCount: true,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-clickhouse',
    displayName: 'ClickHouse',
    icon: 'clickhouse',
    supportsUrl: false,
    fields: [
      {
        key: 'host',
        label: 'Host',
        type: 'string',
        required: true,
        placeholder: 'localhost',
      },
      {
        key: 'port',
        label: 'Port',
        type: 'number',
        required: false,
        default: 8123,
      },
      {
        key: 'database',
        label: 'Database',
        type: 'string',
        required: true,
        placeholder: 'default',
        default: 'default',
      },
      {
        key: 'username',
        label: 'Username',
        type: 'string',
        required: false,
        default: 'default',
        group: 'advanced',
      },
      {
        key: 'password',
        label: 'Password',
        type: 'password',
        required: false,
        group: 'advanced',
      },
      {
        key: 'protocol',
        label: 'Protocol',
        type: 'string',
        required: false,
        default: 'http',
        group: 'advanced',
        hint: 'http or https',
      },
    ],
  }

  private client: ClickHouseClient | null = null
  private readonly cfg: Required<ClickhouseBridgeConfig>

  constructor(config: Record<string, unknown>) {
    const host = config['host'] as string | undefined
    if (!host) throw new Error('ClickhouseBridge requires a "host" config value')

    this.cfg = {
      host,
      port: (config['port'] as number | undefined) ?? 8123,
      database: (config['database'] as string | undefined) ?? 'default',
      username: (config['username'] as string | undefined) ?? 'default',
      password: (config['password'] as string | undefined) ?? '',
      protocol: ((config['protocol'] as string | undefined) ?? 'http') as 'http' | 'https',
    }
  }

  async connect(): Promise<void> {
    const { protocol, host, port, username, password, database } = this.cfg
    const url = `${protocol}://${host}:${port}`
    this.client = createClient({ url, username, password, database })
    // Verify connectivity
    await this.client.ping()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const client = this.assertClient()
    const { database } = this.cfg

    const fields = options?.fields
    const selectClause = fields ? fields.map(quoteId).join(', ') : '*'
    const limit = options?.limit ?? 1000

    // Decode cursor as offset integer
    const offset = options?.cursor ? parseInt(options.cursor, 10) : 0

    const conditions: string[] = []
    const params: Record<string, unknown> = {}
    let paramIdx = 0

    if (options?.changedSince) {
      const col = options.changeTrackingColumn ?? 'updated_at'
      const pName = `p${paramIdx++}`
      conditions.push(`${quoteId(col)} > {${pName}:DateTime64(3)}`)
      params[pName] = options.changedSince.toISOString()
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

    const tableFqn = database
      ? `${quoteId(database)}.${quoteId(target)}`
      : quoteId(target)

    // Fetch limit+1 rows to detect next page
    const sql = `SELECT ${selectClause} FROM ${tableFqn} ${whereClause} LIMIT ${limit + 1} OFFSET ${offset}`

    const resultSet = await client.query({ query: sql, query_params: params, format: 'JSONEachRow' })
    const allRows = (await resultSet.json()) as BridgeRow[]

    const hasMore = allRows.length > limit
    const rows = hasMore ? allRows.slice(0, limit) : allRows
    const nextCursor = hasMore ? String(offset + limit) : undefined

    // Count
    const countSql = `SELECT count() as total FROM ${tableFqn} ${whereClause}`
    const countSet = await client.query({ query: countSql, query_params: params, format: 'JSONEachRow' })
    const countRows = (await countSet.json()) as ClickHouseCountRow[]
    const total = Number(countRows[0]?.total ?? 0)

    return { rows, nextCursor, total }
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    const client = this.assertClient()
    const { database } = this.cfg
    const tableFqn = database
      ? `${quoteId(database)}.${quoteId(target)}`
      : quoteId(target)
    const built = buildWhereSql(options?.where, CH_WHERE_DIALECT, {
      logicalOps: CH_LOGICAL_OPS,
      stringOps: CH_STRING_OPS,
      bridge: CH_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''
    const bound = bindParams(
      `SELECT count() as total FROM ${tableFqn} ${whereClause}`,
      built.params,
    )
    const resultSet = await client.query({
      query: bound.sql,
      query_params: bound.queryParams,
      format: 'JSONEachRow',
    })
    const rows = (await resultSet.json()) as ClickHouseCountRow[]
    return Number(rows[0]?.total ?? 0)
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.close()
      this.client = null
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

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return CLICKHOUSE_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const client = this.assertClient()
    return executeAggregateQueries(
      buildAggregateSql(opts, CLICKHOUSE_DIALECT),
      async (sql, params) => {
        // ClickHouse requires typed placeholders ({p1:UInt32}, {p1:String},
        // ...). The dialect emits everything as `{pN:String}` because it
        // doesn't see the param values at SQL-build time. Re-type each
        // placeholder here based on the JS value's runtime type — numbers
        // (notably the LIMIT param) need UInt32/Float64, booleans need
        // UInt8, everything else stays String.
        let typedSql = sql
        const queryParams: Record<string, unknown> = {}
        ;(params as unknown[]).forEach((v, i) => {
          const name = `p${i + 1}`
          let chType = 'String'
          if (typeof v === 'number') {
            chType = Number.isInteger(v) ? 'Int64' : 'Float64'
          } else if (typeof v === 'boolean') {
            chType = 'UInt8'
          } else if (v instanceof Date) {
            chType = 'DateTime64(3)'
          }
          typedSql = typedSql.replace(
            new RegExp(`\\{${name}:String\\}`, 'g'),
            `{${name}:${chType}}`,
          )
          queryParams[name] = v instanceof Date ? v.toISOString() : v
        })
        const resultSet = await client.query({
          query: typedSql,
          query_params: queryParams,
          format: 'JSONEachRow',
        })
        const rows = (await resultSet.json()) as Array<Record<string, unknown>>
        return rows
      },
    )
  }

  async query(target: string, options: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const client = this.assertClient()
    const { database } = this.cfg

    const tableFqn = database
      ? `${quoteId(database)}.${quoteId(target)}`
      : quoteId(target)

    const selectClause = options.select ? options.select.map(quoteId).join(', ') : '*'

    const built = buildWhereSql(options.where, CH_WHERE_DIALECT, {
      logicalOps: CH_LOGICAL_OPS,
      stringOps: CH_STRING_OPS,
      bridge: CH_BRIDGE_NAME,
      target,
    })
    const whereClause = built.sql ? `WHERE ${built.sql}` : ''

    // ORDER BY
    let orderByClause = ''
    if (options.orderBy) {
      const raw = Array.isArray(options.orderBy) ? options.orderBy : [options.orderBy]
      const parts: string[] = []
      for (const clause of raw) {
        const obj = clause as unknown as Record<string, unknown>
        if (typeof obj.field === 'string') {
          parts.push(`${quoteId(obj.field)} ${obj.dir === 'desc' ? 'DESC' : 'ASC'}`)
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

    const limitClause = options.limit != null ? `LIMIT ${options.limit}` : ''
    const offsetClause = options.offset != null ? `OFFSET ${options.offset}` : ''

    const sql = [
      `SELECT ${selectClause} FROM ${tableFqn}`,
      whereClause,
      orderByClause,
      limitClause,
      offsetClause,
    ]
      .filter(Boolean)
      .join(' ')

    const countSql = `SELECT count() as total FROM ${tableFqn} ${whereClause}`

    const dataBound = bindParams(sql, built.params)
    const countBound = bindParams(countSql, built.params)

    const [dataSet, countSet] = await Promise.all([
      client.query({
        query: dataBound.sql,
        query_params: dataBound.queryParams,
        format: 'JSONEachRow',
      }),
      client.query({
        query: countBound.sql,
        query_params: countBound.queryParams,
        format: 'JSONEachRow',
      }),
    ])

    const rows = (await dataSet.json()) as BridgeRow[]
    const countRows = (await countSet.json()) as ClickHouseCountRow[]
    const total = Number(countRows[0]?.total ?? 0)

    return { rows, total }
  }

  async listTargets(): Promise<string[]> {
    const client = this.assertClient()
    const resultSet = await client.query({ query: 'SHOW TABLES', format: 'JSONEachRow' })
    const rows = (await resultSet.json()) as ClickHouseTableRow[]
    return rows.map((r) => r.name)
  }

  async introspectTarget(target: string): Promise<TargetSchema> {
    const client = this.assertClient()
    const { database } = this.cfg

    const tableFqn = database
      ? `${quoteId(database)}.${quoteId(target)}`
      : quoteId(target)

    const [descSet, countSet] = await Promise.all([
      client.query({ query: `DESCRIBE TABLE ${tableFqn}`, format: 'JSONEachRow' }),
      client.query({ query: `SELECT count() as total FROM ${tableFqn}`, format: 'JSONEachRow' }),
    ])

    const descRows = (await descSet.json()) as ClickHouseDescribeRow[]
    const countRows = (await countSet.json()) as ClickHouseCountRow[]

    const columns: TargetColumnInfo[] = descRows.map((row) => ({
      name: row.name,
      type: row.type,
      nullable: row.type.startsWith('Nullable('),
      primaryKey: false,
    }))

    return {
      name: target,
      columns,
      rowCount: Number(countRows[0]?.total ?? 0),
    }
  }

  private assertClient(): ClickHouseClient {
    if (!this.client) throw new Error('ClickhouseBridge is not connected')
    return this.client
  }
}
