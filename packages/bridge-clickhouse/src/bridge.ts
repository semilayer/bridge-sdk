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
import { createClient, type ClickHouseClient } from '@clickhouse/client'
import {
  buildAggregateSql,
  executeAggregateQueries,
  CLICKHOUSE_DIALECT,
  CLICKHOUSE_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

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

  async count(target: string): Promise<number> {
    const client = this.assertClient()
    const { database } = this.cfg
    const tableFqn = database
      ? `${quoteId(database)}.${quoteId(target)}`
      : quoteId(target)
    const resultSet = await client.query({
      query: `SELECT count() as total FROM ${tableFqn}`,
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
        // Convert positional array → ClickHouse named-param object.
        // Dialect emits {p1:String}, {p2:String}, ... so we key by p1, p2.
        const queryParams: Record<string, unknown> = {}
        ;(params as unknown[]).forEach((v, i) => {
          queryParams[`p${i + 1}`] = v
        })
        const resultSet = await client.query({
          query: sql,
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

    const conditions: string[] = []
    const params: Record<string, unknown> = {}
    let paramIdx = 0

    // WHERE
    if (options.where) {
      for (const [key, value] of Object.entries(options.where)) {
        if (value !== null && typeof value === 'object' && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, opVal] of Object.entries(ops)) {
            const pName = `p${paramIdx++}`
            switch (op) {
              case '$eq':
                conditions.push(`${quoteId(key)} = {${pName}:String}`)
                params[pName] = opVal
                break
              case '$gt':
                conditions.push(`${quoteId(key)} > {${pName}:String}`)
                params[pName] = opVal
                break
              case '$gte':
                conditions.push(`${quoteId(key)} >= {${pName}:String}`)
                params[pName] = opVal
                break
              case '$lt':
                conditions.push(`${quoteId(key)} < {${pName}:String}`)
                params[pName] = opVal
                break
              case '$lte':
                conditions.push(`${quoteId(key)} <= {${pName}:String}`)
                params[pName] = opVal
                break
              case '$in': {
                const inVals = opVal as unknown[]
                const inParams = inVals.map((v) => {
                  const ip = `p${paramIdx++}`
                  params[ip] = v
                  return `{${ip}:String}`
                })
                conditions.push(`${quoteId(key)} IN (${inParams.join(', ')})`)
                break
              }
              default:
                throw new Error(`Unknown operator "${op}" on field "${key}"`)
            }
          }
        } else {
          const pName = `p${paramIdx++}`
          conditions.push(`${quoteId(key)} = {${pName}:String}`)
          params[pName] = value
        }
      }
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : ''

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

    const [dataSet, countSet] = await Promise.all([
      client.query({ query: sql, query_params: params, format: 'JSONEachRow' }),
      client.query({ query: countSql, query_params: params, format: 'JSONEachRow' }),
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
