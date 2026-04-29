import cassandra from 'cassandra-driver'
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
  assertSupportedOps,
  streamingAggregate,
  STREAMING_AGGREGATE_CAPABILITIES,
  UnsupportedOperatorError,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

// CQL is restrictive — many filter shapes require ALLOW FILTERING, and
// `$or`/`$ilike` etc. aren't expressible. Declare empty for both
// operator families. `exactCount` is `false` because COUNT(*) over
// large tables in Cassandra is approximate (and expensive) by design.
const CASSANDRA_LOGICAL_OPS = [] as const
const CASSANDRA_STRING_OPS = [] as const
const CASSANDRA_BRIDGE_NAME = '@semilayer/bridge-cassandra'

export interface CassandraBridgeConfig {
  contactPoints: string[]
  localDataCenter: string
  keyspace: string
  username?: string
  password?: string
}

export class CassandraBridge implements Bridge {
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
    whereLogicalOps: CASSANDRA_LOGICAL_OPS,
    whereStringOps: CASSANDRA_STRING_OPS,
    exactCount: false,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-cassandra',
    displayName: 'Cassandra',
    icon: 'cassandra',
    supportsUrl: false,
    fields: [
      {
        key: 'contactPoints',
        label: 'Contact Points',
        type: 'string',
        required: true,
        placeholder: 'host1.example.com,host2.example.com',
        hint: 'Comma-separated list of contact point hostnames or IPs',
      },
      {
        key: 'localDataCenter',
        label: 'Local Data Center',
        type: 'string',
        required: true,
        placeholder: 'datacenter1',
      },
      {
        key: 'keyspace',
        label: 'Keyspace',
        type: 'string',
        required: true,
      },
      {
        key: 'username',
        label: 'Username',
        type: 'string',
        required: false,
        group: 'advanced',
      },
      {
        key: 'password',
        label: 'Password',
        type: 'password',
        required: false,
        group: 'advanced',
      },
    ],
  }

  private client: cassandra.Client | null = null
  private pkCache = new Map<string, string>()
  private config: CassandraBridgeConfig

  constructor(config: Record<string, unknown>) {
    const rawContactPoints = config['contactPoints']
    const contactPoints: string[] = Array.isArray(rawContactPoints)
      ? (rawContactPoints as string[])
      : typeof rawContactPoints === 'string'
        ? (rawContactPoints as string)
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean)
        : []
    const localDataCenter = config['localDataCenter'] as string | undefined
    const keyspace = config['keyspace'] as string | undefined
    if (!contactPoints.length)
      throw new Error('CassandraBridge requires "contactPoints" config array')
    if (!localDataCenter)
      throw new Error('CassandraBridge requires "localDataCenter" config string')
    if (!keyspace) throw new Error('CassandraBridge requires "keyspace" config string')
    this.config = {
      contactPoints,
      localDataCenter,
      keyspace,
      username: config['username'] as string | undefined,
      password: config['password'] as string | undefined,
    }
  }

  async connect(): Promise<void> {
    const { contactPoints, localDataCenter, keyspace, username, password } = this.config
    this.client = new cassandra.Client({
      contactPoints,
      localDataCenter,
      keyspace,
      authProvider: username
        ? new cassandra.auth.PlainTextAuthProvider(username, password ?? '')
        : undefined,
    })
    await this.client.connect()
  }

  async disconnect(): Promise<void> {
    await this.client?.shutdown()
    this.client = null
    this.pkCache.clear()
  }

  private assertClient(): cassandra.Client {
    if (!this.client) throw new Error('CassandraBridge is not connected')
    return this.client
  }

  async listTargets(): Promise<string[]> {
    const result = await this.assertClient().execute(
      'SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?',
      [this.config.keyspace],
      { prepare: true },
    )
    return result.rows.map(r => String(r['table_name']))
  }

  private async getPrimaryKey(target: string): Promise<string> {
    const cached = this.pkCache.get(target)
    if (cached) return cached
    const result = await this.assertClient().execute(
      `SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND kind = 'partition_key' ORDER BY position LIMIT 1`,
      [this.config.keyspace, target],
      { prepare: true },
    )
    const row = result.rows[0]
    if (!row) throw new Error(`Cannot detect primary key for "${target}"`)
    const pk = String(row['column_name'])
    this.pkCache.set(target, pk)
    return pk
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    assertSupportedOps(options?.where, {
      logicalOps: CASSANDRA_LOGICAL_OPS,
      stringOps: CASSANDRA_STRING_OPS,
      bridge: CASSANDRA_BRIDGE_NAME,
      target,
    })
    if (options?.where && Object.keys(options.where).length > 0) {
      // CQL COUNT(*) WHERE requires ALLOW FILTERING and is expensive on
      // large tables. Reuse the existing query() pipeline (which already
      // emits ALLOW FILTERING) and return the row count — accurate for
      // small/medium result sets, which is what callers passing a where
      // predicate expect. exactCount is declared `false` so callers know
      // not to rely on this for big tables.
      const result = await this.query(target, { where: options.where })
      return result.rows.length
    }
    const result = await this.assertClient().execute(
      `SELECT COUNT(*) as total FROM "${this.config.keyspace}"."${target}"`,
      [],
      { prepare: true },
    )
    return Number(result.rows[0]?.['total']?.toString() ?? '0')
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const client = this.assertClient()
    // PK needed for cache keying (side-effect: validates table exists)
    await this.getPrimaryKey(target)
    const limit = options?.limit ?? 1000

    const result = await client.execute(
      `SELECT * FROM "${this.config.keyspace}"."${target}" ALLOW FILTERING`,
      [],
      {
        prepare: true,
        fetchSize: limit,
        pageState: options?.cursor
          ? Buffer.from(options.cursor, 'base64')
          : undefined,
      },
    )

    const rows = result.rows.map(r => {
      const obj: BridgeRow = {}
      for (const [k, v] of Object.entries(r)) obj[k] = v
      return obj
    })

    // pageState is Buffer | string | undefined; base64-encode Buffer, pass string through
    const nextCursor = result.pageState
      ? Buffer.isBuffer(result.pageState)
        ? result.pageState.toString('base64')
        : result.pageState
      : undefined

    // Approximate total via count query
    const countResult = await client.execute(
      `SELECT COUNT(*) as total FROM "${this.config.keyspace}"."${target}"`,
      [],
      { prepare: true },
    )
    const total = Number(countResult.rows[0]?.['total']?.toString() ?? '0')

    return { rows, nextCursor, total }
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

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
    assertSupportedOps(opts.where, {
      logicalOps: CASSANDRA_LOGICAL_OPS,
      stringOps: CASSANDRA_STRING_OPS,
      bridge: CASSANDRA_BRIDGE_NAME,
      target,
    })
    const client = this.assertClient()
    const conditions: string[] = []
    const params: unknown[] = []

    if (opts.where) {
      for (const [field, value] of Object.entries(opts.where)) {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          for (const [op, v] of Object.entries(value as Record<string, unknown>)) {
            switch (op) {
              case '$eq':
                conditions.push(`"${field}" = ?`)
                params.push(v)
                break
              case '$gt':
                conditions.push(`"${field}" > ?`)
                params.push(v)
                break
              case '$gte':
                conditions.push(`"${field}" >= ?`)
                params.push(v)
                break
              case '$lt':
                conditions.push(`"${field}" < ?`)
                params.push(v)
                break
              case '$lte':
                conditions.push(`"${field}" <= ?`)
                params.push(v)
                break
              case '$in':
                conditions.push(`"${field}" IN ?`)
                params.push(v)
                break
              default:
                throw new UnsupportedOperatorError({
                  op,
                  bridge: CASSANDRA_BRIDGE_NAME,
                  target,
                })
            }
          }
        } else {
          conditions.push(`"${field}" = ?`)
          params.push(value)
        }
      }
    }

    const whereClause = conditions.length
      ? `WHERE ${conditions.join(' AND ')} ALLOW FILTERING`
      : 'ALLOW FILTERING'

    let orderClause = ''
    if (opts.orderBy) {
      const raw = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
      const parts = raw
        .filter((c): c is { field: string; dir?: 'asc' | 'desc' } => 'field' in c)
        .map(c => `"${c.field}" ${c.dir === 'desc' ? 'DESC' : 'ASC'}`)
      if (parts.length) orderClause = `ORDER BY ${parts.join(', ')}`
    }

    const limitClause = opts.limit ? `LIMIT ${opts.limit}` : ''
    const sql = `SELECT * FROM "${this.config.keyspace}"."${target}" ${whereClause} ${orderClause} ${limitClause}`
      .trim()
      .replace(/\s+/g, ' ')

    const result = await client.execute(sql, params, { prepare: true })
    const rows = result.rows.map(r => {
      const obj: BridgeRow = {}
      for (const [k, v] of Object.entries(r)) obj[k] = v
      return obj
    })
    return { rows, total: rows.length }
  }

  /**
   * Aggregate via streaming reducer. Cassandra's CQL supports GROUP BY
   * only on partition keys, which is too restrictive to be useful as a
   * general aggregate path. Streaming via `query()` lets the bridge
   * apply WHERE on partition + clustering keys at the source, then
   * reduces in memory.
   */
  aggregateCapabilities(): BridgeAggregateCapabilities {
    return STREAMING_AGGREGATE_CAPABILITIES
  }

  aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    return streamingAggregate(this, opts)
  }
}
