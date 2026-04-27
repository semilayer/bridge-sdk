import Redis from 'ioredis'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  OrderByClause,
  QueryOptions,
  QueryResult,
  ReadOptions,
  ReadResult,
} from '@semilayer/core'
import {
  streamingAggregate,
  STREAMING_AGGREGATE_CAPABILITIES,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

export interface RedisBridgeConfig {
  url?: string
  host?: string
  port?: number
  password?: string
  db?: number
}

function matchesWhere(row: BridgeRow, where: Record<string, unknown>): boolean {
  for (const [field, value] of Object.entries(where)) {
    const v = row[field]
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      for (const [op, opVal] of Object.entries(value as Record<string, unknown>)) {
        switch (op) {
          case '$eq':
            if (v !== opVal) return false
            break
          case '$gt':
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            if (!((v as any) > (opVal as any))) return false
            break
          case '$gte':
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            if (!((v as any) >= (opVal as any))) return false
            break
          case '$lt':
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            if (!((v as any) < (opVal as any))) return false
            break
          case '$lte':
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            if (!((v as any) <= (opVal as any))) return false
            break
          case '$in':
            if (!(opVal as unknown[]).includes(v)) return false
            break
          default:
            throw new Error(`Unknown operator "${op}"`)
        }
      }
    } else if (v !== value) return false
  }
  return true
}

function sortRows(rows: BridgeRow[], orderBy: OrderByClause | OrderByClause[]): BridgeRow[] {
  const clauses = Array.isArray(orderBy) ? orderBy : [orderBy]
  return [...rows].sort((a, b) => {
    for (const c of clauses) {
      const dir = c.dir === 'desc' ? -1 : 1
      if (a[c.field]! < b[c.field]!) return -dir
      if (a[c.field]! > b[c.field]!) return dir
    }
    return 0
  })
}

export class RedisBridge implements Bridge {
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
    packageName: '@semilayer/bridge-redis',
    displayName: 'Redis',
    icon: 'redis',
    supportsUrl: true,
    urlPlaceholder: 'redis://user:pass@host:6379/0',
    fields: [
      {
        key: 'host',
        label: 'Host',
        type: 'string',
        required: true,
        default: 'localhost',
      },
      {
        key: 'port',
        label: 'Port',
        type: 'number',
        required: false,
        default: 6379,
      },
      {
        key: 'password',
        label: 'Password',
        type: 'password',
        required: false,
      },
      {
        key: 'db',
        label: 'DB',
        type: 'number',
        required: false,
        default: 0,
        group: 'advanced',
        hint: 'Database index (0-15)',
      },
    ],
  }

  private redis: Redis | null = null
  private config: RedisBridgeConfig

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    const host = config['host'] as string | undefined
    if (!url && !host) throw new Error('RedisBridge requires either "url" or "host" config')
    this.config = {
      url,
      host,
      port: config['port'] as number | undefined,
      password: config['password'] as string | undefined,
      db: config['db'] as number | undefined,
    }
  }

  async connect(): Promise<void> {
    const { url, host, port, password, db } = this.config
    this.redis = url
      ? new Redis(url)
      : new Redis({ host, port, password, db })
    await this.redis.ping()
  }

  async disconnect(): Promise<void> {
    this.redis?.disconnect()
    this.redis = null
  }

  private assertRedis(): Redis {
    if (!this.redis) throw new Error('RedisBridge is not connected')
    return this.redis
  }

  async listTargets(): Promise<string[]> {
    const keys = await this.assertRedis().keys('*')
    return [...new Set(keys.map(k => k.split(':')[0]).filter((p): p is string => Boolean(p)))]
  }

  async count(target: string): Promise<number> {
    return (await this.assertRedis().keys(`${target}:*`)).length
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const redis = this.assertRedis()
    const limit = options?.limit ?? 1000
    const allKeys = (await redis.keys(`${target}:*`)).sort()
    const startIdx = options?.cursor
      ? allKeys.findIndex(k => k > options.cursor!)
      : 0
    const effectiveStart = startIdx === -1 ? allKeys.length : Math.max(0, startIdx)
    const pageKeys = allKeys.slice(effectiveStart, effectiveStart + limit + 1)
    const hasMore = pageKeys.length > limit
    const selectedKeys = hasMore ? pageKeys.slice(0, limit) : pageKeys
    const nextCursor = hasMore ? selectedKeys[selectedKeys.length - 1] : undefined

    if (selectedKeys.length === 0) return { rows: [], total: allKeys.length }

    const values = await redis.mget(...selectedKeys)
    const rows: BridgeRow[] = []
    for (let i = 0; i < selectedKeys.length; i++) {
      const key = selectedKeys[i]!
      const val = values[i]
      if (!val) continue
      let parsed: unknown
      try {
        parsed = JSON.parse(val)
      } catch {
        parsed = val
      }
      const obj =
        typeof parsed === 'object' && parsed !== null && !Array.isArray(parsed)
          ? (parsed as Record<string, unknown>)
          : { value: parsed }
      rows.push({ _key: key, ...obj })
    }
    return { rows, nextCursor, total: allKeys.length }
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
    const all = await this.read(target, { limit: 100_000 })
    let rows = all.rows
    if (opts.where) rows = rows.filter(row => matchesWhere(row, opts.where!))
    if (opts.orderBy) rows = sortRows(rows, opts.orderBy)
    const total = rows.length
    if (opts.offset) rows = rows.slice(opts.offset)
    if (opts.limit) rows = rows.slice(0, opts.limit)
    return { rows, total }
  }

  /**
   * Aggregate via streaming reducer. Redis is a key-value store with no
   * native group-by; the bridge reduces in memory after `query()`
   * fetches the matching set of values.
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
