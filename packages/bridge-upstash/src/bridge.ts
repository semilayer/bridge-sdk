import { Redis } from '@upstash/redis'
import type {
  BatchReadOptions,
  Bridge,
  BridgeCapabilities,
  BridgeManifest,
  BridgeRow,
  CountOptions,
  OrderByClause,
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

// Upstash is HTTP-fronted Redis — same operator surface as plain Redis,
// no native filtering. Declare empty for both logical and string ops.
const UPSTASH_LOGICAL_OPS = [] as const
const UPSTASH_STRING_OPS = [] as const
const UPSTASH_BRIDGE_NAME = '@semilayer/bridge-upstash'

export interface UpstashBridgeConfig {
  url: string
  token: string
}

function matchesWhere(row: BridgeRow, where: Record<string, unknown>, target: string): boolean {
  for (const [field, value] of Object.entries(where)) {
    const rowVal = row[field]
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const ops = value as Record<string, unknown>
      for (const [op, v] of Object.entries(ops)) {
        if (op === '$eq' && rowVal !== v) return false
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (op === '$gt' && !((rowVal as any) > (v as any))) return false
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (op === '$gte' && !((rowVal as any) >= (v as any))) return false
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (op === '$lt' && !((rowVal as any) < (v as any))) return false
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        if (op === '$lte' && !((rowVal as any) <= (v as any))) return false
        if (op === '$in' && !(v as unknown[]).includes(rowVal)) return false
        if (!['$eq', '$gt', '$gte', '$lt', '$lte', '$in'].includes(op)) {
          throw new UnsupportedOperatorError({
            op,
            bridge: UPSTASH_BRIDGE_NAME,
            target,
          })
        }
      }
    } else if (rowVal !== value) {
      return false
    }
  }
  return true
}

function sortRows(rows: BridgeRow[], orderBy: OrderByClause | OrderByClause[]): BridgeRow[] {
  const clauses: OrderByClause[] = Array.isArray(orderBy) ? orderBy : [orderBy]
  return [...rows].sort((a, b) => {
    for (const clause of clauses) {
      const av = a[clause.field]
      const bv = b[clause.field]
      const dir = clause.dir === 'desc' ? -1 : 1
      if (av! < bv!) return -dir
      if (av! > bv!) return dir
    }
    return 0
  })
}

export class UpstashBridge implements Bridge {
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
    whereLogicalOps: UPSTASH_LOGICAL_OPS,
    whereStringOps: UPSTASH_STRING_OPS,
    exactCount: true,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-upstash',
    displayName: 'Upstash Redis',
    icon: 'upstash',
    supportsUrl: false,
    fields: [
      {
        key: 'url',
        label: 'URL',
        type: 'string',
        required: true,
        placeholder: 'https://us1-xxx.upstash.io',
        hint: 'REST URL from Upstash console',
      },
      {
        key: 'token',
        label: 'Token',
        type: 'password',
        required: true,
        hint: 'REST token from Upstash console',
      },
    ],
  }

  private redis: Redis | null = null
  private config: UpstashBridgeConfig

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    const token = config['token'] as string | undefined
    if (!url || typeof url !== 'string') {
      throw new Error('UpstashBridge requires a "url" config string')
    }
    if (!token || typeof token !== 'string') {
      throw new Error('UpstashBridge requires a "token" config string')
    }
    this.config = { url, token }
  }

  private assertRedis(): Redis {
    if (!this.redis) throw new Error('UpstashBridge is not connected')
    return this.redis
  }

  async connect(): Promise<void> {
    const { url, token } = this.config
    this.redis = new Redis({ url, token })
    await this.redis.ping()
  }

  async disconnect(): Promise<void> {
    // HTTP client, no socket to close
    this.redis = null
  }

  async listTargets(): Promise<string[]> {
    const keys = await this.assertRedis().keys('*')
    const prefixes = new Set(
      keys.map((k) => k.split(':')[0]).filter((p): p is string => Boolean(p)),
    )
    return [...prefixes]
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    assertSupportedOps(options?.where, {
      logicalOps: UPSTASH_LOGICAL_OPS,
      stringOps: UPSTASH_STRING_OPS,
      bridge: UPSTASH_BRIDGE_NAME,
      target,
    })
    if (options?.where && Object.keys(options.where).length > 0) {
      // No native filter pushdown — fall through to query() which loads
      // the matching keyspace and applies $eq/$gt/$in in JS.
      const result = await this.query(target, { where: options.where })
      return result.rows.length
    }
    const keys = await this.assertRedis().keys(`${target}:*`)
    return keys.length
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const redis = this.assertRedis()
    const limit = options?.limit ?? 1000
    const allKeys = (await redis.keys(`${target}:*`)).sort()

    const startIdx = options?.cursor
      ? allKeys.findIndex((k) => k > options.cursor!)
      : 0
    const effectiveStart = startIdx === -1 ? allKeys.length : Math.max(0, startIdx)
    const pageKeys = allKeys.slice(effectiveStart, effectiveStart + limit + 1)
    const hasMore = pageKeys.length > limit
    const selectedKeys = hasMore ? pageKeys.slice(0, limit) : pageKeys
    const nextCursor = hasMore ? selectedKeys[selectedKeys.length - 1] : undefined

    if (selectedKeys.length === 0) {
      return { rows: [], total: allKeys.length }
    }

    const values = await redis.mget<unknown[]>(...selectedKeys)
    const rows: BridgeRow[] = []
    for (let i = 0; i < selectedKeys.length; i++) {
      const key = selectedKeys[i]!
      const val = values[i]
      if (val === null || val === undefined) continue
      const parsed =
        typeof val === 'string'
          ? (() => {
              try {
                return JSON.parse(val)
              } catch {
                return val
              }
            })()
          : val
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
    assertSupportedOps(opts.where, {
      logicalOps: UPSTASH_LOGICAL_OPS,
      stringOps: UPSTASH_STRING_OPS,
      bridge: UPSTASH_BRIDGE_NAME,
      target,
    })
    const all = await this.read(target, { limit: 100_000 })
    let rows = all.rows

    if (opts.where) rows = rows.filter((row) => matchesWhere(row, opts.where!, target))
    if (opts.orderBy) rows = sortRows(rows, opts.orderBy)

    const total = rows.length
    if (opts.offset) rows = rows.slice(opts.offset)
    if (opts.limit) rows = rows.slice(0, opts.limit)

    return { rows, total }
  }

  /**
   * Aggregate via streaming reducer. Upstash is HTTP-fronted Redis —
   * no native group-by; the bridge reduces in memory after `query()`.
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
