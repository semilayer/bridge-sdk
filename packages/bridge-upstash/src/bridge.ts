import { Redis } from '@upstash/redis'
import type {
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  OrderByClause,
} from '@semilayer/core'

export interface UpstashBridgeConfig {
  url: string
  token: string
}

function matchesWhere(row: BridgeRow, where: Record<string, unknown>): boolean {
  for (const [field, value] of Object.entries(where)) {
    const rowVal = row[field]
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const ops = value as Record<string, unknown>
      for (const [op, v] of Object.entries(ops)) {
        if (op === '$eq' && rowVal !== v) return false
        if (op === '$gt' && !(rowVal! > (v as never))) return false
        if (op === '$gte' && !(rowVal! >= (v as never))) return false
        if (op === '$lt' && !(rowVal! < (v as never))) return false
        if (op === '$lte' && !(rowVal! <= (v as never))) return false
        if (op === '$in' && !(v as unknown[]).includes(rowVal)) return false
        if (!['$eq', '$gt', '$gte', '$lt', '$lte', '$in'].includes(op)) {
          throw new Error(`Unknown operator "${op}"`)
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

  async count(target: string): Promise<number> {
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

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const all = await this.read(target, { limit: 100_000 })
    let rows = all.rows

    if (opts.where) rows = rows.filter((row) => matchesWhere(row, opts.where!))
    if (opts.orderBy) rows = sortRows(rows, opts.orderBy)

    const total = rows.length
    if (opts.offset) rows = rows.slice(opts.offset)
    if (opts.limit) rows = rows.slice(0, opts.limit)

    return { rows, total }
  }
}
