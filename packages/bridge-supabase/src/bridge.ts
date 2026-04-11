import { createClient, type SupabaseClient } from '@supabase/supabase-js'
import type {
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  OrderByClause,
} from '@semilayer/core'

export interface SupabaseBridgeConfig {
  url: string
  key: string
  /** Default primary key column name (default: 'id') */
  primaryKey?: string
  schema?: string
}

export class SupabaseBridge implements Bridge {
  private client: SupabaseClient | null = null
  private config: SupabaseBridgeConfig

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    const key = config['key'] as string | undefined
    if (!url || typeof url !== 'string') {
      throw new Error('SupabaseBridge requires a "url" config string')
    }
    if (!key || typeof key !== 'string') {
      throw new Error('SupabaseBridge requires a "key" config string')
    }
    this.config = {
      url,
      key,
      primaryKey: (config['primaryKey'] as string | undefined) ?? 'id',
      schema: config['schema'] as string | undefined,
    }
  }

  private assertClient(): SupabaseClient {
    if (!this.client) throw new Error('SupabaseBridge is not connected')
    return this.client
  }

  async connect(): Promise<void> {
    const { url, key, schema } = this.config
    this.client = createClient(url, key, schema ? { db: { schema } } : undefined)
    const { status } = await this.client.from('_').select('count').limit(0)
    if (status === 401 || status === 403) {
      throw new Error('SupabaseBridge: authentication failed — check your key')
    }
  }

  async disconnect(): Promise<void> {
    this.client = null
  }

  async count(target: string): Promise<number> {
    const { count, error } = await this.assertClient()
      .from(target)
      .select('*', { count: 'exact', head: true })
    if (error) throw new Error(error.message)
    return count ?? 0
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const client = this.assertClient()
    const pk = this.config.primaryKey ?? 'id'
    const limit = options?.limit ?? 1000

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let q: any = client
      .from(target)
      .select(options?.fields?.join(',') ?? '*', { count: 'exact' })

    if (options?.cursor) q = q.gt(pk, options.cursor)
    if (options?.changedSince) {
      const trackCol = options.changeTrackingColumn ?? 'updated_at'
      q = q.gt(trackCol, options.changedSince.toISOString())
    }
    q = q.order(pk, { ascending: true }).limit(limit + 1)

    const { data, error, count } = await q
    if (error) throw new Error((error as { message: string }).message)

    const rows = (data ?? []) as BridgeRow[]
    const hasMore = rows.length > limit
    const pageRows = hasMore ? rows.slice(0, limit) : rows
    const nextCursor = hasMore ? String(pageRows[pageRows.length - 1]![pk]) : undefined

    return { rows: pageRows, nextCursor, total: count ?? undefined }
  }

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const client = this.assertClient()
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let q: any = client
      .from(target)
      .select(opts.select?.join(',') ?? '*', { count: 'exact' })

    if (opts.where) {
      for (const [field, value] of Object.entries(opts.where)) {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          for (const [op, val] of Object.entries(value as Record<string, unknown>)) {
            switch (op) {
              case '$eq': q = q.eq(field, val); break
              case '$gt': q = q.gt(field, val); break
              case '$gte': q = q.gte(field, val); break
              case '$lt': q = q.lt(field, val); break
              case '$lte': q = q.lte(field, val); break
              case '$in': q = q.in(field, val as unknown[]); break
              default: throw new Error(`Unknown operator "${op}" on field "${field}"`)
            }
          }
        } else {
          q = q.eq(field, value)
        }
      }
    }

    if (opts.orderBy) {
      const raw: OrderByClause[] = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
      for (const clause of raw) {
        if ('field' in clause) {
          q = q.order(clause.field, { ascending: clause.dir !== 'desc' })
        }
      }
    }

    if (opts.limit != null && opts.offset != null) {
      q = q.range(opts.offset, opts.offset + opts.limit - 1)
    } else if (opts.limit != null) {
      q = q.limit(opts.limit)
    }

    const { data, error, count } = await q
    if (error) throw new Error((error as { message: string }).message)

    return { rows: (data ?? []) as BridgeRow[], total: count ?? undefined }
  }
}
