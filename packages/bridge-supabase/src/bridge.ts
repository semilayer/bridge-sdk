import { createClient, type SupabaseClient } from '@supabase/supabase-js'
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

export interface SupabaseBridgeConfig {
  url: string
  key: string
  /** Default primary key column name (default: 'id') */
  primaryKey?: string
  schema?: string
}

export class SupabaseBridge implements Bridge {
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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private client: SupabaseClient<any, any, any> | null = null
  private config: SupabaseBridgeConfig

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-supabase',
    displayName: 'Supabase',
    icon: 'supabase',
    supportsUrl: false,
    fields: [
      { key: 'url', label: 'Project URL', type: 'string', required: true, placeholder: 'https://xxx.supabase.co', hint: 'Your Supabase project URL' },
      { key: 'key', label: 'Service Role Key', type: 'password', required: true, hint: 'Service role key from Project Settings → API' },
      { key: 'schema', label: 'Schema', type: 'string', required: false, default: 'public', group: 'advanced' },
    ],
  }

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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private assertClient(): SupabaseClient<any, any, any> {
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
