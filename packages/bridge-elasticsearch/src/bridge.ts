import { Client } from '@elastic/elasticsearch'
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
  WhereClause,
} from '@semilayer/core'
import {
  streamingAggregate,
  STREAMING_AGGREGATE_CAPABILITIES,
  UnsupportedOperatorError,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'

export interface ElasticsearchBridgeConfig {
  node: string
  username?: string
  password?: string
  apiKey?: string
  tls?: { rejectUnauthorized?: boolean }
}

const ES_LOGICAL_OPS = ['or', 'and', 'not'] as const
const ES_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const ES_BRIDGE_NAME = '@semilayer/bridge-elasticsearch'

/**
 * ES wildcard escapes — `*` and `?` are wildcards, `\` escapes them. Anything
 * else is literal so we don't need to touch regex metas like `.` here.
 */
function escapeWildcard(s: string): string {
  return s.replace(/[\\*?]/g, '\\$&')
}

/**
 * Translate a SQL ILIKE pattern (`%`/`_`, with `\%` / `\_` for escaped
 * literals) to an ES wildcard string (`*`/`?`).
 *
 * - `%` → `*`
 * - `_` → `?`
 * - `\%` → escaped literal `%`, `\_` → escaped literal `_`
 * - everything else is wildcard-escaped (so a literal `*` from the user
 *   stays literal in the wildcard query).
 */
function ilikeToWildcard(pattern: string): string {
  let out = ''
  let i = 0
  while (i < pattern.length) {
    const ch = pattern[i]!
    if (ch === '\\' && i + 1 < pattern.length) {
      const next = pattern[i + 1]!
      if (next === '%' || next === '_') {
        out += escapeWildcard(next)
        i += 2
        continue
      }
      out += escapeWildcard(ch)
      i++
      continue
    }
    if (ch === '%') {
      out += '*'
      i++
      continue
    }
    if (ch === '_') {
      out += '?'
      i++
      continue
    }
    out += escapeWildcard(ch)
    i++
  }
  return out
}

export class ElasticsearchBridge implements Bridge {
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
    whereLogicalOps: ES_LOGICAL_OPS,
    whereStringOps: ES_STRING_OPS,
    exactCount: true,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-elasticsearch',
    displayName: 'Elasticsearch',
    icon: 'elasticsearch',
    supportsUrl: false,
    fields: [
      {
        key: 'node',
        label: 'Node URL',
        type: 'string',
        required: true,
        placeholder: 'https://localhost:9200',
        hint: 'Elasticsearch node URL',
      },
      {
        key: 'apiKey',
        label: 'API Key',
        type: 'password',
        required: false,
        group: 'advanced',
        hint: 'API key (preferred for Elastic Cloud)',
      },
      {
        key: 'username',
        label: 'Username',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Basic auth username',
      },
      {
        key: 'password',
        label: 'Password',
        type: 'password',
        required: false,
        group: 'advanced',
        hint: 'Basic auth password',
      },
      {
        key: 'rejectUnauthorized',
        label: 'Reject Unauthorized',
        type: 'boolean',
        required: false,
        default: true,
        group: 'advanced',
        hint: 'Reject invalid SSL certificates',
      },
    ],
  }

  private client: Client | null = null
  private config: ElasticsearchBridgeConfig

  constructor(config: Record<string, unknown>) {
    const node = config['node'] as string | undefined
    if (!node || typeof node !== 'string')
      throw new Error('ElasticsearchBridge requires a "node" config string')
    this.config = {
      node,
      username: config['username'] as string | undefined,
      password: config['password'] as string | undefined,
      apiKey: config['apiKey'] as string | undefined,
      tls: config['tls'] as ElasticsearchBridgeConfig['tls'],
    }
  }

  async connect(): Promise<void> {
    const { node, username, password, apiKey, tls } = this.config
    this.client = new Client({
      node,
      auth: apiKey
        ? { apiKey }
        : username
          ? { username, password: password ?? '' }
          : undefined,
      tls,
    })
    await this.client.ping()
  }

  async disconnect(): Promise<void> {
    await this.client?.close()
    this.client = null
  }

  private assertClient(): Client {
    if (!this.client) throw new Error('ElasticsearchBridge is not connected')
    return this.client
  }

  async listTargets(): Promise<string[]> {
    const result = await this.assertClient().cat.indices({ format: 'json' })
    return (result as Array<{ index?: string }>)
      .map(i => i.index ?? '')
      .filter(i => i.length > 0 && !i.startsWith('.'))
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    const client = this.assertClient()
    const query = options?.where
      ? this.translateWhere(options.where, target)
      : { match_all: {} }
    const result = await client.count({ index: target, body: { query } })
    return result.count
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const client = this.assertClient()
    const limit = options?.limit ?? 1000

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const searchBody: Record<string, any> = {
      size: limit + 1,
      sort: [{ _id: 'asc' }],
      query: { match_all: {} },
    }
    if (options?.cursor) searchBody['search_after'] = [options.cursor]
    if (options?.changedSince) {
      searchBody['query'] = {
        range: {
          [options.changeTrackingColumn ?? 'updated_at']: {
            gt: options.changedSince.toISOString(),
          },
        },
      }
    }
    if (options?.fields?.length) searchBody['_source'] = options.fields

    const result = await client.search({ index: target, body: searchBody })
    const hits = result.hits.hits
    const hasMore = hits.length > limit
    const selected = hasMore ? hits.slice(0, limit) : hits
    const rows: BridgeRow[] = selected.map(hit => ({
      _id: hit._id,
      ...((hit._source as Record<string, unknown>) ?? {}),
    }))
    const nextCursor = hasMore ? selected[selected.length - 1]!._id : undefined
    const totalObj = result.hits.total
    const total = typeof totalObj === 'number' ? totalObj : totalObj?.value

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
    const client = this.assertClient()

    const query = opts.where
      ? this.translateWhere(opts.where, target)
      : { match_all: {} }

    const sort: Array<Record<string, string>> = []
    if (opts.orderBy) {
      const raw: OrderByClause[] = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
      for (const clause of raw) {
        if ('field' in clause) sort.push({ [clause.field]: clause.dir === 'desc' ? 'desc' : 'asc' })
      }
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const body: Record<string, any> = {
      query,
      ...(sort.length ? { sort } : {}),
      ...(opts.limit != null ? { size: opts.limit } : {}),
      ...(opts.offset != null ? { from: opts.offset } : {}),
      ...(opts.select?.length ? { _source: opts.select } : {}),
    }

    const result = await client.search({ index: target, body })
    const hits = result.hits.hits
    const rows: BridgeRow[] = hits.map(hit => ({
      _id: hit._id,
      ...((hit._source as Record<string, unknown>) ?? {}),
    }))
    const totalObj = result.hits.total
    const total = typeof totalObj === 'number' ? totalObj : totalObj?.value
    return { rows, total }
  }

  /**
   * Translate a SemiLayer `WhereClause` into an Elasticsearch Query DSL
   * fragment. The top-level result is always wrapped in `{bool: {must: […]}}`
   * (or `{match_all: {}}` for an empty clause) so callers can splice it
   * into a `body.query` slot directly. Nested logical combinators emit
   * their own bool wrappers — `$or` → `bool.should`, `$not` →
   * `bool.must_not` — recursively.
   *
   * Mapping notes:
   * - `$or` → `bool.should` with `minimum_should_match: 1`. Without the
   *   minimum, ES treats `should` as a relevance booster, not a filter.
   * - `$and` → `bool.must`. (Could use `bool.filter` for a non-scoring
   *   variant; `must` is fine for our exact-match semantics.)
   * - `$not` → `bool.must_not`.
   * - String operators use `case_insensitive: true` (ES 7.10+). For older
   *   clusters this would need a per-field `lowercase` normalizer at index
   *   time — out of scope for the bridge.
   */
  private translateWhere(
    where: WhereClause,
    target: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  ): Record<string, any> {
    const must = this.compileClauseMust(where, target)
    if (must.length === 0) return { match_all: {} }
    return { bool: { must } }
  }

  private compileClauseMust(
    clause: WhereClause,
    target: string,
  ): unknown[] {
    const must: unknown[] = []

    for (const [key, value] of Object.entries(clause as Record<string, unknown>)) {
      if (key === '$or') {
        const arr = value as WhereClause[]
        if (!Array.isArray(arr) || arr.length === 0) continue
        must.push({
          bool: {
            should: arr.map((c) => this.translateWhere(c, target)),
            minimum_should_match: 1,
          },
        })
        continue
      }
      if (key === '$and') {
        const arr = value as WhereClause[]
        if (!Array.isArray(arr) || arr.length === 0) continue
        must.push({
          bool: {
            must: arr.flatMap((c) => this.compileClauseMust(c, target)),
          },
        })
        continue
      }
      if (key === '$not') {
        must.push({
          bool: {
            must_not: [this.translateWhere(value as WhereClause, target)],
          },
        })
        continue
      }
      // Field clause — bare value or operator object.
      this.compileFieldClause(key, value, target, must)
    }

    return must
  }

  private compileFieldClause(
    field: string,
    rawValue: unknown,
    target: string,
    must: unknown[],
  ): void {
    // Bare value = $eq → term (mirrors existing convention pre-refactor).
    if (
      rawValue === null ||
      rawValue instanceof Date ||
      typeof rawValue !== 'object' ||
      Array.isArray(rawValue)
    ) {
      must.push({ term: { [field]: rawValue } })
      return
    }

    const ops = rawValue as Record<string, unknown>
    for (const [op, v] of Object.entries(ops)) {
      switch (op) {
        case '$eq':
          must.push({ term: { [field]: v } })
          break
        case '$ne':
          must.push({ bool: { must_not: [{ term: { [field]: v } }] } })
          break
        case '$gt':
          must.push({ range: { [field]: { gt: v } } })
          break
        case '$gte':
          must.push({ range: { [field]: { gte: v } } })
          break
        case '$lt':
          must.push({ range: { [field]: { lt: v } } })
          break
        case '$lte':
          must.push({ range: { [field]: { lte: v } } })
          break
        case '$in':
          must.push({ terms: { [field]: v as unknown[] } })
          break
        case '$nin':
          must.push({
            bool: { must_not: [{ terms: { [field]: v as unknown[] } }] },
          })
          break
        case '$ilike':
          if (typeof v !== 'string') break
          must.push({
            wildcard: {
              [field]: { value: ilikeToWildcard(v), case_insensitive: true },
            },
          })
          break
        case '$contains':
          if (typeof v !== 'string') break
          must.push({
            wildcard: {
              [field]: {
                value: '*' + escapeWildcard(v) + '*',
                case_insensitive: true,
              },
            },
          })
          break
        case '$startsWith':
          if (typeof v !== 'string') break
          must.push({
            prefix: { [field]: { value: v, case_insensitive: true } },
          })
          break
        case '$endsWith':
          if (typeof v !== 'string') break
          must.push({
            wildcard: {
              [field]: {
                value: '*' + escapeWildcard(v),
                case_insensitive: true,
              },
            },
          })
          break
        default:
          throw new UnsupportedOperatorError({
            op,
            bridge: ES_BRIDGE_NAME,
            target,
          })
      }
    }
  }

  /**
   * Aggregate via the shared streaming reducer. Elasticsearch's native
   * aggregations DSL (`terms` / `date_histogram` / `composite` /
   * `cardinality` / `percentiles`) is rich enough to deserve a future
   * native adapter — but the v1 path delegates to `streamingAggregate`,
   * which still pre-filters via `bridge.query()` (translating
   * `candidatesWhere` into an ES bool/filter clause) and reduces in
   * memory. Real bytes-on-the-wire win over service-side streaming
   * since ES applies the filter at the index, not after.
   *
   * Future native pushdown can emit a `composite` agg with one source
   * per dim + sub-aggs per measure.
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
