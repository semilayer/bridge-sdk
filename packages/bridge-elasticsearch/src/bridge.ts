import { Client } from '@elastic/elasticsearch'
import type {
  Bridge,
  BridgeManifest,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  OrderByClause,
} from '@semilayer/core'

export interface ElasticsearchBridgeConfig {
  node: string
  username?: string
  password?: string
  apiKey?: string
  tls?: { rejectUnauthorized?: boolean }
}

export class ElasticsearchBridge implements Bridge {
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

  async count(target: string): Promise<number> {
    const result = await this.assertClient().count({ index: target })
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

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const client = this.assertClient()
    const must: unknown[] = []

    if (opts.where) {
      for (const [field, value] of Object.entries(opts.where)) {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          for (const [op, v] of Object.entries(value as Record<string, unknown>)) {
            switch (op) {
              case '$eq':
                must.push({ term: { [field]: v } })
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
              default:
                throw new Error(`Unknown operator "${op}"`)
            }
          }
        } else {
          must.push({ term: { [field]: value } })
        }
      }
    }

    const sort: Array<Record<string, string>> = []
    if (opts.orderBy) {
      const raw: OrderByClause[] = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
      for (const clause of raw) {
        if ('field' in clause) sort.push({ [clause.field]: clause.dir === 'desc' ? 'desc' : 'asc' })
      }
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const body: Record<string, any> = {
      query: must.length ? { bool: { must } } : { match_all: {} },
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
}
