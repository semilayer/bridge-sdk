import { MongoClient, ObjectId, type Document, type Sort } from 'mongodb'
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

export interface MongodbBridgeConfig {
  url: string
  database?: string
  pool?: { min?: number; max?: number }
}

export class MongodbBridge implements Bridge {
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
    packageName: '@semilayer/bridge-mongodb',
    displayName: 'MongoDB',
    icon: 'mongodb',
    supportsUrl: true,
    urlPlaceholder: 'mongodb+srv://user:pass@cluster.mongodb.net/dbname',
    fields: [
      {
        key: 'database',
        label: 'Database',
        type: 'string',
        required: false,
        hint: 'Database name (if not specified in URL)',
      },
    ],
  }

  private client: MongoClient | null = null
  private config: MongodbBridgeConfig
  private dbName: string

  constructor(config: Record<string, unknown>) {
    const url = config['url'] as string | undefined
    if (!url || typeof url !== 'string') {
      throw new Error('MongodbBridge requires a "url" config string')
    }
    const database = config['database'] as string | undefined
    this.dbName = database ?? (() => {
      try {
        return new URL(url).pathname.slice(1) || 'test'
      } catch {
        return 'test'
      }
    })()
    this.config = {
      url,
      database: this.dbName,
      pool: config['pool'] as MongodbBridgeConfig['pool'],
    }
  }

  private assertClient(): MongoClient {
    if (!this.client) throw new Error('MongodbBridge is not connected')
    return this.client
  }

  private db() {
    return this.assertClient().db(this.dbName)
  }

  async connect(): Promise<void> {
    const { url, pool } = this.config
    this.client = new MongoClient(url, {
      minPoolSize: pool?.min,
      maxPoolSize: pool?.max,
    })
    await this.client.connect()
    await this.client.db().command({ ping: 1 })
  }

  async disconnect(): Promise<void> {
    await this.client?.close()
    this.client = null
  }

  async listTargets(): Promise<string[]> {
    const cols = await this.db().listCollections().toArray()
    return cols.map((c) => c.name)
  }

  async count(target: string): Promise<number> {
    return this.db().collection(target).countDocuments()
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const col = this.db().collection(target)
    const limit = options?.limit ?? 1000
    const filter: Document = {}

    if (options?.cursor) {
      try {
        filter['_id'] = { $gt: new ObjectId(options.cursor) }
      } catch {
        filter['_id'] = { $gt: options.cursor }
      }
    }

    if (options?.changedSince) {
      const trackCol = options.changeTrackingColumn ?? 'updated_at'
      filter[trackCol] = { $gt: options.changedSince }
    }

    const projection = options?.fields
      ? Object.fromEntries(options.fields.map((f) => [f, 1]))
      : undefined

    const docs = await col
      .find(filter)
      .sort({ _id: 1 } as Sort)
      .limit(limit + 1)
      .project(projection ?? {})
      .toArray()

    const hasMore = docs.length > limit
    const pageDocs = hasMore ? docs.slice(0, limit) : docs
    const nextCursor = hasMore ? String(pageDocs[pageDocs.length - 1]!['_id']) : undefined
    const total = await col.countDocuments({})
    const rows: BridgeRow[] = pageDocs.map((doc) => ({ ...doc, _id: String(doc['_id']) }))

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
    const col = this.db().collection(target)
    const filter: Document = {}
    const sort: Sort = {}

    if (opts.where) {
      for (const [field, value] of Object.entries(opts.where)) {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          const mapped: Document = {}
          for (const [op, val] of Object.entries(ops)) {
            const opMap: Record<string, string> = {
              $eq: '$eq', $gt: '$gt', $gte: '$gte', $lt: '$lt', $lte: '$lte', $in: '$in',
            }
            const mongoOp = opMap[op]
            if (!mongoOp) throw new Error(`Unknown operator "${op}"`)
            mapped[mongoOp] = val
          }
          filter[field] = mapped
        } else {
          filter[field] = value
        }
      }
    }

    if (opts.orderBy) {
      const raw: OrderByClause[] = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
      for (const clause of raw) {
        if ('field' in clause) {
          ;(sort as Record<string, number>)[clause.field] = clause.dir === 'desc' ? -1 : 1
        }
      }
    }

    const total = await col.countDocuments(filter)
    let cursor2 = col.find(filter)
    if (Object.keys(sort).length) cursor2 = cursor2.sort(sort)
    if (opts.offset) cursor2 = cursor2.skip(opts.offset)
    if (opts.limit) cursor2 = cursor2.limit(opts.limit)
    if (opts.select) {
      cursor2 = cursor2.project(Object.fromEntries(opts.select.map((f) => [f, 1])))
    }

    const docs = await cursor2.toArray()
    return {
      rows: docs.map((d) => ({ ...d, _id: String(d['_id']) })) as BridgeRow[],
      total,
    }
  }
}
