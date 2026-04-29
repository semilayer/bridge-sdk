import { Firestore, FieldPath, type Query, type WhereFilterOp } from '@google-cloud/firestore'
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

// Firestore has restrictive compound queries (one inequality + one
// array-contains per query), so the bridge declines all logical/string
// operators. The base $eq/$gt/$gte/$lt/$lte/$in family stays handled
// by the existing translator below.
const FIRESTORE_LOGICAL_OPS = [] as const
const FIRESTORE_STRING_OPS = [] as const
const FIRESTORE_BRIDGE_NAME = '@semilayer/bridge-firestore'

export interface FirestoreBridgeConfig {
  projectId: string
  credentials?: { client_email: string; private_key: string }
  keyFilename?: string
  databaseId?: string
}

export class FirestoreBridge implements Bridge {
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
    whereLogicalOps: FIRESTORE_LOGICAL_OPS,
    whereStringOps: FIRESTORE_STRING_OPS,
    exactCount: true,
  }

  static manifest: BridgeManifest = {
    packageName: '@semilayer/bridge-firestore',
    displayName: 'Firestore',
    icon: 'firestore',
    supportsUrl: false,
    fields: [
      {
        key: 'projectId',
        label: 'Project ID',
        type: 'string',
        required: true,
        hint: 'Google Cloud project ID',
      },
      {
        key: 'serviceAccountEmail',
        label: 'Service Account Email',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Service account email (alternative to key file)',
      },
      {
        key: 'serviceAccountKey',
        label: 'Service Account Key',
        type: 'password',
        required: false,
        group: 'advanced',
        hint: 'Service account private key',
      },
      {
        key: 'keyFilename',
        label: 'Key Filename',
        type: 'string',
        required: false,
        group: 'advanced',
        hint: 'Path to service account JSON key file',
      },
      {
        key: 'databaseId',
        label: 'Database ID',
        type: 'string',
        required: false,
        group: 'advanced',
        default: '(default)',
        hint: 'Firestore database ID',
      },
    ],
  }

  private db: Firestore | null = null
  private config: FirestoreBridgeConfig

  constructor(config: Record<string, unknown>) {
    const projectId = config['projectId'] as string | undefined
    if (!projectId || typeof projectId !== 'string') {
      throw new Error('FirestoreBridge requires a "projectId" config string')
    }
    const email = config['serviceAccountEmail'] as string | undefined
    const privKey = config['serviceAccountKey'] as string | undefined
    const credentialsFromFields =
      email && privKey ? { client_email: email, private_key: privKey } : undefined
    const credentials =
      credentialsFromFields ?? (config['credentials'] as FirestoreBridgeConfig['credentials'])
    this.config = {
      projectId,
      credentials,
      keyFilename: config['keyFilename'] as string | undefined,
      databaseId: config['databaseId'] as string | undefined,
    }
  }

  private assertDb(): Firestore {
    if (!this.db) throw new Error('FirestoreBridge is not connected')
    return this.db
  }

  async connect(): Promise<void> {
    const { projectId, credentials, keyFilename, databaseId } = this.config
    this.db = new Firestore({ projectId, credentials, keyFilename, databaseId })
    await this.db.listCollections()
  }

  async disconnect(): Promise<void> {
    await this.db?.terminate()
    this.db = null
  }

  async listTargets(): Promise<string[]> {
    const cols = await this.assertDb().listCollections()
    return cols.map((c) => c.id)
  }

  async count(target: string, options?: CountOptions): Promise<number> {
    assertSupportedOps(options?.where, {
      logicalOps: FIRESTORE_LOGICAL_OPS,
      stringOps: FIRESTORE_STRING_OPS,
      bridge: FIRESTORE_BRIDGE_NAME,
      target,
    })
    const db = this.assertDb()
    let q: Query = db.collection(target)
    if (options?.where) {
      q = applyWhere(q, options.where, target)
    }
    const snap = await q.count().get()
    return snap.data().count
  }

  async read(target: string, options?: ReadOptions): Promise<ReadResult> {
    const db = this.assertDb()
    const col = db.collection(target)
    const limit = options?.limit ?? 1000

    let q: Query = col

    if (options?.changedSince) {
      const trackCol = options.changeTrackingColumn ?? 'updated_at'
      q = q.where(trackCol, '>', options.changedSince)
    }

    q = q.orderBy(FieldPath.documentId())

    if (options?.cursor) q = q.startAfter(options.cursor)
    q = q.limit(limit + 1)

    if (options?.fields?.length) q = q.select(...(options.fields as [string, ...string[]]))

    const snapshot = await q.get()
    const docs = snapshot.docs
    const hasMore = docs.length > limit
    const pageDocs = hasMore ? docs.slice(0, limit) : docs
    const nextCursor = hasMore ? pageDocs[pageDocs.length - 1]!.id : undefined
    const rows: BridgeRow[] = pageDocs.map((doc) => ({ id: doc.id, ...doc.data() }))
    const total = await this.count(target)

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
      logicalOps: FIRESTORE_LOGICAL_OPS,
      stringOps: FIRESTORE_STRING_OPS,
      bridge: FIRESTORE_BRIDGE_NAME,
      target,
    })
    const db = this.assertDb()
    let q: Query = db.collection(target)

    if (opts.where) {
      q = applyWhere(q, opts.where, target)
    }

    if (opts.orderBy) {
      const raw: OrderByClause[] = Array.isArray(opts.orderBy) ? opts.orderBy : [opts.orderBy]
      for (const clause of raw) {
        if ('field' in clause) {
          q = q.orderBy(clause.field, clause.dir === 'desc' ? 'desc' : 'asc')
        }
      }
    }

    if (opts.limit != null) q = q.limit(opts.limit)
    if (opts.offset != null) q = q.offset(opts.offset)
    if (opts.select?.length) q = q.select(...(opts.select as [string, ...string[]]))

    const snapshot = await q.get()
    return {
      rows: snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() })) as BridgeRow[],
      total: snapshot.size,
    }
  }

  /**
   * Aggregate via streaming reducer. Firestore has limited native
   * aggregation (`count()`/`sum()`/`avg()` only at the collection
   * level, no group-by) — streaming with `query()` pre-filtering at
   * the source is the right fit.
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

/**
 * Translate a comparator-only `WhereClause` (no `$or`/`$and`/`$not`,
 * no string ops — those are gated by `assertSupportedOps` upstream)
 * into a chained Firestore `Query`. Unknown operators surface as
 * `UnsupportedOperatorError` so callers can tell the difference between
 * an operator the bridge doesn't push down vs. a real DB error.
 */
function applyWhere(q: Query, where: Record<string, unknown>, target: string): Query {
  const fsOpMap: Record<string, WhereFilterOp> = {
    $eq: '==',
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $in: 'in',
  }
  let cursor: Query = q
  for (const [field, value] of Object.entries(where)) {
    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
      const ops = value as Record<string, unknown>
      for (const [op, val] of Object.entries(ops)) {
        const fsOp = fsOpMap[op]
        if (!fsOp) {
          throw new UnsupportedOperatorError({
            op,
            bridge: FIRESTORE_BRIDGE_NAME,
            target,
          })
        }
        cursor = cursor.where(field, fsOp, val as unknown)
      }
    } else {
      cursor = cursor.where(field, '==', value)
    }
  }
  return cursor
}
