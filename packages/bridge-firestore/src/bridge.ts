import { Firestore, FieldPath, type Query, type WhereFilterOp } from '@google-cloud/firestore'
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

  async count(target: string): Promise<number> {
    const snap = await this.assertDb().collection(target).count().get()
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
    const db = this.assertDb()
    let q: Query = db.collection(target)

    if (opts.where) {
      for (const [field, value] of Object.entries(opts.where)) {
        if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
          const ops = value as Record<string, unknown>
          for (const [op, val] of Object.entries(ops)) {
            const fsOpMap: Record<string, WhereFilterOp> = {
              $eq: '==', $gt: '>', $gte: '>=', $lt: '<', $lte: '<=', $in: 'in',
            }
            const fsOp = fsOpMap[op]
            if (!fsOp) throw new Error(`Unknown operator "${op}" on field "${field}"`)
            q = q.where(field, fsOp, val as unknown)
          }
        } else {
          q = q.where(field, '==', value)
        }
      }
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
}
