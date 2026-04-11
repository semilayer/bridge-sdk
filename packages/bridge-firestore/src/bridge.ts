import { Firestore, FieldPath, type Query, type WhereFilterOp } from '@google-cloud/firestore'
import type {
  Bridge,
  BridgeRow,
  ReadOptions,
  ReadResult,
  QueryOptions,
  QueryResult,
  OrderByClause,
} from '@semilayer/core'

export interface FirestoreBridgeConfig {
  projectId: string
  credentials?: { client_email: string; private_key: string }
  keyFilename?: string
  databaseId?: string
}

export class FirestoreBridge implements Bridge {
  private db: Firestore | null = null
  private config: FirestoreBridgeConfig

  constructor(config: Record<string, unknown>) {
    const projectId = config['projectId'] as string | undefined
    if (!projectId || typeof projectId !== 'string') {
      throw new Error('FirestoreBridge requires a "projectId" config string')
    }
    this.config = {
      projectId,
      credentials: config['credentials'] as FirestoreBridgeConfig['credentials'],
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
