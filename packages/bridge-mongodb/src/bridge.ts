import { MongoClient, ObjectId, type Document, type Sort } from 'mongodb'
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
  UnsupportedOperatorError,
  type AggregateOptions,
  type AggregateRow,
  type BridgeAggregateCapabilities,
  type BridgeExecutionContext,
} from '@semilayer/bridge-sdk'
import {
  buildMongoAggregate,
  decodeMongoRows,
  MONGODB_AGGREGATE_CAPABILITIES,
} from './aggregate.js'

export interface MongodbBridgeConfig {
  url: string
  database?: string
  pool?: { min?: number; max?: number }
}

// Strict ISO 8601 — only coerce values that are unambiguously a date string
// so we don't accidentally convert real string identifiers that happen to
// parse as dates (e.g. "2024" → year 2024).
const ISO_DATE_RE =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$/

function coerceMaybeDate(v: unknown): unknown {
  if (typeof v === 'string' && ISO_DATE_RE.test(v)) {
    const d = new Date(v)
    if (!isNaN(d.getTime())) return d
  }
  return v
}

// $in (and similar array-valued operators) need element-wise coercion.
function coerceMaybeDateDeep(v: unknown): unknown {
  if (Array.isArray(v)) return v.map(coerceMaybeDate)
  return coerceMaybeDate(v)
}

const MONGO_LOGICAL_OPS = ['or', 'and', 'not'] as const
const MONGO_STRING_OPS = ['ilike', 'contains', 'startsWith', 'endsWith'] as const
const MONGO_BRIDGE_NAME = '@semilayer/bridge-mongodb'

// Comparison operators that map 1:1 to Mongo's filter language.
const MONGO_COMPARISON_OPS = new Set([
  '$eq',
  '$ne',
  '$gt',
  '$gte',
  '$lt',
  '$lte',
  '$in',
  '$nin',
])

/**
 * Escape regex metacharacters so a literal substring like "a.b" doesn't
 * suddenly mean "a + any-char + b" inside a `$regex` expression.
 */
function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
}

/**
 * Translate a SQL ILIKE pattern (`%`/`_` wildcards, with `\%` / `\_` for
 * escaped literals) into a JS regex source string suitable for `$regex`.
 *
 * - `%` → `.*`
 * - `_` → `.`
 * - `\%` → literal `%`, `\_` → literal `_`
 * - everything else is regex-escaped so e.g. `a.b%` doesn't match `axxxbX`.
 *
 * The result is anchored with `^` and `$` to mirror SQL ILIKE semantics
 * (whole-string match), and the caller passes `$options: 'i'` for case
 * insensitivity.
 */
function ilikeToRegex(pattern: string): string {
  let out = '^'
  let i = 0
  while (i < pattern.length) {
    const ch = pattern[i]!
    if (ch === '\\' && i + 1 < pattern.length) {
      const next = pattern[i + 1]!
      if (next === '%' || next === '_') {
        out += escapeRegex(next)
        i += 2
        continue
      }
      // Other backslash sequences: escape the backslash and let the next
      // pass handle the following char as a literal.
      out += escapeRegex(ch)
      i++
      continue
    }
    if (ch === '%') {
      out += '.*'
      i++
      continue
    }
    if (ch === '_') {
      out += '.'
      i++
      continue
    }
    out += escapeRegex(ch)
    i++
  }
  out += '$'
  return out
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
    whereLogicalOps: MONGO_LOGICAL_OPS,
    whereStringOps: MONGO_STRING_OPS,
    exactCount: true,
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

  async count(target: string, options?: CountOptions): Promise<number> {
    const filter = this.translateWhere(options?.where, target)
    return this.db().collection(target).countDocuments(filter)
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

  aggregateCapabilities(): BridgeAggregateCapabilities {
    return MONGODB_AGGREGATE_CAPABILITIES
  }

  async *aggregate(
    opts: AggregateOptions,
    _ctx?: BridgeExecutionContext,
  ): AsyncIterable<AggregateRow> {
    const col = this.db().collection(opts.target)
    const built = buildMongoAggregate(opts)
    const [main, ...tks] = await Promise.all([
      col.aggregate(built.mainPipeline).toArray(),
      ...built.topKPipelines.map((tk) => col.aggregate(tk.pipeline).toArray()),
    ])
    const tkResults: Record<string, Array<Record<string, unknown>>> = {}
    built.topKPipelines.forEach((tk, i) => {
      tkResults[tk.measureName] = (tks[i] ?? []) as Array<Record<string, unknown>>
    })
    const rows = decodeMongoRows(
      main as Array<Record<string, unknown>>,
      built,
      tkResults,
    )
    for (const r of rows) yield r
  }

  async query(target: string, opts: QueryOptions): Promise<QueryResult<BridgeRow>> {
    const col = this.db().collection(target)
    const filter = this.translateWhere(opts.where, target)
    const sort: Sort = {}

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

  /**
   * Translate a SemiLayer `WhereClause` into a MongoDB filter document.
   *
   * MongoDB's filter language already mirrors most of the SemiLayer shape —
   * comparison ops (`$eq`/`$gt`/`$in`/etc.) match 1:1, and `$or`/`$and` are
   * native top-level operators. The two non-trivial cases:
   *
   * 1. `$not` — Mongo's per-field `$not` only accepts an operator-doc value
   *    (`{field: {$not: {$gt: 5}}}`), not a full where clause. We use
   *    `$nor: [translated]` instead, which is the clause-level negation
   *    Mongo intends for "row does not match this AND/OR tree".
   * 2. The four string operators don't have direct Mongo equivalents; we
   *    compile them to case-insensitive `$regex` expressions, escaping
   *    regex metacharacters so a literal value like `"a.b"` matches that
   *    exact string.
   */
  private translateWhere(
    where: WhereClause | undefined,
    target: string,
  ): Document {
    if (!where) return {}
    return this.compileClause(where, target)
  }

  private compileClause(clause: WhereClause, target: string): Document {
    const filter: Document = {}
    for (const [key, value] of Object.entries(clause as Record<string, unknown>)) {
      if (key === '$or') {
        const arr = value as WhereClause[]
        if (!Array.isArray(arr) || arr.length === 0) continue
        filter['$or'] = arr.map((c) => this.compileClause(c, target))
        continue
      }
      if (key === '$and') {
        const arr = value as WhereClause[]
        if (!Array.isArray(arr) || arr.length === 0) continue
        filter['$and'] = arr.map((c) => this.compileClause(c, target))
        continue
      }
      if (key === '$not') {
        // $nor with a single child = "no rows match this clause"; safer
        // and more general than per-field $not, which only accepts an
        // operator doc. See translateWhere docblock.
        filter['$nor'] = [this.compileClause(value as WhereClause, target)]
        continue
      }
      filter[key] = this.compileFieldValue(value, target)
    }
    return filter
  }

  private compileFieldValue(value: unknown, target: string): unknown {
    // Bare value = $eq (Mongo's implicit equality).
    if (value === null) return null
    if (
      value instanceof Date ||
      typeof value !== 'object' ||
      Array.isArray(value)
    ) {
      return value
    }

    const ops = value as Record<string, unknown>
    const out: Document = {}
    for (const [op, opVal] of Object.entries(ops)) {
      if (MONGO_COMPARISON_OPS.has(op)) {
        // Comparison ops already match Mongo's syntax — pass through after
        // ISO-string → BSON Date coercion so date-typed fields actually
        // match. Without this, $gt: '2026-04-25T...' against a BSON Date
        // field silently returns zero rows.
        out[op] = coerceMaybeDateDeep(opVal)
        continue
      }
      if (op === '$ilike') {
        if (typeof opVal !== 'string') continue
        out['$regex'] = ilikeToRegex(opVal)
        out['$options'] = 'i'
        continue
      }
      if (op === '$contains') {
        if (typeof opVal !== 'string') continue
        out['$regex'] = escapeRegex(opVal)
        out['$options'] = 'i'
        continue
      }
      if (op === '$startsWith') {
        if (typeof opVal !== 'string') continue
        out['$regex'] = '^' + escapeRegex(opVal)
        out['$options'] = 'i'
        continue
      }
      if (op === '$endsWith') {
        if (typeof opVal !== 'string') continue
        out['$regex'] = escapeRegex(opVal) + '$'
        out['$options'] = 'i'
        continue
      }
      throw new UnsupportedOperatorError({
        op,
        bridge: MONGO_BRIDGE_NAME,
        target,
      })
    }
    return out
  }
}
