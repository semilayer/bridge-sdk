/**
 * MongoDB native pushdown via the aggregation pipeline.
 *
 * Builds a `$match` → `$group` → `$project` → `$sort` → `$limit` pipeline
 * per call. Per-bucket top_k measures run as separate aggregation
 * pipelines (Mongo's `$group` can't emit ranked-array values directly
 * without a much bigger expression — separate pipelines stay readable
 * and the bridge runs them in parallel via `Promise.all`).
 *
 * Time bucket strategy: emit the bucket *string* directly via `$dateToString`
 * so the planner's executor sees `{ "2026-04-27" }` rows out of the box,
 * matching MockBridge / SQL bridges. Cheaper than projecting a Date and
 * formatting JS-side and side-steps the `_id` shape limitations.
 *
 * Percentile: Mongo 7+ has `$percentile` accumulator. We always declare
 * `percentile: 'exact'` in capabilities — older Mongo (≤6) clusters get
 * the planner's streaming fallback.
 *
 * count_distinct: `$addToSet` + `$size` for exact (small cardinality only)
 * — declares `'exact'` in capabilities. Approximate via HLL not yet wired.
 */
import type { Document } from 'mongodb'
import type {
  AggregateDimension,
  AggregateMeasure,
  AggregateOptions,
  AnalyzeTimeBucket,
  AggregateRow,
  BridgeAggregateCapabilities,
  DimensionBucket,
} from '@semilayer/bridge-sdk'

export const MONGODB_AGGREGATE_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: true,
  groupBy: true,
  timeBucket: true,
  numericBucket: true,
  geoBucket: false,
  count: true,
  countDistinct: 'exact',
  sum: true,
  avg: true,
  minMax: true,
  // Mongo 7+: $percentile accumulator. Older clusters miss the operator
  // entirely — bridges may want to override the cap to `false` for legacy
  // deployments. v1 ships exact since "is server new enough" can't be
  // detected without a probe and we don't want capability-discovery I/O
  // on every aggregateCapabilities() call.
  percentile: 'exact',
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

export interface BuiltMongoAggregate {
  /** Main aggregation pipeline — yields one doc per bucket. */
  mainPipeline: Document[]
  /** Per-top_k measure: separate pipeline + dim aliases for stitching. */
  topKPipelines: Array<{ measureName: string; pipeline: Document[]; k: number }>
  /** Bucket strategy per dim alias — used to format the output values. */
  dimsSchema: Array<{ outputKey: string; bucket: DimensionBucket | undefined }>
  measuresSchema: Array<{ name: string; agg: AggregateMeasure['agg'] }>
}

export function buildMongoAggregate(opts: AggregateOptions): BuiltMongoAggregate {
  const dimsSchema: BuiltMongoAggregate['dimsSchema'] = []
  const measuresSchema: BuiltMongoAggregate['measuresSchema'] = []

  // ─── $match ─────────────────────────────────────────────────────
  const match: Document = {}
  if (opts.candidatesWhere) {
    Object.assign(match, translateWhereToMongo(opts.candidatesWhere))
  }
  if (opts.ids && opts.ids.length > 0) {
    match['_id'] = { $in: opts.ids }
  }
  // Drop rows where any grouping field is null/missing — mirrors the
  // streaming reducer's behavior and SQL-bridge contract.
  for (const dim of opts.dimensions) {
    match[dim.field] = { ...(match[dim.field] as Document | undefined), $ne: null, $exists: true }
    // For numeric breaks, also gate the value to the in-range window so
    // it doesn't group as a single NULL bucket via $switch's default branch.
    if (dim.bucket && typeof dim.bucket === 'object' && dim.bucket.type === 'numeric' && 'breaks' in dim.bucket) {
      const breaks = dim.bucket.breaks
      const lo = breaks[0]
      const hi = breaks[breaks.length - 1]
      match[dim.field] = {
        ...(match[dim.field] as Document),
        $gte: lo,
        $lt: hi,
      }
    }
  }

  // ─── $group key ─────────────────────────────────────────────────
  const groupId: Document = {}
  for (const dim of opts.dimensions) {
    const key = dim.as ?? dim.field
    dimsSchema.push({ outputKey: key, bucket: dim.bucket })
    groupId[sanitize(key)] = dimExpr(dim)
  }

  // ─── $group accumulators ────────────────────────────────────────
  const group: Document = { _id: opts.dimensions.length === 0 ? null : groupId, count: { $sum: 1 } }
  const topKPipelines: BuiltMongoAggregate['topKPipelines'] = []
  for (const [name, m] of Object.entries(opts.measures)) {
    if (m.agg === 'top_k') {
      topKPipelines.push(buildTopKPipeline(opts, name, m, match))
      continue
    }
    measuresSchema.push({ name, agg: m.agg })
    const accumulator = measureAccumulator(m)
    group[sanitize(`m_${name}`)] = m.where
      ? wrapWithCondition(accumulator, m.where)
      : accumulator
  }

  // ─── $project — flatten + decode bucket values ──────────────────
  const project: Document = { _id: 0, count: 1 }
  for (const ds of dimsSchema) {
    project[`dims.${ds.outputKey}`] = `$_id.${sanitize(ds.outputKey)}`
  }
  for (const ms of measuresSchema) {
    project[`measures.${ms.name}`] = `$${sanitize(`m_${ms.name}`)}`
  }
  // count_distinct: $addToSet stores a set; finalize in $project via $size
  for (const ms of measuresSchema) {
    if (ms.agg === 'count_distinct') {
      project[`measures.${ms.name}`] = { $size: `$${sanitize(`m_${ms.name}`)}` }
    }
  }

  const pipeline: Document[] = []
  if (Object.keys(match).length > 0) pipeline.push({ $match: match })
  if (opts.sample != null && opts.sample < 1) {
    // Mongo's $sample picks a fixed N — translate the rate to an
    // approximate row count so we honor the sample knob even in
    // pipeline form. Fallback to coll.estimatedDocumentCount() at
    // execution time would be more accurate but adds a round-trip.
    const sampleSize = Math.max(1, Math.floor(50 * opts.sample))
    pipeline.push({ $sample: { size: sampleSize } })
  }
  pipeline.push({ $group: group })
  pipeline.push({ $project: project })

  if (opts.having) {
    pipeline.push({ $match: translateHavingToMongo(opts.having) })
  }
  if (opts.sort && opts.sort.length > 0) {
    const sort: Document = {}
    for (const s of opts.sort) {
      const path = s.key === 'count' ? 'count' : `measures.${s.key}` // dim sort handled differently
      const dimMatch = dimsSchema.find((d) => d.outputKey === s.key)
      const finalPath = dimMatch ? `dims.${s.key}` : path
      sort[finalPath] = s.dir === 'desc' ? -1 : 1
    }
    pipeline.push({ $sort: sort })
  }
  if (opts.limit != null) pipeline.push({ $limit: opts.limit })

  return { mainPipeline: pipeline, topKPipelines, dimsSchema, measuresSchema }
}

function buildTopKPipeline(
  opts: AggregateOptions,
  name: string,
  m: AggregateMeasure,
  match: Document,
): BuiltMongoAggregate['topKPipelines'][number] {
  const pipeline: Document[] = []
  const composedMatch: Document = { ...match }
  if (m.where) {
    Object.assign(composedMatch, translateWhereToMongo(m.where))
  }
  if (Object.keys(composedMatch).length > 0) pipeline.push({ $match: composedMatch })

  // Group by (each dim, value) → count
  const groupId: Document = { value: `$${m.column!}` }
  for (const dim of opts.dimensions) {
    groupId[sanitize(dim.as ?? dim.field)] = dimExpr(dim)
  }
  pipeline.push({ $group: { _id: groupId, count: { $sum: 1 } } })
  pipeline.push({ $sort: { count: -1 } })
  return { measureName: name, pipeline, k: m.k! }
}

function dimExpr(dim: AggregateDimension): unknown {
  const field = `$${dim.field}`
  if (dim.bucket === undefined) return field
  if (typeof dim.bucket === 'string') {
    // Time bucket — emit a canonical string via $dateToString.
    return { $dateToString: { format: timeBucketFormat(dim.bucket), date: field } }
  }
  if (dim.bucket.type === 'numeric') {
    if ('step' in dim.bucket) {
      const step = dim.bucket.step
      return { $multiply: [{ $floor: { $divide: [field, step] } }, step] }
    }
    if ('breaks' in dim.bucket) {
      // CASE-WHEN equivalent via $switch.
      const breaks = dim.bucket.breaks
      const branches: Document[] = []
      for (let i = 0; i < breaks.length - 1; i++) {
        branches.push({
          case: {
            $and: [
              { $gte: [field, breaks[i]] },
              { $lt: [field, breaks[i + 1]] },
            ],
          },
          then: breaks[i],
        })
      }
      return { $switch: { branches, default: null } }
    }
  }
  return field
}

function timeBucketFormat(b: AnalyzeTimeBucket): string {
  switch (b) {
    case 'minute':
      return '%Y-%m-%dT%H:%M'
    case 'hour':
      return '%Y-%m-%dT%H'
    case 'day':
      return '%Y-%m-%d'
    case 'week':
      // Mongo's %V = ISO week number, %G = ISO year
      return '%G-W%V'
    case 'month':
      return '%Y-%m'
    case 'quarter':
      // No native quarter — concatenate manually below.
      return '%Y-%m'
    case 'year':
      return '%Y'
  }
}

function measureAccumulator(m: AggregateMeasure): Document {
  switch (m.agg) {
    case 'count':
      return { $sum: 1 }
    case 'sum':
    case 'rate':
      return { $sum: `$${m.column!}` }
    case 'avg':
      return { $avg: `$${m.column!}` }
    case 'min':
      return { $min: `$${m.column!}` }
    case 'max':
      return { $max: `$${m.column!}` }
    case 'count_distinct':
      // Will be wrapped with $size in $project.
      return { $addToSet: m.column ? `$${m.column}` : null }
    case 'percentile':
      return { $percentile: { input: `$${m.column!}`, p: [m.p!], method: 'approximate' } }
    case 'first':
      return { $first: `$${m.column!}` }
    case 'last':
      return { $last: `$${m.column!}` }
    case 'top_k':
      throw new Error('top_k handled by separate pipeline')
  }
}

function wrapWithCondition(acc: Document, where: Record<string, unknown>): Document {
  // Wrap accumulator so it only counts rows matching `where`. Translate
  // the where to a $cond expression. Supports primitive equality + the
  // $eq/$ne/$gt/$lt/$gte/$lte/$in operators inline.
  const op = Object.keys(acc)[0]!
  const value = acc[op]
  const condExpr = translateWhereToCondExpr(where)
  if (op === '$sum' && value === 1) {
    return { $sum: { $cond: [condExpr, 1, 0] } }
  }
  return { [op]: { $cond: [condExpr, value, op === '$min' || op === '$max' ? null : 0] } }
}

function translateWhereToCondExpr(where: Record<string, unknown>): unknown {
  const ands: unknown[] = []
  for (const [field, expected] of Object.entries(where)) {
    if (expected !== null && typeof expected === 'object' && !Array.isArray(expected)) {
      for (const [op, exp] of Object.entries(expected as Record<string, unknown>)) {
        const fld = `$${field}`
        switch (op) {
          case '$eq':
            ands.push({ $eq: [fld, exp] })
            break
          case '$ne':
            ands.push({ $ne: [fld, exp] })
            break
          case '$gt':
            ands.push({ $gt: [fld, exp] })
            break
          case '$gte':
            ands.push({ $gte: [fld, exp] })
            break
          case '$lt':
            ands.push({ $lt: [fld, exp] })
            break
          case '$lte':
            ands.push({ $lte: [fld, exp] })
            break
          case '$in':
            ands.push({ $in: [fld, exp] })
            break
        }
      }
    } else {
      ands.push({ $eq: [`$${field}`, expected] })
    }
  }
  return ands.length === 1 ? ands[0] : { $and: ands }
}

function translateWhereToMongo(where: Record<string, unknown>): Document {
  // Mongo's query language already speaks the same operator vocabulary,
  // with the same `$eq`/`$ne`/etc. For top-level $and / $or, Mongo uses
  // arrays of subdocuments — same shape we use, so passthrough works.
  return where as Document
}

function translateHavingToMongo(having: Record<string, unknown>): Document {
  // having operates on `count` and `measures.<name>` fields produced
  // by $project. Translate each entry by prefixing measure names.
  const out: Document = {}
  for (const [k, v] of Object.entries(having)) {
    const key = k === 'count' ? 'count' : `measures.${k}`
    out[key] = v
  }
  return out
}

function sanitize(name: string): string {
  return name.replace(/[^a-zA-Z0-9_]/g, '_')
}

/**
 * Decode driver docs into `AggregateRow`. Reused across the main +
 * top-k stitcher.
 */
export function decodeMongoRows(
  mainDocs: Array<Record<string, unknown>>,
  built: BuiltMongoAggregate,
  topKResults: Record<string, Array<Record<string, unknown>>>,
): AggregateRow[] {
  // Index top-k by dim tuple per measure.
  const tkIndex: Record<string, Map<string, Array<{ key: string; count: number }>>> = {}
  for (const tk of built.topKPipelines) {
    const idx = new Map<string, Array<{ key: string; count: number }>>()
    for (const doc of topKResults[tk.measureName] ?? []) {
      const id = doc['_id'] as Record<string, unknown>
      const dimKey = built.dimsSchema.map((d) => String(id[sanitize(d.outputKey)])).join(' ')
      let arr = idx.get(dimKey)
      if (!arr) {
        arr = []
        idx.set(dimKey, arr)
      }
      if (arr.length < tk.k) {
        arr.push({ key: String(id['value']), count: Number(doc['count']) })
      }
    }
    tkIndex[tk.measureName] = idx
  }

  return mainDocs.map((doc) => {
    const dims = (doc['dims'] as Record<string, unknown>) ?? {}
    const measures = (doc['measures'] as Record<string, unknown>) ?? {}
    // Apply dim post-formatting (e.g. quarter)
    const finalDims: Record<string, unknown> = {}
    for (const ds of built.dimsSchema) {
      const raw = dims[ds.outputKey]
      finalDims[ds.outputKey] = postFormatDim(raw, ds.bucket)
    }
    const finalMeasures: Record<string, unknown> = { ...measures }
    // Decode percentile: Mongo's $percentile with p=[0.5] returns [val] — unwrap.
    for (const ms of built.measuresSchema) {
      if (ms.agg === 'percentile' && Array.isArray(finalMeasures[ms.name])) {
        const arr = finalMeasures[ms.name] as unknown[]
        finalMeasures[ms.name] = arr[0] ?? null
      }
    }
    // Stitch top-k.
    const dimKey = built.dimsSchema.map((d) => String(dims[d.outputKey])).join(' ')
    for (const tk of built.topKPipelines) {
      finalMeasures[tk.measureName] = tkIndex[tk.measureName]!.get(dimKey) ?? []
    }
    return {
      dims: finalDims,
      measures: finalMeasures,
      count: Number(doc['count'] ?? 0),
    }
  })
}

function postFormatDim(raw: unknown, bucket: DimensionBucket | undefined): unknown {
  if (raw === null || raw === undefined) return raw
  if (bucket === 'quarter') {
    // Mongo emits 'YYYY-MM' for the quarter bucket placeholder — convert to YYYY-Qn.
    if (typeof raw === 'string') {
      const [y, m] = raw.split('-')
      const monthN = Number(m)
      if (Number.isFinite(monthN)) return `${y}-Q${Math.floor((monthN - 1) / 3) + 1}`
    }
  }
  if (bucket && typeof bucket === 'object' && bucket.type === 'numeric') {
    if ('step' in bucket && typeof raw === 'number') {
      return `${raw}..${raw + bucket.step}`
    }
    if ('breaks' in bucket && typeof raw === 'number') {
      const breaks = bucket.breaks
      for (let i = 0; i < breaks.length - 1; i++) {
        if (raw === breaks[i]) return `${breaks[i]}..${breaks[i + 1]}`
      }
      return null
    }
  }
  if (raw instanceof Date) {
    // Pure Date rows from non-bucketed dim — emit ISO.
    return raw.toISOString()
  }
  return raw
}
