/**
 * Aggregate operator vocabulary. Mirrors `@semilayer/core`'s
 * `ANALYZE_MEASURE_AGGS` — the strings stay in lockstep until bridge-sdk
 * imports core types directly (deferred until `@semilayer/core@>=0.2.0`
 * publishes the analyze surface).
 */
export type AnalyzeMeasureAgg =
  | 'count'
  | 'count_distinct'
  | 'sum'
  | 'avg'
  | 'min'
  | 'max'
  | 'percentile'
  | 'top_k'
  | 'first'
  | 'last'
  | 'rate'

export type AnalyzeTimeBucket =
  | 'minute'
  | 'hour'
  | 'day'
  | 'week'
  | 'month'
  | 'quarter'
  | 'year'

/**
 * Spatial input columns for a geo bucket. A caller supplies EITHER a
 * `lat` + `lng` pair OR a single combined `geo` column (PostGIS-style
 * geometry / geography or any wire format the dialect's
 * `decodeGeoField` knows how to unpack). When neither is provided the
 * bridge MUST reject at plan time.
 */
export interface GeoBucketFields {
  /** Latitude column on the base target. */
  latField?: string
  /** Longitude column on the base target. */
  lngField?: string
  /**
   * Single combined column on the base target. Bridges with a
   * `decodeGeoField` dialect method unpack this into a (lat, lng) pair
   * the geohash/h3 expression consumes. Bridges without such a method
   * reject `geoField` configs at plan time.
   */
  geoField?: string
}

/**
 * Bucket strategies a dim can request. `geohash` and `h3` push down
 * onto engines that advertise `geohashBucket` / `h3Bucket` in their
 * capabilities; bridges without native encoders fall through to
 * service-side reduce. `semantic` is reserved.
 */
export type DimensionBucket =
  | AnalyzeTimeBucket
  | { type: 'numeric'; step: number }
  | { type: 'numeric'; breaks: number[] }
  | ({ type: 'geohash'; precision: number } & GeoBucketFields)
  | ({ type: 'h3'; resolution: number } & GeoBucketFields)
  | { type: 'semantic'; clusters: number }

/**
 * Per-dimension request: the column to group on, the optional bucket
 * strategy, and an output alias. Bridges emit dim values under
 * `as ?? field` keys on the resulting `AggregateRow.dims`.
 */
export interface AggregateDimension {
  field: string
  bucket?: DimensionBucket
  as?: string
  /**
   * When set, names an `AggregateJoin.alias` from `opts.joins`. The
   * builder reads `field` off the joined table instead of the base
   * target. Omitted means "base target." Only meaningful when
   * `BridgeAggregateCapabilities.joins === true`.
   */
  from?: string
}

/**
 * One join into the aggregate plan. Single-hop only; `belongsTo`
 * cardinality only (LEFT JOIN, at most one child row per parent). The
 * dialect qualifies every reference to a joined column with `alias`.
 *
 * Limits intentionally kept narrow — multi-hop joins, `hasMany` fan-out,
 * and composite-key joins all push the surface beyond what every SQL
 * bridge can express portably; callers that need them stream-reduce.
 */
export interface AggregateJoin {
  /** Child table / collection name as it appears in the engine. */
  target: string
  /**
   * Stable alias the dialect uses when qualifying columns. Bridges MUST
   * accept it from the caller verbatim; the canonical contract is that
   * `AggregateDimension.from === alias` selects this join's target.
   * Aliases are validated as plain identifiers ([A-Za-z_][A-Za-z0-9_]*)
   * by the builder — values that fail the check are rejected before the
   * SQL is shipped to the driver.
   */
  alias: string
  /**
   * Currently `'left'` only. INNER would silently drop parents with no
   * matching child, which most callers don't want; enumerate so that
   * future relaxations stay opt-in.
   */
  kind: 'left'
  /**
   * `{ local: parentColumn, foreign: childColumn }` — exactly one pair.
   * Composite keys are intentionally deferred.
   */
  on: { local: string; foreign: string }
}

/**
 * Per-measure request. `accuracy` is required at the bridge surface — the
 * planner picks `'fast'` for percentile / count_distinct / top_k by default
 * and `'exact'` otherwise, then passes the resolved choice down.
 */
export interface AggregateMeasure {
  agg: AnalyzeMeasureAgg
  column?: string
  /** Required for percentile. 0 < p < 1. */
  p?: number
  /** Required for top_k. */
  k?: number
  accuracy: 'fast' | 'exact'
  /** Per-measure WHERE — composes with `candidatesWhere`. */
  where?: Record<string, unknown>
}

export interface AggregateOptions {
  /** Required — the table / collection / keyspace to scan. */
  target: string
  /**
   * Pre-aggregate row filter. Same operator vocabulary as `query.where`
   * (`$eq` / `$ne` / `$gt` / `$gte` / `$lt` / `$lte` / `$in` / `$nin` /
   * `$and` / `$or` / `$not`). Bridges that already implement `query()`
   * should reuse the WHERE translator.
   */
  candidatesWhere?: Record<string, unknown>
  /** 0 < x ≤ 1 — bridge takes a uniform random sample if it supports sampling. */
  sample?: number
  /** Restrict to a known set of primary-key ids (used by drilldown joins). */
  ids?: string[]
  dimensions: AggregateDimension[]
  measures: Record<string, AggregateMeasure>
  /** Post-aggregate filter on measure values. Same operator vocabulary. */
  having?: Record<string, unknown>
  sort?: Array<{ key: string; dir: 'asc' | 'desc' }>
  limit?: number
  /**
   * Mapped column the bridge should treat as the change-tracking field for
   * `first` / `last` ordering when those measures are requested.
   */
  changeTrackingColumn?: string
  /**
   * Optional table joins applied before GROUP BY. Bridges that don't
   * advertise `joins: true` on capabilities receive `undefined` here —
   * the planner upstream routes those to streaming reduce.
   *
   * Single-hop, `belongsTo` cardinality only — see `AggregateJoin`. The
   * base target is implicitly aliased; a dim with `from = '<alias>'`
   * reads its `field` off that join's child table. `candidatesWhere`
   * and `ids` are NOT auto-qualified — callers that want to filter on
   * a joined column must qualify it themselves (the where-builder
   * forwards keys to the dialect verbatim).
   */
  joins?: AggregateJoin[]
}

/**
 * One bucket emitted by the aggregate stream. `dims` keys correspond to
 * each `AggregateDimension`'s `as ?? field`. `measures` keys correspond to
 * each Record key in `AggregateOptions.measures`. `count` is always
 * populated (it's effectively a free `count(*)` at the same grouping).
 *
 * `sketches` is optional: bridges that compute approximate measures via
 * sketches (e.g. ClickHouse `uniqCombined64`) MAY emit serialized sketch
 * state alongside the final value so the live-tail engine can merge
 * deltas without recomputing.
 */
export interface AggregateRow {
  dims: Record<string, unknown>
  measures: Record<string, unknown>
  count: number
  sketches?: Record<string, unknown>
}

/**
 * What the bridge can natively push down. The planner inspects this and
 * picks pushdown / hybrid / streaming. Any operator a bridge cannot
 * declare as supported falls through to service-side reduce.
 *
 * `supports: false` short-circuits — the planner skips `aggregate()`
 * entirely and uses the streaming-reduce path on top of `read()`.
 */
export interface BridgeAggregateCapabilities {
  /** Master flag — false means "don't even try to call aggregate()". */
  supports: boolean
  groupBy: boolean
  /**
   * `true` = every `AnalyzeTimeBucket`; `false` = none; an array enumerates
   * exactly which buckets the bridge supports.
   */
  timeBucket: boolean | AnalyzeTimeBucket[]
  numericBucket: boolean
  /**
   * Coarse geo flag retained for backwards compatibility — the resolved
   * value is `geohashBucket || h3Bucket`. Callers that care which
   * encoding the bridge supports should read those finer flags
   * directly. New bridges should set all three consistently.
   */
  geoBucket: boolean
  /**
   * True when `{ type: 'geohash', precision, ... }` dims push down via
   * a native engine encoder (e.g. `ST_GeoHash`, `geohashEncode`). When
   * `false`, the planner upstream falls back to streaming reduce for
   * any geohash dim.
   */
  geohashBucket: boolean
  /**
   * True when `{ type: 'h3', resolution, ... }` dims push down via a
   * native engine encoder (only ClickHouse out of the box at time of
   * writing — `geoToH3`).
   */
  h3Bucket: boolean
  count: boolean
  countDistinct: 'exact' | 'approximate' | 'both' | false
  sum: boolean
  avg: boolean
  minMax: boolean
  percentile: 'exact' | 'approximate' | 'both' | false
  topK: boolean
  havingOnAggregates: boolean
  pushdownOrderLimit: boolean
  /** True if `sample` in `AggregateOptions` is honored natively. */
  sampling: boolean
  /** True if `accuracy: 'fast'` measures emit serialized `sketches`. */
  emitsSketches: boolean
  /**
   * True when the bridge can emit a JOIN clause inside `aggregate()`,
   * answering `AggregateOptions.joins` and `AggregateDimension.from`
   * without falling back. SQL bridges generally `true`; KV / document
   * bridges and any bridge whose engine has no JOIN concept stay
   * `false`. When `false`, callers passing `opts.joins` should expect
   * the planner upstream to route to streaming reduce instead.
   */
  joins: boolean
}

export const DEFAULT_AGGREGATE_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: false,
  groupBy: false,
  timeBucket: false,
  numericBucket: false,
  geoBucket: false,
  geohashBucket: false,
  h3Bucket: false,
  count: false,
  countDistinct: false,
  sum: false,
  avg: false,
  minMax: false,
  percentile: false,
  topK: false,
  havingOnAggregates: false,
  pushdownOrderLimit: false,
  sampling: false,
  emitsSketches: false,
  joins: false,
}

/**
 * Capabilities a bridge inherits when it delegates `aggregate()` to
 * `streamingAggregate(this, opts)` — every operator is satisfied in-process
 * over rows the bridge yields via `query()` / `read()`. Bridges with no
 * native group-by export this as their `aggregateCapabilities()` so the
 * planner picks the pushdown branch (which lands on the in-bridge reducer)
 * instead of falling through to the service-side streaming path.
 *
 * The win over service-side streaming: the bridge's `query()` (when
 * present) applies `candidatesWhere` at the source, narrowing the row
 * stream before reduce. Service-side streaming uses `read()`, which has
 * no WHERE. For NoSQL bridges with native query DSLs (Mongo $match,
 * Cassandra WHERE on partition keys, DynamoDB query keys, ES filter)
 * this is a real bytes-on-the-wire win.
 */
export const STREAMING_AGGREGATE_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: true,
  groupBy: true,
  timeBucket: true,
  numericBucket: true,
  geoBucket: false,
  geohashBucket: false,
  h3Bucket: false,
  count: true,
  countDistinct: 'exact',
  sum: true,
  avg: true,
  minMax: true,
  percentile: 'exact',
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
  // Streaming-reduce path on top of `query()` / `read()` cannot fan out
  // across two targets safely without per-bridge code (no portable JOIN
  // semantics over plain reads). Bridges that delegate to
  // `streamingAggregate` keep this `false`; bridges that implement their
  // own join enrichment (e.g. MockBridge) override the cap to `true`.
  joins: false,
}

/**
 * Lightweight execution context passed to `aggregate()` — independent of
 * `@semilayer/core`'s wider ExecutionContext to keep this file self-
 * contained. Mirrors what `query()` / `read()` are given today.
 */
export interface BridgeExecutionContext {
  /** Optional ms timeout — bridges should reject after this elapses. */
  timeoutMs?: number
  logger?: {
    info?: (...args: unknown[]) => void
    warn?: (...args: unknown[]) => void
    error?: (...args: unknown[]) => void
  }
}

declare module '@semilayer/core' {
  interface Bridge {
    /**
     * Optional: declare what aggregate operators the bridge can push down.
     * Returning `{ supports: false, ... }` (or omitting the method) tells
     * the planner to use streaming-reduce.
     */
    aggregateCapabilities?(): BridgeAggregateCapabilities

    /**
     * Optional: execute a pushdown aggregate. Yields `AggregateRow` per
     * bucket, in `sort` / `limit` order if the bridge supports those —
     * service reorders / clamps post-hoc otherwise.
     *
     * The `opts.candidatesWhere` predicate is the resolved post-RBAC where
     * clause. Bridges should treat it identically to `query()`'s where.
     */
    aggregate?(
      opts: AggregateOptions,
      ctx?: BridgeExecutionContext,
    ): AsyncIterable<AggregateRow>
  }
}
