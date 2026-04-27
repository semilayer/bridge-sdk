/**
 * Dialect-aware SQL aggregate generator. SQL bridges call
 * `buildAggregateSql(opts, dialect)` and get back a parameterized
 * `{ sql, params }` pair ready to ship to their driver. The dialect
 * object captures the per-engine differences:
 *
 *   - identifier quoting (`"id"` vs `` `id` `` vs `[id]`)
 *   - parameter placeholders (`$1` Postgres, `?` everywhere else, `@p1` MSSQL)
 *   - `date_trunc` / `DATE_TRUNC` / `toStartOf*` (ClickHouse) etc.
 *   - percentile syntax (`percentile_cont` vs `quantile()` vs `APPROX_PERCENTILE`)
 *   - sampling (`TABLESAMPLE BERNOULLI(p)` vs `SAMPLE p` vs `WHERE rand() < p`)
 *   - top-K shape (separate query in v1 — see §6.3 of the parent plan)
 *
 * Every shaping decision lives in this file so adding a new SQL bridge
 * is just authoring a `SqlAggregateDialect`.
 *
 * What this returns:
 *   - `mainSql` — the GROUP BY query that produces dim_* + m_* + count
 *   - `topKQueries` — zero or more sub-queries needed for top_k measures.
 *     Bridges run them in parallel and stitch in `decodeRow()`.
 *
 * Decoding shape: every dim alias prefixed `dim_<as>`, every measure
 * prefixed `m_<name>`, plus a literal `count` column. `decodeRow()`
 * normalises raw driver output back into `AggregateRow` (handles
 * time-bucket date → ISO string, numeric step → 'lower..upper', etc.).
 */
import type {
  AggregateOptions,
  AggregateMeasure,
  AggregateRow,
  AnalyzeTimeBucket,
  DimensionBucket,
} from './aggregate.js'
import { formatTimeBucket } from './streaming-aggregate.js'

export interface SqlAggregateDialect {
  /** Wrap an identifier in this engine's quotes. */
  quoteIdent(name: string): string
  /**
   * Render a positional placeholder for a 1-indexed parameter slot.
   * Examples: pg `$1`, mysql `?` (idx ignored), mssql `@p1`.
   */
  placeholder(idx: number): string
  /** Cast a numeric expression to a server-side number-friendly type. */
  toNumeric(expr: string): string
  /**
   * Produce a SQL expression that truncates a timestamp to the given
   * bucket boundary (e.g. `date_trunc('day', x)` in PG / `toStartOfDay(x)`
   * in ClickHouse).
   */
  timeTrunc(bucket: AnalyzeTimeBucket, columnExpr: string): string
  /**
   * Numeric step bucket — `FLOOR(col / step) * step`. Default works for
   * most engines; override only if the engine needs explicit casts.
   */
  numericStep?(columnExpr: string, step: number): string
  /**
   * Numeric `breaks` bucket. Many engines have `width_bucket`; engines
   * that don't get a CASE WHEN expression. The dialect emits the
   * canonical "lower..upper" string so we don't need to translate
   * indexes back JS-side.
   */
  numericBreaks?(columnExpr: string, breaks: number[]): string
  /** Percentile expression — receives column + p (0..1). */
  percentile(columnExpr: string, p: number): string
  /** Top-K runs as a separate query. Returns the SQL + params. */
  /**
   * Sampling clause appended after the FROM, e.g. `TABLESAMPLE
   * BERNOULLI(50)`. Returns `null` to skip (sampling unsupported / fall
   * through to client-side rejection).
   */
  sample?(rate: number): string | null
  /**
   * `count_distinct` expression. Default `COUNT(DISTINCT col)`; engines
   * with native HLL (ClickHouse `uniqHLL12`, BigQuery `APPROX_COUNT_DISTINCT`)
   * can override for the `'fast'` accuracy mode.
   */
  countDistinct?(columnExpr: string, accuracy: 'fast' | 'exact'): string
  /** `first` / `last` aggregate — engines differ wildly. */
  firstLast?(
    kind: 'first' | 'last',
    columnExpr: string,
    orderColumnExpr: string,
  ): string
  /**
   * Optional override of the whole top-k query — defaults to
   * `SELECT <dimsExpr...>, <colExpr> AS k_value, COUNT(*) AS k_count
   *   FROM <table> [WHERE ...] GROUP BY <dimAliases>..., <colExpr>
   *   ORDER BY <dimAliases>..., k_count DESC`.
   */

  /**
   * Whether the dialect supports SQL standard `agg() FILTER (WHERE ...)`
   * for measure-level filters (Postgres/Cockroach/Snowflake/DuckDB do;
   * MySQL/SQLite/MSSQL/BigQuery don't). When false, the builder rewrites
   * to `SUM(CASE WHEN ... THEN expr ELSE 0 END)`.
   */
  supportsFilter?: boolean
}

export interface BuiltAggregateSql {
  /** The main GROUP BY query — yields one row per bucket. */
  mainSql: string
  mainParams: unknown[]
  /** One entry per top_k measure (`opts.measures[name].agg === 'top_k'`). */
  topKQueries: Array<{ measureName: string; sql: string; params: unknown[]; column: string; k: number }>
  /**
   * Schema of the columns the bridge will receive on `mainSql`. The
   * driver returns plain rows; `decodeRow` uses this to map back to
   * `AggregateRow.dims` / `AggregateRow.measures`.
   */
  dimsSchema: Array<{ alias: string; outputKey: string; bucket: DimensionBucket | undefined }>
  measuresSchema: Array<{ alias: string; name: string; agg: AggregateMeasure['agg'] }>
}

/**
 * Build a parameterized aggregate SQL plan from `AggregateOptions`.
 * Pure function — does not touch any driver.
 *
 * SQL injection: every identifier flows through `dialect.quoteIdent`
 * which is the bridge's responsibility to escape. Every value flows
 * through `dialect.placeholder` and lands in `params`. Callers must
 * not concatenate untrusted column names into other places.
 */
export function buildAggregateSql(
  opts: AggregateOptions,
  dialect: SqlAggregateDialect,
): BuiltAggregateSql {
  const params: unknown[] = []
  const ph = (): string => dialect.placeholder(params.length + 1)

  // Build dim projections.
  const dimsSchema: BuiltAggregateSql['dimsSchema'] = []
  const dimSelectParts: string[] = []
  const dimGroupParts: string[] = []
  for (const dim of opts.dimensions) {
    const colExpr = dialect.quoteIdent(dim.field)
    const expr = dimExpr(colExpr, dim.bucket, dialect)
    const outputKey = dim.as ?? dim.field
    const alias = `dim_${sanitizeAlias(outputKey)}`
    dimSelectParts.push(`${expr} AS ${dialect.quoteIdent(alias)}`)
    dimGroupParts.push(expr)
    dimsSchema.push({ alias, outputKey, bucket: dim.bucket })
  }

  // Measures.
  const measuresSchema: BuiltAggregateSql['measuresSchema'] = []
  const measureSelectParts: string[] = []
  const topKQueries: BuiltAggregateSql['topKQueries'] = []
  // Track underlying aggregate expressions so HAVING can reference them
  // (SQL standard doesn't allow HAVING to refer to SELECT aliases).
  const measureExprByName: Record<string, string> = {}
  for (const [name, m] of Object.entries(opts.measures)) {
    if (m.agg === 'top_k') {
      // Defer to a separate query — main row carries no value for this measure.
      topKQueries.push(buildTopKQuery(opts, dialect, name, m))
      continue
    }
    const alias = `m_${sanitizeAlias(name)}`
    let projected: string
    if (m.where) {
      const filterSql = whereSql(m.where, dialect, params, ph)
      projected = dialect.supportsFilter
        ? `${measureExpr(m, dialect, opts.changeTrackingColumn)} FILTER (WHERE ${filterSql})`
        : caseWhenFilteredMeasure(m, filterSql, dialect, opts.changeTrackingColumn)
    } else {
      projected = measureExpr(m, dialect, opts.changeTrackingColumn)
    }
    measureSelectParts.push(`${projected} AS ${dialect.quoteIdent(alias)}`)
    measuresSchema.push({ alias, name, agg: m.agg })
    measureExprByName[name] = projected
  }

  // Always project count as a column called "count".
  measureSelectParts.push(`COUNT(*) AS ${dialect.quoteIdent('count')}`)

  const selectSql = [...dimSelectParts, ...measureSelectParts].join(', ')

  // FROM + sampling.
  const fromTable = qualifyTarget(opts.target, dialect)
  let fromSql = `FROM ${fromTable}`
  if (opts.sample != null && opts.sample < 1 && dialect.sample) {
    const clause = dialect.sample(opts.sample)
    if (clause) fromSql = `FROM ${fromTable} ${clause}`
  }

  // WHERE — candidatesWhere + ids + drop-null-dim filter.
  const whereParts: string[] = []
  // Drop rows where any dim resolves to NULL — matches the streaming
  // reducer's behavior (nulls excluded from the group). Without this,
  // SQL bridges emit a bonus bucket per dim with key=NULL.
  for (const dim of opts.dimensions) {
    whereParts.push(`${dialect.quoteIdent(dim.field)} IS NOT NULL`)
    // For numeric breaks, also drop rows that don't fall in any bucket —
    // the dim expression returns NULL for them and they'd group as a
    // single NULL bucket otherwise.
    if (dim.bucket && typeof dim.bucket === 'object' && dim.bucket.type === 'numeric' && 'breaks' in dim.bucket) {
      const breaks = dim.bucket.breaks
      const lo = breaks[0]
      const hi = breaks[breaks.length - 1]
      const numField = dialect.toNumeric(dialect.quoteIdent(dim.field))
      whereParts.push(`${numField} >= ${lo} AND ${numField} < ${hi}`)
    }
  }
  if (opts.candidatesWhere) {
    whereParts.push(whereSql(opts.candidatesWhere, dialect, params, ph))
  }
  if (opts.ids && opts.ids.length > 0) {
    // Same heuristic as streamingAggregate — assume PK column "id". Bridges
    // that key on a different column should pre-translate via
    // `candidatesWhere` before calling buildAggregateSql.
    const ids = opts.ids
    const placeholders: string[] = []
    for (const v of ids) {
      placeholders.push(ph())
      params.push(v)
    }
    whereParts.push(`${dialect.quoteIdent('id')} IN (${placeholders.join(', ')})`)
  }
  const whereSqlClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : ''

  // GROUP BY.
  const groupBySql = dimGroupParts.length > 0 ? `GROUP BY ${dimGroupParts.join(', ')}` : ''

  // HAVING — operates on aliased measure expressions.
  let havingSql = ''
  if (opts.having) {
    havingSql = `HAVING ${havingExpr(opts.having, dialect, params, ph, measuresSchema, measureExprByName)}`
  }

  // ORDER BY.
  let orderBySql = ''
  if (opts.sort && opts.sort.length > 0) {
    const parts: string[] = []
    for (const s of opts.sort) {
      const isCount = s.key === 'count'
      const measure = measuresSchema.find((m) => m.name === s.key)
      const dim = dimsSchema.find((d) => d.outputKey === s.key)
      if (isCount) {
        parts.push(`${dialect.quoteIdent('count')} ${s.dir === 'desc' ? 'DESC' : 'ASC'}`)
      } else if (measure) {
        parts.push(`${dialect.quoteIdent(measure.alias)} ${s.dir === 'desc' ? 'DESC' : 'ASC'}`)
      } else if (dim) {
        parts.push(`${dialect.quoteIdent(dim.alias)} ${s.dir === 'desc' ? 'DESC' : 'ASC'}`)
      }
    }
    if (parts.length > 0) orderBySql = `ORDER BY ${parts.join(', ')}`
  }

  // LIMIT.
  let limitSql = ''
  if (opts.limit != null) {
    limitSql = `LIMIT ${ph()}`
    params.push(opts.limit)
  }

  const mainSql = [
    `SELECT ${selectSql}`,
    fromSql,
    whereSqlClause,
    groupBySql,
    havingSql,
    orderBySql,
    limitSql,
  ]
    .filter(Boolean)
    .join(' ')

  return { mainSql, mainParams: params, topKQueries, dimsSchema, measuresSchema }
}

/**
 * Decode a driver row + (optional) top_k results into an `AggregateRow`.
 * Bridges call this once per row from the main query, after fetching
 * each top_k sub-query result keyed by the same dim tuple.
 */
export function decodeAggregateRow(
  row: Record<string, unknown>,
  dimsSchema: BuiltAggregateSql['dimsSchema'],
  measuresSchema: BuiltAggregateSql['measuresSchema'],
  topKMeasures: Record<string, Array<{ key: string; count: number }>>,
): AggregateRow {
  const dims: Record<string, unknown> = {}
  for (const ds of dimsSchema) {
    const raw = row[ds.alias]
    dims[ds.outputKey] = decodeDimValue(raw, ds.bucket)
  }
  const measures: Record<string, unknown> = {}
  for (const ms of measuresSchema) {
    const raw = row[ms.alias]
    measures[ms.name] = decodeMeasureValue(raw, ms.agg)
  }
  for (const [name, top] of Object.entries(topKMeasures)) {
    measures[name] = top
  }
  const count = Number(row['count'] ?? 0)
  return { dims, measures, count }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

function dimExpr(
  colExpr: string,
  bucket: DimensionBucket | undefined,
  dialect: SqlAggregateDialect,
): string {
  if (bucket === undefined) return colExpr
  if (typeof bucket === 'string') return dialect.timeTrunc(bucket, colExpr)
  if (bucket.type === 'numeric') {
    if ('step' in bucket) {
      if (dialect.numericStep) return dialect.numericStep(colExpr, bucket.step)
      return `(FLOOR(${dialect.toNumeric(colExpr)} / ${bucket.step}) * ${bucket.step})`
    }
    if ('breaks' in bucket) {
      if (dialect.numericBreaks) return dialect.numericBreaks(colExpr, bucket.breaks)
      // Default: CASE expression that yields lower bound (NULL when out of range).
      const breaks = bucket.breaks
      const conds = breaks
        .slice(0, -1)
        .map((lo, i) => {
          const hi = breaks[i + 1]
          return `WHEN ${dialect.toNumeric(colExpr)} >= ${lo} AND ${dialect.toNumeric(colExpr)} < ${hi} THEN ${lo}`
        })
        .join(' ')
      return `(CASE ${conds} ELSE NULL END)`
    }
  }
  // Geo / semantic — emit the raw column; bridges that need real encoding
  // override numericStep/numericBreaks and add a custom branch.
  return colExpr
}

function measureExpr(
  m: AggregateMeasure,
  dialect: SqlAggregateDialect,
  changeTrackingColumn: string | undefined,
): string {
  switch (m.agg) {
    case 'count':
      return 'COUNT(*)'
    case 'sum':
    case 'rate':
      return `SUM(${dialect.quoteIdent(m.column!)})`
    case 'avg':
      return `AVG(${dialect.quoteIdent(m.column!)})`
    case 'min':
      return `MIN(${dialect.quoteIdent(m.column!)})`
    case 'max':
      return `MAX(${dialect.quoteIdent(m.column!)})`
    case 'count_distinct': {
      const col = m.column ? dialect.quoteIdent(m.column) : '*'
      if (dialect.countDistinct) return dialect.countDistinct(col, m.accuracy)
      return `COUNT(DISTINCT ${col})`
    }
    case 'percentile':
      return dialect.percentile(dialect.quoteIdent(m.column!), m.p!)
    case 'first':
    case 'last': {
      const col = dialect.quoteIdent(m.column!)
      const ts = dialect.quoteIdent(changeTrackingColumn ?? m.column!)
      if (dialect.firstLast) return dialect.firstLast(m.agg, col, ts)
      // Portable default — works on PG / Cockroach / Snowflake / BigQuery.
      const dir = m.agg === 'first' ? 'ASC' : 'DESC'
      return `(ARRAY_AGG(${col} ORDER BY ${ts} ${dir}))[1]`
    }
    case 'top_k':
      throw new Error('top_k handled by separate query — should not hit measureExpr')
  }
}

/**
 * Rewrite a measure-with-filter into a CASE-based form for dialects
 * that don't support standard `FILTER (WHERE …)` syntax (MySQL, SQLite,
 * MSSQL, BigQuery, ClickHouse, Oracle). The wrap goes inside the
 * aggregate so it still groups correctly:
 *
 *   COUNT(*)    → COUNT(CASE WHEN <pred> THEN 1 END)
 *   SUM(col)    → SUM(CASE WHEN <pred> THEN col ELSE 0 END)
 *   AVG(col)    → AVG(CASE WHEN <pred> THEN col END)   -- NULLs ignored
 *   MIN/MAX(col)→ MIN/MAX(CASE WHEN <pred> THEN col END)
 *   COUNT(DISTINCT col) → COUNT(DISTINCT CASE WHEN <pred> THEN col END)
 *   percentile  → percentile over CASE WHEN <pred> THEN col END
 *
 * For `first`/`last` we wrap the column with CASE; on dialects that
 * don't support FILTER they typically also don't support ARRAY_AGG, so
 * those bridges should override `dialect.firstLast` to emit the right
 * thing — the wrap is portable enough to drop in.
 */
function caseWhenFilteredMeasure(
  m: AggregateMeasure,
  filterSql: string,
  dialect: SqlAggregateDialect,
  changeTrackingColumn: string | undefined,
): string {
  switch (m.agg) {
    case 'count':
      return `COUNT(CASE WHEN ${filterSql} THEN 1 END)`
    case 'sum':
    case 'rate':
      return `SUM(CASE WHEN ${filterSql} THEN ${dialect.quoteIdent(m.column!)} ELSE 0 END)`
    case 'avg':
      return `AVG(CASE WHEN ${filterSql} THEN ${dialect.quoteIdent(m.column!)} END)`
    case 'min':
      return `MIN(CASE WHEN ${filterSql} THEN ${dialect.quoteIdent(m.column!)} END)`
    case 'max':
      return `MAX(CASE WHEN ${filterSql} THEN ${dialect.quoteIdent(m.column!)} END)`
    case 'count_distinct': {
      const col = m.column ? dialect.quoteIdent(m.column) : '*'
      const wrapped = `CASE WHEN ${filterSql} THEN ${col} END`
      if (dialect.countDistinct) return dialect.countDistinct(wrapped, m.accuracy)
      return `COUNT(DISTINCT ${wrapped})`
    }
    case 'percentile':
      return dialect.percentile(
        `CASE WHEN ${filterSql} THEN ${dialect.quoteIdent(m.column!)} END`,
        m.p!,
      )
    case 'first':
    case 'last': {
      const col = `CASE WHEN ${filterSql} THEN ${dialect.quoteIdent(m.column!)} END`
      const ts = dialect.quoteIdent(changeTrackingColumn ?? m.column!)
      if (dialect.firstLast) return dialect.firstLast(m.agg, col, ts)
      const dir = m.agg === 'first' ? 'ASC' : 'DESC'
      return `(ARRAY_AGG(${col} ORDER BY ${ts} ${dir}))[1]`
    }
    case 'top_k':
      throw new Error('top_k handled by separate query — should not hit caseWhenFilteredMeasure')
  }
}

function buildTopKQuery(
  opts: AggregateOptions,
  dialect: SqlAggregateDialect,
  name: string,
  m: AggregateMeasure,
): BuiltAggregateSql['topKQueries'][number] {
  const params: unknown[] = []
  const ph = (): string => dialect.placeholder(params.length + 1)

  const dimSelectParts: string[] = []
  const dimGroupParts: string[] = []
  for (const dim of opts.dimensions) {
    const colExpr = dialect.quoteIdent(dim.field)
    const expr = dimExpr(colExpr, dim.bucket, dialect)
    const alias = `dim_${sanitizeAlias(dim.as ?? dim.field)}`
    dimSelectParts.push(`${expr} AS ${dialect.quoteIdent(alias)}`)
    dimGroupParts.push(expr)
  }

  const valueAlias = dialect.quoteIdent('k_value')
  const countAlias = dialect.quoteIdent('k_count')
  const valueCol = dialect.quoteIdent(m.column!)

  const fromTable = qualifyTarget(opts.target, dialect)
  const whereParts: string[] = []
  if (opts.candidatesWhere) {
    whereParts.push(whereSql(opts.candidatesWhere, dialect, params, ph))
  }
  if (m.where) {
    whereParts.push(whereSql(m.where, dialect, params, ph))
  }
  const whereClause = whereParts.length > 0 ? `WHERE ${whereParts.join(' AND ')}` : ''

  const groupBy = [...dimGroupParts, valueCol].join(', ')
  const orderByParts: string[] = []
  for (const dim of opts.dimensions) {
    orderByParts.push(dialect.quoteIdent(`dim_${sanitizeAlias(dim.as ?? dim.field)}`))
  }
  orderByParts.push(`${countAlias} DESC`)

  const sql = [
    `SELECT ${[...dimSelectParts, `${valueCol} AS ${valueAlias}`, `COUNT(*) AS ${countAlias}`].join(', ')}`,
    `FROM ${fromTable}`,
    whereClause,
    `GROUP BY ${groupBy}`,
    `ORDER BY ${orderByParts.join(', ')}`,
  ]
    .filter(Boolean)
    .join(' ')

  return { measureName: name, sql, params, column: m.column!, k: m.k! }
}

function whereSql(
  where: Record<string, unknown>,
  dialect: SqlAggregateDialect,
  params: unknown[],
  ph: () => string,
): string {
  const parts: string[] = []
  for (const [field, expected] of Object.entries(where)) {
    if (field === '$and') {
      const arr = expected as Array<Record<string, unknown>>
      const sub = arr.map((s) => `(${whereSql(s, dialect, params, ph)})`).join(' AND ')
      parts.push(`(${sub})`)
      continue
    }
    if (field === '$or') {
      const arr = expected as Array<Record<string, unknown>>
      const sub = arr.map((s) => `(${whereSql(s, dialect, params, ph)})`).join(' OR ')
      parts.push(`(${sub})`)
      continue
    }
    if (field === '$not') {
      parts.push(`NOT (${whereSql(expected as Record<string, unknown>, dialect, params, ph)})`)
      continue
    }
    const col = dialect.quoteIdent(field)
    if (expected !== null && typeof expected === 'object' && !Array.isArray(expected) && !(expected instanceof Date)) {
      const ops = expected as Record<string, unknown>
      for (const [op, exp] of Object.entries(ops)) {
        switch (op) {
          case '$eq':
            parts.push(`${col} = ${ph()}`)
            params.push(exp)
            break
          case '$ne':
            parts.push(`${col} <> ${ph()}`)
            params.push(exp)
            break
          case '$gt':
            parts.push(`${col} > ${ph()}`)
            params.push(exp)
            break
          case '$gte':
            parts.push(`${col} >= ${ph()}`)
            params.push(exp)
            break
          case '$lt':
            parts.push(`${col} < ${ph()}`)
            params.push(exp)
            break
          case '$lte':
            parts.push(`${col} <= ${ph()}`)
            params.push(exp)
            break
          case '$in': {
            const arr = exp as unknown[]
            if (arr.length === 0) {
              parts.push('FALSE')
              break
            }
            const placeholders: string[] = []
            for (const v of arr) {
              placeholders.push(ph())
              params.push(v)
            }
            parts.push(`${col} IN (${placeholders.join(', ')})`)
            break
          }
          case '$nin': {
            const arr = exp as unknown[]
            if (arr.length === 0) {
              parts.push('TRUE')
              break
            }
            const placeholders: string[] = []
            for (const v of arr) {
              placeholders.push(ph())
              params.push(v)
            }
            parts.push(`${col} NOT IN (${placeholders.join(', ')})`)
            break
          }
          default:
            throw new Error(`Unknown operator "${op}" on field "${field}"`)
        }
      }
    } else if (Array.isArray(expected)) {
      if (expected.length === 0) {
        parts.push('FALSE')
      } else {
        const placeholders: string[] = []
        for (const v of expected) {
          placeholders.push(ph())
          params.push(v)
        }
        parts.push(`${col} IN (${placeholders.join(', ')})`)
      }
    } else {
      parts.push(`${col} = ${ph()}`)
      params.push(expected)
    }
  }
  return parts.length > 0 ? parts.join(' AND ') : 'TRUE'
}

function havingExpr(
  having: Record<string, unknown>,
  dialect: SqlAggregateDialect,
  params: unknown[],
  ph: () => string,
  measuresSchema: BuiltAggregateSql['measuresSchema'],
  measureExprByName: Record<string, string>,
): string {
  const parts: string[] = []
  for (const [field, expected] of Object.entries(having)) {
    const measure = measuresSchema.find((m) => m.name === field)
    // HAVING in standard SQL can't reference SELECT aliases — use the
    // underlying aggregate expression instead.
    const colExpr =
      field === 'count'
        ? 'COUNT(*)'
        : measure
          ? measureExprByName[measure.name] ?? dialect.quoteIdent(measure.alias)
          : dialect.quoteIdent(field)
    if (expected !== null && typeof expected === 'object' && !Array.isArray(expected)) {
      const ops = expected as Record<string, unknown>
      for (const [op, exp] of Object.entries(ops)) {
        switch (op) {
          case '$eq':
            parts.push(`${colExpr} = ${ph()}`)
            params.push(exp)
            break
          case '$ne':
            parts.push(`${colExpr} <> ${ph()}`)
            params.push(exp)
            break
          case '$gt':
            parts.push(`${colExpr} > ${ph()}`)
            params.push(exp)
            break
          case '$gte':
            parts.push(`${colExpr} >= ${ph()}`)
            params.push(exp)
            break
          case '$lt':
            parts.push(`${colExpr} < ${ph()}`)
            params.push(exp)
            break
          case '$lte':
            parts.push(`${colExpr} <= ${ph()}`)
            params.push(exp)
            break
          default:
            throw new Error(`Unknown operator "${op}" in having clause`)
        }
      }
    } else {
      parts.push(`${colExpr} = ${ph()}`)
      params.push(expected)
    }
  }
  return parts.join(' AND ')
}

function decodeDimValue(raw: unknown, bucket: DimensionBucket | undefined): unknown {
  if (raw === null || raw === undefined) return null
  if (bucket === undefined) {
    if (raw instanceof Date) return raw.toISOString()
    return raw
  }
  if (typeof bucket === 'string') {
    // Time bucket — driver may return a Date (postgres date_trunc), a
    // string already shaped like the canonical bucket key (mysql
    // DATE_FORMAT, sqlite strftime), or a string parseable as a date.
    if (raw instanceof Date) {
      if (Number.isNaN(raw.getTime())) return null
      return formatTimeBucket(raw, bucket)
    }
    if (typeof raw === 'string') {
      // If the dialect already emitted the canonical key shape, keep
      // it. Otherwise try to parse + format.
      const d = new Date(raw)
      if (Number.isNaN(d.getTime())) {
        // Trust the string — typically already a bucket key like
        // '2026-04-27' or '2026-W17'.
        return raw
      }
      return formatTimeBucket(d, bucket)
    }
    return null
  }
  if (bucket.type === 'numeric') {
    const num = typeof raw === 'number' ? raw : Number(raw)
    if (!Number.isFinite(num)) return null
    if ('step' in bucket) return `${num}..${num + bucket.step}`
    if ('breaks' in bucket) {
      const breaks = bucket.breaks
      for (let i = 0; i < breaks.length - 1; i++) {
        if (num === breaks[i]) return `${breaks[i]}..${breaks[i + 1]}`
      }
      return null
    }
  }
  return raw
}

function decodeMeasureValue(raw: unknown, agg: AggregateMeasure['agg']): unknown {
  if (raw === null || raw === undefined) return null
  switch (agg) {
    case 'count':
    case 'count_distinct':
      return Number(raw)
    case 'sum':
    case 'avg':
    case 'min':
    case 'max':
    case 'percentile':
    case 'rate': {
      const n = typeof raw === 'number' ? raw : Number(raw)
      return Number.isFinite(n) ? n : null
    }
    case 'first':
    case 'last':
      return raw
    case 'top_k':
      return raw
  }
}

function qualifyTarget(target: string, dialect: SqlAggregateDialect): string {
  const parts = target.split('.')
  return parts.map((p) => dialect.quoteIdent(p)).join('.')
}

function sanitizeAlias(name: string): string {
  return name.replace(/[^a-zA-Z0-9_]/g, '_')
}

// ---------------------------------------------------------------------------
// Stitch helper — combine main rows + top_k subquery results into final
// AggregateRow stream. Pure function, callable by every SQL bridge.
// ---------------------------------------------------------------------------

/**
 * Run the main + top_k queries via a bridge-supplied executor and stitch
 * the results into a stream of `AggregateRow`. Most SQL bridge
 * implementations are 5 lines once they implement this:
 *
 * ```ts
 * async *aggregate(opts) {
 *   yield* executeAggregateQueries(
 *     buildAggregateSql(opts, MY_DIALECT),
 *     (sql, params) => this.driver.query(sql, params).then(r => r.rows),
 *   )
 * }
 * ```
 */
export async function* executeAggregateQueries(
  built: BuiltAggregateSql,
  runQuery: (sql: string, params: unknown[]) => Promise<Array<Record<string, unknown>>>,
): AsyncIterable<AggregateRow> {
  const [main, ...tks] = await Promise.all([
    runQuery(built.mainSql, built.mainParams),
    ...built.topKQueries.map((tk) => runQuery(tk.sql, tk.params)),
  ])
  const topKResults: Record<string, Array<Record<string, unknown>>> = {}
  built.topKQueries.forEach((tk, i) => {
    topKResults[tk.measureName] = tks[i] ?? []
  })
  const rows = stitchTopK(main!, built, topKResults)
  for (const r of rows) yield r
}

export function stitchTopK(
  mainRows: Array<Record<string, unknown>>,
  built: BuiltAggregateSql,
  topKResultsByMeasure: Record<string, Array<Record<string, unknown>>>,
): AggregateRow[] {
  // Index top-k results by dim tuple per measure.
  const indexes: Record<string, Map<string, Array<{ key: string; count: number }>>> = {}
  for (const tk of built.topKQueries) {
    const idx = new Map<string, Array<{ key: string; count: number }>>()
    const rows = topKResultsByMeasure[tk.measureName] ?? []
    for (const r of rows) {
      const dimKey = built.dimsSchema.map((d) => String(r[d.alias])).join(' ')
      let arr = idx.get(dimKey)
      if (!arr) {
        arr = []
        idx.set(dimKey, arr)
      }
      if (arr.length < tk.k) {
        arr.push({ key: String(r['k_value']), count: Number(r['k_count']) })
      }
    }
    indexes[tk.measureName] = idx
  }

  return mainRows.map((row) => {
    const dimKey = built.dimsSchema.map((d) => String(row[d.alias])).join(' ')
    const topKMeasures: Record<string, Array<{ key: string; count: number }>> = {}
    for (const tk of built.topKQueries) {
      topKMeasures[tk.measureName] = indexes[tk.measureName]!.get(dimKey) ?? []
    }
    return decodeAggregateRow(row, built.dimsSchema, built.measuresSchema, topKMeasures)
  })
}
