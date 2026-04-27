/**
 * Pre-baked dialects + capability presets for the most common SQL
 * engines. Each per-bridge package wires `aggregate()` to one of these
 * with at most a couple of overrides. Adding a new SQL bridge to the
 * monorepo is then ~30 lines: import the dialect, declare caps, glue.
 *
 * Why these live in bridge-sdk: dialect objects are small, dependency-
 * free, and the dialect tests run once instead of being duplicated
 * across 15 packages. Bridges can still author bespoke dialects when
 * necessary (e.g. a future TimescaleDB time_bucket override).
 */
import type { BridgeAggregateCapabilities, AnalyzeTimeBucket } from './aggregate.js'
import type { SqlAggregateDialect } from './sql-aggregate.js'

// ─── Capability presets ──────────────────────────────────────────────

export const POSTGRES_FAMILY_CAPABILITIES: BridgeAggregateCapabilities = {
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
  percentile: 'exact',
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

/**
 * CockroachDB — wire-compatible with Postgres but with two relevant
 * differences:
 *   1. `TABLESAMPLE BERNOULLI(...)` is not supported (Cockroach only
 *      accepts `TABLESAMPLE SYSTEM(...)` since v23, and even that has
 *      limitations). Easiest is to declare sampling unsupported and
 *      let the planner reject sampling requests up-front.
 *   2. `percentile_cont(p) WITHIN GROUP (ORDER BY col)` requires the
 *      ORDER BY column to be FLOAT8 — DECIMAL/INT/etc. fail with
 *      `unknown signature: percentile_cont_impl(decimal, int)`. The
 *      `COCKROACH_DIALECT` casts the column to `FLOAT8` so the right
 *      overload resolves.
 */
export const COCKROACH_CAPABILITIES: BridgeAggregateCapabilities = {
  ...POSTGRES_FAMILY_CAPABILITIES,
  sampling: false,
}

/**
 * MySQL 8 / MariaDB 11 / PlanetScale: identifier backticks, `?`
 * placeholders, no percentile_cont as a regular aggregate, no native
 * sampling. Time-bucketing via DATE_FORMAT loses Date type for ORDER BY
 * but the SQL builder applies ORDER BY on the aliased dim column which
 * is fine — the alias is just compared as a string.
 */
export const MYSQL_FAMILY_CAPABILITIES: BridgeAggregateCapabilities = {
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
  percentile: false, // No native percentile_cont aggregate; planner streams.
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: false,
  emitsSketches: false,
}

export const SQLITE_FAMILY_CAPABILITIES: BridgeAggregateCapabilities = {
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
  percentile: false,
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: false,
  emitsSketches: false,
}

export const MSSQL_CAPABILITIES: BridgeAggregateCapabilities = {
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
  // PERCENTILE_CONT in MSSQL is a window function, not a regular
  // aggregate — leaving this false keeps the SQL simple and the
  // planner falls back.
  percentile: false,
  topK: true,
  havingOnAggregates: true,
  // MSSQL has no `LIMIT` — it uses `TOP N` or `OFFSET/FETCH`. Until
  // the dialect grows a `limitClause` hook, declare pushdown
  // unsupported so the planner pulls full results and trims
  // client-side.
  pushdownOrderLimit: false,
  // `TABLESAMPLE (P PERCENT)` in MSSQL samples at the page level,
  // which on small tables routinely returns zero rows. Declare
  // unsupported until we can fall back to a row-level random filter.
  sampling: false,
  emitsSketches: false,
}

export const CLICKHOUSE_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: true,
  groupBy: true,
  timeBucket: true,
  numericBucket: true,
  geoBucket: false,
  count: true,
  countDistinct: 'both', // exact + uniqHLL12 / uniqCombined
  sum: true,
  avg: true,
  minMax: true,
  percentile: 'both', // quantile() is approximate, quantileExact is exact
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

export const BIGQUERY_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: true,
  groupBy: true,
  timeBucket: true,
  numericBucket: true,
  geoBucket: false,
  count: true,
  countDistinct: 'both', // APPROX_COUNT_DISTINCT for fast
  sum: true,
  avg: true,
  minMax: true,
  percentile: 'approximate', // APPROX_QUANTILES
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

export const SNOWFLAKE_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: true,
  groupBy: true,
  timeBucket: true,
  numericBucket: true,
  geoBucket: false,
  count: true,
  countDistinct: 'both', // APPROX_COUNT_DISTINCT
  sum: true,
  avg: true,
  minMax: true,
  percentile: 'exact', // PERCENTILE_CONT
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

export const ORACLE_CAPABILITIES: BridgeAggregateCapabilities = {
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
  percentile: 'exact',
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

export const DUCKDB_CAPABILITIES: BridgeAggregateCapabilities = {
  supports: true,
  groupBy: true,
  timeBucket: true,
  numericBucket: true,
  geoBucket: false,
  count: true,
  countDistinct: 'both',
  sum: true,
  avg: true,
  minMax: true,
  percentile: 'both',
  topK: true,
  havingOnAggregates: true,
  pushdownOrderLimit: true,
  sampling: true,
  emitsSketches: false,
}

// ─── Dialects ────────────────────────────────────────────────────────

/**
 * Postgres / CockroachDB / Neon / Supabase / RedShift-ish: double-quoted
 * idents, `$N` placeholders, `date_trunc`, `percentile_cont`,
 * `TABLESAMPLE BERNOULLI(rate*100)`.
 */
export const POSTGRES_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: (i) => `$${i}`,
  toNumeric: (e) => `(${e})::numeric`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => `date_trunc('${b}', ${e})`,
  percentile: (e, p) => `percentile_cont(${p}) WITHIN GROUP (ORDER BY ${e})`,
  sample: (r) =>
    r > 0 && r < 1 ? `TABLESAMPLE BERNOULLI(${Math.max(0, Math.min(100, r * 100))})` : null,
  supportsFilter: true,
}

/**
 * CockroachDB dialect — wire-compatible with Postgres, but `TABLESAMPLE
 * BERNOULLI(...)` is rejected and `percentile_cont` requires FLOAT8 for
 * the ORDER BY column. See `COCKROACH_CAPABILITIES` for the matching
 * capability declaration.
 */
export const COCKROACH_DIALECT: SqlAggregateDialect = {
  ...POSTGRES_DIALECT,
  // FLOAT8 cast on the ORDER BY column — Cockroach has no
  // percentile_cont overload for decimal/int.
  percentile: (e, p) => `percentile_cont(${p}) WITHIN GROUP (ORDER BY (${e})::FLOAT8)`,
  // No native TABLESAMPLE BERNOULLI — declare unsupported. The planner
  // will reject sampling requests; bridges that *do* want sampling on
  // Cockroach can override this dialect with a custom rewrite.
  sample: () => null,
}

/**
 * MySQL 8 / MariaDB 11 / PlanetScale.
 *
 * Time bucketing: `DATE_FORMAT` returns the canonical bucket string
 * directly (e.g. `'2026-04-27'`) so the SDK's `decodeAggregateRow` finds
 * it already in the expected shape. Cheaper than `DATE_TRUNC` + JS-side
 * formatting and works on every MySQL 5.7+/MariaDB 10+/PlanetScale.
 */
export const MYSQL_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '``') + '`',
  placeholder: () => '?',
  toNumeric: (e) => `CAST(${e} AS DECIMAL(38,10))`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => {
    switch (b) {
      case 'minute':
        return `DATE_FORMAT(${e}, '%Y-%m-%dT%H:%i')`
      case 'hour':
        return `DATE_FORMAT(${e}, '%Y-%m-%dT%H')`
      case 'day':
        return `DATE_FORMAT(${e}, '%Y-%m-%d')`
      case 'week':
        return `CONCAT(YEAR(${e}), '-W', LPAD(WEEK(${e}, 3), 2, '0'))`
      case 'month':
        return `DATE_FORMAT(${e}, '%Y-%m')`
      case 'quarter':
        return `CONCAT(YEAR(${e}), '-Q', QUARTER(${e}))`
      case 'year':
        return `CAST(YEAR(${e}) AS CHAR)`
    }
  },
  percentile: () => {
    throw new Error('MySQL does not support percentile_cont as a regular aggregate')
  },
  // First / last using window subquery is too heavy; emulate via subquery.
  firstLast: (kind, col, ts) => {
    const dir = kind === 'first' ? 'ASC' : 'DESC'
    return `SUBSTRING_INDEX(GROUP_CONCAT(${col} ORDER BY ${ts} ${dir}), ',', 1)`
  },
}

/**
 * SQLite / Turso / Cloudflare D1. SQLite-flavored time bucketing via
 * `strftime`. Numeric div via `CAST(... AS REAL)` since `/` is integer
 * division on integer columns by default.
 */
export const SQLITE_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: () => '?',
  toNumeric: (e) => `CAST(${e} AS REAL)`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => {
    switch (b) {
      case 'minute':
        return `strftime('%Y-%m-%dT%H:%M', ${e})`
      case 'hour':
        return `strftime('%Y-%m-%dT%H', ${e})`
      case 'day':
        return `strftime('%Y-%m-%d', ${e})`
      case 'week':
        // SQLite has no ISO-week formatter; emit "%Y-W%W" as best effort.
        return `strftime('%Y-W%W', ${e})`
      case 'month':
        return `strftime('%Y-%m', ${e})`
      case 'quarter':
        return `(strftime('%Y', ${e}) || '-Q' || ((CAST(strftime('%m', ${e}) AS INTEGER) - 1) / 3 + 1))`
      case 'year':
        return `strftime('%Y', ${e})`
    }
  },
  percentile: () => {
    throw new Error('SQLite has no native percentile_cont')
  },
  firstLast: (kind, col, _ts) => {
    // SQLite has no array_agg or window-aggregate. Fall back: take MIN/MAX
    // of `col` — bridges that need accurate first/last should declare
    // percentile-style fallback. This default is okay when col itself is
    // monotonic with the change-tracking column.
    return kind === 'first' ? `MIN(${col})` : `MAX(${col})`
  },
}

/**
 * SQL Server 2019+. Bracket-quoted idents, `@pN` placeholders, `DATETRUNC`
 * / `FORMAT` for time, `TABLESAMPLE` per-table.
 */
export const MSSQL_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => `[${n.replace(/]/g, ']]')}]`,
  placeholder: (i) => `@p${i}`,
  toNumeric: (e) => `CAST(${e} AS DECIMAL(38,10))`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => {
    switch (b) {
      case 'minute':
        return `FORMAT(${e}, 'yyyy-MM-ddTHH:mm')`
      case 'hour':
        return `FORMAT(${e}, 'yyyy-MM-ddTHH')`
      case 'day':
        return `FORMAT(${e}, 'yyyy-MM-dd')`
      case 'week':
        return `CONCAT(DATEPART(YEAR, ${e}), '-W', RIGHT('0' + CAST(DATEPART(ISO_WEEK, ${e}) AS VARCHAR(2)), 2))`
      case 'month':
        return `FORMAT(${e}, 'yyyy-MM')`
      case 'quarter':
        return `CONCAT(DATEPART(YEAR, ${e}), '-Q', DATEPART(QUARTER, ${e}))`
      case 'year':
        return `CAST(DATEPART(YEAR, ${e}) AS VARCHAR(4))`
    }
  },
  percentile: () => {
    throw new Error('MSSQL percentile_cont is window-only — caps declares percentile=false')
  },
  sample: (r) => (r > 0 && r < 1 ? `TABLESAMPLE (${Math.max(0, Math.min(100, r * 100))} PERCENT)` : null),
}

/**
 * ClickHouse — backtick idents (preferred over double quote because
 * default quote behavior is identifier-as-string in some configs), `?`
 * placeholders (the @clickhouse/client driver substitutes both `{p:Type}`
 * and positional via params), `toStartOfX` time funcs, native `quantile`.
 */
export const CLICKHOUSE_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '``') + '`',
  placeholder: (i) => `{p${i}:String}`,
  toNumeric: (e) => `toFloat64(${e})`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => {
    switch (b) {
      case 'minute':
        return `toStartOfMinute(${e})`
      case 'hour':
        return `toStartOfHour(${e})`
      case 'day':
        return `toStartOfDay(${e})`
      case 'week':
        return `toStartOfWeek(${e}, 1)` // Monday-aligned ISO week
      case 'month':
        return `toStartOfMonth(${e})`
      case 'quarter':
        return `toStartOfQuarter(${e})`
      case 'year':
        return `toStartOfYear(${e})`
    }
  },
  percentile: (e, p) => `quantileExact(${p})(${e})`,
  countDistinct: (col, accuracy) => (accuracy === 'fast' ? `uniqHLL12(${col})` : `uniqExact(${col})`),
  sample: (r) => (r > 0 && r < 1 ? `SAMPLE ${r}` : null),
  firstLast: (kind, col, ts) => (kind === 'first' ? `argMin(${col}, ${ts})` : `argMax(${col}, ${ts})`),
}

/**
 * BigQuery. Backtick idents, `?` placeholders. APPROX_QUANTILES + DATE_TRUNC.
 */
export const BIGQUERY_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '\\`') + '`',
  placeholder: () => '?',
  toNumeric: (e) => `CAST(${e} AS NUMERIC)`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => {
    const part = b.toUpperCase()
    return `TIMESTAMP_TRUNC(${e}, ${part})`
  },
  percentile: (e, p) => `APPROX_QUANTILES(${e}, 100)[OFFSET(${Math.round(p * 100)})]`,
  countDistinct: (col, accuracy) =>
    accuracy === 'fast' ? `APPROX_COUNT_DISTINCT(${col})` : `COUNT(DISTINCT ${col})`,
  sample: (r) => (r > 0 && r < 1 ? `TABLESAMPLE SYSTEM (${Math.max(0, Math.min(100, r * 100))} PERCENT)` : null),
  firstLast: (kind, col, ts) => {
    const dir = kind === 'first' ? 'ASC' : 'DESC'
    return `ARRAY_AGG(${col} ORDER BY ${ts} ${dir} LIMIT 1)[OFFSET(0)]`
  },
}

/**
 * Snowflake. Double-quoted idents, `?` placeholders. PERCENTILE_CONT +
 * APPROX_COUNT_DISTINCT + APPROX_PERCENTILE.
 */
export const SNOWFLAKE_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: () => '?',
  toNumeric: (e) => `CAST(${e} AS NUMBER(38,10))`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => `DATE_TRUNC('${b.toUpperCase()}', ${e})`,
  percentile: (e, p) => `PERCENTILE_CONT(${p}) WITHIN GROUP (ORDER BY ${e})`,
  countDistinct: (col, accuracy) =>
    accuracy === 'fast' ? `APPROX_COUNT_DISTINCT(${col})` : `COUNT(DISTINCT ${col})`,
  sample: (r) => (r > 0 && r < 1 ? `TABLESAMPLE BERNOULLI(${Math.max(0, Math.min(100, r * 100))})` : null),
  supportsFilter: true,
}

/**
 * Oracle 12c+. Double-quoted idents, `:N` placeholders, ANSI SQL.
 */
export const ORACLE_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: (i) => `:${i}`,
  toNumeric: (e) => `TO_NUMBER(${e})`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => {
    const map: Record<AnalyzeTimeBucket, string> = {
      minute: 'MI',
      hour: 'HH',
      day: 'DD',
      week: 'IW',
      month: 'MM',
      quarter: 'Q',
      year: 'YYYY',
    }
    return `TRUNC(${e}, '${map[b]}')`
  },
  percentile: (e, p) => `PERCENTILE_CONT(${p}) WITHIN GROUP (ORDER BY ${e})`,
  sample: (r) => (r > 0 && r < 1 ? `SAMPLE(${Math.max(0, Math.min(100, r * 100))})` : null),
}

/**
 * DuckDB — pretty close to Postgres syntax, `?` placeholders.
 */
export const DUCKDB_DIALECT: SqlAggregateDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: () => '?',
  toNumeric: (e) => `CAST(${e} AS DOUBLE)`,
  timeTrunc: (b: AnalyzeTimeBucket, e) => `date_trunc('${b}', ${e})`,
  percentile: (e, p) => `quantile_cont(${e}, ${p})`,
  sample: (r) => (r > 0 && r < 1 ? `USING SAMPLE ${Math.max(0, Math.min(100, r * 100))}%` : null),
  firstLast: (kind, col, ts) => (kind === 'first' ? `arg_min(${col}, ${ts})` : `arg_max(${col}, ${ts})`),
  supportsFilter: true,
}
