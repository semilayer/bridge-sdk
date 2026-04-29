/**
 * Shared SQL `where`-builder for SQL-family bridges.
 *
 * Every SQL bridge in this monorepo (postgres, mysql, sqlite, mssql,
 * snowflake, etc.) ends up writing the same recursive-where translator
 * with minor dialect tweaks: `$N` vs `?` placeholders, `ILIKE` vs
 * `LOWER(col) LIKE LOWER(?)`, `col = ANY($1)` vs `col IN (?, ?, ?)`.
 * Centralizing it means one tested implementation, one place to fix
 * three-valued-logic edge cases, and a uniform `UnsupportedOperatorError`
 * shape across the ecosystem.
 *
 * Capability-gating: callers pass in the operator subset their bridge
 * declares (`whereLogicalOps` / `whereStringOps` from `BridgeCapabilities`),
 * and any operator outside that set causes the helper to throw an
 * `UnsupportedOperatorError`. Bridges should pass the same arrays they
 * advertise on `bridge.capabilities`, so the SQL builder and the
 * compliance suite agree on what's supported.
 */
import type {
  WhereClause,
  WhereLogicalOp,
  WhereStringOp,
} from '@semilayer/core'
import { UnsupportedOperatorError } from './errors.js'

/**
 * Per-dialect knobs the where-builder needs. Most fields are optional and
 * fall back to ANSI SQL defaults — the postgres dialect, for instance,
 * only needs `quoteIdent` + `placeholder` and gets the rest for free.
 */
export interface WhereSqlDialect {
  /** Quote an identifier (column or table). */
  quoteIdent(name: string): string
  /**
   * Build a placeholder string for the given 1-indexed slot. Postgres:
   * `$1`, `$2`, ... ; MySQL/SQLite: always `?` (slot ignored); MSSQL:
   * `@p1`, `@p2`, ... ; Oracle: `:1`, `:2`, ...
   */
  placeholder(slot: number): string

  /**
   * SQL for `$ilike`. Default: `${col} ILIKE ${param} ESCAPE '\\'`.
   * Override when the dialect lacks ILIKE — e.g. MySQL would use
   * `LOWER(${col}) LIKE LOWER(${param}) ESCAPE '\\'`. The `param`
   * placeholder is bound to the user-supplied pattern verbatim (caller
   * is responsible for `%`/`_` semantics on `$ilike`).
   */
  ilike?(colSql: string, paramSql: string): string
  /**
   * SQL for `$contains` / `$startsWith` / `$endsWith`. The helper has
   * already wrapped the user value in `%`s and escaped any literal
   * `%` / `_` so the bound parameter is a safe LIKE pattern.
   * Default: same as `ilike`.
   */
  containsLike?(colSql: string, paramSql: string): string

  /**
   * Emit `$in` over a list. `placeholders` is one entry per value — the
   * helper has already pushed the values onto `params`. Default:
   * `${col} IN (${placeholders.join(', ')})`. Postgres bridges can
   * override to `${col} = ANY(${arrayParam})` and use the array form.
   */
  inList?(colSql: string, placeholders: string[]): string

  /**
   * Emit `$nin` over a list. Default: `${col} NOT IN (${placeholders.join(', ')})`.
   */
  notInList?(colSql: string, placeholders: string[]): string

  /**
   * Surround a sub-expression with negation. Default: `NOT (${inner})`.
   * Bridges that want NULL-tolerant negation (matching MockBridge JS
   * semantics where `NOT(col=1)` returns rows with `col=NULL`) override
   * to e.g. `(NOT (${inner}) OR ...IS NULL)`. The default ANSI behavior
   * is documented and is what most callers expect.
   */
  notExpr?(innerSql: string): string

  /**
   * Whether the dialect supports `LIKE pattern ESCAPE '\\'`. Most do;
   * BigQuery is a notable exception (use `LIKE` only). Default true.
   */
  supportsLikeEscape?: boolean

  /**
   * When true, `$in` / `$nin` push the entire values array as a single
   * parameter and the dialect's `inList` / `notInList` is called with a
   * one-element placeholder array. Postgres uses this with
   * `${col} = ANY(${ph})` (and `<> ALL(${ph})` for nin), which is faster
   * than expanding to N placeholders for large lists. Default false.
   */
  inUsesArrayParam?: boolean
}

export interface BuildWhereOptions {
  /** Logical ops the bridge declares supported. Empty array = bridge throws on `$or`/`$and`/`$not`. */
  logicalOps: ReadonlyArray<WhereLogicalOp>
  /** String ops the bridge declares supported. Empty array = bridge throws on `$ilike` etc. */
  stringOps: ReadonlyArray<WhereStringOp>
  /** Bridge package name, included on `UnsupportedOperatorError`. */
  bridge?: string
  /** Target name, included on `UnsupportedOperatorError`. */
  target?: string
  /** First parameter slot. Defaults to 1 — bridges that interleave WHERE params with other params (e.g. cursor params on `read`) can shift this. */
  startSlot?: number
}

export interface BuiltWhereSql {
  /**
   * The boolean expression — empty string when the where clause is empty
   * or only contains keys whose values resolve to no-ops. No leading
   * `WHERE`; callers prepend it if non-empty.
   */
  sql: string
  /** Bound parameters in order. */
  params: unknown[]
  /** Final slot reached (next slot to use after the where). */
  nextSlot: number
}

const COMPARISON_OPS = new Set(['$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin'])
const STRING_OP_KEY: Record<string, WhereStringOp> = {
  $ilike: 'ilike',
  $contains: 'contains',
  $startsWith: 'startsWith',
  $endsWith: 'endsWith',
}

export function buildWhereSql(
  where: WhereClause | undefined,
  dialect: WhereSqlDialect,
  opts: BuildWhereOptions,
): BuiltWhereSql {
  const params: unknown[] = []
  const state = { slot: opts.startSlot ?? 1 }
  const sql = where ? compileClause(where, dialect, opts, params, state) : ''
  return { sql, params, nextSlot: state.slot }
}

function compileClause(
  where: WhereClause,
  dialect: WhereSqlDialect,
  opts: BuildWhereOptions,
  params: unknown[],
  state: { slot: number },
): string {
  const entries = Object.entries(where as Record<string, unknown>)
  if (entries.length === 0) return ''
  const parts: string[] = []
  for (const [key, value] of entries) {
    if (key === '$or') {
      assertLogicalOp('or', opts)
      const arr = value as WhereClause[]
      if (!Array.isArray(arr) || arr.length === 0) continue
      const sub = arr.map((c) => compileClause(c, dialect, opts, params, state)).filter(Boolean)
      if (sub.length === 0) continue
      parts.push(sub.length === 1 ? sub[0]! : `(${sub.map(parens).join(' OR ')})`)
      continue
    }
    if (key === '$and') {
      assertLogicalOp('and', opts)
      const arr = value as WhereClause[]
      if (!Array.isArray(arr) || arr.length === 0) continue
      const sub = arr.map((c) => compileClause(c, dialect, opts, params, state)).filter(Boolean)
      if (sub.length === 0) continue
      parts.push(sub.length === 1 ? sub[0]! : `(${sub.map(parens).join(' AND ')})`)
      continue
    }
    if (key === '$not') {
      assertLogicalOp('not', opts)
      const inner = compileClause(value as WhereClause, dialect, opts, params, state)
      if (!inner) continue
      const not = dialect.notExpr ?? defaultNotExpr
      parts.push(not(parens(inner)))
      continue
    }
    parts.push(compileFieldClause(key, value, dialect, opts, params, state))
  }
  return parts.length === 1 ? parts[0]! : parts.map(parens).join(' AND ')
}

function compileFieldClause(
  field: string,
  rawValue: unknown,
  dialect: WhereSqlDialect,
  opts: BuildWhereOptions,
  params: unknown[],
  state: { slot: number },
): string {
  const col = dialect.quoteIdent(field)

  // Bare value = $eq.
  if (rawValue === null) {
    return `${col} IS NULL`
  }
  if (rawValue instanceof Date || typeof rawValue !== 'object' || Array.isArray(rawValue)) {
    if (Array.isArray(rawValue)) {
      // Bare array = $in shorthand (back-compat with existing callsites).
      return emitIn(col, rawValue as unknown[], dialect, params, state)
    }
    const ph = dialect.placeholder(state.slot++)
    params.push(rawValue)
    return `${col} = ${ph}`
  }

  const ops = rawValue as Record<string, unknown>
  const subParts: string[] = []
  for (const [op, opVal] of Object.entries(ops)) {
    if (COMPARISON_OPS.has(op)) {
      subParts.push(emitComparison(col, op, opVal, dialect, params, state))
      continue
    }
    if (op in STRING_OP_KEY) {
      const stringOp = STRING_OP_KEY[op]!
      assertStringOp(stringOp, opts)
      subParts.push(emitStringOp(col, op, opVal, dialect, params, state))
      continue
    }
    throw new UnsupportedOperatorError({
      op,
      bridge: opts.bridge,
      target: opts.target,
    })
  }
  if (subParts.length === 0) return ''
  return subParts.length === 1 ? subParts[0]! : subParts.map(parens).join(' AND ')
}

function emitComparison(
  col: string,
  op: string,
  value: unknown,
  dialect: WhereSqlDialect,
  params: unknown[],
  state: { slot: number },
): string {
  switch (op) {
    case '$eq': {
      if (value === null) return `${col} IS NULL`
      const ph = dialect.placeholder(state.slot++)
      params.push(value)
      return `${col} = ${ph}`
    }
    case '$ne': {
      if (value === null) return `${col} IS NOT NULL`
      const ph = dialect.placeholder(state.slot++)
      params.push(value)
      return `${col} <> ${ph}`
    }
    case '$gt': {
      const ph = dialect.placeholder(state.slot++)
      params.push(value)
      return `${col} > ${ph}`
    }
    case '$gte': {
      const ph = dialect.placeholder(state.slot++)
      params.push(value)
      return `${col} >= ${ph}`
    }
    case '$lt': {
      const ph = dialect.placeholder(state.slot++)
      params.push(value)
      return `${col} < ${ph}`
    }
    case '$lte': {
      const ph = dialect.placeholder(state.slot++)
      params.push(value)
      return `${col} <= ${ph}`
    }
    case '$in':
      return emitIn(col, asArray(value), dialect, params, state)
    case '$nin':
      return emitNotIn(col, asArray(value), dialect, params, state)
    default:
      throw new Error(`Unhandled comparison op "${op}"`)
  }
}

function emitStringOp(
  col: string,
  op: string,
  value: unknown,
  dialect: WhereSqlDialect,
  params: unknown[],
  state: { slot: number },
): string {
  if (typeof value !== 'string') {
    // Wrong-typed input is a no-op (matches the JS oracle, which returns
    // false for type mismatches rather than blowing up).
    return '1=0'
  }
  const ph = dialect.placeholder(state.slot++)
  // For $ilike the user's `%` / `_` are intentional wildcards. For the
  // other three, escape literals so '50%' matches the literal `50%`.
  let bound: string
  switch (op) {
    case '$ilike':
      bound = value
      break
    case '$contains':
      bound = `%${escapeLikeLiteral(value)}%`
      break
    case '$startsWith':
      bound = `${escapeLikeLiteral(value)}%`
      break
    case '$endsWith':
      bound = `%${escapeLikeLiteral(value)}`
      break
    default:
      throw new Error(`Unhandled string op "${op}"`)
  }
  params.push(bound)
  if (op === '$ilike') {
    const fn = dialect.ilike ?? defaultIlike
    return fn(col, ph) + likeEscape(dialect)
  }
  const fn = dialect.containsLike ?? dialect.ilike ?? defaultIlike
  return fn(col, ph) + likeEscape(dialect)
}

function emitIn(
  col: string,
  values: unknown[],
  dialect: WhereSqlDialect,
  params: unknown[],
  state: { slot: number },
): string {
  if (values.length === 0) return '1=0'
  if (dialect.inUsesArrayParam) {
    const ph = dialect.placeholder(state.slot++)
    params.push(values)
    const fn = dialect.inList ?? defaultInList
    return fn(col, [ph])
  }
  const phs: string[] = []
  for (const v of values) {
    phs.push(dialect.placeholder(state.slot++))
    params.push(v)
  }
  const fn = dialect.inList ?? defaultInList
  return fn(col, phs)
}

function emitNotIn(
  col: string,
  values: unknown[],
  dialect: WhereSqlDialect,
  params: unknown[],
  state: { slot: number },
): string {
  if (values.length === 0) return '1=1'
  if (dialect.inUsesArrayParam) {
    const ph = dialect.placeholder(state.slot++)
    params.push(values)
    const fn = dialect.notInList ?? defaultNotInList
    return fn(col, [ph])
  }
  const phs: string[] = []
  for (const v of values) {
    phs.push(dialect.placeholder(state.slot++))
    params.push(v)
  }
  const fn = dialect.notInList ?? defaultNotInList
  return fn(col, phs)
}

function defaultIlike(colSql: string, paramSql: string): string {
  return `${colSql} ILIKE ${paramSql}`
}

function defaultNotExpr(innerSql: string): string {
  return `NOT ${innerSql}`
}

function defaultInList(colSql: string, placeholders: string[]): string {
  return `${colSql} IN (${placeholders.join(', ')})`
}

function defaultNotInList(colSql: string, placeholders: string[]): string {
  return `${colSql} NOT IN (${placeholders.join(', ')})`
}

function likeEscape(dialect: WhereSqlDialect): string {
  if (dialect.supportsLikeEscape === false) return ''
  return ` ESCAPE '\\'`
}

function escapeLikeLiteral(s: string): string {
  return s.replace(/[\\%_]/g, '\\$&')
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function parens(sql: string): string {
  return sql.startsWith('(') && sql.endsWith(')') ? sql : `(${sql})`
}

function assertLogicalOp(op: WhereLogicalOp, opts: BuildWhereOptions): void {
  if (!opts.logicalOps.includes(op)) {
    throw new UnsupportedOperatorError({
      op: `$${op}`,
      bridge: opts.bridge,
      target: opts.target,
    })
  }
}

function assertStringOp(op: WhereStringOp, opts: BuildWhereOptions): void {
  if (!opts.stringOps.includes(op)) {
    throw new UnsupportedOperatorError({
      op: `$${op}`,
      bridge: opts.bridge,
      target: opts.target,
    })
  }
}

/**
 * Walk a `WhereClause` and throw `UnsupportedOperatorError` if it uses a
 * logical combinator or string operator outside the bridge's declared
 * support set. Useful for bridges that keep their hand-rolled where
 * builders for the existing `$eq`/`$gt`/... family but want to declare
 * `whereLogicalOps: []` / `whereStringOps: []` honestly — calling this
 * once at the top of `query()` / `batchRead()` / `count()` ensures
 * unsupported ops fail loudly with the same error type the rest of the
 * ecosystem uses, rather than the bridge's old generic "Unknown operator"
 * `Error` (which the compliance suite would treat as a regression).
 */
export function assertSupportedOps(
  where: WhereClause | undefined,
  opts: BuildWhereOptions,
): void {
  if (!where) return
  walkClause(where, opts)
}

function walkClause(clause: WhereClause, opts: BuildWhereOptions): void {
  for (const [key, value] of Object.entries(clause as Record<string, unknown>)) {
    if (key === '$or' || key === '$and') {
      const op = key.slice(1) as WhereLogicalOp
      assertLogicalOp(op, opts)
      const arr = value as WhereClause[]
      if (Array.isArray(arr)) for (const sub of arr) walkClause(sub, opts)
      continue
    }
    if (key === '$not') {
      assertLogicalOp('not', opts)
      walkClause(value as WhereClause, opts)
      continue
    }
    if (value && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date)) {
      for (const op of Object.keys(value as Record<string, unknown>)) {
        if (op in STRING_OP_KEY) {
          assertStringOp(STRING_OP_KEY[op]!, opts)
        }
        // Comparison ops + unknown ops are the bridge's existing
        // responsibility; leave them alone.
      }
    }
  }
}
