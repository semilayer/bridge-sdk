import { describe, it, expect } from 'vitest'
import { buildWhereSql, assertSupportedOps, type WhereSqlDialect } from './sql-where.js'
import { UnsupportedOperatorError } from './errors.js'
import type { WhereClause } from '@semilayer/core'

const PG: WhereSqlDialect = {
  quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
  placeholder: (i) => `$${i}`,
}

const MYSQL: WhereSqlDialect = {
  quoteIdent: (n) => '`' + n.replace(/`/g, '``') + '`',
  placeholder: () => '?',
  ilike: (col, p) => `LOWER(${col}) LIKE LOWER(${p})`,
}

const FULL_OPTS = {
  logicalOps: ['or', 'and', 'not'] as const,
  stringOps: ['ilike', 'contains', 'startsWith', 'endsWith'] as const,
}

describe('buildWhereSql — basics', () => {
  it('emits empty SQL for an empty where', () => {
    const r = buildWhereSql({}, PG, FULL_OPTS)
    expect(r.sql).toBe('')
    expect(r.params).toEqual([])
  })

  it('emits empty SQL when where is undefined', () => {
    const r = buildWhereSql(undefined, PG, FULL_OPTS)
    expect(r.sql).toBe('')
  })

  it('emits bare-value equality', () => {
    const r = buildWhereSql({ status: 'active' }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"status" = $1`)
    expect(r.params).toEqual(['active'])
  })

  it('emits IS NULL for bare null', () => {
    const r = buildWhereSql({ archived_at: null }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"archived_at" IS NULL`)
    expect(r.params).toEqual([])
  })

  it('emits comparison ops with shifting placeholder slots', () => {
    const r = buildWhereSql(
      { price: { $gte: 10, $lt: 100 } },
      PG,
      FULL_OPTS,
    )
    expect(r.sql).toBe(`("price" >= $1) AND ("price" < $2)`)
    expect(r.params).toEqual([10, 100])
  })

  it('emits $in with one placeholder per value', () => {
    const r = buildWhereSql({ status: { $in: ['active', 'past_due'] } }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"status" IN ($1, $2)`)
    expect(r.params).toEqual(['active', 'past_due'])
  })

  it('emits 1=0 for empty $in (matches no rows)', () => {
    const r = buildWhereSql({ status: { $in: [] } }, PG, FULL_OPTS)
    expect(r.sql).toBe('1=0')
    expect(r.params).toEqual([])
  })

  it('treats bare array as $in shorthand', () => {
    const r = buildWhereSql({ id: [1, 2, 3] }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"id" IN ($1, $2, $3)`)
    expect(r.params).toEqual([1, 2, 3])
  })

  it('emits IS NOT NULL for $ne null', () => {
    const r = buildWhereSql({ archived_at: { $ne: null } }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"archived_at" IS NOT NULL`)
    expect(r.params).toEqual([])
  })
})

describe('buildWhereSql — logical combinators', () => {
  it('emits OR with parenthesized clauses', () => {
    const where: WhereClause = { $or: [{ a: 1 }, { b: 2 }] }
    const r = buildWhereSql(where, PG, FULL_OPTS)
    expect(r.sql).toBe(`(("a" = $1) OR ("b" = $2))`)
    expect(r.params).toEqual([1, 2])
  })

  it('emits AND', () => {
    const where: WhereClause = { $and: [{ a: 1 }, { b: 2 }] }
    const r = buildWhereSql(where, PG, FULL_OPTS)
    expect(r.sql).toBe(`(("a" = $1) AND ("b" = $2))`)
    expect(r.params).toEqual([1, 2])
  })

  it('emits NOT', () => {
    const where: WhereClause = { $not: { a: 1 } }
    const r = buildWhereSql(where, PG, FULL_OPTS)
    expect(r.sql).toBe(`NOT ("a" = $1)`)
    expect(r.params).toEqual([1])
  })

  it('emits nested combinators with stable parameter ordering', () => {
    const where: WhereClause = {
      $or: [{ a: 1 }, { $and: [{ b: 2 }, { $not: { c: 3 } }] }],
    }
    const r = buildWhereSql(where, PG, FULL_OPTS)
    expect(r.params).toEqual([1, 2, 3])
    expect(r.sql).toContain('OR')
    expect(r.sql).toContain('AND')
    expect(r.sql).toContain('NOT')
  })

  it('throws UnsupportedOperatorError when logical op is not declared', () => {
    expect(() =>
      buildWhereSql({ $or: [{ a: 1 }] }, PG, {
        logicalOps: [],
        stringOps: ['ilike'],
        bridge: '@semilayer/bridge-test',
        target: 'orders',
      }),
    ).toThrow(UnsupportedOperatorError)
  })

  it('error carries op + bridge + target', () => {
    try {
      buildWhereSql({ $not: { a: 1 } }, PG, {
        logicalOps: ['or', 'and'],
        stringOps: [],
        bridge: '@semilayer/bridge-test',
        target: 'orders',
      })
    } catch (err) {
      expect(err).toBeInstanceOf(UnsupportedOperatorError)
      const e = err as UnsupportedOperatorError
      expect(e.op).toBe('$not')
      expect(e.bridge).toBe('@semilayer/bridge-test')
      expect(e.target).toBe('orders')
    }
  })
})

describe('buildWhereSql — string operators', () => {
  it('emits ILIKE with escape clause', () => {
    const r = buildWhereSql({ title: { $ilike: 'pasta%' } }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"title" ILIKE $1 ESCAPE '\\'`)
    expect(r.params).toEqual(['pasta%'])
  })

  it('emits LOWER/LOWER for dialects without ILIKE', () => {
    const r = buildWhereSql({ title: { $ilike: 'pasta%' } }, MYSQL, FULL_OPTS)
    expect(r.sql).toBe("LOWER(`title`) LIKE LOWER(?) ESCAPE '\\'")
    expect(r.params).toEqual(['pasta%'])
  })

  it('$contains escapes user wildcards in the bound parameter', () => {
    const r = buildWhereSql({ title: { $contains: '50%' } }, PG, FULL_OPTS)
    expect(r.sql).toBe(`"title" ILIKE $1 ESCAPE '\\'`)
    expect(r.params).toEqual(['%50\\%%'])
  })

  it('$startsWith binds prefix + %', () => {
    const r = buildWhereSql({ title: { $startsWith: 'pa' } }, PG, FULL_OPTS)
    expect(r.params).toEqual(['pa%'])
  })

  it('$endsWith binds % + suffix', () => {
    const r = buildWhereSql({ title: { $endsWith: 'sta' } }, PG, FULL_OPTS)
    expect(r.params).toEqual(['%sta'])
  })

  it('throws when a string op is not declared', () => {
    expect(() =>
      buildWhereSql({ title: { $ilike: 'p%' } }, PG, {
        logicalOps: [],
        stringOps: [],
      }),
    ).toThrow(UnsupportedOperatorError)
  })

  it('emits 1=0 for non-string $contains payload', () => {
    const r = buildWhereSql({ price: { $contains: 5 as unknown as string } }, PG, FULL_OPTS)
    expect(r.sql).toBe('1=0')
  })
})

describe('buildWhereSql — Postgres ARRAY param mode', () => {
  const PG_ANY: WhereSqlDialect = {
    quoteIdent: (n) => `"${n.replace(/"/g, '""')}"`,
    placeholder: (i) => `$${i}`,
    inUsesArrayParam: true,
    inList: (col, [ph]) => `${col} = ANY(${ph})`,
    notInList: (col, [ph]) => `${col} <> ALL(${ph})`,
  }

  it('packs $in values into a single array param', () => {
    const r = buildWhereSql({ id: { $in: [1, 2, 3] } }, PG_ANY, FULL_OPTS)
    expect(r.sql).toBe(`"id" = ANY($1)`)
    expect(r.params).toEqual([[1, 2, 3]])
  })

  it('packs $nin values into a single array param with <> ALL', () => {
    const r = buildWhereSql({ id: { $nin: [1, 2] } }, PG_ANY, FULL_OPTS)
    expect(r.sql).toBe(`"id" <> ALL($1)`)
    expect(r.params).toEqual([[1, 2]])
  })
})

describe('assertSupportedOps', () => {
  it('returns silently when where is undefined', () => {
    expect(() => assertSupportedOps(undefined, { logicalOps: [], stringOps: [] })).not.toThrow()
  })

  it('returns silently when only comparison ops are used', () => {
    expect(() =>
      assertSupportedOps({ a: 1, b: { $gt: 2 }, c: { $in: [1, 2] } }, { logicalOps: [], stringOps: [] }),
    ).not.toThrow()
  })

  it('throws on $or when not declared', () => {
    expect(() =>
      assertSupportedOps({ $or: [{ a: 1 }] }, { logicalOps: [], stringOps: [] }),
    ).toThrow(UnsupportedOperatorError)
  })

  it('throws on nested $or even when outer is fine', () => {
    expect(() =>
      assertSupportedOps(
        { x: 1, $and: [{ $or: [{ a: 1 }] }] },
        { logicalOps: ['and'], stringOps: [] },
      ),
    ).toThrow(UnsupportedOperatorError)
  })

  it('throws on $ilike when not declared', () => {
    expect(() =>
      assertSupportedOps({ name: { $ilike: 'p%' } }, { logicalOps: [], stringOps: [] }),
    ).toThrow(UnsupportedOperatorError)
  })

  it('passes through when ops are declared', () => {
    expect(() =>
      assertSupportedOps(
        { $or: [{ a: 1 }, { name: { $contains: 'x' } }] },
        { logicalOps: ['or'], stringOps: ['contains'] },
      ),
    ).not.toThrow()
  })
})

describe('buildWhereSql — slot continuation', () => {
  it('respects startSlot for callers interleaving other params', () => {
    const r = buildWhereSql({ a: 1 }, PG, { ...FULL_OPTS, startSlot: 5 })
    expect(r.sql).toBe(`"a" = $5`)
    expect(r.nextSlot).toBe(6)
  })
})
