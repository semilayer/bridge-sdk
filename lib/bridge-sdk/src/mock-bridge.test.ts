import { describe, it, expect, beforeAll } from 'vitest'
import { MockBridge } from './mock-bridge.js'
import { createBridgeTestSuite } from './test-suite.js'
import {
  aggregateFixture,
  fixtureToBridgeRows,
  joinChildFixture,
  joinChildFixtureToBridgeRows,
  collectAggregateStream as collect,
} from './aggregate-suite.js'
import type { BridgeRow } from '@semilayer/core'

const seedRows: BridgeRow[] = [
  { id: 1, name: 'Alpha', category: 'a' },
  { id: 2, name: 'Beta', category: 'b' },
  { id: 3, name: 'Gamma', category: 'a' },
  { id: 4, name: 'Delta', category: 'b' },
  { id: 5, name: 'Epsilon', category: 'a' },
]

const FIXTURE_TARGET = 'agg_fixture'
const JOIN_CHILD_TARGET = 'agg_join_child'

function createSeededBridge(): MockBridge {
  const bridge = new MockBridge()
  bridge.seed('items', seedRows)
  bridge.seed(FIXTURE_TARGET, fixtureToBridgeRows(aggregateFixture()))
  bridge.seed(JOIN_CHILD_TARGET, joinChildFixtureToBridgeRows(joinChildFixture()))
  return bridge
}

createBridgeTestSuite({
  factory: () => createSeededBridge(),
  seed: { target: 'items', rows: seedRows, primaryKey: 'id' },
  aggregateFixtureTarget: FIXTURE_TARGET,
  joinChildFixtureTarget: JOIN_CHILD_TARGET,
})

describe('MockBridge extras', () => {
  let bridge: MockBridge

  beforeAll(async () => {
    bridge = createSeededBridge()
    await bridge.connect()
  })

  it('throws when not connected', async () => {
    const b = new MockBridge()
    b.seed('items', seedRows)
    await expect(b.read('items')).rejects.toThrow('not connected')
  })

  it('throws for unseeded target', async () => {
    await expect(bridge.read('nonexistent')).rejects.toThrow('not seeded')
  })

  it('read() with fields picks only requested fields', async () => {
    const result = await bridge.read('items', { fields: ['id', 'name'] })
    for (const row of result.rows) {
      expect(Object.keys(row).sort()).toEqual(['id', 'name'])
    }
  })

  it('query() filters with where clause', async () => {
    const result = await bridge.query('items', { where: { category: 'a' } })
    expect(result.rows).toHaveLength(3)
    expect(result.rows.every((r) => r['category'] === 'a')).toBe(true)
  })

  it('query() sorts descending', async () => {
    const result = await bridge.query('items', {
      orderBy: { field: 'id', dir: 'desc' },
    })
    expect(result.rows[0]!['id']).toBe(5)
    expect(result.rows[4]!['id']).toBe(1)
  })

  it('query() with select picks fields', async () => {
    const result = await bridge.query('items', { select: ['name'] })
    for (const row of result.rows) {
      expect(Object.keys(row)).toEqual(['name'])
    }
  })

  it('query() resolves $or as union', async () => {
    const result = await bridge.query('items', {
      where: { $or: [{ id: 1 }, { id: 5 }] },
    })
    expect(result.rows.map((r) => r['id']).sort()).toEqual([1, 5])
  })

  it('query() resolves $and as intersection', async () => {
    const result = await bridge.query('items', {
      where: { $and: [{ category: 'a' }, { id: { $gt: 1 } }] },
    })
    expect(result.rows.map((r) => r['id']).sort()).toEqual([3, 5])
  })

  it('query() resolves $not against equality', async () => {
    const result = await bridge.query('items', { where: { $not: { category: 'a' } } })
    expect(result.rows.every((r) => r['category'] !== 'a')).toBe(true)
    expect(result.rows.map((r) => r['id']).sort()).toEqual([2, 4])
  })

  it('query() handles nested $or of $and / $not', async () => {
    const result = await bridge.query('items', {
      where: {
        $or: [{ id: 1 }, { $and: [{ category: 'b' }, { $not: { id: 2 } }] }],
      },
    })
    expect(result.rows.map((r) => r['id']).sort()).toEqual([1, 4])
  })

  it('query() $ilike matches with % wildcard, case-insensitive', async () => {
    const result = await bridge.query('items', {
      where: { name: { $ilike: 'al%' } },
    })
    expect(result.rows.map((r) => r['id'])).toEqual([1])
  })

  it('query() $ilike supports _ single-character wildcard', async () => {
    const result = await bridge.query('items', {
      where: { name: { $ilike: 'B_TA' } },
    })
    expect(result.rows.map((r) => r['id'])).toEqual([2])
  })

  it('query() $contains is a literal substring (no wildcards)', async () => {
    const result = await bridge.query('items', {
      where: { name: { $contains: '%' } },
    })
    expect(result.rows).toHaveLength(0)
  })

  it('query() $contains is case-insensitive', async () => {
    const result = await bridge.query('items', {
      where: { name: { $contains: 'PHA' } },
    })
    expect(result.rows.map((r) => r['id'])).toEqual([1])
  })

  it('query() $startsWith is case-insensitive', async () => {
    const result = await bridge.query('items', {
      where: { name: { $startsWith: 'GAM' } },
    })
    expect(result.rows.map((r) => r['id'])).toEqual([3])
  })

  it('query() $endsWith is case-insensitive', async () => {
    const result = await bridge.query('items', {
      where: { name: { $endsWith: 'ON' } },
    })
    expect(result.rows.map((r) => r['id'])).toEqual([5])
  })
})

describe('MockBridge — count(target, options?)', () => {
  let bridge: MockBridge

  beforeAll(async () => {
    bridge = createSeededBridge()
    await bridge.connect()
  })

  it('counts all rows when options omitted', async () => {
    expect(await bridge.count('items')).toBe(seedRows.length)
  })

  it('counts all rows when given empty options', async () => {
    expect(await bridge.count('items', {})).toBe(seedRows.length)
  })

  it('counts all rows when where is empty', async () => {
    expect(await bridge.count('items', { where: {} })).toBe(seedRows.length)
  })

  it('counts rows matching equality', async () => {
    expect(await bridge.count('items', { where: { category: 'a' } })).toBe(3)
  })

  it('counts rows matching $or', async () => {
    expect(
      await bridge.count('items', { where: { $or: [{ id: 1 }, { id: 4 }] } }),
    ).toBe(2)
  })

  it('counts rows matching $ilike (Alpha/Beta/Gamma/Delta but not Epsilon)', async () => {
    expect(await bridge.count('items', { where: { name: { $ilike: '%a%' } } })).toBe(4)
  })
})

describe('MockBridge.aggregate — direct reducer checks', () => {
  let bridge: MockBridge

  beforeAll(async () => {
    bridge = createSeededBridge()
    await bridge.connect()
  })

  it('count over no dims yields one row, count = total', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    expect(rows).toHaveLength(1)
    expect(rows[0]!.measures['c']).toBe(50)
  })

  it('two dims emit nested keys', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [{ field: 'cuisine' }, { field: 'country', as: 'where' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    for (const r of rows) {
      expect('cuisine' in r.dims).toBe(true)
      expect('where' in r.dims).toBe(true)
    }
  })

  it('having filters buckets', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [{ field: 'cuisine' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
        having: { count: { $gte: 100 } },
      }),
    )
    expect(rows).toHaveLength(0)
  })

  it('sort + limit picks largest bucket', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [{ field: 'cuisine' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
        sort: [{ key: 'count', dir: 'desc' }],
        limit: 1,
      }),
    )
    expect(rows).toHaveLength(1)
  })

  it('candidatesWhere narrows pool', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        candidatesWhere: { status: 'published' },
        dimensions: [],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    const drafts = aggregateFixture().filter((r) => r.status === 'draft').length
    expect(rows[0]!.measures['c']).toBe(50 - drafts)
  })

  it('measure-level where narrows just that measure', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [],
        measures: {
          all: { agg: 'count', accuracy: 'exact' },
          pub: { agg: 'count', accuracy: 'exact', where: { status: 'published' } },
        },
      }),
    )
    expect(rows[0]!.measures['all']).toBe(50)
    expect(Number(rows[0]!.measures['pub'])).toBeLessThan(50)
  })

  it('top_k k=2 returns top 2 by frequency', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [],
        measures: { t: { agg: 'top_k', column: 'country', k: 2, accuracy: 'exact' } },
      }),
    )
    const top = rows[0]!.measures['t'] as Array<{ key: string; count: number }>
    expect(top.length).toBeLessThanOrEqual(2)
    expect(top[0]!.count).toBeGreaterThanOrEqual(top[1]?.count ?? 0)
  })

  it('first / last with changeTrackingColumn', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        changeTrackingColumn: 'createdAt',
        dimensions: [],
        measures: {
          first: { agg: 'first', column: 'id', accuracy: 'exact' },
          last: { agg: 'last', column: 'id', accuracy: 'exact' },
        },
      }),
    )
    expect(rows[0]!.measures['first']).not.toBe(rows[0]!.measures['last'])
  })

  it('numeric step bucket emits "lower..upper" strings', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [{ field: 'prepTime', bucket: { type: 'numeric', step: 30 } }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    for (const r of rows) {
      expect(r.dims['prepTime']).toMatch(/^-?\d+\.\.\d+$/)
    }
  })

  it('aggregate stream is consumable by for-await', async () => {
    let n = 0
    for await (const _ of bridge.aggregate({
      target: FIXTURE_TARGET,
      dimensions: [{ field: 'cuisine' }],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
    })) {
      n++
    }
    expect(n).toBeGreaterThan(0)
  })
})

describe('MockBridge.aggregate — joins', () => {
  let bridge: MockBridge

  beforeAll(async () => {
    bridge = createSeededBridge()
    await bridge.connect()
  })

  it('declares joins capability', () => {
    expect(bridge.aggregateCapabilities().joins).toBe(true)
  })

  it('groups by a joined dim with from-alias resolution', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        joins: [
          { target: JOIN_CHILD_TARGET, alias: 'c', kind: 'left', on: { local: 'id', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    const total = rows.reduce((s, r) => s + r.count, 0)
    // 50 parents - 2 unmatched (id=15, id=20 skipped) = 48.
    expect(total).toBe(48)
    const buckets = new Set(rows.map((r) => r.dims['region']))
    expect(buckets.has('EMEA')).toBe(true)
    expect(buckets.has('AMER')).toBe(true)
    expect(buckets.has('APAC')).toBe(true)
  })

  it('combines a base dim with a joined dim', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        joins: [
          { target: JOIN_CHILD_TARGET, alias: 'c', kind: 'left', on: { local: 'id', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'cuisine' }, { field: 'region', from: 'c' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    const total = rows.reduce((s, r) => s + r.count, 0)
    // 50 - 2 unmatched - 1 null cuisine (id=50) = 47.
    expect(total).toBe(47)
    for (const r of rows) {
      expect(r.dims['cuisine']).not.toBeNull()
      expect(r.dims['region']).not.toBeNull()
    }
  })

  it('measure on base + dim on joined sums correctly', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        joins: [
          { target: JOIN_CHILD_TARGET, alias: 'c', kind: 'left', on: { local: 'id', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { s: { agg: 'sum', column: 'views', accuracy: 'exact' } },
      }),
    )
    const fix = aggregateFixture()
    const childPks = new Set(joinChildFixture().map((r) => r.pk))
    const expected = fix.filter((r) => childPks.has(r.id)).reduce((s, r) => s + r.views, 0)
    const observed = rows.reduce((s, r) => s + Number(r.measures['s']), 0)
    expect(observed).toBe(expected)
  })

  it('unmatched parents drop on the joined dim drop-null filter', async () => {
    const noJoinRows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        dimensions: [],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    expect(noJoinRows[0]!.measures['c']).toBe(50)

    const withJoinRows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        joins: [
          { target: JOIN_CHILD_TARGET, alias: 'c', kind: 'left', on: { local: 'id', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    )
    const total = withJoinRows.reduce((s, r) => s + r.count, 0)
    expect(total).toBe(48)
  })

  it('top_k composes with a joined dim', async () => {
    const rows = await collect(
      bridge.aggregate({
        target: FIXTURE_TARGET,
        joins: [
          { target: JOIN_CHILD_TARGET, alias: 'c', kind: 'left', on: { local: 'id', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { t: { agg: 'top_k', column: 'country', k: 2, accuracy: 'exact' } },
      }),
    )
    expect(rows.length).toBeGreaterThan(0)
    for (const r of rows) {
      const top = r.measures['t'] as Array<{ key: string; count: number }>
      expect(top.length).toBeLessThanOrEqual(2)
    }
  })
})
