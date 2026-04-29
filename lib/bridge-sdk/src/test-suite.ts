import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import type { Bridge, BridgeRow, WhereClause } from '@semilayer/core'
import { resolveBridgeCapabilities } from '@semilayer/core'
import { runAggregateCompliance } from './aggregate-suite.js'
import { MockBridge } from './mock-bridge.js'
import { UnsupportedOperatorError } from './errors.js'

export interface BridgeTestSuiteOptions {
  factory: () => Bridge
  seed: {
    target: string
    rows: BridgeRow[]
    primaryKey: string
    /**
     * Optional: a string column on the seed rows used by the where
     * compliance suite to exercise `$ilike` / `$contains` /
     * `$startsWith` / `$endsWith`. Default `'name'` — the SDK's
     * MockBridge fixture uses that column. Override when your seed
     * names the column differently.
     */
     stringColumn?: string
  }
  beforeSeed?: (bridge: Bridge) => Promise<void>
  afterCleanup?: (bridge: Bridge) => Promise<void>
  /**
   * Optional: a separate target seeded with the canonical aggregate
   * fixture (`aggregateFixture()`). When provided AND the bridge
   * declares `aggregateCapabilities().supports = true`, the universal
   * 30-case aggregate compliance suite runs against this target.
   *
   * Bridges that can't host the rich fixture (extra columns, dates)
   * may omit this — the aggregate suite is then skipped.
   */
  aggregateFixtureTarget?: string
}

export function createBridgeTestSuite(opts: BridgeTestSuiteOptions): void {
  const { seed } = opts

  describe('Bridge compliance', () => {
    let bridge: Bridge

    beforeAll(async () => {
      bridge = opts.factory()
      if (opts.beforeSeed) await opts.beforeSeed(bridge)
      await bridge.connect()
    })

    afterAll(async () => {
      await bridge.disconnect()
      if (opts.afterCleanup) await opts.afterCleanup(bridge)
    })

    it('connect() resolves without throwing', async () => {
      const b = opts.factory()
      await expect(b.connect()).resolves.toBeUndefined()
      await b.disconnect()
    })

    it('read() returns rows', async () => {
      const result = await bridge.read(seed.target)
      expect(result.rows.length).toBeGreaterThan(0)
    })

    it('read() with limit respects page size', async () => {
      const result = await bridge.read(seed.target, { limit: 2 })
      expect(result.rows.length).toBeLessThanOrEqual(2)
    })

    it('read() with cursor returns next page without overlap', async () => {
      const page1 = await bridge.read(seed.target, { limit: 2 })
      expect(page1.rows.length).toBe(2)
      expect(page1.nextCursor).toBeDefined()

      const page2 = await bridge.read(seed.target, {
        limit: 2,
        cursor: page1.nextCursor,
      })

      const page1Pks = page1.rows.map((r) => r[seed.primaryKey])
      const page2Pks = page2.rows.map((r) => r[seed.primaryKey])
      const overlap = page1Pks.filter((pk) => page2Pks.includes(pk))
      expect(overlap).toHaveLength(0)
    })

    it('read() returns nextCursor undefined on last page', async () => {
      const result = await bridge.read(seed.target, {
        limit: seed.rows.length + 10,
      })
      expect(result.nextCursor).toBeUndefined()
    })

    it('count() returns correct total', async () => {
      const count = await bridge.count(seed.target)
      expect(count).toBe(seed.rows.length)
    })

    it('disconnect() resolves without throwing', async () => {
      const b = opts.factory()
      await b.connect()
      await expect(b.disconnect()).resolves.toBeUndefined()
    })

    describe('query() support', () => {
      it('query() with where filters correctly', async () => {
        if (!bridge.query) return

        const firstRow = seed.rows[0]!
        const pk = seed.primaryKey
        const pkVal = firstRow[pk]

        const result = await bridge.query(seed.target, {
          where: { [pk]: pkVal },
        })
        expect(result.rows.length).toBeGreaterThanOrEqual(1)
        expect(result.rows.every((r) => r[pk] === pkVal)).toBe(true)
      })

      it('query() with orderBy sorts correctly', async () => {
        if (!bridge.query) return

        const result = await bridge.query(seed.target, {
          orderBy: { field: seed.primaryKey, dir: 'asc' },
        })
        for (let i = 1; i < result.rows.length; i++) {
          const prev = result.rows[i - 1]![seed.primaryKey]
          const curr = result.rows[i]![seed.primaryKey]
          expect(prev! <= curr!).toBe(true)
        }
      })

      it('query() with limit + offset paginates', async () => {
        if (!bridge.query) return

        const all = await bridge.query(seed.target, {
          orderBy: { field: seed.primaryKey, dir: 'asc' },
        })
        if (all.rows.length < 3) return

        const page = await bridge.query(seed.target, {
          orderBy: { field: seed.primaryKey, dir: 'asc' },
          limit: 2,
          offset: 1,
        })
        expect(page.rows[0]![seed.primaryKey]).toBe(
          all.rows[1]![seed.primaryKey],
        )
      })
    })

    // ─── batchRead (used by the join planner) ─────────────────────────
    //
    // Bridges that advertise capabilities.batchRead must answer a `$in`
    // filter on the primary key and return matching rows. These tests
    // run only against bridges that declare the capability — bridges
    // that explicitly opt out are skipped rather than failed so we can
    // test graceful-degradation paths against the same harness.

    if (opts.aggregateFixtureTarget) {
      const probe = opts.factory()
      const probeCaps = probe.aggregateCapabilities?.()
      if (probeCaps && probeCaps.supports) {
        runAggregateCompliance({
          getBridge: () => bridge,
          target: opts.aggregateFixtureTarget,
          capabilities: probeCaps,
        })
      }
    }

    describe('batchRead() support', () => {
      it('skips if capabilities.batchRead is false', async () => {
        const caps = resolveBridgeCapabilities(bridge)
        if (!caps.batchRead) {
          // Sanity check: if not declared, the method should not exist
          // either (so callers checking method presence also skip cleanly).
          expect(typeof bridge.batchRead).toBe('undefined')
        }
      })

      it('batchRead() returns rows matching $in', async () => {
        const caps = resolveBridgeCapabilities(bridge)
        if (!caps.batchRead || !bridge.batchRead) return

        const pk = seed.primaryKey
        const ids = seed.rows.slice(0, 3).map((r) => r[pk])
        const rows = await bridge.batchRead(seed.target, {
          where: { [pk]: { $in: ids } },
        })
        expect(rows.length).toBe(ids.length)
        const returned = new Set(rows.map((r) => r[pk]))
        for (const id of ids) expect(returned.has(id)).toBe(true)
      })

      it('batchRead() returns [] for empty $in', async () => {
        const caps = resolveBridgeCapabilities(bridge)
        if (!caps.batchRead || !bridge.batchRead) return

        const rows = await bridge.batchRead(seed.target, {
          where: { [seed.primaryKey]: { $in: [] } },
        })
        expect(rows).toEqual([])
      })

      it('batchRead() drops missing ids without erroring', async () => {
        const caps = resolveBridgeCapabilities(bridge)
        if (!caps.batchRead || !bridge.batchRead) return

        const pk = seed.primaryKey
        const realIds = seed.rows.slice(0, 2).map((r) => r[pk])
        // Mix a real id with two that can't possibly exist.
        const mixed = [...realIds, '__missing_1__', '__missing_2__']
        const rows = await bridge.batchRead(seed.target, {
          where: { [pk]: { $in: mixed } },
        })
        expect(rows.length).toBe(realIds.length)
      })

      it('batchRead() respects select projection', async () => {
        const caps = resolveBridgeCapabilities(bridge)
        if (!caps.batchRead || !bridge.batchRead) return

        const pk = seed.primaryKey
        const ids = [seed.rows[0]![pk]]
        const rows = await bridge.batchRead(seed.target, {
          where: { [pk]: { $in: ids } },
          select: [pk],
        })
        expect(rows.length).toBe(1)
        // With projection, only the pk column should be present. Some bridges
        // (notably NoSQL with rigid row shapes) may still include extra
        // fields — we accept as long as pk round-trips.
        expect(rows[0]![pk]).toBe(ids[0])
      })

      it('batchRead() respects limit cap', async () => {
        const caps = resolveBridgeCapabilities(bridge)
        if (!caps.batchRead || !bridge.batchRead) return

        const pk = seed.primaryKey
        const allIds = seed.rows.map((r) => r[pk])
        if (allIds.length < 3) return
        const rows = await bridge.batchRead(seed.target, {
          where: { [pk]: { $in: allIds } },
          limit: 2,
        })
        expect(rows.length).toBeLessThanOrEqual(2)
      })
    })

    // ─── count(target, { where }) ─────────────────────────────────────
    //
    // The optional `where` predicate on `count()` is new — the old
    // single-arg signature is preserved for back-compat. We only run
    // these when the bridge has declared `exactCount` on its capabilities
    // (which signals new-API awareness). Pre-update bridges that haven't
    // adopted the new interface skip the block; the existing
    // `count() returns correct total` test above still covers them.

    describe('count(target, { where })', () => {
      it('counts all rows when options omitted (back-compat)', async () => {
        const n = await bridge.count(seed.target)
        expect(n).toBe(seed.rows.length)
      })

      it('counts all rows when given empty where', async () => {
        if (!hasNewFlags(bridge)) return
        const n = await bridge.count(seed.target, {})
        expect(n).toBe(seed.rows.length)
      })

      it('counts rows matching a primary-key where', async () => {
        if (!hasNewFlags(bridge)) return
        const pk = seed.primaryKey
        const pkVal = seed.rows[0]![pk]
        const oracle = makeOracle(seed.target, seed.rows)
        const expected = await oracle.count(seed.target, { where: { [pk]: pkVal } })
        const actual = await bridge.count(seed.target, { where: { [pk]: pkVal } })
        const caps = resolveBridgeCapabilities(bridge)
        if (caps.exactCount) {
          expect(actual).toBe(expected)
        } else {
          // Loose tolerance for estimating engines — within ±50% or ±5
          // (whichever is larger) is "in the ballpark" for a small fixture.
          const tol = Math.max(5, Math.ceil(expected * 0.5))
          expect(Math.abs(actual - expected)).toBeLessThanOrEqual(tol)
        }
      })
    })

    // ─── where compliance — logical + string ops ──────────────────────
    //
    // Each test computes the expected row set via MockBridge (the oracle)
    // over the same seed.rows, then asks the bridge under test to run the
    // same query. Tests are gated on `BridgeCapabilities.whereLogicalOps`
    // / `whereStringOps` — bridges that decline an operator skip the
    // body, and a final block asserts that calling a declared-unsupported
    // operator throws `UnsupportedOperatorError` rather than silently
    // returning wrong results.
    //
    // Bridges that haven't adopted the new capability flags at all
    // (pre-update) skip this block entirely — the suite is opt-in via
    // declaration. Once a bridge sets `whereLogicalOps` / `whereStringOps`
    // / `exactCount` in its `capabilities` object, the full suite runs.
    runWhereCompliance({
      describe,
      it,
      expect,
      getBridge: () => bridge,
      seed,
    })
  })
}

/**
 * A bridge has "adopted" the new where + count interface if its declared
 * capabilities include any of the new flags. Used to gate the new
 * compliance blocks during the staged rollout — pre-update bridges keep
 * their existing test surface, post-update bridges get the full suite.
 */
function hasNewFlags(bridge: Bridge): boolean {
  const declared = bridge.capabilities ?? {}
  return (
    'whereLogicalOps' in declared ||
    'whereStringOps' in declared ||
    'exactCount' in declared
  )
}

// ---------------------------------------------------------------------------
// Where compliance — logical + string ops, oracle-driven via MockBridge
// ---------------------------------------------------------------------------

interface WhereComplianceOpts {
  describe: typeof describe
  it: typeof it
  expect: typeof expect
  getBridge: () => Bridge
  seed: BridgeTestSuiteOptions['seed']
}

function runWhereCompliance(opts: WhereComplianceOpts): void {
  const { getBridge, seed } = opts
  const stringColumn = seed.stringColumn ?? 'name'

  opts.describe('where compliance — logical operators', () => {
    opts.it('$or returns union of matching rows', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      if (!caps.whereLogicalOps.includes('or')) {
        await expectThrowsUnsupported(bridge, seed.target, {
          $or: [
            { [seed.primaryKey]: seed.rows[0]![seed.primaryKey] },
            { [seed.primaryKey]: seed.rows[1]![seed.primaryKey] },
          ],
        })
        return
      }
      const where: WhereClause = {
        $or: [
          { [seed.primaryKey]: seed.rows[0]![seed.primaryKey] },
          { [seed.primaryKey]: seed.rows[1]![seed.primaryKey] },
        ],
      }
      await assertSameRows(bridge, seed, where)
    })

    opts.it('$and returns rows satisfying all clauses', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const pk = seed.primaryKey
      const sample = seed.rows[0]!
      const where: WhereClause = {
        $and: [{ [pk]: sample[pk] }, { [pk]: sample[pk] }],
      }
      if (!caps.whereLogicalOps.includes('and')) {
        await expectThrowsUnsupported(bridge, seed.target, where)
        return
      }
      await assertSameRows(bridge, seed, where)
    })

    opts.it('$not returns rows not matching the nested clause', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const pk = seed.primaryKey
      const where: WhereClause = {
        $not: { [pk]: seed.rows[0]![pk] },
      }
      if (!caps.whereLogicalOps.includes('not')) {
        await expectThrowsUnsupported(bridge, seed.target, where)
        return
      }
      await assertSameRows(bridge, seed, where)
    })

    opts.it('nested combinators ($or of $and / $not) match oracle', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const supportsAll =
        caps.whereLogicalOps.includes('or') &&
        caps.whereLogicalOps.includes('and') &&
        caps.whereLogicalOps.includes('not')
      if (!supportsAll) return
      const pk = seed.primaryKey
      const where: WhereClause = {
        $or: [
          { [pk]: seed.rows[0]![pk] },
          { $and: [{ [pk]: seed.rows[1]![pk] }, { $not: { [pk]: seed.rows[0]![pk] } }] },
        ],
      }
      await assertSameRows(bridge, seed, where)
    })
  })

  opts.describe('where compliance — string operators', () => {
    const probeRow = seed.rows.find((r) => typeof r[stringColumn] === 'string')

    opts.it('$ilike matches case-insensitively with % wildcard', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!probeRow) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const value = probeRow[stringColumn] as string
      // First two characters as a prefix probe.
      const prefix = value.slice(0, 2).toUpperCase()
      const where: WhereClause = { [stringColumn]: { $ilike: `${prefix}%` } }
      if (!caps.whereStringOps.includes('ilike')) {
        await expectThrowsUnsupported(bridge, seed.target, where)
        return
      }
      await assertSameRows(bridge, seed, where)
    })

    opts.it('$contains matches case-insensitive substring', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!probeRow) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const value = probeRow[stringColumn] as string
      const middle = value.slice(0, Math.max(1, Math.floor(value.length / 2))).toUpperCase()
      const where: WhereClause = { [stringColumn]: { $contains: middle } }
      if (!caps.whereStringOps.includes('contains')) {
        await expectThrowsUnsupported(bridge, seed.target, where)
        return
      }
      await assertSameRows(bridge, seed, where)
    })

    opts.it('$startsWith matches case-insensitive prefix', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!probeRow) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const value = probeRow[stringColumn] as string
      const prefix = value.slice(0, Math.max(1, Math.floor(value.length / 2))).toUpperCase()
      const where: WhereClause = { [stringColumn]: { $startsWith: prefix } }
      if (!caps.whereStringOps.includes('startsWith')) {
        await expectThrowsUnsupported(bridge, seed.target, where)
        return
      }
      await assertSameRows(bridge, seed, where)
    })

    opts.it('$endsWith matches case-insensitive suffix', async () => {
      const bridge = getBridge()
      if (!bridge.query) return
      if (!probeRow) return
      if (!hasNewFlags(bridge)) return
      const caps = resolveBridgeCapabilities(bridge)
      const value = probeRow[stringColumn] as string
      const suffix = value.slice(-Math.max(1, Math.floor(value.length / 2))).toUpperCase()
      const where: WhereClause = { [stringColumn]: { $endsWith: suffix } }
      if (!caps.whereStringOps.includes('endsWith')) {
        await expectThrowsUnsupported(bridge, seed.target, where)
        return
      }
      await assertSameRows(bridge, seed, where)
    })
  })
}

/**
 * Build an oracle bridge over the same seed rows. MockBridge declares the
 * full operator surface, so its query() result is the canonical truth the
 * bridge under test is compared against.
 */
function makeOracle(target: string, rows: BridgeRow[]): MockBridge {
  const m = new MockBridge()
  m.seed(target, rows)
  void m.connect()
  return m
}

async function assertSameRows(
  bridge: Bridge,
  seed: BridgeTestSuiteOptions['seed'],
  where: WhereClause,
): Promise<void> {
  if (!bridge.query) return
  const oracle = makeOracle(seed.target, seed.rows)
  const expected = await oracle.query(seed.target, { where })
  const actual = await bridge.query(seed.target, { where })
  const expectedPks = new Set(expected.rows.map((r) => r[seed.primaryKey]))
  const actualPks = new Set(actual.rows.map((r) => r[seed.primaryKey]))
  expect(actualPks.size).toBe(expectedPks.size)
  for (const pk of expectedPks) expect(actualPks.has(pk)).toBe(true)
}

async function expectThrowsUnsupported(
  bridge: Bridge,
  target: string,
  where: WhereClause,
): Promise<void> {
  if (!bridge.query) return
  await expect(bridge.query(target, { where })).rejects.toBeInstanceOf(
    UnsupportedOperatorError,
  )
}
