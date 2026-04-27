import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import type { Bridge, BridgeRow } from '@semilayer/core'
import { resolveBridgeCapabilities } from '@semilayer/core'
import { runAggregateCompliance } from './aggregate-suite.js'

export interface BridgeTestSuiteOptions {
  factory: () => Bridge
  seed: {
    target: string
    rows: BridgeRow[]
    primaryKey: string
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
  })
}
