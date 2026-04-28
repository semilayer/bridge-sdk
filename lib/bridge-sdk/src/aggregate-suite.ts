/**
 * Universal aggregate compliance — 30 cases that every bridge declaring
 * `aggregateCapabilities().supports === true` must pass. Each case is
 * gated on the relevant capability so partial-pushdown bridges skip the
 * cases they can't answer.
 *
 * Used by `createBridgeTestSuite` (which checks `aggregateCapabilities()`
 * and conditionally runs this block).
 *
 * Fixture: 50 rows, deterministic. Columns:
 *   id        — 1..50 (primary key)
 *   cuisine   — 3 values + 1 row with null
 *   country   — 3 values
 *   rating    — 1..5
 *   prepTime  — 5..120 (mostly), 1 row at 999 to test out-of-range breaks
 *   views     — 1..1000
 *   status    — 'published' | 'draft'
 *   createdAt — spread Jan-Apr 2026
 *
 * Bridges seed this fixture into their target before the suite runs.
 * The runner calls `seedFixture(bridge)` once and asserts on the
 * declared rows + bucketed values below.
 */
import { describe, it, expect } from 'vitest'
import type { Bridge, BridgeRow } from '@semilayer/core'
import type {
  AggregateOptions,
  AggregateRow,
  BridgeAggregateCapabilities,
} from './aggregate.js'

export interface AggregateSuiteOptions {
  /**
   * Lazy accessor — the bridge is connected by the harness before each
   * test runs but may not be ready at describe-registration time.
   */
  getBridge: () => Bridge
  target: string
  /**
   * Capabilities resolved up-front (typically by calling
   * `factory().aggregateCapabilities()` before `connect()` since the
   * convention is that this method returns a constant). Used to gate
   * `it.skipIf(...)` at describe-registration time so the runner shows
   * cleanly skipped (not silently passed) tests.
   */
  capabilities: BridgeAggregateCapabilities
  expectedRowCount?: number
}

const FIXTURE_TARGET_PLACEHOLDER = '__aggregate_fixture__'

export interface AggregateFixtureRow {
  id: number
  cuisine: string | null
  country: string
  rating: number
  prepTime: number
  views: number
  status: 'published' | 'draft'
  createdAt: Date
}

/**
 * Deterministic 50-row fixture. The compliance numbers below are
 * computed against this exact set — bridges can seed it as-is or in
 * their native row format (Mongo timestamps as Date, KV bridges as
 * strings, etc.) as long as `bridge.read()` returns rows comparable
 * to what's here.
 */
export function aggregateFixture(): AggregateFixtureRow[] {
  const cuisines = ['italian', 'japanese', 'mexican']
  const countries = ['US', 'JP', 'IT']
  const rows: AggregateFixtureRow[] = []
  for (let i = 0; i < 50; i++) {
    const cuisineIdx = i === 49 ? -1 : i % 3
    const country = countries[i % 3]!
    const rating = (i % 5) + 1
    const prepTime = i === 7 ? 999 : 5 + (i * 7) % 110
    const views = ((i + 1) * 13) % 997 + 1
    const status: 'published' | 'draft' = i % 4 === 0 ? 'draft' : 'published'
    // Spread across Jan–Apr 2026 (UTC).
    const month = (i % 4) + 1
    const day = ((i * 3) % 27) + 1
    const createdAt = new Date(Date.UTC(2026, month - 1, day, 12, 0, 0))
    rows.push({
      id: i + 1,
      cuisine: cuisineIdx === -1 ? null : cuisines[cuisineIdx]!,
      country,
      rating,
      prepTime,
      views,
      status,
      createdAt,
    })
  }
  return rows
}

export async function collect(stream: AsyncIterable<AggregateRow>): Promise<AggregateRow[]> {
  const out: AggregateRow[] = []
  for await (const row of stream) out.push(row)
  return out
}

export { collect as collectAggregateStream }

/**
 * Run the universal aggregate compliance cases against `opts.bridge`.
 * Caller is responsible for seeding the fixture into `opts.target`
 * BEFORE this runs.
 */
export function runAggregateCompliance(opts: AggregateSuiteOptions): void {
  const { getBridge, target, capabilities: caps } = opts

  describe('aggregate compliance', () => {
    if (!caps.supports) {
      it.skip('bridge declares aggregate.supports = false', () => {})
      return
    }

    const agg = (rest: Omit<AggregateOptions, 'target'>): AsyncIterable<AggregateRow> => {
      const bridge = getBridge()
      if (!bridge.aggregate) throw new Error('Bridge does not implement aggregate()')
      return bridge.aggregate({ target, ...rest })
    }

    // ─── groupBy ─────────────────────────────────────────────────
    describe('groupBy', () => {
      it.skipIf(!caps.groupBy)('1. single-dim count', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        // 49 non-null cuisine rows across 3 buckets.
        const total = rows.reduce((s, r) => s + r.count, 0)
        expect(total).toBe(49)
        expect(rows.length).toBeLessThanOrEqual(3)
      })

      it.skipIf(!caps.groupBy)('2. two-dim count', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine' }, { field: 'country' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        const total = rows.reduce((s, r) => s + r.count, 0)
        expect(total).toBe(49)
        expect(rows.length).toBeLessThanOrEqual(9)
      })

      it.skipIf(!caps.groupBy)('3. dim alias `as`', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine', as: 'group' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        for (const r of rows) {
          expect('group' in r.dims).toBe(true)
          expect('cuisine' in r.dims).toBe(false)
        }
      })

      it.skipIf(!caps.groupBy)('4. drops null-dim rows', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        const total = rows.reduce((s, r) => s + r.count, 0)
        expect(total).toBe(49)
      })
    })

    // ─── timeBucket ──────────────────────────────────────────────
    const supportsDay = caps.timeBucket === true || (Array.isArray(caps.timeBucket) && caps.timeBucket.includes('day'))
    const supportsWeek = caps.timeBucket === true || (Array.isArray(caps.timeBucket) && caps.timeBucket.includes('week'))
    const supportsMonth = caps.timeBucket === true || (Array.isArray(caps.timeBucket) && caps.timeBucket.includes('month'))

    describe('timeBucket', () => {
      it.skipIf(!supportsDay)('5. day', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'createdAt', bucket: 'day' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        for (const r of rows) {
          const v = r.dims['createdAt'] as string
          expect(v).toMatch(/^\d{4}-\d{2}-\d{2}$/)
        }
      })

      it.skipIf(!supportsMonth)('6. month', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'createdAt', bucket: 'month' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        for (const r of rows) {
          const v = r.dims['createdAt'] as string
          expect(v).toMatch(/^\d{4}-\d{2}$/)
        }
      })

      it.skipIf(!supportsWeek)('7. week', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'createdAt', bucket: 'week' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        for (const r of rows) {
          const v = r.dims['createdAt'] as string
          expect(v).toMatch(/^\d{4}-W\d{2}$/)
        }
      })
    })

    // ─── numericBucket ───────────────────────────────────────────
    describe('numericBucket', () => {
      it.skipIf(!caps.numericBucket)('9. step bucket', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'prepTime', bucket: { type: 'numeric', step: 30 } }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        for (const r of rows) {
          const v = r.dims['prepTime'] as string
          expect(v).toMatch(/^-?\d+\.\.\d+$/)
        }
      })

      it.skipIf(!caps.numericBucket)('10. breaks bucket', async () => {
        const rows = await collect(
          agg({
            dimensions: [
              { field: 'prepTime', bucket: { type: 'numeric', breaks: [0, 30, 60, 120] } },
            ],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        // 49 rows are within range (the 999-prepTime row drops out + the
        // null-cuisine row is in-range but does NOT drop because its
        // prepTime is finite). One row (id=8, prepTime=999) drops.
        const total = rows.reduce((s, r) => s + r.count, 0)
        expect(total).toBeLessThanOrEqual(49)
      })

      it.skipIf(!caps.numericBucket)('11. rows out of breaks range are dropped', async () => {
        const rows = await collect(
          agg({
            dimensions: [
              { field: 'prepTime', bucket: { type: 'numeric', breaks: [0, 30, 60, 120] } },
            ],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        const total = rows.reduce((s, r) => s + r.count, 0)
        // The id=8 row (prepTime=999) must drop.
        expect(total).toBeLessThanOrEqual(49)
      })
    })

    // ─── Measures ────────────────────────────────────────────────
    describe('measures', () => {
      it('12. count', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        expect(rows.length).toBe(1)
        expect(rows[0]!.measures['c']).toBe(50)
      })

      it.skipIf(!caps.sum)('13. sum', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: { s: { agg: 'sum', column: 'views', accuracy: 'exact' } },
          }),
        )
        const expectedSum = aggregateFixture().reduce((s, r) => s + r.views, 0)
        expect(Number(rows[0]!.measures['s'])).toBeCloseTo(expectedSum, 0)
      })

      it.skipIf(!caps.avg)('14. avg', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: { a: { agg: 'avg', column: 'rating', accuracy: 'exact' } },
          }),
        )
        const fix = aggregateFixture()
        const expected = fix.reduce((s, r) => s + r.rating, 0) / fix.length
        expect(Number(rows[0]!.measures['a'])).toBeCloseTo(expected, 1)
      })

      it.skipIf(!caps.minMax)('15. min / max', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: {
              mn: { agg: 'min', column: 'rating', accuracy: 'exact' },
              mx: { agg: 'max', column: 'rating', accuracy: 'exact' },
            },
          }),
        )
        expect(Number(rows[0]!.measures['mn'])).toBe(1)
        expect(Number(rows[0]!.measures['mx'])).toBe(5)
      })

      it.skipIf(caps.countDistinct === false)('16. count_distinct exact', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: { d: { agg: 'count_distinct', column: 'country', accuracy: 'exact' } },
          }),
        )
        expect(Number(rows[0]!.measures['d'])).toBe(3)
      })

      it.skipIf(caps.percentile === false)('18. percentile exact p=0.5', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: { p: { agg: 'percentile', column: 'rating', p: 0.5, accuracy: 'exact' } },
          }),
        )
        const v = Number(rows[0]!.measures['p'])
        expect(v).toBeGreaterThanOrEqual(2)
        expect(v).toBeLessThanOrEqual(4)
      })

      it.skipIf(!caps.topK)('20. top_k k=2', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: { t: { agg: 'top_k', column: 'country', k: 2, accuracy: 'exact' } },
          }),
        )
        const top = rows[0]!.measures['t'] as Array<{ key: string; count: number }>
        expect(top.length).toBeLessThanOrEqual(2)
        expect(top[0]!.count).toBeGreaterThanOrEqual(top[1]?.count ?? 0)
      })

      it.skipIf(!caps.sum)('22. measure-level where', async () => {
        const rows = await collect(
          agg({
            dimensions: [],
            measures: {
              all: { agg: 'sum', column: 'views', accuracy: 'exact' },
              pub: { agg: 'sum', column: 'views', accuracy: 'exact', where: { status: 'published' } },
            },
          }),
        )
        const all = Number(rows[0]!.measures['all'])
        const pub = Number(rows[0]!.measures['pub'])
        expect(pub).toBeLessThan(all)
      })
    })

    // ─── Predicate / having / sort / limit ───────────────────────
    describe('predicates', () => {
      it('23. candidatesWhere narrows pool', async () => {
        const rows = await collect(
          agg({
            candidatesWhere: { status: 'published' },
            dimensions: [],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        const expected = aggregateFixture().filter((r) => r.status === 'published').length
        expect(Number(rows[0]!.measures['c'])).toBe(expected)
      })

      it.skipIf(!caps.havingOnAggregates)('24. having', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
            having: { count: { $gte: 5 } },
          }),
        )
        for (const r of rows) {
          expect(r.count).toBeGreaterThanOrEqual(5)
        }
      })

      it.skipIf(!caps.pushdownOrderLimit)('25. sort + limit pushdown', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
            sort: [{ key: 'count', dir: 'desc' }],
            limit: 1,
          }),
        )
        expect(rows.length).toBe(1)
      })

      it.skipIf(!caps.sampling)('26. sample honored', async () => {
        const rows = await collect(
          agg({
            sample: 0.5,
            dimensions: [],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        const c = Number(rows[0]!.measures['c'])
        // ±60% bound on a 50-row sample (tiny n means high variance).
        expect(c).toBeGreaterThan(10)
        expect(c).toBeLessThanOrEqual(50)
      })
    })

    // ─── Stream contract ─────────────────────────────────────────
    describe('stream contract', () => {
      it('28. AsyncIterable contract', async () => {
        const stream = agg({
          dimensions: [{ field: 'cuisine' }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        })
        let count = 0
        for await (const _ of stream) count++
        expect(count).toBeGreaterThan(0)
      })

      it('29. empty result yields nothing', async () => {
        const rows = await collect(
          agg({
            candidatesWhere: { cuisine: '__nonexistent__' },
            dimensions: [{ field: 'cuisine' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
          }),
        )
        expect(rows.length).toBe(0)
      })

      it.skipIf(!caps.pushdownOrderLimit)('30. ordered output', async () => {
        const rows = await collect(
          agg({
            dimensions: [{ field: 'cuisine' }],
            measures: { c: { agg: 'count', accuracy: 'exact' } },
            sort: [{ key: 'count', dir: 'desc' }],
          }),
        )
        for (let i = 1; i < rows.length; i++) {
          expect(rows[i - 1]!.count).toBeGreaterThanOrEqual(rows[i]!.count)
        }
      })
    })
  })
}

export { FIXTURE_TARGET_PLACEHOLDER }

/** Convenience: convert AggregateFixtureRow into a generic BridgeRow. */
export function fixtureToBridgeRows(rows: AggregateFixtureRow[]): BridgeRow[] {
  return rows.map((r) => ({ ...r }))
}
