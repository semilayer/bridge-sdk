/**
 * SQL-generation unit tests for the Postgres aggregate adapter.
 * These run without a live Postgres — we snapshot the SQL string and
 * params from `buildPostgresAggregate(opts)` to catch accidental query
 * shape changes. Integration tests against real Postgres live in
 * `bridge.integration.test.ts`.
 */
import { describe, it, expect } from 'vitest'
import { buildPostgresAggregate } from './aggregate.js'

describe('buildPostgresAggregate — SQL generator', () => {
  it('1. simple count over single dim', () => {
    const built = buildPostgresAggregate({
      target: 'public.events',
      dimensions: [{ field: 'cuisine' }],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
    })
    expect(built.mainSql).toContain('"cuisine" AS "dim_cuisine"')
    expect(built.mainSql).toContain('COUNT(*) AS "count"')
    expect(built.mainSql).toContain('GROUP BY "cuisine"')
    expect(built.topKQueries).toHaveLength(0)
  })

  it('2. time bucket day uses date_trunc', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [{ field: 'createdAt', bucket: 'day' }],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
    })
    expect(built.mainSql).toContain(`date_trunc('day', "createdAt")`)
  })

  it('3. numeric step bucket emits FLOOR expression', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [
        { field: 'prepTime', bucket: { type: 'numeric', step: 30 } },
      ],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
    })
    expect(built.mainSql).toContain(`FLOOR(("prepTime")::numeric / 30) * 30`)
  })

  it('4. percentile uses percentile_cont WITHIN GROUP', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [],
      measures: {
        p99: { agg: 'percentile', column: 'duration', p: 0.99, accuracy: 'exact' },
      },
    })
    expect(built.mainSql).toContain(
      `percentile_cont(0.99) WITHIN GROUP (ORDER BY "duration") AS "m_p99"`,
    )
  })

  it('5. measure-level WHERE wraps with FILTER', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [],
      measures: {
        pub: {
          agg: 'sum',
          column: 'views',
          accuracy: 'exact',
          where: { status: 'published' },
        },
      },
    })
    expect(built.mainSql).toMatch(
      /SUM\("views"\) FILTER \(WHERE "status" = \$\d+\) AS "m_pub"/,
    )
  })

  it('6. sample appends TABLESAMPLE BERNOULLI', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      sample: 0.5,
      dimensions: [],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
    })
    expect(built.mainSql).toContain('TABLESAMPLE BERNOULLI(50)')
  })

  it('7. having translates to HAVING with aliased column', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [{ field: 'cuisine' }],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
      having: { count: { $gte: 5 } },
    })
    expect(built.mainSql).toContain('HAVING COUNT(*) >= $')
  })

  it('8. sort + limit appended', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [{ field: 'cuisine' }],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
      sort: [{ key: 'count', dir: 'desc' }],
      limit: 5,
    })
    expect(built.mainSql).toContain('ORDER BY "count" DESC')
    expect(built.mainSql).toMatch(/LIMIT \$\d+/)
    expect(built.mainParams[built.mainParams.length - 1]).toBe(5)
  })

  it('9. top_k spawns separate subquery', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [{ field: 'cuisine' }],
      measures: {
        top: { agg: 'top_k', column: 'country', k: 3, accuracy: 'exact' },
      },
    })
    expect(built.topKQueries).toHaveLength(1)
    const tk = built.topKQueries[0]!
    expect(tk.measureName).toBe('top')
    expect(tk.sql).toContain('GROUP BY')
    expect(tk.sql).toContain('"k_count" DESC')
    expect(tk.k).toBe(3)
  })

  it('10. parameter placeholders are 1-indexed and unique', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      candidatesWhere: { status: 'published', country: { $in: ['US', 'JP'] } },
      dimensions: [],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
      limit: 100,
    })
    // params: 'published', 'US', 'JP', 100 (in order)
    expect(built.mainParams).toEqual(['published', 'US', 'JP', 100])
    expect(built.mainSql).toContain('$1')
    expect(built.mainSql).toContain('$2')
    expect(built.mainSql).toContain('$3')
    expect(built.mainSql).toContain('$4')
  })

  it('11. count_distinct exact uses COUNT(DISTINCT)', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [],
      measures: {
        d: { agg: 'count_distinct', column: 'country', accuracy: 'exact' },
      },
    })
    expect(built.mainSql).toContain('COUNT(DISTINCT "country") AS "m_d"')
  })

  it('12. dim alias `as` reflected in SELECT alias', () => {
    const built = buildPostgresAggregate({
      target: 'events',
      dimensions: [{ field: 'cuisine', as: 'group' }],
      measures: { c: { agg: 'count', accuracy: 'exact' } },
    })
    expect(built.mainSql).toContain('"cuisine" AS "dim_group"')
    expect(built.dimsSchema[0]!.outputKey).toBe('group')
  })

  it('13. joins emit LEFT JOIN with qualified columns', () => {
    const built = buildPostgresAggregate({
      target: 'orders',
      joins: [
        { target: 'customers', alias: 'c', kind: 'left', on: { local: 'customer_id', foreign: 'id' } },
      ],
      dimensions: [{ field: 'country', from: 'c' }],
      measures: { total: { agg: 'sum', column: 'amount', accuracy: 'exact' } },
    })
    expect(built.mainSql).toContain('FROM "orders" AS "t"')
    expect(built.mainSql).toContain('LEFT JOIN "customers" AS "c" ON "t"."customer_id" = "c"."id"')
    expect(built.mainSql).toContain('"c"."country"')
    expect(built.mainSql).toContain('SUM("t"."amount")')
  })

  it('14. enablePostgis flips geohash bucket to native ST_GeoHash', () => {
    const built = buildPostgresAggregate(
      {
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      { enablePostgis: true },
    )
    expect(built.mainSql).toContain('ST_GeoHash(ST_SetSRID(ST_MakePoint("lng", "lat"), 4326), 5)')
  })

  it('15. without enablePostgis, geohash dim throws (cap not advertised)', () => {
    expect(() =>
      buildPostgresAggregate({
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      }),
    ).toThrow(/geohashExpr/)
  })
})
