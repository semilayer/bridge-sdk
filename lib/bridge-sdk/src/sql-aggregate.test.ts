/**
 * Unit tests for `buildAggregateSql` covering the join surface — pure
 * builder behavior, no driver. Postgres is the dialect of record because
 * its `quoteIdent` / placeholder shape is the simplest to assert on.
 */
import { describe, it, expect } from 'vitest'
import { buildAggregateSql } from './sql-aggregate.js'
import {
  POSTGRES_DIALECT,
  postgisGeohashExpr,
  postgisDecodeGeoField,
  clickhouseGeohashExpr,
  clickhouseH3Expr,
  mysqlGeohashExpr,
  bigqueryGeohashExpr,
  snowflakeGeohashExpr,
} from './sql-dialects.js'
import { CLICKHOUSE_DIALECT, MYSQL_DIALECT, BIGQUERY_DIALECT, SNOWFLAKE_DIALECT } from './sql-dialects.js'

describe('buildAggregateSql — joins', () => {
  it('emits LEFT JOIN with base alias and qualified columns', () => {
    const built = buildAggregateSql(
      {
        target: 'parents',
        joins: [
          {
            target: 'children',
            alias: 'c',
            kind: 'left',
            on: { local: 'fk', foreign: 'pk' },
          },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      POSTGRES_DIALECT,
    )
    expect(built.mainSql).toContain('FROM "parents" AS "t"')
    expect(built.mainSql).toContain('LEFT JOIN "children" AS "c" ON "t"."fk" = "c"."pk"')
    expect(built.mainSql).toContain('"c"."region"')
    expect(built.mainSql).toContain('"c"."region" IS NOT NULL')
  })

  it('omits aliasing when no joins are declared (back-compat)', () => {
    const built = buildAggregateSql(
      {
        target: 'parents',
        dimensions: [{ field: 'region' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      POSTGRES_DIALECT,
    )
    expect(built.mainSql).toContain('FROM "parents"')
    expect(built.mainSql).not.toContain(' AS "t"')
    expect(built.mainSql).toContain('"region" IS NOT NULL')
    expect(built.mainSql).not.toContain('"t"."region"')
  })

  it('qualifies base columns with base alias when joins are present', () => {
    const built = buildAggregateSql(
      {
        target: 'parents',
        joins: [
          { target: 'children', alias: 'c', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'cuisine' }, { field: 'region', from: 'c' }],
        measures: { s: { agg: 'sum', column: 'views', accuracy: 'exact' } },
      },
      POSTGRES_DIALECT,
    )
    // Base dim qualifies as "t"."cuisine"; joined dim as "c"."region".
    expect(built.mainSql).toContain('"t"."cuisine"')
    expect(built.mainSql).toContain('"c"."region"')
    // Measure on base column qualifies on base alias.
    expect(built.mainSql).toContain('SUM("t"."views")')
  })

  it('copies JOINs into the top_k subquery', () => {
    const built = buildAggregateSql(
      {
        target: 'parents',
        joins: [
          { target: 'children', alias: 'c', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { t: { agg: 'top_k', column: 'country', k: 3, accuracy: 'exact' } },
      },
      POSTGRES_DIALECT,
    )
    expect(built.topKQueries).toHaveLength(1)
    const tk = built.topKQueries[0]!
    expect(tk.sql).toContain('LEFT JOIN "children" AS "c"')
    expect(tk.sql).toContain('"c"."region"')
    expect(tk.sql).toContain('"t"."country"')
  })

  it('rejects an alias that collides with the reserved base alias', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'parents',
          joins: [
            { target: 'children', alias: 't', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
          ],
          dimensions: [{ field: 'region', from: 't' }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        POSTGRES_DIALECT,
      ),
    ).toThrow(/reserved base alias/)
  })

  it('rejects duplicate join aliases', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'parents',
          joins: [
            { target: 'children', alias: 'c', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
            { target: 'others', alias: 'c', kind: 'left', on: { local: 'fk2', foreign: 'pk2' } },
          ],
          dimensions: [{ field: 'region', from: 'c' }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        POSTGRES_DIALECT,
      ),
    ).toThrow(/Duplicate join alias/)
  })

  it('rejects an alias that is not a plain identifier', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'parents',
          joins: [
            { target: 'children', alias: 'c"; DROP TABLE x;--', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
          ],
          dimensions: [{ field: 'region', from: 'c' }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        POSTGRES_DIALECT,
      ),
    ).toThrow(/Invalid join alias/)
  })

  it('rejects a dim referencing an unknown alias', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'parents',
          joins: [
            { target: 'children', alias: 'c', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
          ],
          dimensions: [{ field: 'region', from: 'unknown' }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        POSTGRES_DIALECT,
      ),
    ).toThrow(/unknown join alias/)
  })

  it('honors a dialect-supplied joinClause override', () => {
    const built = buildAggregateSql(
      {
        target: 'parents',
        joins: [
          { target: 'children', alias: 'c', kind: 'left', on: { local: 'fk', foreign: 'pk' } },
        ],
        dimensions: [{ field: 'region', from: 'c' }],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      {
        ...POSTGRES_DIALECT,
        joinClause: (j, base) =>
          `LEFT OUTER JOIN "${j.target}" "${j.alias}" ON "${base}"."${j.on.local}" = "${j.alias}"."${j.on.foreign}"`,
      },
    )
    expect(built.mainSql).toContain('LEFT OUTER JOIN "children" "c" ON "t"."fk" = "c"."pk"')
  })
})

describe('buildAggregateSql — geo bucket pushdown', () => {
  const POSTGIS_DIALECT = {
    ...POSTGRES_DIALECT,
    geohashExpr: postgisGeohashExpr,
    decodeGeoField: postgisDecodeGeoField,
  }

  it('emits geohashExpr with qualified lat/lng cols', () => {
    const built = buildAggregateSql(
      {
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      POSTGIS_DIALECT,
    )
    expect(built.mainSql).toContain('ST_GeoHash')
    expect(built.mainSql).toContain('"lat"')
    expect(built.mainSql).toContain('"lng"')
    expect(built.mainSql).toContain('"lat" IS NOT NULL')
    expect(built.mainSql).toContain('"lng" IS NOT NULL')
  })

  it('uses decodeGeoField when caller supplies geoField', () => {
    const built = buildAggregateSql(
      {
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 6, geoField: 'geom' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      POSTGIS_DIALECT,
    )
    expect(built.mainSql).toContain('ST_GeoHash')
    expect(built.mainSql).toContain('ST_Y(("geom")::geometry)')
    expect(built.mainSql).toContain('ST_X(("geom")::geometry)')
    expect(built.mainSql).toContain('"geom" IS NOT NULL')
  })

  it('throws when neither lat/lng nor geoField is supplied', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'places',
          dimensions: [{ field: 'cell', bucket: { type: 'geohash', precision: 5 } }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        POSTGIS_DIALECT,
      ),
    ).toThrow(/requires either latField\+lngField or geoField/)
  })

  it('throws when caller supplies geoField but dialect lacks decodeGeoField', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'places',
          dimensions: [{ field: 'cell', bucket: { type: 'geohash', precision: 5, geoField: 'geom' } }],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        { ...POSTGRES_DIALECT, geohashExpr: postgisGeohashExpr },
      ),
    ).toThrow(/cannot decode "geoField"/)
  })

  it('throws when caller asks for h3 on a dialect without h3Expr', () => {
    expect(() =>
      buildAggregateSql(
        {
          target: 'places',
          dimensions: [
            { field: 'cell', bucket: { type: 'h3', resolution: 7, latField: 'lat', lngField: 'lng' } },
          ],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        },
        POSTGIS_DIALECT,
      ),
    ).toThrow(/h3Expr/)
  })

  it('clamps precision to [1, 12]', () => {
    expect(postgisGeohashExpr('a', 'b', 99)).toContain(', 12)')
    expect(postgisGeohashExpr('a', 'b', 0)).toContain(', 1)')
  })

  it('emits ClickHouse-native geohashEncode + geoToH3', () => {
    const dialect = {
      ...CLICKHOUSE_DIALECT,
      geohashExpr: clickhouseGeohashExpr,
      h3Expr: clickhouseH3Expr,
    }
    const geo = buildAggregateSql(
      {
        target: 'pings',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 7, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      dialect,
    )
    expect(geo.mainSql).toContain('geohashEncode(')

    const h3 = buildAggregateSql(
      {
        target: 'pings',
        dimensions: [
          { field: 'cell', bucket: { type: 'h3', resolution: 8, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      dialect,
    )
    expect(h3.mainSql).toContain('geoToH3(')
  })

  it('emits MySQL native ST_GeoHash on POINT', () => {
    const dialect = { ...MYSQL_DIALECT, geohashExpr: mysqlGeohashExpr }
    const built = buildAggregateSql(
      {
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      dialect,
    )
    expect(built.mainSql).toContain('ST_GeoHash(POINT(')
  })

  it('emits BigQuery ST_GEOGPOINT geohash', () => {
    const dialect = { ...BIGQUERY_DIALECT, geohashExpr: bigqueryGeohashExpr }
    const built = buildAggregateSql(
      {
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      dialect,
    )
    expect(built.mainSql).toContain('ST_GEOHASH(ST_GEOGPOINT(')
  })

  it('emits Snowflake ST_MAKEPOINT geohash', () => {
    const dialect = { ...SNOWFLAKE_DIALECT, geohashExpr: snowflakeGeohashExpr }
    const built = buildAggregateSql(
      {
        target: 'places',
        dimensions: [
          { field: 'cell', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      dialect,
    )
    expect(built.mainSql).toContain('ST_GEOHASH(ST_MAKEPOINT(')
  })

  it('composes geo dim with joins (qualifies lat/lng on the joined alias)', () => {
    const built = buildAggregateSql(
      {
        target: 'orders',
        joins: [
          { target: 'addresses', alias: 'a', kind: 'left', on: { local: 'address_id', foreign: 'id' } },
        ],
        dimensions: [
          { field: 'cell', from: 'a', bucket: { type: 'geohash', precision: 5, latField: 'lat', lngField: 'lng' } },
        ],
        measures: { c: { agg: 'count', accuracy: 'exact' } },
      },
      POSTGIS_DIALECT,
    )
    expect(built.mainSql).toContain('"a"."lat"')
    expect(built.mainSql).toContain('"a"."lng"')
    expect(built.mainSql).toContain('LEFT JOIN "addresses" AS "a"')
  })
})
