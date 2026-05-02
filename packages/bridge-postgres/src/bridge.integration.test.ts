/**
 * Postgres integration tests — seed the canonical aggregate fixture into
 * a real Postgres and run the universal compliance suite + Postgres-
 * specific assertions (TABLESAMPLE BERNOULLI, percentile_cont, etc.).
 *
 * Skips entirely when DATABASE_URL is not set. The GHA workflow at
 * `.github/workflows/integration-postgres.yml` spins up a `postgres:16`
 * service and points DATABASE_URL at it.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import pg from 'pg'
import { PostgresBridge } from './bridge.js'
import { postgresAggregateCapabilities } from './aggregate.js'
import {
  aggregateFixture,
  collectAggregateStream as collect,
  geoFixture,
  joinChildFixture,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'
const JOIN_CHILD_TABLE = 'sl_agg_join_child'
const GEO_TABLE = 'sl_agg_geo_fixture'

// PostGIS is optional — the integration job uses the postgis-enabled
// image. When the extension isn't available we still want every other
// case to run, so the geo bits are gated on a probe at beforeAll.
const ENABLE_POSTGIS = process.env['POSTGIS'] !== '0'

describe.skipIf(!DATABASE_URL)('PostgresBridge aggregate integration', () => {
  let setup: pg.Client
  let bridge: PostgresBridge

  beforeAll(async () => {
    setup = new pg.Client({ connectionString: DATABASE_URL })
    await setup.connect()

    if (ENABLE_POSTGIS) {
      // Must succeed when ENABLE_POSTGIS is on — the workflow uses the
      // postgis/postgis image. Set POSTGIS=0 to skip on plain postgres.
      await setup.query('CREATE EXTENSION IF NOT EXISTS postgis')
    }

    await setup.query(`DROP TABLE IF EXISTS ${TABLE}`)
    await setup.query(`
      CREATE TABLE ${TABLE} (
        id          INTEGER PRIMARY KEY,
        cuisine     TEXT,
        country     TEXT NOT NULL,
        rating      INTEGER NOT NULL,
        "prepTime"  NUMERIC NOT NULL,
        views       INTEGER NOT NULL,
        status      TEXT NOT NULL,
        "createdAt" TIMESTAMPTZ NOT NULL
      )
    `)

    for (const r of aggregateFixture()) {
      await setup.query(
        `INSERT INTO ${TABLE} (id, cuisine, country, rating, "prepTime", views, status, "createdAt")
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [r.id, r.cuisine, r.country, r.rating, r.prepTime, r.views, r.status, r.createdAt],
      )
    }

    await setup.query(`DROP TABLE IF EXISTS ${JOIN_CHILD_TABLE}`)
    await setup.query(`
      CREATE TABLE ${JOIN_CHILD_TABLE} (
        pk     INTEGER PRIMARY KEY,
        region TEXT NOT NULL,
        tier   TEXT NOT NULL
      )
    `)
    for (const r of joinChildFixture()) {
      await setup.query(
        `INSERT INTO ${JOIN_CHILD_TABLE} (pk, region, tier) VALUES ($1, $2, $3)`,
        [r.pk, r.region, r.tier],
      )
    }

    await setup.query(`DROP TABLE IF EXISTS ${GEO_TABLE}`)
    await setup.query(`
      CREATE TABLE ${GEO_TABLE} (
        id   INTEGER PRIMARY KEY,
        lat  DOUBLE PRECISION NOT NULL,
        lng  DOUBLE PRECISION NOT NULL,
        city TEXT NOT NULL
      )
    `)
    for (const r of geoFixture()) {
      await setup.query(
        `INSERT INTO ${GEO_TABLE} (id, lat, lng, city) VALUES ($1, $2, $3, $4)`,
        [r.id, r.lat, r.lng, r.city],
      )
    }

    bridge = new PostgresBridge({
      url: DATABASE_URL!,
      ipFamily: 0,
      enablePostgis: ENABLE_POSTGIS,
    })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.query(`DROP TABLE IF EXISTS ${TABLE}`)
    await setup?.query(`DROP TABLE IF EXISTS ${JOIN_CHILD_TABLE}`)
    await setup?.query(`DROP TABLE IF EXISTS ${GEO_TABLE}`)
    await setup?.end()
  })

  // ─── Universal compliance — base + joins + geo when enabled ───────
  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: postgresAggregateCapabilities({ enablePostgis: ENABLE_POSTGIS }),
    joinChildFixtureTarget: JOIN_CHILD_TABLE,
    geoFixtureTarget: GEO_TABLE,
  })

  // ─── Postgres-specific behavior ───────────────────────────────────
  describe('Postgres-specific', () => {
    it('percentile_cont returns float — coerced to number', async () => {
      const rows = await collect(
        bridge.aggregate({
          target: TABLE,
          dimensions: [],
          measures: { p: { agg: 'percentile', column: 'rating', p: 0.5, accuracy: 'exact' } },
        }),
      )
      expect(typeof rows[0]!.measures['p']).toBe('number')
    })

    it('width_bucket-equivalent ignores out-of-range rows', async () => {
      const rows = await collect(
        bridge.aggregate({
          target: TABLE,
          dimensions: [
            { field: 'prepTime', bucket: { type: 'numeric', breaks: [0, 30, 60, 120] } },
          ],
          measures: { c: { agg: 'count', accuracy: 'exact' } },
        }),
      )
      const total = rows.reduce((s, r) => s + r.count, 0)
      // The id=8 row has prepTime=999 which is outside [0,120) — must drop.
      expect(total).toBeLessThanOrEqual(49)
    })

    it('measure FILTER (WHERE …) does not bleed across measures', async () => {
      const rows = await collect(
        bridge.aggregate({
          target: TABLE,
          dimensions: [],
          measures: {
            allViews: { agg: 'sum', column: 'views', accuracy: 'exact' },
            pubViews: {
              agg: 'sum',
              column: 'views',
              accuracy: 'exact',
              where: { status: 'published' },
            },
          },
        }),
      )
      const all = Number(rows[0]!.measures['allViews'])
      const pub = Number(rows[0]!.measures['pubViews'])
      expect(pub).toBeLessThan(all)
    })
  })
})
