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
import { POSTGRES_FAMILY_CAPABILITIES } from '@semilayer/bridge-sdk'
import {
  aggregateFixture,
  collectAggregateStream as collect,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'

describe.skipIf(!DATABASE_URL)('PostgresBridge aggregate integration', () => {
  let setup: pg.Client
  let bridge: PostgresBridge

  beforeAll(async () => {
    setup = new pg.Client({ connectionString: DATABASE_URL })
    await setup.connect()

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

    bridge = new PostgresBridge({ url: DATABASE_URL!, ipFamily: 0 })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.query(`DROP TABLE IF EXISTS ${TABLE}`)
    await setup?.end()
  })

  // ─── Universal compliance — ~30 cases ─────────────────────────────
  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: POSTGRES_FAMILY_CAPABILITIES,
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
