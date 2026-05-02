/**
 * CockroachDB aggregate integration — same fixture, Postgres dialect.
 * CockroachDB speaks the wire protocol so the dialect carries over
 * directly; the only practical difference is `TABLESAMPLE BERNOULLI`
 * is supported but with slightly different optimizer behavior.
 */
import pg from 'pg'
import { describe, beforeAll, afterAll } from 'vitest'
import { CockroachdbBridge } from './bridge.js'
import { COCKROACH_CAPABILITIES } from '@semilayer/bridge-sdk'
import {
  aggregateFixture,
  joinChildFixture,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'
const JOIN_CHILD_TABLE = 'sl_agg_join_child'

describe.skipIf(!DATABASE_URL)('CockroachdbBridge aggregate integration', () => {
  let setup: pg.Client
  let bridge: CockroachdbBridge

  beforeAll(async () => {
    setup = new pg.Client({ connectionString: DATABASE_URL })
    await setup.connect()
    await setup.query(`DROP TABLE IF EXISTS ${TABLE}`)
    await setup.query(`
      CREATE TABLE ${TABLE} (
        id          INT PRIMARY KEY,
        cuisine     STRING,
        country     STRING NOT NULL,
        rating      INT NOT NULL,
        "prepTime"  DECIMAL NOT NULL,
        views       INT NOT NULL,
        status      STRING NOT NULL,
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
        pk     INT PRIMARY KEY,
        region STRING NOT NULL,
        tier   STRING NOT NULL
      )
    `)
    for (const r of joinChildFixture()) {
      await setup.query(
        `INSERT INTO ${JOIN_CHILD_TABLE} (pk, region, tier) VALUES ($1, $2, $3)`,
        [r.pk, r.region, r.tier],
      )
    }

    bridge = new CockroachdbBridge({ url: DATABASE_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.query(`DROP TABLE IF EXISTS ${TABLE}`)
    await setup?.query(`DROP TABLE IF EXISTS ${JOIN_CHILD_TABLE}`)
    await setup?.end()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: COCKROACH_CAPABILITIES,
    joinChildFixtureTarget: JOIN_CHILD_TABLE,
  })
})
