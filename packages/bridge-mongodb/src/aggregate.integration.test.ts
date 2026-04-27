/**
 * MongoDB aggregate integration — runs the universal compliance suite
 * against the canonical fixture in a real MongoDB instance using the
 * native `$group` aggregation pipeline.
 *
 * Skips Mongo 6.x or older clusters that don't ship the `$percentile`
 * accumulator. Cluster-version detection isn't done here — if the
 * server is too old, the percentile cases will fail loudly. To run
 * against an older Mongo, override `MONGODB_AGGREGATE_CAPABILITIES` in
 * a fork.
 */
import { MongoClient } from 'mongodb'
import { describe, beforeAll, afterAll } from 'vitest'
import { MongodbBridge } from './bridge.js'
import { MONGODB_AGGREGATE_CAPABILITIES } from './aggregate.js'
import { aggregateFixture, runAggregateCompliance } from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const COLLECTION = 'sl_agg_fixture'

describe.skipIf(!DATABASE_URL)('MongodbBridge aggregate integration', () => {
  let setup: MongoClient
  let bridge: MongodbBridge

  beforeAll(async () => {
    setup = new MongoClient(DATABASE_URL!)
    await setup.connect()
    const db = setup.db()
    await db.collection(COLLECTION).drop().catch(() => {})
    const docs = aggregateFixture().map((r) => ({
      _id: String(r.id),
      cuisine: r.cuisine,
      country: r.country,
      rating: r.rating,
      prepTime: r.prepTime,
      views: r.views,
      status: r.status,
      createdAt: r.createdAt,
    }))
    await db.collection(COLLECTION).insertMany(docs as never)

    bridge = new MongodbBridge({ url: DATABASE_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.db().collection(COLLECTION).drop().catch(() => {})
    await setup?.close()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: COLLECTION,
    capabilities: MONGODB_AGGREGATE_CAPABILITIES,
  })
})
