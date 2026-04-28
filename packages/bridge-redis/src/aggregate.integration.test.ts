/**
 * Redis aggregate integration — seeds the canonical fixture as JSON
 * blobs under `sl_agg_fixture:<id>` keys and runs the universal
 * compliance suite via `streamingAggregate`. Real Redis exercises the
 * KEYS + MGET pagination path, including JSON parse on each value.
 */
import Redis from 'ioredis'
import { describe, beforeAll, afterAll } from 'vitest'
import { RedisBridge } from './bridge.js'
import {
  aggregateFixture,
  STREAMING_AGGREGATE_CAPABILITIES,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk'

const REDIS_URL = process.env['REDIS_URL']
const PREFIX = 'sl_agg_fixture'

describe.skipIf(!REDIS_URL)('RedisBridge aggregate integration', () => {
  let setup: Redis
  let bridge: RedisBridge

  beforeAll(async () => {
    setup = new Redis(REDIS_URL!)
    // Clear any stale keys from prior runs.
    const stale = await setup.keys(`${PREFIX}:*`)
    if (stale.length > 0) await setup.del(...stale)

    for (const r of aggregateFixture()) {
      await setup.set(`${PREFIX}:${r.id}`, JSON.stringify({
        ...r,
        createdAt: r.createdAt.toISOString(),
      }))
    }

    bridge = new RedisBridge({ url: REDIS_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    if (setup) {
      const stale = await setup.keys(`${PREFIX}:*`)
      if (stale.length > 0) await setup.del(...stale)
      await setup.quit()
    }
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: PREFIX,
    capabilities: STREAMING_AGGREGATE_CAPABILITIES,
  })
})
