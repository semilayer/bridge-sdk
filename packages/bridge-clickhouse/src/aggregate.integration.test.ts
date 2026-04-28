/**
 * ClickHouse aggregate integration — seeds the canonical fixture into a
 * real ClickHouse and runs the universal compliance suite. The dialect
 * uses native `quantileExact()` and `uniqExact()` so percentile +
 * count_distinct exact paths get hit end-to-end.
 */
import { createClient } from '@clickhouse/client'
import { describe, beforeAll, afterAll } from 'vitest'
import { ClickhouseBridge } from './bridge.js'
import { CLICKHOUSE_CAPABILITIES } from '@semilayer/bridge-sdk'
import { aggregateFixture, runAggregateCompliance } from '@semilayer/bridge-sdk/testing'

const CH_URL = process.env['CLICKHOUSE_URL']
const TABLE = 'sl_agg_fixture'

describe.skipIf(!CH_URL)('ClickhouseBridge aggregate integration', () => {
  let bridge: ClickhouseBridge
  let setup: ReturnType<typeof createClient>

  beforeAll(async () => {
    const url = new URL(CH_URL!)
    setup = createClient({
      host: `${url.protocol}//${url.host}`,
      username: url.username || 'default',
      password: url.password,
      database: url.pathname.replace(/^\//, '') || 'default',
    })

    await setup.command({ query: `DROP TABLE IF EXISTS ${TABLE}` })
    await setup.command({
      query: `
        CREATE TABLE ${TABLE} (
          id          UInt32,
          cuisine     Nullable(String),
          country     String,
          rating      UInt32,
          prepTime    Float64,
          views       UInt32,
          status      String,
          createdAt   DateTime
        ) ENGINE = MergeTree() ORDER BY id SAMPLE BY id
      `,
    })

    const rows = aggregateFixture().map((r) => ({
      id: r.id,
      cuisine: r.cuisine,
      country: r.country,
      rating: r.rating,
      prepTime: r.prepTime,
      views: r.views,
      status: r.status,
      createdAt: r.createdAt.toISOString().replace('T', ' ').replace(/\..*/, ''),
    }))
    await setup.insert({ table: TABLE, values: rows, format: 'JSONEachRow' })

    bridge = new ClickhouseBridge({
      // ClickhouseBridge takes host + port separately. Pass `hostname`,
      // not `host` — the latter is `host:port` and gets concatenated
      // with the default port producing an invalid URL.
      host: url.hostname,
      port: url.port ? parseInt(url.port, 10) : 8123,
      protocol: url.protocol === 'https:' ? 'https' : 'http',
      username: url.username || 'default',
      password: url.password,
      database: url.pathname.replace(/^\//, '') || 'default',
    })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.command({ query: `DROP TABLE IF EXISTS ${TABLE}` })
    await setup?.close()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: CLICKHOUSE_CAPABILITIES,
  })
})
