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
import {
  aggregateFixture,
  geoFixture,
  joinChildFixture,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const CH_URL = process.env['CLICKHOUSE_URL']
const TABLE = 'sl_agg_fixture'
const JOIN_CHILD_TABLE = 'sl_agg_join_child'
const GEO_TABLE = 'sl_agg_geo_fixture'

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

    await setup.command({ query: `DROP TABLE IF EXISTS ${JOIN_CHILD_TABLE}` })
    await setup.command({
      query: `
        CREATE TABLE ${JOIN_CHILD_TABLE} (
          pk     UInt32,
          region String,
          tier   String
        ) ENGINE = MergeTree() ORDER BY pk
      `,
    })
    await setup.insert({
      table: JOIN_CHILD_TABLE,
      values: joinChildFixture(),
      format: 'JSONEachRow',
    })

    await setup.command({ query: `DROP TABLE IF EXISTS ${GEO_TABLE}` })
    await setup.command({
      query: `
        CREATE TABLE ${GEO_TABLE} (
          id   UInt32,
          lat  Float64,
          lng  Float64,
          city String
        ) ENGINE = MergeTree() ORDER BY id
      `,
    })
    await setup.insert({
      table: GEO_TABLE,
      values: geoFixture(),
      format: 'JSONEachRow',
    })

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
    await setup?.command({ query: `DROP TABLE IF EXISTS ${JOIN_CHILD_TABLE}` })
    await setup?.command({ query: `DROP TABLE IF EXISTS ${GEO_TABLE}` })
    await setup?.close()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    // ClickHouse advertises native geohash + h3, so the suite runs both
    // geo blocks against the live engine.
    capabilities: {
      ...CLICKHOUSE_CAPABILITIES,
      geoBucket: true,
      geohashBucket: true,
      h3Bucket: true,
    },
    joinChildFixtureTarget: JOIN_CHILD_TABLE,
    geoFixtureTarget: GEO_TABLE,
  })
})
