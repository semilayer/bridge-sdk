/**
 * MariaDB aggregate integration — runs the universal compliance suite
 * against the canonical fixture in a real MariaDB instance. Geohash
 * cases run when `MARIADB_GEOHASH=1` is set; the function landed in
 * MariaDB 11+ so older servers should leave it off (default off).
 */
import * as mariadb from 'mariadb'
import { describe, beforeAll, afterAll } from 'vitest'
import { MariadbBridge } from './bridge.js'
import { MYSQL_FAMILY_CAPABILITIES } from '@semilayer/bridge-sdk'
import {
  aggregateFixture,
  geoFixture,
  joinChildFixture,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'
const JOIN_CHILD_TABLE = 'sl_agg_join_child'
const GEO_TABLE = 'sl_agg_geo_fixture'
const ENABLE_GEOHASH = process.env['MARIADB_GEOHASH'] === '1'

describe.skipIf(!DATABASE_URL)('MariadbBridge aggregate integration', () => {
  let setup: mariadb.Connection
  let bridge: MariadbBridge

  beforeAll(async () => {
    setup = await mariadb.createConnection(DATABASE_URL!)
    await setup.query(`DROP TABLE IF EXISTS \`${TABLE}\``)
    await setup.query(`
      CREATE TABLE \`${TABLE}\` (
        id          INT PRIMARY KEY,
        cuisine     VARCHAR(50) NULL,
        country     VARCHAR(10) NOT NULL,
        rating      INT NOT NULL,
        prepTime    DECIMAL(10,2) NOT NULL,
        views       INT NOT NULL,
        status      VARCHAR(20) NOT NULL,
        createdAt   DATETIME NOT NULL
      )
    `)

    for (const r of aggregateFixture()) {
      await setup.query(
        `INSERT INTO \`${TABLE}\` (id, cuisine, country, rating, prepTime, views, status, createdAt)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [r.id, r.cuisine, r.country, r.rating, r.prepTime, r.views, r.status, r.createdAt],
      )
    }

    await setup.query(`DROP TABLE IF EXISTS \`${JOIN_CHILD_TABLE}\``)
    await setup.query(`
      CREATE TABLE \`${JOIN_CHILD_TABLE}\` (
        pk     INT PRIMARY KEY,
        region VARCHAR(10) NOT NULL,
        tier   VARCHAR(10) NOT NULL
      )
    `)
    for (const r of joinChildFixture()) {
      await setup.query(
        `INSERT INTO \`${JOIN_CHILD_TABLE}\` (pk, region, tier) VALUES (?, ?, ?)`,
        [r.pk, r.region, r.tier],
      )
    }

    await setup.query(`DROP TABLE IF EXISTS \`${GEO_TABLE}\``)
    await setup.query(`
      CREATE TABLE \`${GEO_TABLE}\` (
        id   INT PRIMARY KEY,
        lat  DOUBLE NOT NULL,
        lng  DOUBLE NOT NULL,
        city VARCHAR(50) NOT NULL
      )
    `)
    for (const r of geoFixture()) {
      await setup.query(
        `INSERT INTO \`${GEO_TABLE}\` (id, lat, lng, city) VALUES (?, ?, ?, ?)`,
        [r.id, r.lat, r.lng, r.city],
      )
    }

    bridge = new MariadbBridge({ url: DATABASE_URL!, enableGeohash: ENABLE_GEOHASH })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.query(`DROP TABLE IF EXISTS \`${TABLE}\``)
    await setup?.query(`DROP TABLE IF EXISTS \`${JOIN_CHILD_TABLE}\``)
    await setup?.query(`DROP TABLE IF EXISTS \`${GEO_TABLE}\``)
    await setup?.end()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: ENABLE_GEOHASH
      ? { ...MYSQL_FAMILY_CAPABILITIES, geoBucket: true, geohashBucket: true }
      : MYSQL_FAMILY_CAPABILITIES,
    joinChildFixtureTarget: JOIN_CHILD_TABLE,
    geoFixtureTarget: GEO_TABLE,
  })
})
