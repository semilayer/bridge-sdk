/**
 * MySQL aggregate integration — runs the universal compliance suite
 * against the canonical fixture in a real MySQL instance.
 */
import mysql from 'mysql2/promise'
import { describe, beforeAll, afterAll } from 'vitest'
import { MysqlBridge } from './bridge.js'
import { MYSQL_FAMILY_CAPABILITIES } from '@semilayer/bridge-sdk'
import { aggregateFixture, runAggregateCompliance } from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'

describe.skipIf(!DATABASE_URL)('MysqlBridge aggregate integration', () => {
  let setup: mysql.Connection
  let bridge: MysqlBridge

  beforeAll(async () => {
    setup = await mysql.createConnection(DATABASE_URL!)
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

    bridge = new MysqlBridge({ url: DATABASE_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setup?.query(`DROP TABLE IF EXISTS \`${TABLE}\``)
    await setup?.end()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: MYSQL_FAMILY_CAPABILITIES,
  })
})
