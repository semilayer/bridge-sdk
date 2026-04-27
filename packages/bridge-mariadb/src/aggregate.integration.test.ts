/**
 * MariaDB aggregate integration — runs the universal compliance suite
 * against the canonical fixture in a real MariaDB instance.
 */
import * as mariadb from 'mariadb'
import { describe, beforeAll, afterAll } from 'vitest'
import { MariadbBridge } from './bridge.js'
import { MYSQL_FAMILY_CAPABILITIES } from '@semilayer/bridge-sdk'
import { aggregateFixture, runAggregateCompliance } from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'

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

    bridge = new MariadbBridge({ url: DATABASE_URL! })
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
