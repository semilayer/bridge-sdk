/**
 * SQL Server aggregate integration — seeds the canonical fixture into a
 * real SQL Server and runs the universal compliance suite.
 *
 * The MSSQL dialect bracket-quotes idents and uses `@p1`/`@p2`
 * placeholders. Skips percentile/sampling cases per declared caps.
 */
import mssqlLib from 'mssql'
import type { ConnectionPool, config as MssqlConfig } from 'mssql'
import { describe, beforeAll, afterAll } from 'vitest'
import { MssqlBridge } from './bridge.js'
import {
  aggregateFixture,
  MSSQL_CAPABILITIES,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'

describe.skipIf(!DATABASE_URL)('MssqlBridge aggregate integration', () => {
  let setup: ConnectionPool
  let bridge: MssqlBridge

  beforeAll(async () => {
    setup = await mssqlLib.connect(DATABASE_URL! as unknown as MssqlConfig)

    await setup.request().query(`IF OBJECT_ID('${TABLE}', 'U') IS NOT NULL DROP TABLE [${TABLE}]`)
    await setup.request().query(`
      CREATE TABLE [${TABLE}] (
        [id]        INT PRIMARY KEY,
        [cuisine]   VARCHAR(50) NULL,
        [country]   VARCHAR(10) NOT NULL,
        [rating]    INT NOT NULL,
        [prepTime]  DECIMAL(10,2) NOT NULL,
        [views]     INT NOT NULL,
        [status]    VARCHAR(20) NOT NULL,
        [createdAt] DATETIME2 NOT NULL
      )
    `)

    // Fixture seeding via raw INSERT with literal values — sidesteps the
    // mssql.Int / mssql.VarChar types which aren't reliably typed across
    // mssql v11 ESM builds. Strings are single-quoted with quote-doubling.
    const sqlEscape = (v: string): string => `'${v.replace(/'/g, "''")}'`
    for (const r of aggregateFixture()) {
      const cuisine = r.cuisine === null ? 'NULL' : sqlEscape(r.cuisine)
      const createdAt = sqlEscape(r.createdAt.toISOString())
      await setup.request().query(
        `INSERT INTO [${TABLE}] ([id], [cuisine], [country], [rating], [prepTime], [views], [status], [createdAt])
         VALUES (${r.id}, ${cuisine}, ${sqlEscape(r.country)}, ${r.rating}, ${r.prepTime}, ${r.views}, ${sqlEscape(r.status)}, ${createdAt})`,
      )
    }

    // Parse DATABASE_URL to MssqlBridge config.
    bridge = new MssqlBridge({ url: DATABASE_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    if (setup) {
      await setup.request().query(`IF OBJECT_ID('${TABLE}', 'U') IS NOT NULL DROP TABLE [${TABLE}]`)
      await setup.close()
    }
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: MSSQL_CAPABILITIES,
  })
})
