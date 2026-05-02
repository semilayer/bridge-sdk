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
import { MSSQL_CAPABILITIES } from '@semilayer/bridge-sdk'
import {
  aggregateFixture,
  joinChildFixture,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const DATABASE_URL = process.env['DATABASE_URL']
const TABLE = 'sl_agg_fixture'
const JOIN_CHILD_TABLE = 'sl_agg_join_child'

describe.skipIf(!DATABASE_URL)('MssqlBridge aggregate integration', () => {
  let setup: ConnectionPool
  let bridge: MssqlBridge

  beforeAll(async () => {
    // mssqlLib.connect(string) does not natively parse mssql:// URLs in
    // v11 — feed it a config object instead.
    const u = new URL(DATABASE_URL!)
    setup = await mssqlLib.connect({
      server: u.hostname,
      port: u.port ? parseInt(u.port, 10) : 1433,
      user: decodeURIComponent(u.username),
      password: decodeURIComponent(u.password),
      database: u.pathname.replace(/^\//, '') || 'master',
      options: { encrypt: false, trustServerCertificate: true },
    } as unknown as MssqlConfig)

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

    await setup.request().query(
      `IF OBJECT_ID('${JOIN_CHILD_TABLE}', 'U') IS NOT NULL DROP TABLE [${JOIN_CHILD_TABLE}]`,
    )
    await setup.request().query(`
      CREATE TABLE [${JOIN_CHILD_TABLE}] (
        [pk]     INT PRIMARY KEY,
        [region] VARCHAR(10) NOT NULL,
        [tier]   VARCHAR(10) NOT NULL
      )
    `)
    for (const r of joinChildFixture()) {
      await setup.request().query(
        `INSERT INTO [${JOIN_CHILD_TABLE}] ([pk], [region], [tier]) VALUES (${r.pk}, ${sqlEscape(r.region)}, ${sqlEscape(r.tier)})`,
      )
    }

    // Parse DATABASE_URL to MssqlBridge config.
    bridge = new MssqlBridge({ url: DATABASE_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    // mssql's `connect()` returns the global pool — bridge.disconnect()
    // would close it out from under our `setup` handle, so drop the
    // table first while both still point at a live pool.
    if (setup) {
      await setup.request().query(`IF OBJECT_ID('${TABLE}', 'U') IS NOT NULL DROP TABLE [${TABLE}]`)
      await setup.request().query(
        `IF OBJECT_ID('${JOIN_CHILD_TABLE}', 'U') IS NOT NULL DROP TABLE [${JOIN_CHILD_TABLE}]`,
      )
      await setup.close()
    }
    await bridge?.disconnect()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: MSSQL_CAPABILITIES,
    joinChildFixtureTarget: JOIN_CHILD_TABLE,
  })
})
