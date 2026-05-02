/**
 * SQLite aggregate tests — runs in-memory so no docker is required and
 * the suite ships in regular CI.
 */
import { describe, beforeAll, afterAll } from 'vitest'
import Database from 'better-sqlite3'
import { SqliteBridge } from './bridge.js'
import { SQLITE_FAMILY_CAPABILITIES } from '@semilayer/bridge-sdk'
import {
  aggregateFixture,
  joinChildFixture,
  runAggregateCompliance,
} from '@semilayer/bridge-sdk/testing'

const TABLE = 'sl_agg_fixture'
const JOIN_CHILD_TABLE = 'sl_agg_join_child'
const DB_PATH = ':memory:'

describe('SqliteBridge aggregate', () => {
  let setup: Database.Database
  let bridge: SqliteBridge

  beforeAll(async () => {
    // SqliteBridge opens its own connection so we share via a temp file —
    // ":memory:" can't be shared across two clients. Use a temp DB file.
    const fs = await import('node:fs/promises')
    const path = await import('node:path')
    const os = await import('node:os')
    const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'sl-sqlite-agg-'))
    const dbPath = path.join(tmp, 'fixture.db')

    setup = new Database(dbPath)
    setup.exec(`
      CREATE TABLE ${TABLE} (
        id          INTEGER PRIMARY KEY,
        cuisine     TEXT,
        country     TEXT NOT NULL,
        rating      INTEGER NOT NULL,
        prepTime    REAL NOT NULL,
        views       INTEGER NOT NULL,
        status      TEXT NOT NULL,
        createdAt   TEXT NOT NULL
      )
    `)

    const insert = setup.prepare(`
      INSERT INTO ${TABLE} (id, cuisine, country, rating, prepTime, views, status, createdAt)
      VALUES (@id, @cuisine, @country, @rating, @prepTime, @views, @status, @createdAt)
    `)
    for (const r of aggregateFixture()) {
      insert.run({
        id: r.id,
        cuisine: r.cuisine,
        country: r.country,
        rating: r.rating,
        prepTime: r.prepTime,
        views: r.views,
        status: r.status,
        createdAt: r.createdAt.toISOString(),
      })
    }

    setup.exec(`
      CREATE TABLE ${JOIN_CHILD_TABLE} (
        pk     INTEGER PRIMARY KEY,
        region TEXT NOT NULL,
        tier   TEXT NOT NULL
      )
    `)
    const insertChild = setup.prepare(
      `INSERT INTO ${JOIN_CHILD_TABLE} (pk, region, tier) VALUES (@pk, @region, @tier)`,
    )
    for (const r of joinChildFixture()) insertChild.run(r)

    bridge = new SqliteBridge({ path: dbPath })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    setup?.close()
  })

  runAggregateCompliance({
    getBridge: () => bridge,
    target: TABLE,
    capabilities: SQLITE_FAMILY_CAPABILITIES,
    joinChildFixtureTarget: JOIN_CHILD_TABLE,
  })
})

// Silence unused-import warning for the in-memory placeholder constant
void DB_PATH
