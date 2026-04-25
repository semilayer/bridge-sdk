/**
 * MongoDB integration tests.
 *
 * These tests run against a real MongoDB instance and are skipped when
 * DATABASE_URL is not set. Run locally with a live MongoDB or via the
 * GitHub Actions `integration-mongodb` workflow which spins one up.
 *
 *   DATABASE_URL=mongodb://127.0.0.1:27017/testdb pnpm test:integration
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { MongodbBridge } from './bridge.js'

const DATABASE_URL = process.env['DATABASE_URL']

// ---------------------------------------------------------------------------
// Test data — events with BSON Date timestamps. This mirrors the real-world
// shape that surfaced the $gt-vs-string bug: a feed engine sends an ISO
// string for the time window, the bridge passed it through unchanged, and
// MongoDB compared a string to a BSON Date and returned nothing.
// ---------------------------------------------------------------------------

const NOW = new Date('2026-04-25T12:00:00Z')
const MIN = 60_000
const HOUR = 60 * MIN
const DAY = 24 * HOUR

type Event = { user: string; type: string; timestamp: Date }

// 4 "old" events, well outside any 7d/24h window.
const OLD_EVENTS: Event[] = [
  { user: 'alice', type: 'view', timestamp: new Date(NOW.getTime() - 30 * DAY) },
  { user: 'bob', type: 'click', timestamp: new Date(NOW.getTime() - 60 * DAY) },
  { user: 'carol', type: 'view', timestamp: new Date(NOW.getTime() - 90 * DAY) },
  { user: 'dave', type: 'click', timestamp: new Date(NOW.getTime() - 120 * DAY) },
]

// 6 "recent" events, all inside a 24h window.
const RECENT_EVENTS: Event[] = [
  { user: 'alice', type: 'view', timestamp: new Date(NOW.getTime() - 1 * HOUR) },
  { user: 'alice', type: 'click', timestamp: new Date(NOW.getTime() - 2 * HOUR) },
  { user: 'bob', type: 'view', timestamp: new Date(NOW.getTime() - 3 * HOUR) },
  { user: 'bob', type: 'view', timestamp: new Date(NOW.getTime() - 5 * HOUR) },
  { user: 'carol', type: 'view', timestamp: new Date(NOW.getTime() - 12 * HOUR) },
  { user: 'dave', type: 'click', timestamp: new Date(NOW.getTime() - 20 * HOUR) },
]

const TOTAL = OLD_EVENTS.length + RECENT_EVENTS.length // 10

// ---------------------------------------------------------------------------

describe.skipIf(!DATABASE_URL)('MongodbBridge integration', () => {
  const COLLECTION = 'sl_itest_events'

  let setupClient: MongoClient
  let setupDb: Db
  let bridge: MongodbBridge

  // -------------------------------------------------------------------------
  // Setup / teardown
  // -------------------------------------------------------------------------

  beforeAll(async () => {
    setupClient = new MongoClient(DATABASE_URL!)
    await setupClient.connect()
    setupDb = setupClient.db()

    await setupDb.collection(COLLECTION).drop().catch(() => {
      /* collection may not exist on first run */
    })

    await setupDb
      .collection(COLLECTION)
      .insertMany([...OLD_EVENTS, ...RECENT_EVENTS])

    bridge = new MongodbBridge({ url: DATABASE_URL! })
    await bridge.connect()
  })

  afterAll(async () => {
    await bridge?.disconnect()
    await setupDb?.collection(COLLECTION).drop().catch(() => {
      /* ignore */
    })
    await setupClient?.close()
  })

  // -------------------------------------------------------------------------
  // count()
  // -------------------------------------------------------------------------

  it('count() returns total document count', async () => {
    const n = await bridge.count(COLLECTION)
    expect(n).toBe(TOTAL)
  })

  // -------------------------------------------------------------------------
  // read()
  // -------------------------------------------------------------------------

  it('read() returns all documents and reports total', async () => {
    const result = await bridge.read(COLLECTION)
    expect(result.rows).toHaveLength(TOTAL)
    expect(result.total).toBe(TOTAL)
    // _id should be surfaced as a string
    for (const row of result.rows) {
      expect(typeof row['_id']).toBe('string')
    }
  })

  it('read() with changedSince filters to recent docs only', async () => {
    const since = new Date(NOW.getTime() - 1 * DAY)
    const result = await bridge.read(COLLECTION, {
      changedSince: since,
      changeTrackingColumn: 'timestamp',
    })
    expect(result.rows).toHaveLength(RECENT_EVENTS.length)
  })

  // -------------------------------------------------------------------------
  // query() — date coercion (the bug this PR fixes)
  // -------------------------------------------------------------------------

  describe('query() date coercion', () => {
    it('$gt with ISO-string operand matches BSON Date field (regression)', async () => {
      // This is the bug: feed engine sends an ISO string, bridge passes it
      // through, MongoDB compares string vs Date and returns []. Fixed by
      // coercing strings that match strict ISO 8601 to Date.
      const since = new Date(NOW.getTime() - 1 * DAY).toISOString()
      const result = await bridge.query(COLLECTION, {
        where: { timestamp: { $gt: since } },
      })
      expect(result.rows).toHaveLength(RECENT_EVENTS.length)
      expect(result.total).toBe(RECENT_EVENTS.length)
    })

    it('$gt with native Date still works (no double-wrap)', async () => {
      const since = new Date(NOW.getTime() - 1 * DAY)
      const result = await bridge.query(COLLECTION, {
        where: { timestamp: { $gt: since } },
      })
      expect(result.rows).toHaveLength(RECENT_EVENTS.length)
    })

    it('$gte / $lt / $lte with ISO strings all coerce', async () => {
      const oneDayAgo = new Date(NOW.getTime() - 1 * DAY).toISOString()
      const tenDaysAgo = new Date(NOW.getTime() - 10 * DAY).toISOString()

      const gte = await bridge.query(COLLECTION, {
        where: { timestamp: { $gte: oneDayAgo } },
      })
      expect(gte.rows).toHaveLength(RECENT_EVENTS.length)

      const lt = await bridge.query(COLLECTION, {
        where: { timestamp: { $lt: oneDayAgo } },
      })
      expect(lt.rows).toHaveLength(OLD_EVENTS.length)

      const lte = await bridge.query(COLLECTION, {
        where: { timestamp: { $lte: tenDaysAgo } },
      })
      expect(lte.rows).toHaveLength(OLD_EVENTS.length)
    })

    it('$in with mixed ISO strings + Dates coerces element-wise', async () => {
      // Pick two specific timestamps — one as string, one as Date — and prove
      // both match.
      const t1 = RECENT_EVENTS[0]!.timestamp.toISOString()
      const t2 = RECENT_EVENTS[1]!.timestamp
      const result = await bridge.query(COLLECTION, {
        where: { timestamp: { $in: [t1, t2] } },
      })
      expect(result.rows).toHaveLength(2)
    })

    it('non-date strings are NOT coerced (e.g. "2024" stays a string)', async () => {
      // user is a string field; the value "alice" must not be turned into
      // anything. This guards against an over-eager coercion regex.
      const result = await bridge.query(COLLECTION, {
        where: { user: { $eq: 'alice' } },
      })
      const expected = [...OLD_EVENTS, ...RECENT_EVENTS].filter(
        (e) => e.user === 'alice',
      ).length
      expect(result.rows).toHaveLength(expected)
    })
  })

  // -------------------------------------------------------------------------
  // query() — non-date filters (regression coverage)
  // -------------------------------------------------------------------------

  it('query() filters with shorthand equality', async () => {
    const result = await bridge.query(COLLECTION, {
      where: { type: 'click' },
    })
    const expected = [...OLD_EVENTS, ...RECENT_EVENTS].filter(
      (e) => e.type === 'click',
    ).length
    expect(result.rows).toHaveLength(expected)
  })

  it('query() filters with $in on string field', async () => {
    const result = await bridge.query(COLLECTION, {
      where: { user: { $in: ['alice', 'bob'] } },
    })
    const expected = [...OLD_EVENTS, ...RECENT_EVENTS].filter((e) =>
      ['alice', 'bob'].includes(e.user),
    ).length
    expect(result.rows).toHaveLength(expected)
  })

  it('query() orders by timestamp descending', async () => {
    const result = await bridge.query(COLLECTION, {
      orderBy: { field: 'timestamp', dir: 'desc' },
      limit: 3,
    })
    expect(result.rows).toHaveLength(3)
    const times = result.rows.map((r) => new Date(r['timestamp'] as Date).getTime())
    expect(times).toEqual([...times].sort((a, b) => b - a))
  })

  it('query() applies LIMIT and OFFSET', async () => {
    const all = await bridge.query(COLLECTION, {
      orderBy: { field: 'timestamp', dir: 'asc' },
    })
    const paged = await bridge.query(COLLECTION, {
      orderBy: { field: 'timestamp', dir: 'asc' },
      limit: 3,
      offset: 2,
    })
    expect(paged.rows).toHaveLength(3)
    expect(paged.rows[0]!['_id']).toBe(all.rows[2]!['_id'])
  })

  it('query() total reflects WHERE, not LIMIT', async () => {
    const result = await bridge.query(COLLECTION, {
      where: { type: 'view' },
      limit: 2,
    })
    expect(result.rows).toHaveLength(2)
    const expectedTotal = [...OLD_EVENTS, ...RECENT_EVENTS].filter(
      (e) => e.type === 'view',
    ).length
    expect(result.total).toBe(expectedTotal)
  })

  it('query() throws on unknown operators', async () => {
    await expect(
      bridge.query(COLLECTION, { where: { user: { $regex: '^a' } } }),
    ).rejects.toThrow('Unknown operator')
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  it('listTargets() includes the test collection', async () => {
    const targets = await bridge.listTargets()
    expect(targets).toContain(COLLECTION)
  })
})
