/**
 * MariaDB integration tests.
 *
 * These tests run against a real MariaDB instance and are skipped when
 * DATABASE_URL is not set. Run locally with a live MariaDB or via the
 * GitHub Actions `integration-mariadb` workflow which spins one up.
 *
 *   DATABASE_URL=mariadb://user:pass@127.0.0.1:3306/testdb pnpm test:integration
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import * as mariadb from 'mariadb'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { MariadbBridge } from './bridge.js'

const DATABASE_URL = process.env['DATABASE_URL']

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------

// 4 "old" rows — updated_at explicitly set to 2020 so changedSince tests work.
const OLD_ROWS: [string, number, number, string][] = [
  ['Widget A', 9.99, 100, 'widgets'],
  ['Widget B', 14.99, 50, 'widgets'],
  ['Widget C', 4.99, 200, 'widgets'],
  ['Gadget X', 49.99, 10, 'gadgets'],
]

// 8 "new" rows — updated_at left as DEFAULT (current time).
const NEW_ROWS: [string, number, number, string][] = [
  ['Gadget Y', 99.99, 5, 'gadgets'],
  ['Gadget Z', 149.99, 3, 'gadgets'],
  ['Doohickey 1', 2.49, 500, 'misc'],
  ['Doohickey 2', 3.49, 400, 'misc'],
  ['Doohickey 3', 4.49, 300, 'misc'],
  ['Doohickey 4', 5.49, 200, 'misc'],
  ['Super Tool', 199.99, 1, 'tools'],
  ['Mega Tool', 299.99, 2, 'tools'],
]

const TOTAL_ROWS = OLD_ROWS.length + NEW_ROWS.length // 12

// ---------------------------------------------------------------------------

describe.skipIf(!DATABASE_URL)('MariadbBridge integration', () => {
  const TABLE = 'sl_itest_products'

  let setup: mariadb.Connection
  let bridge: MariadbBridge

  // -------------------------------------------------------------------------
  // Setup / teardown
  // -------------------------------------------------------------------------

  beforeAll(async () => {
    // Raw connection for DDL and seeding — bypass the bridge entirely.
    setup = await mariadb.createConnection(DATABASE_URL!)

    await setup.query(`DROP TABLE IF EXISTS \`${TABLE}\``)
    await setup.query(`
      CREATE TABLE \`${TABLE}\` (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        name        VARCHAR(255) NOT NULL,
        price       DECIMAL(10,2) NOT NULL,
        stock       INT NOT NULL DEFAULT 0,
        category    VARCHAR(100),
        updated_at  TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
      )
    `)

    // Old rows — explicit 2020 timestamp.
    for (const [name, price, stock, category] of OLD_ROWS) {
      await setup.query(
        `INSERT INTO \`${TABLE}\` (name, price, stock, category, updated_at)
         VALUES (?, ?, ?, ?, ?)`,
        [name, price, stock, category, new Date('2020-06-15T00:00:00Z')],
      )
    }

    // New rows — rely on DEFAULT CURRENT_TIMESTAMP.
    for (const [name, price, stock, category] of NEW_ROWS) {
      await setup.query(
        `INSERT INTO \`${TABLE}\` (name, price, stock, category) VALUES (?, ?, ?, ?)`,
        [name, price, stock, category],
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

  // -------------------------------------------------------------------------
  // count()
  // -------------------------------------------------------------------------

  it('count() returns total row count', async () => {
    const n = await bridge.count(TABLE)
    expect(n).toBe(TOTAL_ROWS)
  })

  // -------------------------------------------------------------------------
  // read()
  // -------------------------------------------------------------------------

  it('read() returns all rows when no options given', async () => {
    const result = await bridge.read(TABLE)
    expect(result.rows).toHaveLength(TOTAL_ROWS)
    expect(result.total).toBe(TOTAL_ROWS)
    expect(result.nextCursor).toBeUndefined()
    // Rows ordered by PK ascending
    const ids = result.rows.map((r) => Number(r['id']))
    expect(ids).toEqual([...ids].sort((a, b) => a - b))
  })

  it('read() paginates across three pages with nextCursor', async () => {
    const limit = 5

    // Page 1 — 5 rows, cursor points to id=5
    const p1 = await bridge.read(TABLE, { limit })
    expect(p1.rows).toHaveLength(5)
    expect(p1.total).toBe(TOTAL_ROWS)
    expect(p1.nextCursor).toBeDefined()

    // Page 2 — next 5 rows
    const p2 = await bridge.read(TABLE, { limit, cursor: p1.nextCursor })
    expect(p2.rows).toHaveLength(5)
    expect(p2.nextCursor).toBeDefined()

    // Every id in page2 must be strictly greater than every id in page1
    const maxP1 = Math.max(...p1.rows.map((r) => Number(r['id'])))
    const minP2 = Math.min(...p2.rows.map((r) => Number(r['id'])))
    expect(minP2).toBeGreaterThan(maxP1)

    // Page 3 — remaining 2 rows, no further cursor
    const p3 = await bridge.read(TABLE, { limit, cursor: p2.nextCursor })
    expect(p3.rows).toHaveLength(2)
    expect(p3.nextCursor).toBeUndefined()

    // Concatenated, pages cover all 12 rows with no overlap
    const allIds = [...p1.rows, ...p2.rows, ...p3.rows].map((r) => Number(r['id']))
    expect(new Set(allIds).size).toBe(TOTAL_ROWS)
  })

  it('read() respects field projection', async () => {
    const result = await bridge.read(TABLE, { fields: ['name', 'price'], limit: 3 })
    expect(result.rows).toHaveLength(3)
    for (const row of result.rows) {
      expect(Object.keys(row)).toContain('name')
      expect(Object.keys(row)).toContain('price')
      expect(Object.keys(row)).not.toContain('stock')
      expect(Object.keys(row)).not.toContain('category')
    }
  })

  it('read() with changedSince filters to newer rows only', async () => {
    // All 4 OLD_ROWS have updated_at=2020; all NEW_ROWS have ~now.
    const since = new Date('2022-01-01T00:00:00Z')
    const result = await bridge.read(TABLE, { changedSince: since })
    expect(result.rows).toHaveLength(NEW_ROWS.length) // 8
    // None of the returned rows should be from the old-timestamp set
    const names = result.rows.map((r) => r['name'])
    for (const [name] of OLD_ROWS) {
      expect(names).not.toContain(name)
    }
  })

  it('read() with custom changeTrackingColumn', async () => {
    // Should behave identically to the default when column name is the same.
    const since = new Date('2022-01-01T00:00:00Z')
    const result = await bridge.read(TABLE, {
      changedSince: since,
      changeTrackingColumn: 'updated_at',
    })
    expect(result.rows).toHaveLength(NEW_ROWS.length)
  })

  // -------------------------------------------------------------------------
  // query()
  // -------------------------------------------------------------------------

  it('query() filters with simple equality ($eq)', async () => {
    const result = await bridge.query(TABLE, {
      where: { category: { $eq: 'gadgets' } },
    })
    expect(result.total).toBe(3)
    expect(result.rows).toHaveLength(3)
    expect(result.rows.every((r) => r['category'] === 'gadgets')).toBe(true)
  })

  it('query() filters with shorthand equality', async () => {
    const result = await bridge.query(TABLE, {
      where: { category: 'widgets' },
    })
    expect(result.rows).toHaveLength(3)
    expect(result.rows.every((r) => r['category'] === 'widgets')).toBe(true)
  })

  it('query() filters with $gt', async () => {
    // price > 100 → Gadget Z (149.99), Super Tool (199.99), Mega Tool (299.99)
    const result = await bridge.query(TABLE, {
      where: { price: { $gt: 100 } },
    })
    expect(result.rows).toHaveLength(3)
    expect(result.rows.every((r) => Number(r['price']) > 100)).toBe(true)
  })

  it('query() filters with $gte', async () => {
    // price >= 99.99 → Gadget Y, Z, Super Tool, Mega Tool = 4 rows
    const result = await bridge.query(TABLE, {
      where: { price: { $gte: 99.99 } },
    })
    expect(result.rows).toHaveLength(4)
    expect(result.rows.every((r) => Number(r['price']) >= 99.99)).toBe(true)
  })

  it('query() filters with $lt', async () => {
    // price < 5 → Widget C (4.99), Doohickey 1 (2.49), 2 (3.49), 3 (4.49) = 4 rows
    const result = await bridge.query(TABLE, {
      where: { price: { $lt: 5 } },
    })
    expect(result.rows).toHaveLength(4)
    expect(result.rows.every((r) => Number(r['price']) < 5)).toBe(true)
  })

  it('query() filters with $lte', async () => {
    // price <= 5.49 → Widget C (4.99) + all 4 Doohickeys = 5 rows
    const result = await bridge.query(TABLE, {
      where: { price: { $lte: 5.49 } },
    })
    expect(result.rows).toHaveLength(5)
    expect(result.rows.every((r) => Number(r['price']) <= 5.49)).toBe(true)
  })

  it('query() filters with $in', async () => {
    // category IN ['tools', 'gadgets'] → 5 rows (3 gadgets + 2 tools)
    const result = await bridge.query(TABLE, {
      where: { category: { $in: ['tools', 'gadgets'] } },
    })
    expect(result.rows).toHaveLength(5)
    expect(
      result.rows.every((r) =>
        ['tools', 'gadgets'].includes(r['category'] as string),
      ),
    ).toBe(true)
  })

  it('query() orders by price descending', async () => {
    const result = await bridge.query(TABLE, {
      orderBy: { field: 'price', dir: 'desc' },
      limit: 3,
    })
    expect(result.rows).toHaveLength(3)
    // Mega Tool > Super Tool > Gadget Z
    expect(result.rows[0]!['name']).toBe('Mega Tool')
    expect(result.rows[1]!['name']).toBe('Super Tool')
    expect(result.rows[2]!['name']).toBe('Gadget Z')
  })

  it('query() orders by price ascending (shorthand)', async () => {
    const result = await bridge.query(TABLE, {
      orderBy: { field: 'price', dir: 'asc' },
      limit: 3,
    })
    // Doohickey 1 (2.49) < Doohickey 2 (3.49) < Doohickey 3 (4.49)
    expect(result.rows[0]!['name']).toBe('Doohickey 1')
    expect(result.rows[1]!['name']).toBe('Doohickey 2')
    expect(result.rows[2]!['name']).toBe('Doohickey 3')
  })

  it('query() applies LIMIT and OFFSET', async () => {
    // All rows ordered by id, take rows 3..5 (offset=2, limit=3)
    const all = await bridge.query(TABLE, { orderBy: { field: 'id', dir: 'asc' } })
    const paged = await bridge.query(TABLE, {
      orderBy: { field: 'id', dir: 'asc' },
      limit: 3,
      offset: 2,
    })
    expect(paged.rows).toHaveLength(3)
    expect(paged.rows[0]!['id']).toBe(all.rows[2]!['id'])
    expect(paged.rows[2]!['id']).toBe(all.rows[4]!['id'])
  })

  it('query() respects field selection', async () => {
    const result = await bridge.query(TABLE, {
      select: ['name', 'category'],
      limit: 5,
    })
    for (const row of result.rows) {
      expect(Object.keys(row)).toContain('name')
      expect(Object.keys(row)).toContain('category')
      expect(Object.keys(row)).not.toContain('price')
      expect(Object.keys(row)).not.toContain('stock')
    }
  })

  it('query() total reflects WHERE, not LIMIT', async () => {
    const result = await bridge.query(TABLE, {
      where: { category: 'misc' },
      limit: 2,
    })
    expect(result.rows).toHaveLength(2)   // only 2 fetched …
    expect(result.total).toBe(4)           // … but 4 exist matching WHERE
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  it('listTargets() includes the test table', async () => {
    const tables = await bridge.listTargets()
    expect(tables).toContain(TABLE)
  })

  it('listTargets() returns only base tables (no views)', async () => {
    const tables = await bridge.listTargets()
    // All entries should be strings, none empty
    expect(tables.every((t) => typeof t === 'string' && t.length > 0)).toBe(true)
  })

  // -------------------------------------------------------------------------
  // introspectTarget()
  // -------------------------------------------------------------------------

  it('introspectTarget() returns correct schema metadata', async () => {
    const schema = await bridge.introspectTarget(TABLE)

    expect(schema.name).toBe(TABLE)
    expect(schema.rowCount).toBe(TOTAL_ROWS)

    const byName = Object.fromEntries(schema.columns.map((c) => [c.name, c]))

    // id — INT AUTO_INCREMENT PRIMARY KEY
    expect(byName['id']!.primaryKey).toBe(true)
    expect(byName['id']!.nullable).toBe(false)

    // name — VARCHAR NOT NULL
    expect(byName['name']!.nullable).toBe(false)
    expect(byName['name']!.primaryKey).toBe(false)

    // category — VARCHAR NULL
    expect(byName['category']!.nullable).toBe(true)

    // All expected columns present
    const colNames = schema.columns.map((c) => c.name)
    for (const expected of ['id', 'name', 'price', 'stock', 'category', 'updated_at']) {
      expect(colNames).toContain(expected)
    }
  })

  // -------------------------------------------------------------------------
  // Error cases
  // -------------------------------------------------------------------------

  it('read() rejects invalid table names', async () => {
    await expect(bridge.read('bad-table-name!')).rejects.toThrow()
  })

  it('query() throws on unknown operators', async () => {
    await expect(
      bridge.query(TABLE, { where: { price: { $regex: '.*' } } }),
    ).rejects.toThrow(UnsupportedOperatorError)
  })
})
