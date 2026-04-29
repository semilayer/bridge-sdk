import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { TursoBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock @libsql/client
// ---------------------------------------------------------------------------

const mockExecute = vi.fn()
const mockClose = vi.fn()

vi.mock('@libsql/client', () => ({
  createClient: vi.fn(() => ({ execute: mockExecute, close: mockClose })),
}))

// ResultSet shape: { rows, columns, rowsAffected }
// Rows are array-like — we pass plain arrays and the bridge maps via columns index.
function seedResult(columns: string[], rowArrays: unknown[][]): void {
  const rows = rowArrays.map(arr => arr) // keep as arrays
  mockExecute.mockResolvedValueOnce({ rows, columns, rowsAffected: 0 })
}

// Seed a simple SELECT 1 result for connect()
function seedConnect(): void {
  seedResult(['1'], [[1]])
}

// Seed PK response: pragma_table_info returns { name }
function seedPK(col = 'id'): void {
  seedResult(['name'], [[col]])
}

// Seed a table of rows with given column names
function seedRows(columns: string[], rowArrays: unknown[][]): void {
  seedResult(columns, rowArrays)
}

// Seed a count result: SELECT count(*) as total
function seedCount(n: number): void {
  seedResult(['total'], [[n]])
}

async function createConnectedBridge(): Promise<TursoBridge> {
  seedConnect()
  const bridge = new TursoBridge({ url: 'libsql://db.turso.io', authToken: 'tok' })
  await bridge.connect()
  return bridge
}

describe('TursoBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // -------------------------------------------------------------------------
  // constructor
  // -------------------------------------------------------------------------

  describe('constructor', () => {
    it('throws when url is missing', () => {
      expect(() => new TursoBridge({})).toThrow(
        'TursoBridge requires a "url" config string',
      )
    })

    it('accepts valid config', () => {
      expect(
        () => new TursoBridge({ url: 'libsql://db.turso.io' }),
      ).not.toThrow()
    })
  })

  // -------------------------------------------------------------------------
  // connect / disconnect
  // -------------------------------------------------------------------------

  describe('connect / disconnect', () => {
    it('connect() creates client and verifies connectivity', async () => {
      const { createClient } = await import('@libsql/client')
      seedConnect()
      const bridge = new TursoBridge({ url: 'libsql://db.turso.io', authToken: 'mytoken' })
      await bridge.connect()

      expect(createClient).toHaveBeenCalledWith({
        url: 'libsql://db.turso.io',
        authToken: 'mytoken',
      })
      expect(mockExecute).toHaveBeenCalledWith({ sql: 'SELECT 1', args: [] })
    })

    it('disconnect() closes client and clears cache', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockClose).toHaveBeenCalled()
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })

    it('read() throws when not connected', async () => {
      const bridge = new TursoBridge({ url: 'libsql://db.turso.io' })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  // -------------------------------------------------------------------------
  // read()
  // -------------------------------------------------------------------------

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows(['id', 'name'], [[1, 'a'], [2, 'b'], [3, 'c']]) // limit+1 sentinel
      seedCount(5)

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      const selectCall = mockExecute.mock.calls[2]! // 0=connect, 1=PK, 2=SELECT
      expect(selectCall[0].sql).toContain('ORDER BY "id" ASC')
      expect(selectCall[0].sql).toContain('LIMIT ?')
      expect(selectCall[0].args).toContain(3) // limit+1
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows(['id', 'name'], [[1, 'a']])
      seedCount(1)

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor in WHERE clause', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows(['id', 'name'], [[4, 'd']])
      seedCount(5)

      await bridge.read('items', { limit: 10, cursor: '3' })

      const selectCall = mockExecute.mock.calls[2]!
      expect(selectCall[0].sql).toContain('"id" > ?')
      expect(selectCall[0].args).toContain('3')
    })

    it('rejects invalid table names', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.read('DROP TABLE--')).rejects.toThrow('Invalid table name')
    })

    it('uses PK cache on second call', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows(['id'], [])
      seedCount(0)
      await bridge.read('items', { limit: 10 })

      const callCount = mockExecute.mock.calls.length

      // Second read — PK cache hit
      seedRows(['id'], [])
      seedCount(0)
      await bridge.read('items', { limit: 10 })

      expect(mockExecute.mock.calls.length).toBe(callCount + 2)
    })
  })

  // -------------------------------------------------------------------------
  // count()
  // -------------------------------------------------------------------------

  describe('count()', () => {
    it('returns row count', async () => {
      const bridge = await createConnectedBridge()

      seedCount(99)

      const count = await bridge.count('items')
      expect(count).toBe(99)

      const call = mockExecute.mock.calls[1]!
      expect(call[0].sql).toContain('count(*) as total')
      expect(call[0].sql).toContain('"items"')
    })

    it('throws on invalid table name', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.count("'; DROP--")).rejects.toThrow('Invalid table name')
    })

    it('count(target, {where}) calls SELECT count(*) with WHERE', async () => {
      const bridge = await createConnectedBridge()

      seedCount(7)

      const count = await bridge.count('items', {
        where: { status: 'active' },
      })
      expect(count).toBe(7)

      // index 0 = connect, 1 = the count call
      const call = mockExecute.mock.calls[1]!
      expect(call[0].sql).toContain('count(*) as total')
      expect(call[0].sql).toContain('FROM "items"')
      expect(call[0].sql).toContain('"status" = ?')
      expect(call[0].args).toContain('active')
    })
  })

  // -------------------------------------------------------------------------
  // query()
  // -------------------------------------------------------------------------

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id', 'status'], [[1, 'active']])
      seedCount(1)

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('"status" = ?')
      expect(dataCall[0].args).toContain('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('"age" > ?')
      expect(dataCall[0].args).toContain(18)
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { where: { score: { $gte: 90 } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('"score" >= ?')
    })

    it('builds WHERE with $lt and $lte operators', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lt: 65, $lte: 64 } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('"age" < ?')
      expect(dataCall[0].sql).toContain('"age" <= ?')
    })

    it('builds WHERE with $in operator (individual ? placeholders)', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataCall = mockExecute.mock.calls[1]!
      // buildWhereSql emits `IN (?, ?)` (with a space after the comma)
      expect(dataCall[0].sql).toContain('"status" IN (?, ?)')
      expect(dataCall[0].args).toContain('active')
      expect(dataCall[0].args).toContain('pending')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('builds OR SQL via $or logical op', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', {
        where: { $or: [{ status: 'active' }, { status: 'pending' }] },
      })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toMatch(/"status" = \?\) OR \("status" = \?/)
      expect(dataCall[0].args).toContain('active')
      expect(dataCall[0].args).toContain('pending')
    })

    it('builds NOT SQL via $not logical op', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { where: { $not: { status: 'archived' } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('NOT ("status" = ?)')
      expect(dataCall[0].args).toContain('archived')
    })

    it('builds $ilike via LOWER(col) LIKE LOWER(?)', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { where: { name: { $ilike: 'Foo%' } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('LOWER("name") LIKE LOWER(?)')
      expect(dataCall[0].args).toContain('Foo%')
    })

    it('builds ORDER BY with canonical array form', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('ORDER BY "id" DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', {
        orderBy: { name: 'asc', score: 'desc' } as unknown as never,
      })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('ORDER BY "name" ASC, "score" DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id'], [])
      seedCount(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('LIMIT ?')
      expect(dataCall[0].sql).toContain('OFFSET ?')
      expect(dataCall[0].args).toContain(10)
      expect(dataCall[0].args).toContain(20)
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['id', 'name'], [])
      seedCount(0)

      await bridge.query('items', { select: ['id', 'name'] })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0].sql).toContain('SELECT "id", "name"')
    })
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  describe('listTargets()', () => {
    it('returns list of table names from sqlite_master', async () => {
      const bridge = await createConnectedBridge()

      seedRows(['name'], [['users'], ['posts']])

      const tables = await bridge.listTargets()
      expect(tables).toEqual(['users', 'posts'])
    })
  })
})
