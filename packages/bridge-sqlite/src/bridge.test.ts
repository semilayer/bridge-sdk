import { describe, it, expect, vi, beforeEach } from 'vitest'
import { SqliteBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock better-sqlite3 — sync API
// ---------------------------------------------------------------------------

const mockAll = vi.fn()
const mockGet = vi.fn()
const mockPrepare = vi.fn((_sql: string) => ({ all: mockAll, get: mockGet }))
const mockClose = vi.fn()

vi.mock('better-sqlite3', () => ({
  default: vi.fn(() => ({ prepare: mockPrepare, close: mockClose })),
}))

// Seed helpers
function seedGet(row: unknown): void {
  mockGet.mockReturnValueOnce(row)
}
function seedAll(rows: unknown[]): void {
  mockAll.mockReturnValueOnce(rows)
}

async function createConnectedBridge(): Promise<SqliteBridge> {
  // connect() calls prepare('SELECT 1').get() for health check
  seedGet({ 1: 1 })
  const bridge = new SqliteBridge({ path: ':memory:' })
  await bridge.connect()
  return bridge
}

describe('SqliteBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('constructor', () => {
    it('throws when path is missing', () => {
      expect(() => new SqliteBridge({})).toThrow(
        'SqliteBridge requires a "path" config string',
      )
    })

    it('accepts valid config', () => {
      expect(() => new SqliteBridge({ path: ':memory:' })).not.toThrow()
    })
  })

  describe('connect / disconnect', () => {
    it('connect() opens database and verifies connectivity', async () => {
      seedGet({ 1: 1 })
      const bridge = new SqliteBridge({ path: ':memory:' })
      await bridge.connect()
      expect(mockPrepare).toHaveBeenCalledWith('SELECT 1')
      expect(mockGet).toHaveBeenCalled()
    })

    it('disconnect() closes the database', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockClose).toHaveBeenCalled()
    })

    it('read() throws when not connected', async () => {
      const bridge = new SqliteBridge({ path: ':memory:' })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering', async () => {
      const bridge = await createConnectedBridge()

      // PK detection: pragma_table_info get
      seedGet({ name: 'id' })
      // SELECT rows
      seedAll([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ])
      // COUNT get
      seedGet({ total: 5 })

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      // Verify the SELECT statement used double-quote identifiers and ORDER BY
      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find(
        (s) => s.includes('"items"') && s.includes('ORDER BY'),
      )
      expect(selectCall).toContain('ORDER BY "id" ASC')
      expect(selectCall).toContain('FROM "items"')
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedGet({ name: 'id' })
      seedAll([{ id: 1, name: 'a' }])
      seedGet({ total: 1 })

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor in WHERE clause', async () => {
      const bridge = await createConnectedBridge()

      seedGet({ name: 'id' })
      seedAll([{ id: 4, name: 'd' }])
      seedGet({ total: 5 })

      await bridge.read('items', { limit: 10, cursor: '3' })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find(
        (s) => s.includes('"items"') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('"id" > ?')
    })

    it('rejects invalid table names', async () => {
      const bridge = await createConnectedBridge()

      await expect(bridge.read('DROP TABLE--')).rejects.toThrow(
        'Invalid table name',
      )
    })
  })

  describe('count()', () => {
    it('returns row count', async () => {
      const bridge = await createConnectedBridge()

      seedGet({ total: 42 })

      const count = await bridge.count('items')
      expect(count).toBe(42)
    })
  })

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedAll([{ id: 1, status: 'active' }])
      seedGet({ total: 1 })

      await bridge.query('items', { where: { status: 'active' } })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find(
        (s) => s.includes('SELECT') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('"status" = ?')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedAll([])
      seedGet({ total: 0 })

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find(
        (s) => s.includes('SELECT') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('"age" > ?')
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()
      seedAll([])
      seedGet({ total: 0 })
      await bridge.query('items', { where: { age: { $gte: 21 } } })
      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" >= ?')
    })

    it('builds WHERE with $lt operator', async () => {
      const bridge = await createConnectedBridge()
      seedAll([])
      seedGet({ total: 0 })
      await bridge.query('items', { where: { age: { $lt: 65 } } })
      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" < ?')
    })

    it('builds WHERE with $lte operator', async () => {
      const bridge = await createConnectedBridge()
      seedAll([])
      seedGet({ total: 0 })
      await bridge.query('items', { where: { age: { $lte: 64 } } })
      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" <= ?')
    })

    it('builds WHERE with $in operator', async () => {
      const bridge = await createConnectedBridge()

      seedAll([])
      seedGet({ total: 0 })

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find(
        (s) => s.includes('SELECT') && s.includes('IN'),
      )
      expect(selectCall).toContain('"status" IN (?,?)')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow('Unknown operator "$invalid"')
    })

    it('builds ORDER BY clause', async () => {
      const bridge = await createConnectedBridge()

      seedAll([])
      seedGet({ total: 0 })

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find((s) => s.includes('ORDER BY'))
      expect(selectCall).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedAll([])
      seedGet({ total: 0 })

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find((s) => s.includes('ORDER BY'))
      expect(selectCall).toContain('ORDER BY "id" DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedAll([])
      seedGet({ total: 0 })

      await bridge.query('items', { limit: 10, offset: 20 })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find((s) => s.includes('LIMIT'))
      expect(selectCall).toContain('LIMIT ?')
      expect(selectCall).toContain('OFFSET ?')
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedAll([])
      seedGet({ total: 0 })

      await bridge.query('items', { select: ['id', 'name'] })

      const prepareCalls = mockPrepare.mock.calls.map((c) => c[0] as string)
      const selectCall = prepareCalls.find(
        (s) => s.startsWith('SELECT') && s.includes('"id"'),
      )
      expect(selectCall).toContain('SELECT "id", "name"')
    })
  })
})
