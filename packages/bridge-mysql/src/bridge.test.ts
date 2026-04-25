import { describe, it, expect, vi, beforeEach } from 'vitest'
import { MysqlBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock mysql2/promise — pool.query / pool.execute both delegate to the same
// shared mock so callers can mix-and-match (read() and query() use pool.query
// to dodge the LIMIT-prepared-stmt footgun; introspection helpers still use
// pool.execute).
// ---------------------------------------------------------------------------

const mockExecute = vi.fn()
const mockRelease = vi.fn()
const mockGetConnection = vi.fn().mockResolvedValue({
  execute: vi.fn().mockResolvedValue([[{ 1: 1 }], []]),
  release: mockRelease,
})
const mockEnd = vi.fn()

vi.mock('mysql2/promise', () => ({
  default: {
    createPool: vi.fn(() => ({
      execute: mockExecute,
      query: mockExecute,
      getConnection: mockGetConnection,
      end: mockEnd,
    })),
  },
}))

// Helper: seed PK detection response
function seedPK(col = 'id'): void {
  mockExecute.mockResolvedValueOnce([[{ column_name: col }], []])
}

// Helper: seed a SELECT result
function seedRows(rows: Record<string, unknown>[]): void {
  mockExecute.mockResolvedValueOnce([rows, []])
}

// Helper: seed a COUNT result
function seedCount(n: number): void {
  mockExecute.mockResolvedValueOnce([[{ total: n }], []])
}

async function createConnectedBridge(): Promise<MysqlBridge> {
  const bridge = new MysqlBridge({ url: 'mysql://root:pass@localhost/test' })
  await bridge.connect()
  return bridge
}

describe('MysqlBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Default: mock the getConnection connectivity check
    // connect() calls conn.query('SELECT 1') for the health check
    mockGetConnection.mockResolvedValueOnce({
      query: vi.fn().mockResolvedValue([[{ 1: 1 }], []]),
      execute: vi.fn().mockResolvedValue([[{ 1: 1 }], []]),
      release: mockRelease,
    })
  })

  describe('constructor', () => {
    it('throws when url and host are both missing', () => {
      expect(() => new MysqlBridge({})).toThrow(
        'MysqlBridge requires either a "url" or ("host" + "database") config',
      )
    })

    it('accepts url config', () => {
      expect(
        () => new MysqlBridge({ url: 'mysql://localhost/test' }),
      ).not.toThrow()
    })

    it('accepts host + database config', () => {
      expect(
        () => new MysqlBridge({ host: 'localhost', database: 'test' }),
      ).not.toThrow()
    })
  })

  describe('connect / disconnect', () => {
    it('connect() creates pool and verifies connectivity', async () => {
      await createConnectedBridge()
      expect(mockGetConnection).toHaveBeenCalled()
      expect(mockRelease).toHaveBeenCalled()
    })

    it('disconnect() ends the pool', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockEnd).toHaveBeenCalled()
    })

    it('read() throws when not connected', async () => {
      const bridge = new MysqlBridge({ url: 'mysql://localhost/test' })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering and backtick quoting', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ])
      seedCount(5)

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      // Call index 0: PK query, call index 1: SELECT, call index 2: COUNT
      const selectCall = mockExecute.mock.calls[1]!
      expect(selectCall[0]).toContain('ORDER BY `id` ASC')
      expect(selectCall[0]).toContain('LIMIT ?')
      expect(selectCall[0]).toContain('FROM `items`')
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows([{ id: 1, name: 'a' }])
      seedCount(1)

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor in WHERE clause', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows([{ id: 4, name: 'd' }])
      seedCount(5)

      await bridge.read('items', { limit: 10, cursor: '3' })

      const selectCall = mockExecute.mock.calls[1]!
      expect(selectCall[0]).toContain('`id` > ?')
      expect(selectCall[1]![0]).toBe('3')
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

      seedCount(42)

      const count = await bridge.count('items')
      expect(count).toBe(42)
    })
  })

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedRows([{ id: 1, status: 'active' }])
      seedCount(1)

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('`status` = ?')
      expect(dataCall[1]![0]).toBe('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` > ?')
      expect(dataCall[1]![0]).toBe(18)
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gte: 21 } } })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` >= ?')
    })

    it('builds WHERE with $lt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lt: 65 } } })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` < ?')
    })

    it('builds WHERE with $lte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lte: 64 } } })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` <= ?')
    })

    it('builds WHERE with $in operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('`status` IN (?)')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow('Unknown operator "$invalid"')
    })

    it('builds ORDER BY clause', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY `name` ASC, `created_at` DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY `id` DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: { cuisine: 'asc', title: 'desc' } as unknown as never,
      })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY `cuisine` ASC, `title` DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('LIMIT ?')
      expect(dataCall[0]).toContain('OFFSET ?')
      expect(dataCall[1]).toContain(10)
      expect(dataCall[1]).toContain(20)
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { select: ['id', 'name'] })

      const dataCall = mockExecute.mock.calls[0]!
      expect(dataCall[0]).toContain('SELECT `id`, `name`')
    })
  })
})
