import { describe, it, expect, vi, beforeEach } from 'vitest'
import { MariadbBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock mariadb — pool.query + pool.getConnection
// ---------------------------------------------------------------------------

const mockQuery = vi.fn()
const mockRelease = vi.fn()
const mockGetConnection = vi.fn().mockResolvedValue({
  query: vi.fn().mockResolvedValue([{ 1: 1 }]),
  release: mockRelease,
})
const mockEnd = vi.fn()

vi.mock('mariadb', () => ({
  createPool: vi.fn(() => ({
    query: mockQuery,
    getConnection: mockGetConnection,
    end: mockEnd,
  })),
}))

// Helper: seed a PK detection response
function seedPK(col = 'id'): void {
  mockQuery.mockResolvedValueOnce([{ column_name: col }])
}

// Helper: seed a SELECT result
function seedRows(rows: Record<string, unknown>[]): void {
  mockQuery.mockResolvedValueOnce(rows)
}

// Helper: seed a COUNT result (mariadb returns BigInt for COUNT)
function seedCount(n: number): void {
  mockQuery.mockResolvedValueOnce([{ total: BigInt(n) }])
}

async function createConnectedBridge(): Promise<MariadbBridge> {
  const bridge = new MariadbBridge({ url: 'mariadb://root:pass@localhost/test' })
  await bridge.connect()
  return bridge
}

describe('MariadbBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Default: mock the getConnection connectivity check
    mockGetConnection.mockResolvedValueOnce({
      query: vi.fn().mockResolvedValue([{ 1: 1 }]),
      release: mockRelease,
    })
  })

  describe('constructor', () => {
    it('throws when url and host+database are both missing', () => {
      expect(() => new MariadbBridge({})).toThrow(
        'MariadbBridge requires either a "url" or ("host" + "database") config',
      )
    })

    it('accepts url config', () => {
      expect(
        () => new MariadbBridge({ url: 'mariadb://localhost/test' }),
      ).not.toThrow()
    })

    it('accepts host + database config', () => {
      expect(
        () => new MariadbBridge({ host: 'localhost', database: 'test' }),
      ).not.toThrow()
    })
  })

  describe('connect / disconnect', () => {
    it('connect() creates pool and verifies connectivity via getConnection', async () => {
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
      const bridge = new MariadbBridge({ url: 'mariadb://localhost/test' })
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
      const selectCall = mockQuery.mock.calls[1]!
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

      const selectCall = mockQuery.mock.calls[1]!
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
    it('returns row count as a number (coercing BigInt)', async () => {
      const bridge = await createConnectedBridge()

      seedCount(42)

      const count = await bridge.count('items')
      expect(count).toBe(42)
      expect(typeof count).toBe('number')
    })
  })

  describe('disconnect()', () => {
    it('calls pool.end() and clears the pool reference', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockEnd).toHaveBeenCalledOnce()
    })

    it('is safe to call when already disconnected', async () => {
      const bridge = new MariadbBridge({ url: 'mariadb://localhost/test' })
      // pool is null — should not throw
      await expect(bridge.disconnect()).resolves.toBeUndefined()
    })
  })

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedRows([{ id: 1, status: 'active' }])
      seedCount(1)

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('`status` = ?')
      expect(dataCall[1]![0]).toBe('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` > ?')
      expect(dataCall[1]![0]).toBe(18)
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gte: 21 } } })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` >= ?')
    })

    it('builds WHERE with $lt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lt: 65 } } })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` < ?')
    })

    it('builds WHERE with $lte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lte: 64 } } })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('`age` <= ?')
    })

    it('builds WHERE with $in operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataCall = mockQuery.mock.calls[0]!
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

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY `name` ASC, `created_at` DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY `id` DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: { cuisine: 'asc', title: 'desc' } as unknown as never,
      })

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY `cuisine` ASC, `title` DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataCall = mockQuery.mock.calls[0]!
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

      const dataCall = mockQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('SELECT `id`, `name`')
    })
  })

  describe('introspectTarget()', () => {
    it('maps SHOW COLUMNS output to TargetSchema', async () => {
      const bridge = await createConnectedBridge()

      mockQuery.mockResolvedValueOnce([
        { Field: 'id', Type: 'int(11)', Null: 'NO', Key: 'PRI' },
        { Field: 'name', Type: 'varchar(255)', Null: 'YES', Key: '' },
      ])
      seedCount(7)

      const schema = await bridge.introspectTarget('users')

      expect(schema.name).toBe('users')
      expect(schema.rowCount).toBe(7)
      expect(schema.columns).toHaveLength(2)
      expect(schema.columns[0]).toMatchObject({
        name: 'id',
        type: 'int(11)',
        nullable: false,
        primaryKey: true,
      })
      expect(schema.columns[1]).toMatchObject({
        name: 'name',
        type: 'varchar(255)',
        nullable: true,
        primaryKey: false,
      })
    })
  })

  describe('listTargets()', () => {
    it('returns list of base table names', async () => {
      const bridge = await createConnectedBridge()

      mockQuery.mockResolvedValueOnce([
        { table_name: 'users' },
        { table_name: 'orders' },
      ])

      const tables = await bridge.listTargets()
      expect(tables).toEqual(['users', 'orders'])
    })
  })
})
