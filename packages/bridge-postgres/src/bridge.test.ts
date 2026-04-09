import { describe, it, expect, vi, beforeEach } from 'vitest'
import { PostgresBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock pg module — separate pool.query from client.query
// ---------------------------------------------------------------------------

const mockPoolQuery = vi.fn()
const mockClientQuery = vi.fn()
const mockRelease = vi.fn()
const mockConnect = vi.fn().mockResolvedValue({
  query: mockClientQuery,
  release: mockRelease,
})
const mockEnd = vi.fn()

vi.mock('pg', () => {
  class Pool {
    query = mockPoolQuery
    connect = mockConnect
    end = mockEnd
  }
  return { default: { Pool } }
})

// Helper: seed PK detection response
function seedPrimaryKey(column = 'id'): void {
  mockPoolQuery.mockResolvedValueOnce({
    rows: [{ column_name: column }],
    rowCount: 1,
  })
}

// Helper: seed a SELECT result
function seedSelectResult(rows: Record<string, unknown>[]): void {
  mockPoolQuery.mockResolvedValueOnce({ rows, rowCount: rows.length })
}

// Helper: seed a count result
function seedCountResult(total: number): void {
  mockPoolQuery.mockResolvedValueOnce({ rows: [{ total }], rowCount: 1 })
}

async function createConnectedBridge(): Promise<PostgresBridge> {
  const bridge = new PostgresBridge({ url: 'postgres://localhost/test' })
  await bridge.connect()
  return bridge
}

describe('PostgresBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Default: mock the SELECT 1 connectivity check (client.query)
    mockClientQuery.mockResolvedValueOnce({
      rows: [{ '?column?': 1 }],
      rowCount: 1,
    })
  })

  describe('constructor', () => {
    it('throws when url is missing', () => {
      expect(() => new PostgresBridge({})).toThrow(
        'PostgresBridge requires a "url" config string',
      )
    })

    it('accepts valid config', () => {
      expect(
        () => new PostgresBridge({ url: 'postgres://localhost/test' }),
      ).not.toThrow()
    })
  })

  describe('connect / disconnect', () => {
    it('connect() creates pool and verifies connectivity', async () => {
      await createConnectedBridge()
      expect(mockConnect).toHaveBeenCalled()
      expect(mockClientQuery).toHaveBeenCalledWith('SELECT 1')
      expect(mockRelease).toHaveBeenCalled()
    })

    it('disconnect() ends the pool', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockEnd).toHaveBeenCalled()
    })

    it('read() throws when not connected', async () => {
      const bridge = new PostgresBridge({ url: 'postgres://localhost/test' })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering', async () => {
      const bridge = await createConnectedBridge()

      seedPrimaryKey('id')
      seedSelectResult([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ])
      seedCountResult(5)

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      const selectCall = mockPoolQuery.mock.calls[1]!
      expect(selectCall[0]).toContain('ORDER BY "id" ASC')
      expect(selectCall[0]).toContain('LIMIT $1')
      expect(selectCall[1]).toEqual([3])
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedPrimaryKey('id')
      seedSelectResult([{ id: 1, name: 'a' }])
      seedCountResult(1)

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor in WHERE clause', async () => {
      const bridge = await createConnectedBridge()

      seedPrimaryKey('id')
      seedSelectResult([{ id: 4, name: 'd' }])
      seedCountResult(5)

      await bridge.read('items', { limit: 10, cursor: '3' })

      const selectCall = mockPoolQuery.mock.calls[1]!
      expect(selectCall[0]).toContain('"id" > $1')
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

      seedCountResult(42)

      const count = await bridge.count('items')
      expect(count).toBe(42)
    })
  })

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([{ id: 1, status: 'active' }])
      seedCountResult(1)

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('"status" = $1')
      expect(dataCall[1]![0]).toBe('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', {
        where: { age: { $gt: 18 } },
      })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('"age" > $1')
      expect(dataCall[1]![0]).toBe(18)
    })

    it('builds WHERE with $in operator', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('"status" = ANY($1)')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', {
          where: { age: { $invalid: 1 } },
        }),
      ).rejects.toThrow('Unknown operator "$invalid"')
    })

    it('builds ORDER BY clause', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('LIMIT $1')
      expect(dataCall[0]).toContain('OFFSET $2')
      expect(dataCall[1]).toEqual([10, 20])
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', { select: ['id', 'name'] })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('SELECT "id", "name"')
    })
  })
})
