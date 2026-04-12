import { describe, it, expect, vi, beforeEach } from 'vitest'
import { D1Bridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock global fetch
// ---------------------------------------------------------------------------

const mockFetch = vi.fn()
vi.stubGlobal('fetch', mockFetch)

function seedD1(results: Record<string, unknown>[]): void {
  mockFetch.mockResolvedValueOnce({
    json: () =>
      Promise.resolve({
        success: true,
        result: [{ results, success: true, meta: {} }],
        errors: [],
      }),
  })
}

function seedD1Error(message: string): void {
  mockFetch.mockResolvedValueOnce({
    json: () =>
      Promise.resolve({
        success: false,
        result: [],
        errors: [{ code: 1001, message }],
      }),
  })
}

const CONFIG = {
  accountId: 'acc123',
  databaseId: 'db456',
  apiToken: 'tok789',
}

async function createConnectedBridge(): Promise<D1Bridge> {
  // connect() calls SELECT 1
  seedD1([{ 1: 1 }])
  const bridge = new D1Bridge(CONFIG)
  await bridge.connect()
  return bridge
}

describe('D1Bridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // -------------------------------------------------------------------------
  // constructor
  // -------------------------------------------------------------------------

  describe('constructor', () => {
    it('throws when accountId is missing', () => {
      expect(() => new D1Bridge({ databaseId: 'x', apiToken: 'y' })).toThrow(
        'D1Bridge requires an "accountId" config string',
      )
    })

    it('throws when databaseId is missing', () => {
      expect(() => new D1Bridge({ accountId: 'x', apiToken: 'y' })).toThrow(
        'D1Bridge requires a "databaseId" config string',
      )
    })

    it('throws when apiToken is missing', () => {
      expect(() => new D1Bridge({ accountId: 'x', databaseId: 'y' })).toThrow(
        'D1Bridge requires an "apiToken" config string',
      )
    })

    it('accepts valid config', () => {
      expect(() => new D1Bridge(CONFIG)).not.toThrow()
    })
  })

  // -------------------------------------------------------------------------
  // connect / disconnect
  // -------------------------------------------------------------------------

  describe('connect / disconnect', () => {
    it('connect() calls the D1 REST API and verifies connectivity', async () => {
      seedD1([{ 1: 1 }])
      const bridge = new D1Bridge(CONFIG)
      await bridge.connect()

      expect(mockFetch).toHaveBeenCalledWith(
        `https://api.cloudflare.com/client/v4/accounts/acc123/d1/database/db456/query`,
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({
            Authorization: 'Bearer tok789',
            'Content-Type': 'application/json',
          }),
        }),
      )

      const body = JSON.parse(mockFetch.mock.calls[0]![1].body as string)
      expect(body.sql).toBe('SELECT 1')
    })

    it('disconnect() clears connected flag', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })

    it('read() throws when not connected', async () => {
      const bridge = new D1Bridge(CONFIG)
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })

    it('throws when D1 returns an error', async () => {
      seedD1Error('syntax error')
      const bridge = new D1Bridge(CONFIG)
      await expect(bridge.connect()).rejects.toThrow('syntax error')
    })
  })

  // -------------------------------------------------------------------------
  // read()
  // -------------------------------------------------------------------------

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ name: 'id' }]) // PK
      seedD1([{ id: 1, name: 'a' }, { id: 2, name: 'b' }, { id: 3, name: 'c' }]) // limit+1
      seedD1([{ total: 5 }]) // count

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      const selectBody = JSON.parse(mockFetch.mock.calls[2]![1].body as string)
      expect(selectBody.sql).toContain('ORDER BY "id" ASC')
      expect(selectBody.sql).toContain('LIMIT ?')
      expect(selectBody.params).toContain(3) // limit+1
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ name: 'id' }])
      seedD1([{ id: 1, name: 'a' }])
      seedD1([{ total: 1 }])

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor in WHERE clause', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ name: 'id' }])
      seedD1([{ id: 4, name: 'd' }])
      seedD1([{ total: 5 }])

      await bridge.read('items', { limit: 10, cursor: '3' })

      const selectBody = JSON.parse(mockFetch.mock.calls[2]![1].body as string)
      expect(selectBody.sql).toContain('"id" > ?')
      expect(selectBody.params).toContain('3')
    })

    it('rejects invalid table names', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.read('DROP TABLE--')).rejects.toThrow('Invalid table name')
    })

    it('uses PK cache on second call', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ name: 'id' }])
      seedD1([])
      seedD1([{ total: 0 }])
      await bridge.read('items', { limit: 10 })

      const callCount = mockFetch.mock.calls.length

      // Second read — PK from cache
      seedD1([])
      seedD1([{ total: 0 }])
      await bridge.read('items', { limit: 10 })

      expect(mockFetch.mock.calls.length).toBe(callCount + 2)
    })
  })

  // -------------------------------------------------------------------------
  // count()
  // -------------------------------------------------------------------------

  describe('count()', () => {
    it('returns row count', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ total: 55 }])

      const count = await bridge.count('items')
      expect(count).toBe(55)

      const body = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(body.sql).toContain('COUNT(*) as total')
      expect(body.sql).toContain('"items"')
    })

    it('throws on invalid table name', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.count("'; DROP--")).rejects.toThrow('Invalid table name')
    })
  })

  // -------------------------------------------------------------------------
  // query()
  // -------------------------------------------------------------------------

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ id: 1, status: 'active' }])
      seedD1([{ total: 1 }])

      await bridge.query('items', { where: { status: 'active' } })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('"status" = ?')
      expect(dataBody.params).toContain('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('"age" > ?')
      expect(dataBody.params).toContain(18)
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', { where: { score: { $gte: 90 } } })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('"score" >= ?')
    })

    it('builds WHERE with $lt and $lte operators', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', { where: { age: { $lt: 65, $lte: 64 } } })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('"age" < ?')
      expect(dataBody.sql).toContain('"age" <= ?')
    })

    it('builds WHERE with $in operator (individual ? placeholders)', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('"status" IN (?,?)')
      expect(dataBody.params).toContain('active')
      expect(dataBody.params).toContain('pending')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow('Unknown operator "$invalid"')
    })

    it('builds ORDER BY with canonical array form', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('ORDER BY "id" DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', {
        orderBy: { name: 'asc', score: 'desc' } as unknown as never,
      })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('ORDER BY "name" ASC, "score" DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('LIMIT ?')
      expect(dataBody.sql).toContain('OFFSET ?')
      expect(dataBody.params).toContain(10)
      expect(dataBody.params).toContain(20)
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedD1([])
      seedD1([{ total: 0 }])

      await bridge.query('items', { select: ['id', 'name'] })

      const dataBody = JSON.parse(mockFetch.mock.calls[1]![1].body as string)
      expect(dataBody.sql).toContain('SELECT "id", "name"')
    })
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  describe('listTargets()', () => {
    it('returns list of table names from sqlite_master', async () => {
      const bridge = await createConnectedBridge()

      seedD1([{ name: 'users' }, { name: 'posts' }])

      const tables = await bridge.listTargets()
      expect(tables).toEqual(['users', 'posts'])
    })
  })
})
