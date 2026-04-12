import { describe, it, expect, vi, beforeEach } from 'vitest'
import { PlanetscaleBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock @planetscale/database
// ---------------------------------------------------------------------------

const mockExecute = vi.fn()

vi.mock('@planetscale/database', () => ({
  connect: vi.fn(() => ({ execute: mockExecute })),
}))

function seedResult(rows: Record<string, unknown>[]): void {
  mockExecute.mockResolvedValueOnce({ rows, fields: [], rowsAffected: 0 })
}

function seedPK(col = 'id'): void {
  seedResult([{ column_name: col }])
}

function seedRows(rows: Record<string, unknown>[]): void {
  seedResult(rows)
}

function seedCount(n: number): void {
  seedResult([{ total: n }])
}

async function createConnectedBridge(): Promise<PlanetscaleBridge> {
  // connect() calls SELECT 1
  seedResult([{ '1': 1 }])
  const bridge = new PlanetscaleBridge({ url: 'mysql://user:pass@aws.connect.psdb.cloud/mydb' })
  await bridge.connect()
  return bridge
}

describe('PlanetscaleBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // -------------------------------------------------------------------------
  // constructor
  // -------------------------------------------------------------------------

  describe('constructor', () => {
    it('throws when neither url nor host+username+password provided', () => {
      expect(() => new PlanetscaleBridge({})).toThrow(
        'PlanetscaleBridge requires either a "url" or "host"+"username"+"password" config',
      )
    })

    it('accepts url config', () => {
      expect(
        () => new PlanetscaleBridge({ url: 'mysql://user:pass@host/db' }),
      ).not.toThrow()
    })

    it('accepts host+username+password config', () => {
      expect(
        () =>
          new PlanetscaleBridge({
            host: 'aws.connect.psdb.cloud',
            username: 'user',
            password: 'pass',
          }),
      ).not.toThrow()
    })
  })

  // -------------------------------------------------------------------------
  // connect / disconnect
  // -------------------------------------------------------------------------

  describe('connect / disconnect', () => {
    it('connect() calls psConnect and verifies connectivity', async () => {
      const { connect } = await import('@planetscale/database')
      seedResult([{ '1': 1 }])
      const bridge = new PlanetscaleBridge({ url: 'mysql://user:pass@host/db' })
      await bridge.connect()
      expect(connect).toHaveBeenCalledWith({ url: 'mysql://user:pass@host/db' })
      expect(mockExecute).toHaveBeenCalledWith('SELECT 1', [])
    })

    it('disconnect() clears connection (HTTP — no socket)', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })

    it('read() throws when not connected', async () => {
      const bridge = new PlanetscaleBridge({ url: 'mysql://user:pass@host/db' })
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
      seedRows([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' }, // limit+1 sentinel
      ])
      seedCount(5)

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      const selectCall = mockExecute.mock.calls[2]! // 0=connect, 1=PK, 2=SELECT
      expect(selectCall[0]).toContain('ORDER BY `id` ASC')
      expect(selectCall[0]).toContain('LIMIT ?')
      expect(selectCall[1]).toContain(3) // limit+1
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

      const selectCall = mockExecute.mock.calls[2]!
      expect(selectCall[0]).toContain('`id` > ?')
      expect(selectCall[1]).toContain('3')
    })

    it('rejects invalid table names', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.read('DROP TABLE--')).rejects.toThrow('Invalid table name')
    })

    it('uses PK cache on second call', async () => {
      const bridge = await createConnectedBridge()

      seedPK('id')
      seedRows([])
      seedCount(0)
      await bridge.read('items', { limit: 10 })

      const callCount = mockExecute.mock.calls.length

      // Second read — PK cache hit
      seedRows([])
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

      seedCount(77)

      const count = await bridge.count('items')
      expect(count).toBe(77)

      const call = mockExecute.mock.calls[1]!
      expect(call[0]).toContain('COUNT(*) as total')
      expect(call[0]).toContain('`items`')
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

      seedRows([{ id: 1, status: 'active' }])
      seedCount(1)

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('`status` = ?')
      expect(dataCall[1]).toContain('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('`age` > ?')
      expect(dataCall[1]).toContain(18)
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { score: { $gte: 90 } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('`score` >= ?')
    })

    it('builds WHERE with $lt and $lte operators', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lt: 65, $lte: 64 } } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('`age` < ?')
      expect(dataCall[0]).toContain('`age` <= ?')
    })

    it('builds WHERE with $in operator (individual ? placeholders)', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('`status` IN (?,?)')
      expect(dataCall[1]).toContain('active')
      expect(dataCall[1]).toContain('pending')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow('Unknown operator "$invalid"')
    })

    it('builds ORDER BY with canonical array form', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('ORDER BY `name` ASC, `created_at` DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('ORDER BY `id` DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: { name: 'asc', score: 'desc' } as unknown as never,
      })

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('ORDER BY `name` ASC, `score` DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataCall = mockExecute.mock.calls[1]!
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

      const dataCall = mockExecute.mock.calls[1]!
      expect(dataCall[0]).toContain('SELECT `id`, `name`')
    })
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  describe('listTargets()', () => {
    it('returns list of table names', async () => {
      const bridge = await createConnectedBridge()

      seedRows([
        { table_name: 'users' },
        { table_name: 'posts' },
      ])

      const tables = await bridge.listTargets()
      expect(tables).toEqual(['users', 'posts'])
    })
  })
})
