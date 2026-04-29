import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { NeonBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock @neondatabase/serverless
// ---------------------------------------------------------------------------

const mockSql = vi.fn()

vi.mock('@neondatabase/serverless', () => ({
  neon: vi.fn(() => mockSql),
}))

// Seed a result with { rows: [...] } shape (fullResults: true)
function seedRows(rows: Record<string, unknown>[]): void {
  mockSql.mockResolvedValueOnce({ rows, rowCount: rows.length })
}

function seedPK(col = 'id'): void {
  seedRows([{ column_name: col }])
}

function seedCount(n: number): void {
  seedRows([{ total: n }])
}

async function createConnectedBridge(): Promise<NeonBridge> {
  // connect() calls SELECT 1
  seedRows([{ '?column?': 1 }])
  const bridge = new NeonBridge({ url: 'postgres://user:pass@ep-xxx.us-east-1.aws.neon.tech/neondb' })
  await bridge.connect()
  return bridge
}

describe('NeonBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // -------------------------------------------------------------------------
  // constructor
  // -------------------------------------------------------------------------

  describe('constructor', () => {
    it('throws when url is missing', () => {
      expect(() => new NeonBridge({})).toThrow(
        'NeonBridge requires a "url" config string',
      )
    })

    it('accepts valid config', () => {
      expect(
        () => new NeonBridge({ url: 'postgres://localhost/test' }),
      ).not.toThrow()
    })
  })

  // -------------------------------------------------------------------------
  // connect / disconnect
  // -------------------------------------------------------------------------

  describe('connect / disconnect', () => {
    it('connect() calls neon() factory and verifies connectivity', async () => {
      const { neon } = await import('@neondatabase/serverless')
      seedRows([{ '?column?': 1 }])
      const bridge = new NeonBridge({ url: 'postgres://localhost/test' })
      await bridge.connect()
      expect(neon).toHaveBeenCalledWith('postgres://localhost/test', {
        fullResults: true,
        arrayMode: false,
      })
      expect(mockSql).toHaveBeenCalledWith('SELECT 1')
    })

    it('disconnect() clears sql and pkCache (HTTP — stateless)', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })

    it('read() throws when not connected', async () => {
      const bridge = new NeonBridge({ url: 'postgres://localhost/test' })
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

      const selectCall = mockSql.mock.calls[2]! // 0=connect SELECT1, 1=PK, 2=SELECT
      expect(selectCall[0]).toContain('ORDER BY "id" ASC')
      expect(selectCall[0]).toContain('LIMIT $1')
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

      const selectCall = mockSql.mock.calls[2]!
      expect(selectCall[0]).toContain('"id" > $1')
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

      const callCount = mockSql.mock.calls.length

      // Second read — no PK query (cached)
      seedRows([])
      seedCount(0)
      await bridge.read('items', { limit: 10 })

      expect(mockSql.mock.calls.length).toBe(callCount + 2)
    })
  })

  // -------------------------------------------------------------------------
  // count()
  // -------------------------------------------------------------------------

  describe('count()', () => {
    it('returns row count', async () => {
      const bridge = await createConnectedBridge()

      seedCount(42)

      const count = await bridge.count('items')
      expect(count).toBe(42)

      const call = mockSql.mock.calls[1]!
      expect(call[0]).toContain('count(*)::int AS total')
      expect(call[0]).toContain('"items"')
    })

    it('throws on invalid table name', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.count("'; DROP TABLE--")).rejects.toThrow('Invalid table name')
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

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('"status" = $1')
      expect(dataCall[1]).toContain('active')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('"age" > $1')
      expect(dataCall[1]).toContain(18)
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { score: { $gte: 90 } } })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('"score" >= $1')
    })

    it('builds WHERE with $lt and $lte operators', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lt: 65, $lte: 64 } } })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('"age" < $1')
      expect(dataCall[0]).toContain('"age" <= $2')
    })

    it('builds WHERE with $in operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('"status" = ANY($1)')
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
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

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('ORDER BY "id" DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: { name: 'asc', score: 'desc' } as unknown as never,
      })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('ORDER BY "name" ASC, "score" DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('LIMIT $1')
      expect(dataCall[0]).toContain('OFFSET $2')
      expect(dataCall[1]).toContain(10)
      expect(dataCall[1]).toContain(20)
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { select: ['id', 'name'] })

      const dataCall = mockSql.mock.calls[1]!
      expect(dataCall[0]).toContain('SELECT "id", "name"')
    })
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  describe('listTargets()', () => {
    it('returns list of public table names', async () => {
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
