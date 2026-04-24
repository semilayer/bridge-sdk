import { describe, it, expect, vi, beforeEach } from 'vitest'
import { PostgresBridge, buildPoolConfig } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock pg module — separate pool.query from client.query. Capture the
// constructor arg so tests can assert what pool config was produced.
// ---------------------------------------------------------------------------

const mockPoolQuery = vi.fn()
const mockClientQuery = vi.fn()
const mockRelease = vi.fn()
const mockConnect = vi.fn().mockResolvedValue({
  query: mockClientQuery,
  release: mockRelease,
})
const mockEnd = vi.fn()
const mockPoolCtorArgs: unknown[] = []

vi.mock('pg', () => {
  class Pool {
    query = mockPoolQuery
    connect = mockConnect
    end = mockEnd
    constructor(opts: unknown) {
      mockPoolCtorArgs.push(opts)
    }
  }
  return { default: { Pool } }
})

// dns.lookup is pre-resolved inside connect(); point it at a predictable
// IPv4 unless a test overrides it.
const mockDnsLookup = vi.fn(async (_host: string, _opts: unknown) => ({
  address: '203.0.113.9',
  family: 4,
}))

vi.mock('node:dns/promises', () => ({
  lookup: (host: string, opts: unknown) => mockDnsLookup(host, opts),
}))

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
    mockPoolCtorArgs.length = 0
    mockDnsLookup.mockImplementation(async () => ({
      address: '203.0.113.9',
      family: 4,
    }))
    // Default: mock the SELECT 1 connectivity check (client.query)
    mockClientQuery.mockResolvedValueOnce({
      rows: [{ '?column?': 1 }],
      rowCount: 1,
    })
  })

  describe('constructor', () => {
    it('throws when url is missing', () => {
      expect(() => new PostgresBridge({})).toThrow(
        'PostgresBridge requires a "url" or ("host" + "database") config',
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

  describe('DNS pre-resolution (ipFamily)', () => {
    it('defaults to IPv4 and rewrites host to resolved IP', async () => {
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com:5432/neondb?sslmode=require',
      })
      await bridge.connect()

      expect(mockDnsLookup).toHaveBeenCalledWith('db.example.com', {
        family: 4,
      })
      const poolConfig = mockPoolCtorArgs[0] as {
        host?: string
        ssl?: { servername?: string; rejectUnauthorized?: boolean } | boolean
      }
      expect(poolConfig.host).toBe('203.0.113.9')
      expect(poolConfig.ssl).toMatchObject({
        servername: 'db.example.com',
        rejectUnauthorized: false,
      })
    })

    it('honors explicit ipFamily: 6', async () => {
      mockDnsLookup.mockImplementation(async () => ({
        address: '2001:db8::1',
        family: 6,
      }))
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com/db?sslmode=require',
        ipFamily: 6,
      })
      await bridge.connect()

      expect(mockDnsLookup).toHaveBeenCalledWith('db.example.com', {
        family: 6,
      })
      const poolConfig = mockPoolCtorArgs[0] as { host?: string }
      expect(poolConfig.host).toBe('2001:db8::1')
    })

    it('skips resolution when ipFamily is 0 (legacy opt-out)', async () => {
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com/db?sslmode=require',
        ipFamily: 0,
      })
      await bridge.connect()

      expect(mockDnsLookup).not.toHaveBeenCalled()
      const poolConfig = mockPoolCtorArgs[0] as { host?: string }
      expect(poolConfig.host).toBeUndefined()
    })

    it('skips resolution when host is already an IP literal', async () => {
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@10.0.0.5:5432/db',
      })
      await bridge.connect()

      expect(mockDnsLookup).not.toHaveBeenCalled()
      const poolConfig = mockPoolCtorArgs[0] as { host?: string }
      expect(poolConfig.host).toBeUndefined()
    })

    it('builds no ssl config when sslmode is absent', async () => {
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com/db',
      })
      await bridge.connect()

      const poolConfig = mockPoolCtorArgs[0] as { ssl?: unknown }
      expect(poolConfig.ssl).toBeUndefined()
    })

    it('sets ssl=false when sslmode=disable', async () => {
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com/db?sslmode=disable',
      })
      await bridge.connect()

      const poolConfig = mockPoolCtorArgs[0] as { ssl?: unknown }
      expect(poolConfig.ssl).toBe(false)
    })

    it('sets rejectUnauthorized=true for sslmode=verify-full', async () => {
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com/db?sslmode=verify-full',
      })
      await bridge.connect()

      const poolConfig = mockPoolCtorArgs[0] as {
        ssl?: { rejectUnauthorized?: boolean; servername?: string }
      }
      expect(poolConfig.ssl).toMatchObject({
        rejectUnauthorized: true,
        servername: 'db.example.com',
      })
    })

    it('throws a clear error when DNS lookup fails', async () => {
      mockDnsLookup.mockImplementationOnce(async () => {
        const err = new Error('queryA ENOTFOUND db.example.com') as Error & {
          code?: string
        }
        err.code = 'ENOTFOUND'
        throw err
      })
      const bridge = new PostgresBridge({
        url: 'postgresql://u:p@db.example.com/db?sslmode=require',
      })
      await expect(bridge.connect()).rejects.toThrow(
        'Failed to resolve "db.example.com" to IPv4: queryA ENOTFOUND db.example.com',
      )
    })

    it('buildPoolConfig is a pure function — no network, no side effects', async () => {
      // Smoke-test the exported helper so future refactors keep it pure.
      const cfg = await buildPoolConfig({
        url: 'postgresql://u:p@db.example.com/db?sslmode=require',
        pool: { min: 1, max: 5 },
        connectionTimeoutMillis: 2500,
      })
      expect(cfg).toMatchObject({
        connectionString: 'postgresql://u:p@db.example.com/db?sslmode=require',
        host: '203.0.113.9',
        min: 1,
        max: 5,
        connectionTimeoutMillis: 2500,
        ssl: { servername: 'db.example.com', rejectUnauthorized: false },
      })
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

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', {
        orderBy: { field: 'id', dir: 'desc' },
      })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY "id" DESC')
    })

    it('accepts record-shorthand orderBy { col: dir }', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', {
        orderBy: { cuisine: 'asc', title: 'desc' } as unknown as never,
      })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY "cuisine" ASC, "title" DESC')
    })

    it('skips malformed orderBy entries instead of crashing', async () => {
      const bridge = await createConnectedBridge()

      seedSelectResult([])
      seedCountResult(0)

      await bridge.query('items', {
        orderBy: [{} as never, { field: 'name', dir: 'asc' }],
      })

      const dataCall = mockPoolQuery.mock.calls[0]!
      expect(dataCall[0]).toContain('ORDER BY "name" ASC')
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
