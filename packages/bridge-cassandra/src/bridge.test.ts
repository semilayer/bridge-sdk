import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { CassandraBridge } from './bridge.js'

const mockExecute = vi.fn()
const mockConnect = vi.fn()
const mockShutdown = vi.fn()

vi.mock('cassandra-driver', () => ({
  default: {
    Client: vi.fn(() => ({
      execute: mockExecute,
      connect: mockConnect,
      shutdown: mockShutdown,
    })),
    auth: { PlainTextAuthProvider: vi.fn() },
  },
}))

// --- Seed helpers ---

/** Seed a result row set (for SELECT * queries) */
function seedRows(rows: Record<string, unknown>[], pageState?: Buffer | null) {
  mockExecute.mockResolvedValueOnce({ rows, pageState: pageState ?? null })
}

/** Seed the primary key detection query */
function seedPK(col: string) {
  mockExecute.mockResolvedValueOnce({ rows: [{ column_name: col }] })
}

/** Seed a COUNT(*) result (total may be a Cassandra Long-like object with .toString()) */
function seedCount(n: number) {
  mockExecute.mockResolvedValueOnce({
    rows: [{ total: { toString: () => String(n) } }],
  })
}

/** Seed a table listing result */
function seedTables(names: string[]) {
  mockExecute.mockResolvedValueOnce({ rows: names.map(n => ({ table_name: n })) })
}

function makeRows(count: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, i) => ({ id: `r${i + 1}`, name: `Row ${i + 1}` }))
}

async function connectedBridge() {
  const bridge = new CassandraBridge({
    contactPoints: ['127.0.0.1'],
    localDataCenter: 'datacenter1',
    keyspace: 'mykeyspace',
  })
  await bridge.connect()
  return bridge
}

describe('CassandraBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockConnect.mockResolvedValue(undefined)
    mockShutdown.mockResolvedValue(undefined)
  })

  describe('constructor', () => {
    it('throws if contactPoints is missing', () => {
      expect(
        () =>
          new CassandraBridge({ localDataCenter: 'datacenter1', keyspace: 'ks' }),
      ).toThrow('CassandraBridge requires "contactPoints" config array')
    })

    it('throws if contactPoints is empty', () => {
      expect(
        () =>
          new CassandraBridge({ contactPoints: [], localDataCenter: 'dc1', keyspace: 'ks' }),
      ).toThrow('CassandraBridge requires "contactPoints" config array')
    })

    it('throws if localDataCenter is missing', () => {
      expect(
        () => new CassandraBridge({ contactPoints: ['127.0.0.1'], keyspace: 'ks' }),
      ).toThrow('CassandraBridge requires "localDataCenter" config string')
    })

    it('throws if keyspace is missing', () => {
      expect(
        () =>
          new CassandraBridge({ contactPoints: ['127.0.0.1'], localDataCenter: 'dc1' }),
      ).toThrow('CassandraBridge requires "keyspace" config string')
    })

    it('accepts all required fields', () => {
      expect(
        () =>
          new CassandraBridge({
            contactPoints: ['127.0.0.1'],
            localDataCenter: 'datacenter1',
            keyspace: 'mykeyspace',
          }),
      ).not.toThrow()
    })

    it('accepts optional username and password', () => {
      expect(
        () =>
          new CassandraBridge({
            contactPoints: ['127.0.0.1'],
            localDataCenter: 'datacenter1',
            keyspace: 'mykeyspace',
            username: 'cassandra',
            password: 'password',
          }),
      ).not.toThrow()
    })
  })

  describe('connect', () => {
    it('calls client.connect()', async () => {
      const bridge = await connectedBridge()
      expect(mockConnect).toHaveBeenCalledOnce()
      void bridge
    })
  })

  describe('disconnect', () => {
    it('calls client.shutdown() and clears client', async () => {
      const bridge = await connectedBridge()
      await bridge.disconnect()
      expect(mockShutdown).toHaveBeenCalledOnce()
    })

    it('throws after disconnect', async () => {
      const bridge = await connectedBridge()
      await bridge.disconnect()
      await expect(bridge.count('users')).rejects.toThrow('CassandraBridge is not connected')
    })
  })

  describe('listTargets', () => {
    it('queries system_schema.tables and returns table names', async () => {
      const bridge = await connectedBridge()
      seedTables(['users', 'products', 'orders'])
      const result = await bridge.listTargets()
      expect(result).toEqual(['users', 'products', 'orders'])
      expect(mockExecute).toHaveBeenCalledWith(
        expect.stringContaining('system_schema.tables'),
        ['mykeyspace'],
        { prepare: true },
      )
    })
  })

  describe('count', () => {
    it('returns the COUNT value as a number', async () => {
      const bridge = await connectedBridge()
      seedCount(99)
      const result = await bridge.count('users')
      expect(result).toBe(99)
      expect(mockExecute).toHaveBeenCalledWith(
        expect.stringContaining('COUNT(*)'),
        [],
        { prepare: true },
      )
    })

    it('returns 0 when total row is absent', async () => {
      const bridge = await connectedBridge()
      mockExecute.mockResolvedValueOnce({ rows: [{}] })
      const result = await bridge.count('users')
      expect(result).toBe(0)
    })

    it('count(target, {where}) routes via query() and returns row count', async () => {
      const bridge = await connectedBridge()
      // single query() execute call returns 2 matching rows
      seedRows([
        { id: 'r1', status: 'active' },
        { id: 'r2', status: 'active' },
      ])
      const n = await bridge.count('users', { where: { status: { $eq: 'active' } } })
      expect(n).toBe(2)
    })
  })

  describe('read', () => {
    it('queries PK, then rows, then count — returning rows and total', async () => {
      const bridge = await connectedBridge()
      seedPK('id')
      seedRows(makeRows(2))
      seedCount(2)
      const result = await bridge.read('users')
      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ id: 'r1', name: 'Row 1' })
      expect(result.total).toBe(2)
      expect(result.nextCursor).toBeUndefined()
    })

    it('sets nextCursor when pageState is returned', async () => {
      const bridge = await connectedBridge()
      const pageState = Buffer.from('page-token-abc')
      seedPK('id')
      seedRows(makeRows(3), pageState)
      seedCount(10)
      const result = await bridge.read('users', { limit: 3 })
      expect(result.nextCursor).toBe(pageState.toString('base64'))
    })

    it('passes cursor as pageState Buffer to execute', async () => {
      const bridge = await connectedBridge()
      const pageState = Buffer.from('cursor-data')
      const cursorStr = pageState.toString('base64')
      seedPK('id')
      seedRows(makeRows(1))
      seedCount(5)
      await bridge.read('users', { cursor: cursorStr })
      const executeCall = mockExecute.mock.calls.find(
        (c: unknown[]) => typeof c[0] === 'string' && (c[0] as string).includes('SELECT *'),
      )!
      const opts = executeCall[2] as { prepare: boolean; fetchSize: number; pageState?: Buffer }
      expect(Buffer.isBuffer(opts.pageState)).toBe(true)
      expect(opts.pageState!.toString('base64')).toBe(cursorStr)
    })

    it('caches PK — only calls getPrimaryKey query once per target', async () => {
      const bridge = await connectedBridge()
      seedPK('id')
      seedRows(makeRows(1))
      seedCount(1)
      seedRows(makeRows(1)) // 2nd read - PK cached, no extra getPK query
      seedCount(1)
      await bridge.read('users')
      await bridge.read('users')
      const pkCalls = mockExecute.mock.calls.filter(
        (c: unknown[]) => typeof c[0] === 'string' && (c[0] as string).includes('system_schema.columns'),
      )
      expect(pkCalls).toHaveLength(1)
    })
  })

  describe('query', () => {
    it('uses ALLOW FILTERING with no WHERE for empty opts', async () => {
      const bridge = await connectedBridge()
      seedRows(makeRows(2))
      const result = await bridge.query('users', {})
      expect(result.rows).toHaveLength(2)
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain('ALLOW FILTERING')
      expect(sql).not.toContain('WHERE')
    })

    it('builds WHERE clause for $eq', async () => {
      const bridge = await connectedBridge()
      seedRows([{ id: 'r1', status: 'active' }])
      await bridge.query('users', { where: { status: { $eq: 'active' } } })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain(`"status" = ?`)
      const params = mockExecute.mock.calls[0]![1] as unknown[]
      expect(params).toContain('active')
    })

    it('builds WHERE clause for $gt', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      await bridge.query('users', { where: { age: { $gt: 18 } } })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain(`"age" > ?`)
      const params = mockExecute.mock.calls[0]![1] as unknown[]
      expect(params).toContain(18)
    })

    it('builds WHERE clause for $in', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      await bridge.query('users', { where: { role: { $in: ['admin', 'editor'] } } })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain(`"role" IN ?`)
      const params = mockExecute.mock.calls[0]![1] as unknown[]
      expect(params).toContainEqual(['admin', 'editor'])
    })

    it('builds WHERE clause for $gte, $lt, $lte', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      await bridge.query('users', {
        where: { score: { $gte: 5, $lt: 100 } },
      })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain(`"score" >= ?`)
      expect(sql).toContain(`"score" < ?`)
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      const bridge = await connectedBridge()
      await expect(
        bridge.query('users', { where: { age: { $bad: 5 } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('throws UnsupportedOperatorError on $or (logical op not declared)', async () => {
      const bridge = await connectedBridge()
      await expect(
        bridge.query('users', {
          where: { $or: [{ status: 'active' }, { status: 'pending' }] },
        }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('builds ORDER BY clause', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      await bridge.query('users', { orderBy: { field: 'age', dir: 'desc' } })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain(`ORDER BY "age" DESC`)
    })

    it('builds LIMIT clause', async () => {
      const bridge = await connectedBridge()
      seedRows(makeRows(5))
      await bridge.query('users', { limit: 5 })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain('LIMIT 5')
    })

    it('handles plain equality (non-operator) value', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      await bridge.query('users', { where: { active: true } })
      const sql = mockExecute.mock.calls[0]![0] as string
      expect(sql).toContain(`"active" = ?`)
      const params = mockExecute.mock.calls[0]![1] as unknown[]
      expect(params).toContain(true)
    })
  })
})
