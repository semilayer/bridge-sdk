import { describe, it, expect, vi, beforeEach } from 'vitest'

// ---------------------------------------------------------------------------
// Mock oracledb — must use vi.hoisted so variables are available when the
// factory runs (vi.mock is hoisted to the top of the file by vitest).
// ---------------------------------------------------------------------------

const { mockExecute, mockConnClose, mockGetConnection, mockPoolClose, mockCreatePool } =
  vi.hoisted(() => {
    const mockExecute = vi.fn()
    const mockConnClose = vi.fn().mockResolvedValue(undefined)

    const mockConn = { execute: mockExecute, close: mockConnClose }

    const mockPoolClose = vi.fn().mockResolvedValue(undefined)
    const mockGetConnection = vi.fn().mockResolvedValue(mockConn)
    const mockPool = { getConnection: mockGetConnection, close: mockPoolClose }

    const mockCreatePool = vi.fn().mockResolvedValue(mockPool)

    return { mockExecute, mockConnClose, mockGetConnection, mockPoolClose, mockCreatePool }
  })

vi.mock('oracledb', () => ({
  default: {
    OUT_FORMAT_OBJECT: 4001,
    createPool: mockCreatePool,
  },
}))

import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { OracleBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Seed helpers — execute response sequences
// ---------------------------------------------------------------------------

function seedRows(rows: Record<string, unknown>[]): void {
  mockExecute.mockResolvedValueOnce({ rows })
}

function seedCount(total: number): void {
  mockExecute.mockResolvedValueOnce({ rows: [{ TOTAL: total }] })
}

async function createConnectedBridge(
  overrides: Record<string, unknown> = {},
): Promise<OracleBridge> {
  const bridge = new OracleBridge({
    user: 'testuser',
    password: 'secret',
    connectString: 'localhost:1521/XE',
    ...overrides,
  })
  // connect() → createPool → getConnection → execute('SELECT 1 FROM DUAL') → close
  seedRows([{ 1: 1 }])
  await bridge.connect()
  return bridge
}

describe('OracleBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Re-wire mocks after clearAllMocks wipes their implementations
    const conn = { execute: mockExecute, close: mockConnClose }
    const pool = { getConnection: mockGetConnection, close: mockPoolClose }
    mockConnClose.mockResolvedValue(undefined)
    mockPoolClose.mockResolvedValue(undefined)
    mockGetConnection.mockResolvedValue(conn)
    mockCreatePool.mockResolvedValue(pool)
  })

  // -------------------------------------------------------------------------
  // Constructor
  // -------------------------------------------------------------------------

  describe('constructor', () => {
    it('throws when user is missing', () => {
      expect(
        () =>
          new OracleBridge({ password: 'secret', connectString: 'host/svc' }),
      ).toThrow('OracleBridge requires "user"')
    })

    it('throws when password is missing', () => {
      expect(
        () =>
          new OracleBridge({ user: 'bob', connectString: 'host/svc' }),
      ).toThrow('OracleBridge requires "password"')
    })

    it('throws when connectString is missing', () => {
      expect(
        () => new OracleBridge({ user: 'bob', password: 'secret' }),
      ).toThrow('OracleBridge requires "connectString"')
    })

    it('accepts connectionString as alias for connectString', () => {
      expect(
        () =>
          new OracleBridge({
            user: 'bob',
            password: 'secret',
            connectionString: 'host:1521/XE',
          }),
      ).not.toThrow()
    })

    it('accepts valid config', () => {
      expect(
        () =>
          new OracleBridge({
            user: 'bob',
            password: 'secret',
            connectString: 'host:1521/XE',
          }),
      ).not.toThrow()
    })
  })

  // -------------------------------------------------------------------------
  // connect / disconnect
  // -------------------------------------------------------------------------

  describe('connect / disconnect', () => {
    it('connect() calls createPool with correct options', async () => {
      seedRows([{ 1: 1 }])
      const bridge = new OracleBridge({
        user: 'bob',
        password: 'secret',
        connectString: 'host:1521/XE',
        poolMax: 5,
      })
      await bridge.connect()

      expect(mockCreatePool).toHaveBeenCalledWith(
        expect.objectContaining({
          user: 'bob',
          password: 'secret',
          connectString: 'host:1521/XE',
          poolMax: 5,
          poolMin: 0,
        }),
      )
    })

    it('connect() defaults poolMax to 3', async () => {
      seedRows([{ 1: 1 }])
      const bridge = new OracleBridge({
        user: 'bob',
        password: 'secret',
        connectString: 'host:1521/XE',
      })
      await bridge.connect()

      expect(mockCreatePool).toHaveBeenCalledWith(
        expect.objectContaining({ poolMax: 3 }),
      )
    })

    it('connect() tests connectivity via SELECT 1 FROM DUAL', async () => {
      seedRows([{ 1: 1 }])
      const bridge = new OracleBridge({
        user: 'bob',
        password: 'secret',
        connectString: 'host:1521/XE',
      })
      await bridge.connect()

      expect(mockExecute).toHaveBeenCalledWith('SELECT 1 FROM DUAL')
      expect(mockConnClose).toHaveBeenCalled()
    })

    it('disconnect() calls pool.close()', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockPoolClose).toHaveBeenCalledWith(0)
    })

    it('read() throws when not connected', async () => {
      const bridge = new OracleBridge({
        user: 'bob',
        password: 'secret',
        connectString: 'host:1521/XE',
      })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  // -------------------------------------------------------------------------
  // read()
  // -------------------------------------------------------------------------

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering and double-quote identifiers', async () => {
      const bridge = await createConnectedBridge()

      // getPrimaryKey query
      seedRows([{ COLUMN_NAME: 'ID' }])
      // changedSince hasColumn check skipped (no changedSince)
      // main SELECT
      seedRows([
        { ID: 1, NAME: 'a' },
        { ID: 2, NAME: 'b' },
        { ID: 3, NAME: 'c' },
      ])
      // COUNT
      seedCount(5)

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('SELECT') && s.includes('FETCH NEXT'),
      )
      expect(selectCall).toContain('"items"')
      expect(selectCall).toContain('ORDER BY "ID" ASC')
      expect(selectCall).toContain('FETCH NEXT')
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedRows([{ COLUMN_NAME: 'ID' }])
      seedRows([{ ID: 1, NAME: 'a' }])
      seedCount(1)

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor as OFFSET', async () => {
      const bridge = await createConnectedBridge()

      seedRows([{ COLUMN_NAME: 'ID' }])
      seedRows([{ ID: 4, NAME: 'd' }])
      seedCount(5)

      await bridge.read('items', { limit: 2, cursor: '2' })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('OFFSET') && s.includes('FETCH'),
      )
      expect(selectCall).toContain('OFFSET')

      // Verify the params include offset=2
      const selectCallIdx = mockExecute.mock.calls.findIndex(
        (c) => (c[0] as string).includes('OFFSET') && (c[0] as string).includes('FETCH'),
      )
      const callParams = mockExecute.mock.calls[selectCallIdx]![1] as unknown[]
      expect(callParams).toContain(2) // offset value
    })

    it('rejects invalid table names', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.read('DROP TABLE--')).rejects.toThrow(
        'Invalid table name',
      )
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
    })

    it('throws when not connected', async () => {
      const bridge = new OracleBridge({
        user: 'bob',
        password: 'secret',
        connectString: 'host:1521/XE',
      })
      await expect(bridge.count('items')).rejects.toThrow('not connected')
    })

    it('count(target, {where}) calls SELECT COUNT(*) with WHERE', async () => {
      const bridge = await createConnectedBridge()

      seedCount(7)

      const count = await bridge.count('items', { where: { status: 'active' } })
      expect(count).toBe(7)

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const countCall = allSqlCalls.find((s) => s.includes('COUNT(*)'))
      expect(countCall).toContain('FROM "items"')
      expect(countCall).toContain('"status" = :1')

      // Find the COUNT call and verify the bound params include 'active'
      const countCallIdx = mockExecute.mock.calls.findIndex(
        (c) => (c[0] as string).includes('COUNT(*)'),
      )
      const countParams = mockExecute.mock.calls[countCallIdx]![1] as unknown[]
      expect(countParams).toContain('active')
    })
  })

  // -------------------------------------------------------------------------
  // disconnect() clears PK cache
  // -------------------------------------------------------------------------

  describe('disconnect()', () => {
    it('clears pk cache so next connect starts fresh', async () => {
      const bridge = await createConnectedBridge()

      // Prime the cache via a read
      seedRows([{ COLUMN_NAME: 'ID' }])
      seedRows([])
      seedCount(0)
      await bridge.read('items', { limit: 1 })

      await bridge.disconnect()
      // After disconnect, pool is null — reading again should throw
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  // -------------------------------------------------------------------------
  // query()
  // -------------------------------------------------------------------------

  describe('query()', () => {
    it('builds WHERE with simple equality and positional params', async () => {
      const bridge = await createConnectedBridge()

      seedRows([{ ID: 1, STATUS: 'active' }])
      seedCount(1)

      await bridge.query('items', { where: { status: 'active' } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('SELECT') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('"status" = :1')
    })

    it('builds WHERE with $eq operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { id: { $eq: 5 } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"id" = :1')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" > :1')
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $gte: 21 } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" >= :1')
    })

    it('builds WHERE with $lt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lt: 65 } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" < :1')
    })

    it('builds WHERE with $lte operator', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { age: { $lte: 64 } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('"age" <= :1')
    })

    it('builds WHERE with $in operator using individual positional params', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('SELECT') && s.includes('IN'),
      )
      expect(selectCall).toContain('"status" IN (:1, :2)')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('builds OR SQL via $or logical op', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        where: { $or: [{ status: 'active' }, { status: 'pending' }] },
      })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('SELECT') && s.includes('OR'),
      )
      expect(selectCall).toMatch(/"status" = :1\) OR \("status" = :2/)
    })

    it('builds NOT SQL via $not logical op', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { $not: { status: 'archived' } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('SELECT') && s.includes('NOT'),
      )
      expect(selectCall).toContain('NOT ("status" = :1)')
    })

    it('builds $ilike via LOWER("col") LIKE LOWER(:N)', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { where: { name: { $ilike: 'Foo%' } } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('LOWER'))
      expect(selectCall).toContain('LOWER("name") LIKE LOWER(:1)')
    })

    it('builds ORDER BY clause with canonical { field, dir } form', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('ORDER BY'))
      expect(selectCall).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('ORDER BY'))
      expect(selectCall).toContain('ORDER BY "id" DESC')
    })

    it('builds LIMIT and OFFSET using OFFSET/FETCH syntax', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { limit: 10, offset: 20 })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find((s) => s.includes('OFFSET'))
      expect(selectCall).toContain('OFFSET :1 ROWS')
      expect(selectCall).toContain('FETCH NEXT :2 ROWS ONLY')
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { select: ['id', 'name'] })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.startsWith('SELECT') && s.includes('"id"'),
      )
      expect(selectCall).toContain('SELECT "id", "name"')
    })

    it('adds ORDER BY 1 when OFFSET/FETCH used without explicit orderBy', async () => {
      const bridge = await createConnectedBridge()

      seedRows([])
      seedCount(0)

      await bridge.query('items', { limit: 5 })

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const selectCall = allSqlCalls.find(
        (s) => s.includes('FETCH NEXT') && s.includes('ORDER BY 1'),
      )
      expect(selectCall).toBeDefined()
    })
  })

  // -------------------------------------------------------------------------
  // listTargets()
  // -------------------------------------------------------------------------

  describe('listTargets()', () => {
    it('queries all_tables filtered by owner', async () => {
      const bridge = await createConnectedBridge()

      seedRows([
        { TABLE_NAME: 'CUSTOMERS' },
        { TABLE_NAME: 'ORDERS' },
      ])

      const targets = await bridge.listTargets()
      expect(targets).toEqual(['CUSTOMERS', 'ORDERS'])

      const allSqlCalls = mockExecute.mock.calls.map((c) => c[0] as string)
      const listCall = allSqlCalls.find((s) => s.includes('all_tables'))
      expect(listCall).toContain('all_tables')
      expect(listCall).toContain('owner = :1')

      // Verify owner is uppercased user
      const listCallIdx = mockExecute.mock.calls.findIndex(
        (c) => (c[0] as string).includes('all_tables'),
      )
      const listParams = mockExecute.mock.calls[listCallIdx]![1] as unknown[]
      expect(listParams[0]).toBe('TESTUSER')
    })

    it('uses custom schema as owner when provided', async () => {
      const bridge = await createConnectedBridge({ schema: 'myschema' })

      seedRows([{ TABLE_NAME: 'WIDGETS' }])

      await bridge.listTargets()

      const listCallIdx = mockExecute.mock.calls.findIndex(
        (c) => (c[0] as string).includes('all_tables'),
      )
      const listParams = mockExecute.mock.calls[listCallIdx]![1] as unknown[]
      expect(listParams[0]).toBe('MYSCHEMA')
    })
  })

  // -------------------------------------------------------------------------
  // introspectTarget()
  // -------------------------------------------------------------------------

  describe('introspectTarget()', () => {
    it('returns columns with primaryKey flag and row count', async () => {
      const bridge = await createConnectedBridge()

      // all_tab_columns result
      seedRows([
        { COLUMN_NAME: 'ID', DATA_TYPE: 'NUMBER', NULLABLE: 'N' },
        { COLUMN_NAME: 'NAME', DATA_TYPE: 'VARCHAR2', NULLABLE: 'Y' },
      ])
      // PK result
      seedRows([{ COLUMN_NAME: 'ID' }])
      // COUNT
      seedCount(10)

      const schema = await bridge.introspectTarget('CUSTOMERS')

      expect(schema.name).toBe('CUSTOMERS')
      expect(schema.rowCount).toBe(10)
      expect(schema.columns).toHaveLength(2)

      const idCol = schema.columns.find((c) => c.name === 'ID')
      expect(idCol?.primaryKey).toBe(true)
      expect(idCol?.nullable).toBe(false)
      expect(idCol?.type).toBe('NUMBER')

      const nameCol = schema.columns.find((c) => c.name === 'NAME')
      expect(nameCol?.primaryKey).toBe(false)
      expect(nameCol?.nullable).toBe(true)
    })

    it('rejects invalid table names', async () => {
      const bridge = await createConnectedBridge()
      await expect(bridge.introspectTarget('DROP TABLE--')).rejects.toThrow(
        'Invalid table name',
      )
    })
  })
})
