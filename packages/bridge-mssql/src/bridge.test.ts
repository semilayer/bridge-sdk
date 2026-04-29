import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { MssqlBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock mssql — CJS default export (mssqlLib.connect) to match bridge import
// ---------------------------------------------------------------------------

// vi.mock is hoisted above variable declarations, so we must use vi.hoisted()
// to ensure these variables are initialised before the factory runs.
const { mockRecordset, mockInput, mockRequest, mockClose, mockConnect } =
  vi.hoisted(() => {
    const mockRecordset = vi.fn()
    const mockInput = vi.fn()
    const mockRequest = vi.fn(() => ({
      input: mockInput,
      query: mockRecordset,
    }))
    const mockClose = vi.fn()
    const mockConnect = vi.fn()
    return { mockRecordset, mockInput, mockRequest, mockClose, mockConnect }
  })

vi.mock('mssql', () => ({
  default: { connect: mockConnect },
}))

// Seed helpers — recordset responses
function seedRecordset(rows: Record<string, unknown>[]): void {
  mockRecordset.mockResolvedValueOnce({ recordset: rows })
}

async function createConnectedBridge(): Promise<MssqlBridge> {
  const bridge = new MssqlBridge({ server: 'localhost', database: 'test' })
  await bridge.connect()
  return bridge
}

describe('MssqlBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    // Re-wire implementations that were cleared by clearAllMocks
    mockInput.mockReturnThis()
    mockConnect.mockResolvedValue({ request: mockRequest, close: mockClose })
  })

  describe('constructor', () => {
    it('throws when url and server are both missing', () => {
      expect(() => new MssqlBridge({})).toThrow(
        'MssqlBridge requires either a "url" or a "server" config',
      )
    })

    it('accepts server config', () => {
      expect(() => new MssqlBridge({ server: 'localhost' })).not.toThrow()
    })

    it('accepts url config', () => {
      expect(
        () =>
          new MssqlBridge({
            url: 'mssql://user:pass@localhost:1433/testdb',
          }),
      ).not.toThrow()
    })
  })

  describe('connect / disconnect', () => {
    it('connect() creates pool', async () => {
      const bridge = await createConnectedBridge()
      expect(bridge).toBeDefined()
    })

    it('disconnect() closes the pool', async () => {
      const bridge = await createConnectedBridge()
      await bridge.disconnect()
      expect(mockClose).toHaveBeenCalled()
    })

    it('read() throws when not connected', async () => {
      const bridge = new MssqlBridge({ server: 'localhost' })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  describe('read()', () => {
    it('performs paginated SELECT with PK ordering and bracket quoting', async () => {
      const bridge = await createConnectedBridge()

      // PK query
      seedRecordset([{ column_name: 'id' }])
      // SELECT (TOP N+1)
      seedRecordset([
        { id: 1, name: 'a' },
        { id: 2, name: 'b' },
        { id: 3, name: 'c' },
      ])
      // COUNT
      seedRecordset([{ total: 5 }])

      const result = await bridge.read('items', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)

      // Verify the SELECT SQL uses bracket identifiers
      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('SELECT TOP') && s.includes('ORDER BY'),
      )
      expect(selectCall).toContain('[id]')
      expect(selectCall).toContain('[items]')
      expect(selectCall).toContain('ORDER BY [id] ASC')
    })

    it('returns no nextCursor on last page', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([{ column_name: 'id' }])
      seedRecordset([{ id: 1, name: 'a' }])
      seedRecordset([{ total: 1 }])

      const result = await bridge.read('items', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor in WHERE clause', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([{ column_name: 'id' }])
      seedRecordset([{ id: 4, name: 'd' }])
      seedRecordset([{ total: 5 }])

      await bridge.read('items', { limit: 10, cursor: '3' })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('[items]') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('[id] > @p1')
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

      seedRecordset([{ total: 42 }])

      const count = await bridge.count('items')
      expect(count).toBe(42)
    })

    it('count(target, {where}) calls SELECT count(*) with WHERE', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([{ total: 7 }])

      const count = await bridge.count('items', { where: { status: 'active' } })
      expect(count).toBe(7)

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const countCall = queryCalls.find((s) => s.includes('COUNT(*)'))
      expect(countCall).toContain('FROM [items]')
      expect(countCall).toContain('[status] = @p1')

      // The second arg passed to req.input via mockInput records the bound value
      const inputs = mockInput.mock.calls.map((c) => c[1])
      expect(inputs).toContain('active')
    })
  })

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([{ id: 1, status: 'active' }])
      seedRecordset([{ total: 1 }])

      await bridge.query('items', { where: { status: 'active' } })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('SELECT') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('[status] = @p1')
    })

    it('builds WHERE with $gt operator', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', { where: { age: { $gt: 18 } } })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('SELECT') && s.includes('WHERE'),
      )
      expect(selectCall).toContain('[age] > @p1')
    })

    it('builds WHERE with $gte operator', async () => {
      const bridge = await createConnectedBridge()
      seedRecordset([])
      seedRecordset([{ total: 0 }])
      await bridge.query('items', { where: { age: { $gte: 21 } } })
      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('[age] >= @p1')
    })

    it('builds WHERE with $lt operator', async () => {
      const bridge = await createConnectedBridge()
      seedRecordset([])
      seedRecordset([{ total: 0 }])
      await bridge.query('items', { where: { age: { $lt: 65 } } })
      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('[age] < @p1')
    })

    it('builds WHERE with $lte operator', async () => {
      const bridge = await createConnectedBridge()
      seedRecordset([])
      seedRecordset([{ total: 0 }])
      await bridge.query('items', { where: { age: { $lte: 64 } } })
      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('WHERE'))
      expect(selectCall).toContain('[age] <= @p1')
    })

    it('builds WHERE with $in operator', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', {
        where: { status: { $in: ['active', 'pending'] } },
      })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('SELECT') && s.includes('IN'),
      )
      expect(selectCall).toContain('[status] IN (@p1, @p2)')
    })

    it('throws on unknown operator', async () => {
      const bridge = await createConnectedBridge()

      await expect(
        bridge.query('items', { where: { age: { $invalid: 1 } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('builds OR SQL via $or logical op', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', {
        where: { $or: [{ status: 'active' }, { status: 'pending' }] },
      })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('SELECT') && s.includes('OR'),
      )
      expect(selectCall).toMatch(/\[status\] = @p1\) OR \(\[status\] = @p2/)
    })

    it('builds NOT SQL via $not logical op', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', { where: { $not: { status: 'archived' } } })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.includes('SELECT') && s.includes('NOT'),
      )
      expect(selectCall).toContain('NOT ([status] = @p1)')
    })

    it('builds $ilike via LOWER([col]) LIKE LOWER(@pN)', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', { where: { name: { $ilike: 'Foo%' } } })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('LOWER'))
      expect(selectCall).toContain('LOWER([name]) LIKE LOWER(@p1)')
    })

    it('builds ORDER BY clause', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('ORDER BY'))
      expect(selectCall).toContain('ORDER BY [name] ASC, [created_at] DESC')
    })

    it('accepts single canonical orderBy without array', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', { orderBy: { field: 'id', dir: 'desc' } })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('ORDER BY'))
      expect(selectCall).toContain('ORDER BY [id] DESC')
    })

    it('builds LIMIT and OFFSET using OFFSET/FETCH syntax', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', { limit: 10, offset: 20 })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find((s) => s.includes('OFFSET'))
      expect(selectCall).toContain('OFFSET @p1 ROWS')
      expect(selectCall).toContain('FETCH NEXT @p2 ROWS ONLY')
    })

    it('selects specific fields', async () => {
      const bridge = await createConnectedBridge()

      seedRecordset([])
      seedRecordset([{ total: 0 }])

      await bridge.query('items', { select: ['id', 'name'] })

      const queryCalls = mockRecordset.mock.calls.map((c) => c[0] as string)
      const selectCall = queryCalls.find(
        (s) => s.startsWith('SELECT') && s.includes('[id]'),
      )
      expect(selectCall).toContain('SELECT [id], [name]')
    })
  })
})
