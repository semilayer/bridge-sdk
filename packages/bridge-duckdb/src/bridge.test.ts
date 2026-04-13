import { describe, it, expect, vi, beforeEach } from 'vitest'
import { DuckdbBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock duckdb — real DuckDB always calls its constructor callback
// asynchronously (after the database file is opened). We mirror that with
// process.nextTick so the `db` variable is assigned before resolve(db) runs,
// which is the correct production behaviour.
// ---------------------------------------------------------------------------

const mockAll = vi.fn()
const mockClose = vi.fn()
const mockConnect = vi.fn()
const MockDatabase = vi.fn()

vi.mock('duckdb', () => ({
  default: { Database: MockDatabase },
}))

function setupHappyDb() {
  const mockConn = { all: mockAll }
  mockConnect.mockReturnValue(mockConn)
  MockDatabase.mockImplementation(
    (_path: string, cb: (err: null) => void) => {
      const db = { connect: mockConnect, close: mockClose }
      // Real DuckDB opens the file async — nextTick ensures `db` is assigned
      // before the callback fires, avoiding a TDZ in openDb.
      process.nextTick(() => cb(null))
      return db
    },
  )
  mockClose.mockImplementation((cb: () => void) => process.nextTick(cb))
}

function seedAll(rows: Record<string, unknown>[]) {
  mockAll.mockImplementationOnce(
    (_sql: string, ...args: unknown[]) => {
      const cb = args[args.length - 1] as (err: null, rows: unknown[]) => void
      cb(null, rows)
    },
  )
}

async function connectedBridge(): Promise<DuckdbBridge> {
  setupHappyDb()
  const bridge = new DuckdbBridge({ path: ':memory:' })
  await bridge.connect()
  return bridge
}

describe('DuckdbBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  // -------------------------------------------------------------------------
  // Constructor
  // -------------------------------------------------------------------------

  describe('constructor', () => {
    it('accepts a path', () => {
      expect(() => new DuckdbBridge({ path: '/data/analytics.duckdb' })).not.toThrow()
    })

    it('defaults to :memory: when path is omitted', () => {
      expect(() => new DuckdbBridge({})).not.toThrow()
    })
  })

  // -------------------------------------------------------------------------
  // connect / disconnect
  // -------------------------------------------------------------------------

  describe('connect / disconnect', () => {
    it('connect() opens a Database at the configured path', async () => {
      setupHappyDb()
      const bridge = new DuckdbBridge({ path: '/tmp/test.duckdb' })
      await bridge.connect()
      expect(MockDatabase).toHaveBeenCalledWith('/tmp/test.duckdb', expect.any(Function))
    })

    it('disconnect() closes the database', async () => {
      const bridge = await connectedBridge()
      await bridge.disconnect()
      expect(mockClose).toHaveBeenCalled()
    })

    it('read() throws when not connected', async () => {
      const bridge = new DuckdbBridge({ path: ':memory:' })
      await expect(bridge.read('items')).rejects.toThrow('not connected')
    })
  })

  // -------------------------------------------------------------------------
  // count()
  // -------------------------------------------------------------------------

  describe('count()', () => {
    it('returns row count from SELECT count(*)', async () => {
      const bridge = await connectedBridge()
      seedAll([{ total: 42 }])
      const n = await bridge.count('items')
      expect(n).toBe(42)
      expect(mockAll).toHaveBeenCalledWith(
        expect.stringContaining('count(*)'),
        expect.any(Function),
      )
    })
  })

  // -------------------------------------------------------------------------
  // read()
  // -------------------------------------------------------------------------

  describe('read()', () => {
    it('performs a paginated SELECT with LIMIT and OFFSET', async () => {
      const bridge = await connectedBridge()

      seedAll([{ id: 1, name: 'a' }, { id: 2, name: 'b' }, { id: 3, name: 'c' }])
      seedAll([{ total: 5 }])

      const result = await bridge.read('items', { limit: 2 })
      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
      expect(result.total).toBe(5)
    })

    it('returns no nextCursor on the last page', async () => {
      const bridge = await connectedBridge()
      seedAll([{ id: 1 }])
      seedAll([{ total: 1 }])
      const result = await bridge.read('items', { limit: 10 })
      expect(result.nextCursor).toBeUndefined()
    })

    it('uses cursor as OFFSET', async () => {
      const bridge = await connectedBridge()
      seedAll([{ id: 6 }])
      seedAll([{ total: 10 }])
      await bridge.read('items', { limit: 5, cursor: '5' })
      const sql = (mockAll.mock.calls[0] as [string, ...unknown[]])[0]!
      expect(sql).toContain('OFFSET ?')
    })
  })

  // -------------------------------------------------------------------------
  // query()
  // -------------------------------------------------------------------------

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await connectedBridge()
      seedAll([{ id: 1, status: 'active' }])
      seedAll([{ total: 1 }])
      await bridge.query('items', { where: { status: 'active' } })
      const sql = (mockAll.mock.calls[0] as [string, ...unknown[]])[0]!
      expect(sql).toContain('"status" = ?')
    })

    it('builds WHERE with $gt', async () => {
      const bridge = await connectedBridge()
      seedAll([])
      seedAll([{ total: 0 }])
      await bridge.query('items', { where: { age: { $gt: 18 } } })
      const sql = (mockAll.mock.calls[0] as [string, ...unknown[]])[0]!
      expect(sql).toContain('"age" > ?')
    })

    it('builds WHERE with $in', async () => {
      const bridge = await connectedBridge()
      seedAll([])
      seedAll([{ total: 0 }])
      await bridge.query('items', { where: { status: { $in: ['active', 'pending'] } } })
      const sql = (mockAll.mock.calls[0] as [string, ...unknown[]])[0]!
      expect(sql).toContain('IN (?, ?)')
    })

    it('builds ORDER BY clause', async () => {
      const bridge = await connectedBridge()
      seedAll([])
      seedAll([{ total: 0 }])
      await bridge.query('items', {
        orderBy: [
          { field: 'name', dir: 'asc' },
          { field: 'created_at', dir: 'desc' },
        ],
      })
      const sql = (mockAll.mock.calls[0] as [string, ...unknown[]])[0]!
      expect(sql).toContain('ORDER BY "name" ASC, "created_at" DESC')
    })

    it('builds LIMIT and OFFSET', async () => {
      const bridge = await connectedBridge()
      seedAll([])
      seedAll([{ total: 0 }])
      await bridge.query('items', { limit: 10, offset: 20 })
      const sql = (mockAll.mock.calls[0] as [string, ...unknown[]])[0]!
      expect(sql).toContain('LIMIT ?')
      expect(sql).toContain('OFFSET ?')
    })

    it('throws on unknown operator', async () => {
      const bridge = await connectedBridge()
      await expect(
        bridge.query('items', { where: { age: { $regex: '.*' } } }),
      ).rejects.toThrow('Unknown operator "$regex"')
    })
  })
})
