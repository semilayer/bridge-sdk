import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { SnowflakeBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock snowflake-sdk — capture every execute() call so we can assert on the
// exact SQL the bridge emitted. Real snowflake-sdk is callback-based; we
// mirror that with `process.nextTick` so connect()/destroy()/execute() resolve
// asynchronously the way the SDK does.
// ---------------------------------------------------------------------------

interface ExecuteCall {
  sqlText: string
  binds: unknown[]
}

const executeCalls: ExecuteCall[] = []

// Each test seeds a queue of row arrays — every execute() shifts the head.
let rowsQueue: Array<Array<Record<string, unknown>>> = []

function seedRows(rows: Array<Record<string, unknown>>): void {
  rowsQueue.push(rows)
}

const mockConnect = vi.fn((cb: (err: Error | null) => void) =>
  process.nextTick(() => cb(null)),
)
const mockDestroy = vi.fn((cb: (err: Error | null) => void) =>
  process.nextTick(() => cb(null)),
)
const mockExecute = vi.fn(
  (opts: {
    sqlText: string
    binds: unknown[]
    complete: (err: Error | null, stmt: unknown, rows: unknown[]) => void
  }) => {
    executeCalls.push({ sqlText: opts.sqlText, binds: opts.binds })
    const rows = rowsQueue.shift() ?? []
    process.nextTick(() => opts.complete(null, undefined, rows))
  },
)

vi.mock('snowflake-sdk', () => ({
  default: {
    configure: vi.fn(),
    createConnection: vi.fn(() => ({
      connect: mockConnect,
      destroy: mockDestroy,
      execute: mockExecute,
    })),
  },
}))

async function connectedBridge(): Promise<SnowflakeBridge> {
  const bridge = new SnowflakeBridge({
    account: 'test',
    username: 'u',
    password: 'p',
    database: 'D',
  })
  await bridge.connect()
  return bridge
}

describe('SnowflakeBridge — buildWhereSql wiring', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    executeCalls.length = 0
    rowsQueue = []
  })

  describe('query()', () => {
    it('builds WHERE with simple equality', async () => {
      const bridge = await connectedBridge()
      seedRows([{ id: 1 }]) // data
      seedRows([{ TOTAL: 1 }]) // count

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = executeCalls[0]!
      expect(dataCall.sqlText).toContain('"status" = ?')
      expect(dataCall.binds).toContain('active')
    })

    it('throws UnsupportedOperatorError on unknown op', async () => {
      const bridge = await connectedBridge()
      await expect(
        bridge.query('items', { where: { age: { $regex: '.*' } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('builds OR via $or logical op', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ TOTAL: 0 }])

      await bridge.query('items', {
        where: { $or: [{ status: 'active' }, { status: 'pending' }] },
      })

      const dataCall = executeCalls[0]!
      expect(dataCall.sqlText).toMatch(/"status" = \?\) OR \("status" = \?/)
      expect(dataCall.binds).toContain('active')
      expect(dataCall.binds).toContain('pending')
    })

    it('builds NOT via $not logical op', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ TOTAL: 0 }])

      await bridge.query('items', { where: { $not: { status: 'archived' } } })

      const dataCall = executeCalls[0]!
      expect(dataCall.sqlText).toContain('NOT ("status" = ?)')
      expect(dataCall.binds).toContain('archived')
    })

    it('emits native ILIKE for $ilike (Snowflake)', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ TOTAL: 0 }])

      await bridge.query('items', { where: { name: { $ilike: 'Foo%' } } })

      const dataCall = executeCalls[0]!
      expect(dataCall.sqlText).toContain('"name" ILIKE ?')
      expect(dataCall.binds).toContain('Foo%')
    })
  })

  describe('count(target, options)', () => {
    it('emits SELECT COUNT(*) with WHERE clause', async () => {
      const bridge = await connectedBridge()
      seedRows([{ TOTAL: 7 }])

      const n = await bridge.count('items', { where: { status: 'active' } })
      expect(n).toBe(7)

      const call = executeCalls[0]!
      expect(call.sqlText).toContain('SELECT COUNT(*)')
      expect(call.sqlText).toContain('"items"')
      expect(call.sqlText).toContain('"status" = ?')
      expect(call.binds).toContain('active')
    })

    it('count() with no options returns full row count', async () => {
      const bridge = await connectedBridge()
      seedRows([{ TOTAL: 42 }])

      const n = await bridge.count('items')
      expect(n).toBe(42)
    })
  })
})
