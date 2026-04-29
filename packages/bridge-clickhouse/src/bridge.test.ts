import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { ClickhouseBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock @clickhouse/client — capture every query() call so we can assert on
// the SQL the bridge emitted (with placeholders rebound to ClickHouse's
// `{pN:Type}` form).
// ---------------------------------------------------------------------------

interface QueryCall {
  query: string
  query_params?: Record<string, unknown>
}

const queryCalls: QueryCall[] = []
let rowsQueue: Array<Array<Record<string, unknown>>> = []

function seedRows(rows: Array<Record<string, unknown>>): void {
  rowsQueue.push(rows)
}

const mockQuery = vi.fn(
  (opts: { query: string; query_params?: Record<string, unknown> }) => {
    queryCalls.push({ query: opts.query, query_params: opts.query_params })
    const rows = rowsQueue.shift() ?? []
    return Promise.resolve({
      json: () => Promise.resolve(rows),
    })
  },
)

const mockPing = vi.fn(() => Promise.resolve({ success: true }))
const mockClose = vi.fn(() => Promise.resolve())

vi.mock('@clickhouse/client', () => ({
  createClient: vi.fn(() => ({
    query: mockQuery,
    ping: mockPing,
    close: mockClose,
  })),
}))

async function connectedBridge(): Promise<ClickhouseBridge> {
  const bridge = new ClickhouseBridge({
    host: 'localhost',
    database: 'default',
  })
  await bridge.connect()
  return bridge
}

describe('ClickhouseBridge — buildWhereSql wiring', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    queryCalls.length = 0
    rowsQueue = []
  })

  describe('query()', () => {
    it('builds WHERE with simple equality (rebound to {p0:String})', async () => {
      const bridge = await connectedBridge()
      seedRows([{ id: 1 }]) // data
      seedRows([{ total: 1 }]) // count

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toContain('`status` = {p0:String}')
      expect(dataCall.query_params).toEqual({ p0: 'active' })
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
      seedRows([{ total: 0 }])

      await bridge.query('items', {
        where: { $or: [{ status: 'active' }, { status: 'pending' }] },
      })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toMatch(
        /`status` = \{p0:String\}\) OR \(`status` = \{p1:String\}/,
      )
      expect(dataCall.query_params).toEqual({ p0: 'active', p1: 'pending' })
    })

    it('builds NOT via $not logical op', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ total: 0 }])

      await bridge.query('items', { where: { $not: { status: 'archived' } } })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toContain('NOT (`status` = {p0:String})')
      expect(dataCall.query_params).toEqual({ p0: 'archived' })
    })

    it('emits lower(col) LIKE lower(?) for $ilike (ClickHouse has no native ILIKE)', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ total: 0 }])

      await bridge.query('items', { where: { name: { $ilike: 'Foo%' } } })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toContain('lower(`name`) LIKE lower({p0:String})')
      expect(dataCall.query_params).toEqual({ p0: 'Foo%' })
    })
  })

  describe('count(target, options)', () => {
    it('emits SELECT count() with WHERE clause and rebound params', async () => {
      const bridge = await connectedBridge()
      seedRows([{ total: 7 }])

      const n = await bridge.count('items', { where: { status: 'active' } })
      expect(n).toBe(7)

      const call = queryCalls[0]!
      expect(call.query).toContain('SELECT count()')
      expect(call.query).toContain('`default`.`items`')
      expect(call.query).toContain('`status` = {p0:String}')
      expect(call.query_params).toEqual({ p0: 'active' })
    })
  })
})
