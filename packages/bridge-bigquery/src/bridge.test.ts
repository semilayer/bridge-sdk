import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { BigqueryBridge } from './bridge.js'

// ---------------------------------------------------------------------------
// Mock @google-cloud/bigquery — `BigQuery` is a class with `getDatasets`,
// `createQueryJob`, etc. We capture every createQueryJob() call so we can
// assert on the SQL emitted by the bridge. `getQueryResults()` returns the
// next queued row set.
// ---------------------------------------------------------------------------

interface QueryCall {
  query: string
  params: unknown
}

const queryCalls: QueryCall[] = []
let rowsQueue: Array<Array<Record<string, unknown>>> = []

function seedRows(rows: Array<Record<string, unknown>>): void {
  rowsQueue.push(rows)
}

const mockGetQueryResults = vi.fn(() => {
  const rows = rowsQueue.shift() ?? []
  return Promise.resolve([rows])
})

const mockCreateQueryJob = vi.fn(
  (opts: { query: string; params: unknown }) => {
    queryCalls.push({ query: opts.query, params: opts.params })
    return Promise.resolve([{ getQueryResults: mockGetQueryResults }])
  },
)

const mockGetDatasets = vi.fn(() => Promise.resolve([[]]))

vi.mock('@google-cloud/bigquery', () => ({
  BigQuery: vi.fn(() => ({
    createQueryJob: mockCreateQueryJob,
    getDatasets: mockGetDatasets,
    dataset: vi.fn(),
    query: vi.fn(),
  })),
}))

async function connectedBridge(): Promise<BigqueryBridge> {
  const bridge = new BigqueryBridge({
    projectId: 'proj',
    dataset: 'ds',
  })
  await bridge.connect()
  return bridge
}

describe('BigqueryBridge — buildWhereSql wiring', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    queryCalls.length = 0
    rowsQueue = []
  })

  describe('query()', () => {
    it('builds WHERE with simple equality and `?` positional binds', async () => {
      const bridge = await connectedBridge()
      seedRows([{ id: 1 }]) // data
      seedRows([{ total: 1 }]) // count

      await bridge.query('items', { where: { status: 'active' } })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toContain('`status` = ?')
      expect(dataCall.params).toEqual(['active'])
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
      expect(dataCall.query).toMatch(/`status` = \?\) OR \(`status` = \?/)
      expect(dataCall.params).toEqual(expect.arrayContaining(['active', 'pending']))
    })

    it('builds NOT via $not logical op', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ total: 0 }])

      await bridge.query('items', { where: { $not: { status: 'archived' } } })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toContain('NOT (`status` = ?)')
      expect(dataCall.params).toEqual(['archived'])
    })

    it('emits LOWER(col) LIKE LOWER(?) for $ilike (BigQuery has no native ILIKE)', async () => {
      const bridge = await connectedBridge()
      seedRows([])
      seedRows([{ total: 0 }])

      await bridge.query('items', { where: { name: { $ilike: 'Foo%' } } })

      const dataCall = queryCalls[0]!
      expect(dataCall.query).toContain('LOWER(`name`) LIKE LOWER(?)')
      // BigQuery LIKE does NOT accept `ESCAPE '\\'` — make sure we don't emit it.
      expect(dataCall.query).not.toContain("ESCAPE '\\'")
      expect(dataCall.params).toEqual(['Foo%'])
    })
  })

  describe('count(target, options)', () => {
    it('emits SELECT COUNT(*) with WHERE clause and bound params', async () => {
      const bridge = await connectedBridge()
      seedRows([{ total: 7 }])

      const n = await bridge.count('items', { where: { status: 'active' } })
      expect(n).toBe(7)

      const call = queryCalls[0]!
      expect(call.query).toContain('SELECT COUNT(*)')
      expect(call.query).toContain('`proj.ds.items`')
      expect(call.query).toContain('`status` = ?')
      expect(call.params).toEqual(['active'])
    })
  })
})
