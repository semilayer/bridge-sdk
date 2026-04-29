import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { ElasticsearchBridge } from './bridge.js'

const mockPing = vi.fn()
const mockSearch = vi.fn()
const mockCount = vi.fn()
const mockClose = vi.fn()
const mockCatIndices = vi.fn()

vi.mock('@elastic/elasticsearch', () => ({
  Client: vi.fn(() => ({
    ping: mockPing,
    search: mockSearch,
    count: mockCount,
    close: mockClose,
    cat: { indices: mockCatIndices },
  })),
}))

type Hit = { _id: string; _source: Record<string, unknown> }

function makeHit(id: string, source: Record<string, unknown>): Hit {
  return { _id: id, _source: source }
}

function seedSearch(hits: Hit[], total = hits.length, hasExtra = false) {
  const allHits = hasExtra ? [...hits, makeHit('extra', {})] : hits
  mockSearch.mockResolvedValueOnce({
    hits: {
      hits: allHits,
      total: { value: total, relation: 'eq' },
    },
  })
}

describe('ElasticsearchBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockPing.mockResolvedValue(true)
  })

  describe('constructor', () => {
    it('throws if node is missing', () => {
      expect(() => new ElasticsearchBridge({})).toThrow(
        'ElasticsearchBridge requires a "node" config string',
      )
    })

    it('throws if node is not a string', () => {
      expect(() => new ElasticsearchBridge({ node: 9200 })).toThrow(
        'ElasticsearchBridge requires a "node" config string',
      )
    })

    it('accepts node config', () => {
      expect(() => new ElasticsearchBridge({ node: 'http://localhost:9200' })).not.toThrow()
    })

    it('accepts apiKey auth', () => {
      expect(
        () => new ElasticsearchBridge({ node: 'http://localhost:9200', apiKey: 'my-key' }),
      ).not.toThrow()
    })

    it('accepts username/password auth', () => {
      expect(
        () =>
          new ElasticsearchBridge({
            node: 'http://localhost:9200',
            username: 'elastic',
            password: 'pass',
          }),
      ).not.toThrow()
    })
  })

  describe('connect', () => {
    it('calls ping after creating client', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      expect(mockPing).toHaveBeenCalledOnce()
    })
  })

  describe('disconnect', () => {
    it('calls close on the client', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      await bridge.disconnect()
      expect(mockClose).toHaveBeenCalledOnce()
    })

    it('throws when trying to use after disconnect', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      await bridge.disconnect()
      await expect(bridge.count('users')).rejects.toThrow('ElasticsearchBridge is not connected')
    })
  })

  describe('listTargets', () => {
    it('returns index names, filtering out dot-prefixed system indices', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockCatIndices.mockResolvedValueOnce([
        { index: 'users' },
        { index: 'products' },
        { index: '.kibana' },
        { index: '.security' },
      ])
      const result = await bridge.listTargets()
      expect(result).toEqual(['users', 'products'])
    })

    it('filters out entries without index property', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockCatIndices.mockResolvedValueOnce([{ index: 'users' }, {}])
      const result = await bridge.listTargets()
      expect(result).toEqual(['users'])
    })
  })

  describe('count', () => {
    it('returns the document count for an index', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockCount.mockResolvedValueOnce({ count: 150 })
      const result = await bridge.count('users')
      expect(result).toBe(150)
      expect(mockCount).toHaveBeenCalledWith({
        index: 'users',
        body: { query: { match_all: {} } },
      })
    })

    it('passes translated query when options.where is provided', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockCount.mockResolvedValueOnce({ count: 7 })
      const result = await bridge.count('users', {
        where: { status: { $eq: 'active' } },
      })
      expect(result).toBe(7)
      const args = mockCount.mock.calls[0]![0] as {
        index: string
        body: { query: { bool: { must: unknown[] } } }
      }
      expect(args.index).toBe('users')
      expect(args.body.query.bool.must).toContainEqual({
        term: { status: 'active' },
      })
    })
  })

  describe('read', () => {
    it('returns rows with _id merged from _source', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      const hits = [makeHit('1', { name: 'Alice' }), makeHit('2', { name: 'Bob' })]
      seedSearch(hits)
      const result = await bridge.read('users')
      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toEqual({ _id: '1', name: 'Alice' })
      expect(result.rows[1]).toEqual({ _id: '2', name: 'Bob' })
      expect(result.total).toBe(2)
      expect(result.nextCursor).toBeUndefined()
    })

    it('paginates with limit: fetches limit+1, sets nextCursor to last selected id', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      const hits = [makeHit('1', { name: 'Alice' }), makeHit('2', { name: 'Bob' })]
      seedSearch(hits, 3, true) // 3 hits returned (limit=2, hasExtra=true → 3 items)
      const result = await bridge.read('users', { limit: 2 })
      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
    })

    it('passes search_after when cursor is set', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      seedSearch([makeHit('5', { name: 'Eve' })])
      await bridge.read('users', { cursor: '4' })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.search_after).toEqual(['4'])
    })

    it('uses range query when changedSince is set', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      const since = new Date('2024-01-01T00:00:00Z')
      seedSearch([])
      await bridge.read('users', { changedSince: since, changeTrackingColumn: 'modified_at' })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.query).toEqual({ range: { modified_at: { gt: since.toISOString() } } })
    })

    it('sets _source to fields array when fields option is given', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      seedSearch([makeHit('1', { name: 'Alice' })])
      await bridge.read('users', { fields: ['name', 'email'] })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body._source).toEqual(['name', 'email'])
    })
  })

  describe('query', () => {
    it('uses match_all when no where clause', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', {})
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.query).toEqual({ match_all: {} })
    })

    it('builds term query for $eq', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { where: { status: { $eq: 'active' } } })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.query).toEqual({ bool: { must: [{ term: { status: 'active' } }] } })
    })

    it('builds range query for $gt', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { where: { age: { $gt: 18 } } })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.query.bool.must).toContainEqual({ range: { age: { gt: 18 } } })
    })

    it('builds range query for $gte, $lt, $lte', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { where: { score: { $gte: 5, $lte: 10 } } })
      const body = mockSearch.mock.calls[0]![0].body
      const must = body.query.bool.must as unknown[]
      expect(must.some((m: unknown) => JSON.stringify(m).includes('"gte":5'))).toBe(true)
      expect(must.some((m: unknown) => JSON.stringify(m).includes('"lte":10'))).toBe(true)
    })

    it('builds terms query for $in', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { where: { role: { $in: ['admin', 'editor'] } } })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.query.bool.must).toContainEqual({ terms: { role: ['admin', 'editor'] } })
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      await expect(
        bridge.query('users', { where: { age: { $bad: 5 } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('translates $or to bool.should with minimum_should_match', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', {
        where: {
          $or: [{ status: { $eq: 'active' } }, { role: { $eq: 'admin' } }],
        },
      })
      const body = mockSearch.mock.calls[0]![0].body
      const must = body.query.bool.must as unknown[]
      // Top-level wrapper is bool.must with one entry — the bool.should.
      expect(must).toHaveLength(1)
      const should = (must[0] as { bool: { should: unknown[]; minimum_should_match: number } })
        .bool
      expect(should.minimum_should_match).toBe(1)
      expect(should.should).toHaveLength(2)
    })

    it('translates $not to bool.must_not', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', {
        where: { $not: { status: { $eq: 'banned' } } },
      })
      const body = mockSearch.mock.calls[0]![0].body
      const must = body.query.bool.must as unknown[]
      expect(must).toHaveLength(1)
      const mustNot = (must[0] as { bool: { must_not: unknown[] } }).bool.must_not
      expect(mustNot).toHaveLength(1)
    })

    it('translates $ilike to wildcard with case_insensitive', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', {
        where: { name: { $ilike: 'ali%' } },
      })
      const body = mockSearch.mock.calls[0]![0].body
      // ilikeToWildcard maps % → *
      expect(body.query.bool.must).toContainEqual({
        wildcard: { name: { value: 'ali*', case_insensitive: true } },
      })
    })

    it('builds sort from orderBy', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { orderBy: [{ field: 'name', dir: 'asc' }, { field: 'age', dir: 'desc' }] })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.sort).toEqual([{ name: 'asc' }, { age: 'desc' }])
    })

    it('sets size and from for limit/offset', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { limit: 20, offset: 40 })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.size).toBe(20)
      expect(body.from).toBe(40)
    })

    it('sets _source from select option', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { select: ['id', 'email'] })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body._source).toEqual(['id', 'email'])
    })

    it('uses plain term for non-operator equality', async () => {
      const bridge = new ElasticsearchBridge({ node: 'http://localhost:9200' })
      await bridge.connect()
      mockSearch.mockResolvedValueOnce({ hits: { hits: [], total: { value: 0 } } })
      await bridge.query('users', { where: { active: true } })
      const body = mockSearch.mock.calls[0]![0].body
      expect(body.query.bool.must).toContainEqual({ term: { active: true } })
    })
  })
})
