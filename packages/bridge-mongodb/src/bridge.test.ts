import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { MongodbBridge } from './bridge.js'

const mockToArray = vi.fn()
const mockLimit = vi.fn().mockReturnThis()
const mockSkip = vi.fn().mockReturnThis()
const mockSort = vi.fn().mockReturnThis()
const mockProject = vi.fn().mockReturnThis()
const mockFind = vi.fn(() => ({
  sort: mockSort,
  limit: mockLimit,
  skip: mockSkip,
  project: mockProject,
  toArray: mockToArray,
}))
const mockCountDocuments = vi.fn()
const mockListCollections = vi.fn(() => ({
  toArray: vi.fn().mockResolvedValue([{ name: 'users' }, { name: 'posts' }]),
}))
const mockCollection = vi.fn(() => ({
  find: mockFind,
  countDocuments: mockCountDocuments,
}))
const mockCommand = vi.fn().mockResolvedValue({ ok: 1 })
const mockDb = vi.fn(() => ({
  collection: mockCollection,
  listCollections: mockListCollections,
  command: mockCommand,
}))
const mockConnect = vi.fn()
const mockClose = vi.fn()

vi.mock('mongodb', () => ({
  MongoClient: vi.fn(() => ({
    connect: mockConnect,
    close: mockClose,
    db: mockDb,
  })),
  ObjectId: class {
    constructor(public id: string) {}
    toString() {
      return this.id
    }
  },
}))

describe('MongodbBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockFind.mockReturnValue({
      sort: mockSort,
      limit: mockLimit,
      skip: mockSkip,
      project: mockProject,
      toArray: mockToArray,
    })
    mockSort.mockReturnThis()
    mockLimit.mockReturnThis()
    mockSkip.mockReturnThis()
    mockProject.mockReturnThis()
  })

  it('throws if url is missing', () => {
    expect(() => new MongodbBridge({})).toThrow('MongodbBridge requires a "url" config string')
  })

  it('throws if url is not a string', () => {
    expect(() => new MongodbBridge({ url: 42 })).toThrow('MongodbBridge requires a "url" config string')
  })

  it('constructs with valid url', () => {
    expect(() => new MongodbBridge({ url: 'mongodb://localhost:27017/mydb' })).not.toThrow()
  })

  it('uses url pathname as db name', () => {
    const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/myapp' })
    expect(bridge).toBeDefined()
  })

  it('falls back to "test" db when no pathname', () => {
    const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/' })
    expect(bridge).toBeDefined()
  })

  describe('connect / disconnect', () => {
    it('connect calls MongoClient.connect and pings', async () => {
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      expect(mockConnect).toHaveBeenCalledOnce()
      expect(mockCommand).toHaveBeenCalledWith({ ping: 1 })
    })

    it('disconnect calls close', async () => {
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.disconnect()
      expect(mockClose).toHaveBeenCalledOnce()
    })

    it('disconnect is safe when not connected', async () => {
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await expect(bridge.disconnect()).resolves.toBeUndefined()
    })

    it('throws when calling read without connect', async () => {
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await expect(bridge.read('users')).rejects.toThrow('MongodbBridge is not connected')
    })
  })

  describe('listTargets', () => {
    it('returns collection names', async () => {
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const targets = await bridge.listTargets()
      expect(targets).toEqual(['users', 'posts'])
    })
  })

  describe('count', () => {
    it('calls countDocuments with no filter', async () => {
      mockCountDocuments.mockResolvedValue(42)
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.count('users')
      expect(result).toBe(42)
      expect(mockCountDocuments).toHaveBeenCalledOnce()
      expect(mockCountDocuments).toHaveBeenCalledWith({})
    })

    it('passes translated filter when options.where is provided', async () => {
      mockCountDocuments.mockResolvedValue(7)
      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.count('users', {
        where: { status: 'active', age: { $gte: 18 } },
      })
      expect(result).toBe(7)
      expect(mockCountDocuments).toHaveBeenCalledWith({
        status: 'active',
        age: { $gte: 18 },
      })
    })
  })

  describe('read', () => {
    it('returns rows and calls countDocuments for total', async () => {
      const fakeDocs = [
        { _id: 'id1', name: 'Alice' },
        { _id: 'id2', name: 'Bob' },
      ]
      mockToArray.mockResolvedValue(fakeDocs)
      mockCountDocuments.mockResolvedValue(2)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.read('users', { limit: 10 })

      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ _id: 'id1', name: 'Alice' })
      expect(result.nextCursor).toBeUndefined()
      expect(result.total).toBe(2)
      expect(mockCountDocuments).toHaveBeenCalledWith({})
    })

    it('sets nextCursor when there are more rows than limit', async () => {
      // limit=2, return 3 docs → hasMore
      const fakeDocs = [
        { _id: 'id1', name: 'Alice' },
        { _id: 'id2', name: 'Bob' },
        { _id: 'id3', name: 'Carol' },
      ]
      mockToArray.mockResolvedValue(fakeDocs)
      mockCountDocuments.mockResolvedValue(10)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.read('users', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('id2')
      expect(result.total).toBe(10)
    })

    it('uses ObjectId for cursor when cursor is a valid ObjectId string', async () => {
      mockToArray.mockResolvedValue([{ _id: 'id2', name: 'Bob' }])
      mockCountDocuments.mockResolvedValue(1)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.read('users', { cursor: 'abc123', limit: 10 })

      expect(mockFind).toHaveBeenCalled()
    })

    it('no nextCursor on last page', async () => {
      const fakeDocs = [{ _id: 'id1', name: 'Alice' }]
      mockToArray.mockResolvedValue(fakeDocs)
      mockCountDocuments.mockResolvedValue(1)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.read('users', { limit: 5 })

      expect(result.nextCursor).toBeUndefined()
    })

    it('uses default limit of 1000', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.read('users')

      expect(mockLimit).toHaveBeenCalledWith(1001)
    })
  })

  describe('query', () => {
    it('filters with $eq operator', async () => {
      const fakeDocs = [{ _id: 'id1', name: 'Alice', age: 30 }]
      mockToArray.mockResolvedValue(fakeDocs)
      mockCountDocuments.mockResolvedValue(1)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.query('users', { where: { name: { $eq: 'Alice' } } })

      expect(result.rows).toHaveLength(1)
      expect(result.total).toBe(1)
    })

    it('filters with plain equality', async () => {
      mockToArray.mockResolvedValue([{ _id: 'id1', status: 'active' }])
      mockCountDocuments.mockResolvedValue(1)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.query('users', { where: { status: 'active' } })

      expect(result.rows).toHaveLength(1)
    })

    it('filters with $gt operator', async () => {
      mockToArray.mockResolvedValue([{ _id: 'id1', age: 35 }])
      mockCountDocuments.mockResolvedValue(1)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.query('users', { where: { age: { $gt: 30 } } })

      expect(result.rows).toHaveLength(1)
    })

    it('filters with $in operator', async () => {
      mockToArray.mockResolvedValue([
        { _id: 'id1', role: 'admin' },
        { _id: 'id2', role: 'editor' },
      ])
      mockCountDocuments.mockResolvedValue(2)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      const result = await bridge.query('users', { where: { role: { $in: ['admin', 'editor'] } } })

      expect(result.rows).toHaveLength(2)
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await expect(
        bridge.query('users', { where: { name: { $bogus: 'Ali' } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('translates $or to native $or filter', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.query('users', {
        where: {
          $or: [{ status: 'active' }, { role: { $eq: 'admin' } }],
        },
      })

      // The translated filter is what gets passed both to countDocuments
      // and to find(). countDocuments is called first.
      const filter = mockCountDocuments.mock.calls[0]![0] as Record<string, unknown>
      expect(filter).toEqual({
        $or: [{ status: 'active' }, { role: { $eq: 'admin' } }],
      })
    })

    it('translates $not to $nor wrapper', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.query('users', {
        where: { $not: { status: 'banned' } },
      })

      const filter = mockCountDocuments.mock.calls[0]![0] as Record<string, unknown>
      expect(filter).toEqual({ $nor: [{ status: 'banned' }] })
    })

    it('translates $ilike to case-insensitive $regex', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.query('users', {
        where: { name: { $ilike: 'ali%' } },
      })

      const filter = mockCountDocuments.mock.calls[0]![0] as Record<string, unknown>
      // ilikeToRegex anchors with ^/$ and maps % → .*
      expect(filter).toEqual({
        name: { $regex: '^ali.*$', $options: 'i' },
      })
    })

    it('applies orderBy', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.query('users', { orderBy: { field: 'name', dir: 'desc' } })

      expect(mockSort).toHaveBeenCalled()
    })

    it('applies limit and offset', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.query('users', { limit: 5, offset: 10 })

      expect(mockLimit).toHaveBeenCalledWith(5)
      expect(mockSkip).toHaveBeenCalledWith(10)
    })

    it('applies select projection', async () => {
      mockToArray.mockResolvedValue([])
      mockCountDocuments.mockResolvedValue(0)

      const bridge = new MongodbBridge({ url: 'mongodb://localhost:27017/test' })
      await bridge.connect()
      await bridge.query('users', { select: ['name', 'email'] })

      expect(mockProject).toHaveBeenCalledWith({ name: 1, email: 1 })
    })
  })
})
