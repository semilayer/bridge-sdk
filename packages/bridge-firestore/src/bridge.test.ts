import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { FirestoreBridge } from './bridge.js'

const mockGet = vi.fn()
const mockLimit = vi.fn().mockReturnThis()
const mockOffset = vi.fn().mockReturnThis()
const mockStartAfter = vi.fn().mockReturnThis()
const mockOrderBy = vi.fn().mockReturnThis()
const mockWhere = vi.fn().mockReturnThis()
const mockSelect = vi.fn().mockReturnThis()
const mockCountGet = vi.fn()
const mockCount = vi.fn(() => ({ get: mockCountGet }))
const mockCollection = vi.fn(() => ({
  orderBy: mockOrderBy,
  where: mockWhere,
  limit: mockLimit,
  offset: mockOffset,
  startAfter: mockStartAfter,
  select: mockSelect,
  get: mockGet,
  count: mockCount,
}))
const mockListCollections = vi.fn()
const mockTerminate = vi.fn()

vi.mock('@google-cloud/firestore', () => ({
  Firestore: vi.fn(() => ({
    collection: mockCollection,
    listCollections: mockListCollections,
    terminate: mockTerminate,
  })),
  FieldPath: { documentId: vi.fn(() => '__docId__') },
}))

function makeDocs(items: Array<{ id: string; [k: string]: unknown }>) {
  return items.map((item) => ({
    id: item['id'],
    data: () => {
      const { id: _id, ...rest } = item
      return rest
    },
  }))
}

function seedDocs(items: Array<{ id: string; [k: string]: unknown }>) {
  const docs = makeDocs(items)
  mockGet.mockResolvedValueOnce({ docs, size: docs.length })
}

function seedCount(n: number) {
  mockCountGet.mockResolvedValueOnce({ data: () => ({ count: n }) })
}

describe('FirestoreBridge', () => {
  beforeEach(() => {
    // resetAllMocks clears queued return values too (unlike clearAllMocks)
    vi.resetAllMocks()
    // Re-apply chain mocks after resetting
    mockOrderBy.mockReturnThis()
    mockWhere.mockReturnThis()
    mockLimit.mockReturnThis()
    mockOffset.mockReturnThis()
    mockStartAfter.mockReturnThis()
    mockSelect.mockReturnThis()
    // Rebuild collection mock (reset clears the factory implementation)
    mockCollection.mockReturnValue({
      orderBy: mockOrderBy,
      where: mockWhere,
      limit: mockLimit,
      offset: mockOffset,
      startAfter: mockStartAfter,
      select: mockSelect,
      get: mockGet,
      count: mockCount,
    })
    // Default resolved values (can be overridden per-test)
    mockListCollections.mockResolvedValue([{ id: 'users' }, { id: 'orders' }])
    mockTerminate.mockResolvedValue(undefined)
    // Default empty snapshot so tests that don't seed get a safe fallback
    mockGet.mockResolvedValue({ docs: [], size: 0 })
    mockCountGet.mockResolvedValue({ data: () => ({ count: 0 }) })
    mockCount.mockReturnValue({ get: mockCountGet })
  })

  it('throws if projectId is missing', () => {
    expect(() => new FirestoreBridge({})).toThrow('FirestoreBridge requires a "projectId" config string')
  })

  it('throws if projectId is not a string', () => {
    expect(() => new FirestoreBridge({ projectId: 123 })).toThrow(
      'FirestoreBridge requires a "projectId" config string',
    )
  })

  it('constructs with valid projectId', () => {
    expect(() => new FirestoreBridge({ projectId: 'my-project' })).not.toThrow()
  })

  describe('connect / disconnect', () => {
    it('connect creates Firestore and calls listCollections', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      expect(mockListCollections).toHaveBeenCalledOnce()
    })

    it('disconnect calls terminate', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.disconnect()
      expect(mockTerminate).toHaveBeenCalledOnce()
    })

    it('disconnect is safe when not connected', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await expect(bridge.disconnect()).resolves.toBeUndefined()
    })

    it('throws when calling read without connect', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await expect(bridge.read('users')).rejects.toThrow('FirestoreBridge is not connected')
    })
  })

  describe('listTargets', () => {
    it('returns collection ids', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      // Reset so second call returns ids
      mockListCollections.mockResolvedValueOnce([{ id: 'users' }, { id: 'orders' }])
      const targets = await bridge.listTargets()
      expect(targets).toEqual(['users', 'orders'])
    })
  })

  describe('count', () => {
    it('calls count().get() and returns data().count (back-compat, no options)', async () => {
      seedCount(15)
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      const n = await bridge.count('users')
      expect(n).toBe(15)
      expect(mockCount).toHaveBeenCalledOnce()
      expect(mockCountGet).toHaveBeenCalledOnce()
    })

    it('applies where predicates before counting', async () => {
      seedCount(3)
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      const n = await bridge.count('users', { where: { status: { $eq: 'active' } } })
      expect(n).toBe(3)
      expect(mockWhere).toHaveBeenCalledWith('status', '==', 'active')
    })
  })

  describe('read', () => {
    it('returns rows with id field merged', async () => {
      seedDocs([
        { id: 'doc1', name: 'Alice' },
        { id: 'doc2', name: 'Bob' },
      ])
      seedCount(2)

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      const result = await bridge.read('users', { limit: 10 })

      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ id: 'doc1', name: 'Alice' })
      expect(result.nextCursor).toBeUndefined()
      expect(result.total).toBe(2)
    })

    it('sets nextCursor when more rows than limit', async () => {
      seedDocs([
        { id: 'doc1', name: 'Alice' },
        { id: 'doc2', name: 'Bob' },
        { id: 'doc3', name: 'Carol' },
      ])
      seedCount(10)

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      const result = await bridge.read('users', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('doc2')
    })

    it('calls startAfter when cursor is provided', async () => {
      seedDocs([{ id: 'doc2', name: 'Bob' }])
      seedCount(1)

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.read('users', { cursor: 'doc1', limit: 10 })

      expect(mockStartAfter).toHaveBeenCalledWith('doc1')
    })

    it('uses default limit of 1000', async () => {
      seedDocs([])
      seedCount(0)

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.read('users')

      expect(mockLimit).toHaveBeenCalledWith(1001)
    })
  })

  describe('query', () => {
    it('applies $eq as == operator', async () => {
      mockGet.mockResolvedValueOnce({
        docs: makeDocs([{ id: 'doc1', name: 'Alice' }]),
        size: 1,
      })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      const result = await bridge.query('users', { where: { name: { $eq: 'Alice' } } })

      expect(result.rows).toHaveLength(1)
      expect(mockWhere).toHaveBeenCalledWith('name', '==', 'Alice')
    })

    it('applies plain equality', async () => {
      mockGet.mockResolvedValueOnce({
        docs: makeDocs([{ id: 'doc1', status: 'active' }]),
        size: 1,
      })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.query('users', { where: { status: 'active' } })

      expect(mockWhere).toHaveBeenCalledWith('status', '==', 'active')
    })

    it('applies $gt operator', async () => {
      mockGet.mockResolvedValueOnce({ docs: [], size: 0 })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.query('users', { where: { age: { $gt: 18 } } })

      expect(mockWhere).toHaveBeenCalledWith('age', '>', 18)
    })

    it('applies $in operator', async () => {
      mockGet.mockResolvedValueOnce({ docs: [], size: 0 })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.query('users', { where: { role: { $in: ['admin', 'editor'] } } })

      expect(mockWhere).toHaveBeenCalledWith('role', 'in', ['admin', 'editor'])
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await expect(
        bridge.query('users', { where: { name: { $regex: 'Ali' } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('throws UnsupportedOperatorError on $or (logical op not declared)', async () => {
      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await expect(
        bridge.query('users', {
          where: { $or: [{ status: 'active' }, { status: 'pending' }] },
        }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('applies orderBy', async () => {
      mockGet.mockResolvedValueOnce({ docs: [], size: 0 })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.query('users', { orderBy: { field: 'name', dir: 'desc' } })

      expect(mockOrderBy).toHaveBeenCalledWith('name', 'desc')
    })

    it('applies limit and offset', async () => {
      mockGet.mockResolvedValueOnce({ docs: [], size: 0 })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      await bridge.query('users', { limit: 5, offset: 10 })

      expect(mockLimit).toHaveBeenCalledWith(5)
      expect(mockOffset).toHaveBeenCalledWith(10)
    })

    it('returns total as snapshot.size', async () => {
      mockGet.mockResolvedValueOnce({
        docs: makeDocs([{ id: 'doc1', name: 'Alice' }]),
        size: 42,
      })

      const bridge = new FirestoreBridge({ projectId: 'my-project' })
      await bridge.connect()
      const result = await bridge.query('users', {})

      expect(result.total).toBe(42)
    })
  })
})
