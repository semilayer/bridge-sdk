import { describe, it, expect, vi, beforeEach } from 'vitest'
import { SupabaseBridge } from './bridge.js'

const mockQueryResult = vi.fn()

const mockBuilder = {
  select: vi.fn().mockReturnThis(),
  gt: vi.fn().mockReturnThis(),
  gte: vi.fn().mockReturnThis(),
  lt: vi.fn().mockReturnThis(),
  lte: vi.fn().mockReturnThis(),
  eq: vi.fn().mockReturnThis(),
  in: vi.fn().mockReturnThis(),
  order: vi.fn().mockReturnThis(),
  limit: vi.fn().mockReturnThis(),
  range: vi.fn().mockReturnThis(),
  then: (resolve: (v: unknown) => void) => mockQueryResult().then(resolve),
}

const mockFrom = vi.fn(() => mockBuilder)

vi.mock('@supabase/supabase-js', () => ({
  createClient: vi.fn(() => ({ from: mockFrom })),
}))

function seedResult(data: unknown[], count = data.length, error: null | { message: string } = null) {
  mockQueryResult.mockResolvedValueOnce({ data, error, count })
}

describe('SupabaseBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    Object.assign(mockBuilder, {
      select: vi.fn().mockReturnThis(),
      gt: vi.fn().mockReturnThis(),
      gte: vi.fn().mockReturnThis(),
      lt: vi.fn().mockReturnThis(),
      lte: vi.fn().mockReturnThis(),
      eq: vi.fn().mockReturnThis(),
      in: vi.fn().mockReturnThis(),
      order: vi.fn().mockReturnThis(),
      limit: vi.fn().mockReturnThis(),
      range: vi.fn().mockReturnThis(),
      then: (resolve: (v: unknown) => void) => mockQueryResult().then(resolve),
    })
    mockFrom.mockReturnValue(mockBuilder)
  })

  it('throws if url is missing', () => {
    expect(() => new SupabaseBridge({ key: 'abc' })).toThrow(
      'SupabaseBridge requires a "url" config string',
    )
  })

  it('throws if key is missing', () => {
    expect(() => new SupabaseBridge({ url: 'https://x.supabase.co' })).toThrow(
      'SupabaseBridge requires a "key" config string',
    )
  })

  it('constructs with valid url and key', () => {
    expect(
      () => new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon-key' }),
    ).not.toThrow()
  })

  describe('connect / disconnect', () => {
    it('connect creates client and does a probe select', async () => {
      // probe returns 200
      seedResult([], 0)

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      expect(mockFrom).toHaveBeenCalledWith('_')
    })

    it('connect throws on 401 status', async () => {
      mockQueryResult.mockResolvedValueOnce({ data: null, error: null, count: 0, status: 401 })

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'bad-key' })
      await expect(bridge.connect()).rejects.toThrow('authentication failed')
    })

    it('disconnect clears client', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()
      await bridge.disconnect()

      await expect(bridge.read('users')).rejects.toThrow('SupabaseBridge is not connected')
    })
  })

  describe('count', () => {
    it('calls select with head:true and returns count', async () => {
      seedResult([], 7)

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([], 7)
      const n = await bridge.count('users')

      expect(n).toBe(7)
    })

    it('throws on error', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      mockQueryResult.mockResolvedValueOnce({ data: null, error: { message: 'Table not found' }, count: null })
      await expect(bridge.count('nonexistent')).rejects.toThrow('Table not found')
    })
  })

  describe('read', () => {
    it('returns rows and total', async () => {
      seedResult([]) // connect probe

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }], 2)
      const result = await bridge.read('users', { limit: 10 })

      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ id: 1, name: 'Alice' })
      expect(result.nextCursor).toBeUndefined()
      expect(result.total).toBe(2)
    })

    it('sets nextCursor when more rows than limit', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult(
        [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }, { id: 3, name: 'Carol' }],
        10,
      )
      const result = await bridge.read('users', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('2')
    })

    it('calls gt with cursor value', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([{ id: 5, name: 'Eve' }], 1)
      await bridge.read('users', { cursor: '4', limit: 10 })

      expect(mockBuilder.gt).toHaveBeenCalledWith('id', '4')
    })

    it('throws on error response', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      mockQueryResult.mockResolvedValueOnce({ data: null, error: { message: 'Bad query' }, count: null })
      await expect(bridge.read('users')).rejects.toThrow('Bad query')
    })
  })

  describe('query', () => {
    it('applies $eq operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([{ id: 1, name: 'Alice' }], 1)
      const result = await bridge.query('users', { where: { name: { $eq: 'Alice' } } })

      expect(result.rows).toHaveLength(1)
      expect(mockBuilder.eq).toHaveBeenCalledWith('name', 'Alice')
    })

    it('applies $gt operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { where: { age: { $gt: 30 } } })

      expect(mockBuilder.gt).toHaveBeenCalledWith('age', 30)
    })

    it('applies $gte operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { where: { age: { $gte: 18 } } })

      expect(mockBuilder.gte).toHaveBeenCalledWith('age', 18)
    })

    it('applies $lt operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { where: { score: { $lt: 100 } } })

      expect(mockBuilder.lt).toHaveBeenCalledWith('score', 100)
    })

    it('applies $lte operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { where: { score: { $lte: 50 } } })

      expect(mockBuilder.lte).toHaveBeenCalledWith('score', 50)
    })

    it('applies $in operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { where: { role: { $in: ['admin', 'editor'] } } })

      expect(mockBuilder.in).toHaveBeenCalledWith('role', ['admin', 'editor'])
    })

    it('applies plain equality', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { where: { active: true } })

      expect(mockBuilder.eq).toHaveBeenCalledWith('active', true)
    })

    it('throws on unknown operator', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      await expect(
        bridge.query('users', { where: { name: { $regex: 'Al' } } }),
      ).rejects.toThrow('Unknown operator "$regex" on field "name"')
    })

    it('applies orderBy', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { orderBy: { field: 'name', dir: 'asc' } })

      expect(mockBuilder.order).toHaveBeenCalledWith('name', { ascending: true })
    })

    it('applies limit and offset via range', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { limit: 5, offset: 10 })

      expect(mockBuilder.range).toHaveBeenCalledWith(10, 14)
    })

    it('applies limit only (no offset)', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      seedResult([])
      await bridge.query('users', { limit: 3 })

      expect(mockBuilder.limit).toHaveBeenCalledWith(3)
    })

    it('propagates error', async () => {
      seedResult([])

      const bridge = new SupabaseBridge({ url: 'https://x.supabase.co', key: 'anon' })
      await bridge.connect()

      mockQueryResult.mockResolvedValueOnce({ data: null, error: { message: 'Permission denied' }, count: null })
      await expect(bridge.query('secret', {})).rejects.toThrow('Permission denied')
    })
  })
})
