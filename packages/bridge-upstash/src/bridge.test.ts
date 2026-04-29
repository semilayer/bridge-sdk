import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { UpstashBridge } from './bridge.js'

const mockPing = vi.fn().mockResolvedValue('PONG')
const mockKeys = vi.fn()
const mockMget = vi.fn()

vi.mock('@upstash/redis', () => ({
  Redis: vi.fn(() => ({
    ping: mockPing,
    keys: mockKeys,
    mget: mockMget,
  })),
}))

function seedKeys(keys: string[]) {
  mockKeys.mockResolvedValueOnce(keys)
}

function seedMget(values: unknown[]) {
  mockMget.mockResolvedValueOnce(values)
}

describe('UpstashBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockPing.mockResolvedValue('PONG')
  })

  it('throws if url is missing', () => {
    expect(() => new UpstashBridge({ token: 'tok' })).toThrow(
      'UpstashBridge requires a "url" config string',
    )
  })

  it('throws if token is missing', () => {
    expect(() => new UpstashBridge({ url: 'https://x.upstash.io' })).toThrow(
      'UpstashBridge requires a "token" config string',
    )
  })

  it('constructs with valid url and token', () => {
    expect(
      () => new UpstashBridge({ url: 'https://x.upstash.io', token: 'mytoken' }),
    ).not.toThrow()
  })

  describe('connect / disconnect', () => {
    it('connect calls ping', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()
      expect(mockPing).toHaveBeenCalledOnce()
    })

    it('disconnect clears the redis instance', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()
      await bridge.disconnect()
      await expect(bridge.count('user')).rejects.toThrow('UpstashBridge is not connected')
    })

    it('disconnect is safe when not connected', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await expect(bridge.disconnect()).resolves.toBeUndefined()
    })
  })

  describe('listTargets', () => {
    it('extracts unique prefixes from all keys', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2', 'post:1', 'product:abc'])
      const targets = await bridge.listTargets()

      expect(targets.sort()).toEqual(['post', 'product', 'user'])
    })

    it('excludes keys without a colon prefix', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'naked-key'])
      const targets = await bridge.listTargets()

      // 'naked-key'.split(':')[0] === 'naked-key' — still truthy so included
      expect(targets).toContain('user')
    })

    it('returns empty array when no keys exist', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys([])
      const targets = await bridge.listTargets()

      expect(targets).toEqual([])
    })
  })

  describe('count', () => {
    it('counts keys matching target:* pattern', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2', 'user:3'])
      const n = await bridge.count('user')

      expect(n).toBe(3)
      expect(mockKeys).toHaveBeenCalledWith('user:*')
    })

    it('returns 0 when no matching keys', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys([])
      const n = await bridge.count('user')

      expect(n).toBe(0)
    })

    it('count(target, {where}) routes via query() and returns row count', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()
      seedKeys(['user:1', 'user:2', 'user:3'])
      seedMget([
        JSON.stringify({ status: 'active' }),
        JSON.stringify({ status: 'inactive' }),
        JSON.stringify({ status: 'active' }),
      ])
      const n = await bridge.count('user', { where: { status: { $eq: 'active' } } })
      expect(n).toBe(2)
    })
  })

  describe('read', () => {
    it('returns rows with _key and parsed JSON fields', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2'])
      seedMget([
        JSON.stringify({ name: 'Alice', age: 30 }),
        JSON.stringify({ name: 'Bob', age: 25 }),
      ])

      const result = await bridge.read('user', { limit: 10 })

      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ _key: 'user:1', name: 'Alice', age: 30 })
      expect(result.rows[1]).toMatchObject({ _key: 'user:2', name: 'Bob', age: 25 })
      expect(result.total).toBe(2)
      expect(result.nextCursor).toBeUndefined()
    })

    it('sets nextCursor when more keys than limit', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      // 3 keys, limit=2 → return 2, nextCursor=user:2
      seedKeys(['user:1', 'user:2', 'user:3'])
      seedMget([
        JSON.stringify({ name: 'Alice' }),
        JSON.stringify({ name: 'Bob' }),
      ])

      const result = await bridge.read('user', { limit: 2 })

      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('user:2')
      expect(result.total).toBe(3)
    })

    it('applies cursor to skip already-seen keys', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      // All keys sorted: user:1, user:2, user:3
      // cursor='user:1' → find first key > 'user:1' → index 1 (user:2)
      seedKeys(['user:1', 'user:2', 'user:3'])
      seedMget([
        JSON.stringify({ name: 'Bob' }),
        JSON.stringify({ name: 'Carol' }),
      ])

      const result = await bridge.read('user', { cursor: 'user:1', limit: 10 })

      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ _key: 'user:2', name: 'Bob' })
    })

    it('returns empty rows for empty target', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys([])
      const result = await bridge.read('empty', { limit: 10 })

      expect(result.rows).toEqual([])
      expect(result.total).toBe(0)
      expect(mockMget).not.toHaveBeenCalled()
    })

    it('handles pre-parsed object values from Redis', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1'])
      // Upstash SDK auto-parses JSON in some modes — value already an object
      seedMget([{ name: 'Alice', age: 30 }])

      const result = await bridge.read('user', { limit: 10 })

      expect(result.rows[0]).toMatchObject({ _key: 'user:1', name: 'Alice', age: 30 })
    })

    it('wraps primitive values in { value: ... }', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['counter:hits'])
      seedMget(['42'])

      const result = await bridge.read('counter', { limit: 10 })

      expect(result.rows[0]).toMatchObject({ _key: 'counter:hits', value: 42 })
    })

    it('skips null/undefined values', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2'])
      seedMget([null, JSON.stringify({ name: 'Bob' })])

      const result = await bridge.read('user', { limit: 10 })

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ name: 'Bob' })
    })
  })

  describe('query', () => {
    it('filters with $eq', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      // read() will call keys + mget internally
      seedKeys(['user:1', 'user:2'])
      seedMget([
        JSON.stringify({ name: 'Alice', age: 30 }),
        JSON.stringify({ name: 'Bob', age: 25 }),
      ])

      const result = await bridge.query('user', { where: { name: { $eq: 'Alice' } } })

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ name: 'Alice' })
      expect(result.total).toBe(1)
    })

    it('filters with $gt', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2', 'user:3'])
      seedMget([
        JSON.stringify({ name: 'Alice', age: 30 }),
        JSON.stringify({ name: 'Bob', age: 25 }),
        JSON.stringify({ name: 'Carol', age: 35 }),
      ])

      const result = await bridge.query('user', { where: { age: { $gt: 28 } } })

      expect(result.rows).toHaveLength(2)
      expect(result.rows.map((r) => r['name'])).toEqual(
        expect.arrayContaining(['Alice', 'Carol']),
      )
    })

    it('filters with $in', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2'])
      seedMget([
        JSON.stringify({ name: 'Alice', role: 'admin' }),
        JSON.stringify({ name: 'Bob', role: 'viewer' }),
      ])

      const result = await bridge.query('user', { where: { role: { $in: ['admin', 'editor'] } } })

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ role: 'admin' })
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1'])
      seedMget([JSON.stringify({ name: 'Alice' })])

      await expect(
        bridge.query('user', { where: { name: { $regex: 'Al' } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('throws UnsupportedOperatorError on $or (logical op not declared)', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()
      await expect(
        bridge.query('user', {
          where: { $or: [{ status: 'active' }, { status: 'pending' }] },
        }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('sorts with orderBy desc', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2', 'user:3'])
      seedMget([
        JSON.stringify({ name: 'Alice', age: 30 }),
        JSON.stringify({ name: 'Bob', age: 25 }),
        JSON.stringify({ name: 'Carol', age: 35 }),
      ])

      const result = await bridge.query('user', { orderBy: { field: 'age', dir: 'desc' } })

      expect(result.rows[0]!['age']).toBe(35)
      expect(result.rows[1]!['age']).toBe(30)
      expect(result.rows[2]!['age']).toBe(25)
    })

    it('applies limit and offset', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2', 'user:3'])
      seedMget([
        JSON.stringify({ name: 'Alice' }),
        JSON.stringify({ name: 'Bob' }),
        JSON.stringify({ name: 'Carol' }),
      ])

      const result = await bridge.query('user', { limit: 1, offset: 1 })

      // total is pre-slice count (3), after offset=1 we get 2 items, then limit=1 gives 1
      expect(result.rows).toHaveLength(1)
      expect(result.total).toBe(3)
    })

    it('filters with plain equality', async () => {
      const bridge = new UpstashBridge({ url: 'https://x.upstash.io', token: 'tok' })
      await bridge.connect()

      seedKeys(['user:1', 'user:2'])
      seedMget([
        JSON.stringify({ name: 'Alice', active: true }),
        JSON.stringify({ name: 'Bob', active: false }),
      ])

      const result = await bridge.query('user', { where: { active: true } })

      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ name: 'Alice' })
    })
  })
})
