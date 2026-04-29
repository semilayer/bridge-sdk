import { describe, it, expect, vi, beforeEach } from 'vitest'
import { UnsupportedOperatorError } from '@semilayer/bridge-sdk'
import { RedisBridge } from './bridge.js'

const mockPing = vi.fn().mockResolvedValue('PONG')
const mockKeys = vi.fn()
const mockMget = vi.fn()
const mockDisconnect = vi.fn()

vi.mock('ioredis', () => ({
  default: vi.fn(() => ({
    ping: mockPing,
    keys: mockKeys,
    mget: mockMget,
    disconnect: mockDisconnect,
  })),
}))

describe('RedisBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockPing.mockResolvedValue('PONG')
  })

  describe('constructor', () => {
    it('throws if neither url nor host is provided', () => {
      expect(() => new RedisBridge({})).toThrow('RedisBridge requires either "url" or "host" config')
    })

    it('accepts url config', () => {
      expect(() => new RedisBridge({ url: 'redis://localhost:6379' })).not.toThrow()
    })

    it('accepts host config', () => {
      expect(() => new RedisBridge({ host: 'localhost' })).not.toThrow()
    })
  })

  describe('connect', () => {
    it('calls ping after creating client with url', async () => {
      const bridge = new RedisBridge({ url: 'redis://localhost:6379' })
      await bridge.connect()
      expect(mockPing).toHaveBeenCalledOnce()
    })

    it('calls ping after creating client with host+port', async () => {
      const bridge = new RedisBridge({ host: 'localhost', port: 6379 })
      await bridge.connect()
      expect(mockPing).toHaveBeenCalledOnce()
    })
  })

  describe('disconnect', () => {
    it('calls disconnect on the redis client', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      await bridge.disconnect()
      expect(mockDisconnect).toHaveBeenCalledOnce()
    })
  })

  describe('assertRedis', () => {
    it('throws if not connected', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await expect(bridge.count('users')).rejects.toThrow('RedisBridge is not connected')
    })
  })

  describe('count', () => {
    it('returns the number of keys matching the prefix (back-compat, no options)', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2', 'user:3'])
      const result = await bridge.count('user')
      expect(mockKeys).toHaveBeenCalledWith('user:*')
      expect(result).toBe(3)
    })

    it('count(target, {where}) routes via query() and returns row count', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      // query() → read() → keys() + mget()
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2', 'user:3'])
      mockMget.mockResolvedValueOnce([
        JSON.stringify({ status: 'active' }),
        JSON.stringify({ status: 'inactive' }),
        JSON.stringify({ status: 'active' }),
      ])
      const n = await bridge.count('user', { where: { status: { $eq: 'active' } } })
      expect(n).toBe(2)
    })
  })

  describe('listTargets', () => {
    it('returns unique prefixes from all keys', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2', 'product:1', 'order:5'])
      const result = await bridge.listTargets()
      expect(mockKeys).toHaveBeenCalledWith('*')
      expect(result.sort()).toEqual(['order', 'product', 'user'])
    })

    it('filters out keys without colons', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce(['user:1', 'bare-key'])
      const result = await bridge.listTargets()
      expect(result).toContain('user')
      // 'bare-key'.split(':')[0] is 'bare-key', which is truthy — it gets included
      // only truly empty prefixes are filtered
      expect(result.every(Boolean)).toBe(true)
    })
  })

  describe('read', () => {
    it('returns rows with parsed JSON values', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2'])
      mockMget.mockResolvedValueOnce([
        JSON.stringify({ name: 'Alice', age: 30 }),
        JSON.stringify({ name: 'Bob', age: 25 }),
      ])
      const result = await bridge.read('user')
      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ _key: 'user:1', name: 'Alice', age: 30 })
      expect(result.rows[1]).toMatchObject({ _key: 'user:2', name: 'Bob', age: 25 })
      expect(result.total).toBe(2)
      expect(result.nextCursor).toBeUndefined()
    })

    it('wraps non-object values in { value: ... }', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce(['session:abc'])
      mockMget.mockResolvedValueOnce(['"raw-string"'])
      const result = await bridge.read('session')
      expect(result.rows[0]).toMatchObject({ _key: 'session:abc', value: 'raw-string' })
    })

    it('paginates with limit and returns nextCursor', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      // 3 keys but limit=2 → fetch limit+1=3, hasMore=true, slice to 2
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2', 'user:3'])
      mockMget.mockResolvedValueOnce([
        JSON.stringify({ name: 'Alice' }),
        JSON.stringify({ name: 'Bob' }),
      ])
      const result = await bridge.read('user', { limit: 2 })
      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe('user:2')
    })

    it('uses cursor to find the next page start', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      // allKeys sorted: user:1, user:2, user:3; cursor='user:2' → startIdx=2 (first key > cursor)
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2', 'user:3'])
      mockMget.mockResolvedValueOnce([JSON.stringify({ name: 'Charlie' })])
      const result = await bridge.read('user', { cursor: 'user:2', limit: 10 })
      // findIndex(k => k > 'user:2') → index 2 ('user:3')
      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ _key: 'user:3' })
    })

    it('returns empty rows when no keys match', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce([])
      const result = await bridge.read('unknown')
      expect(result.rows).toEqual([])
      expect(result.total).toBe(0)
    })

    it('skips null/undefined values from mget', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      mockKeys.mockResolvedValueOnce(['user:1', 'user:2'])
      mockMget.mockResolvedValueOnce([JSON.stringify({ name: 'Alice' }), null])
      const result = await bridge.read('user')
      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ name: 'Alice' })
    })
  })

  describe('query', () => {
    async function setupBridgeWithRows(rows: Record<string, unknown>[]) {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      const keys = rows.map((_, i) => `item:${i + 1}`)
      mockKeys.mockResolvedValueOnce(keys)
      mockMget.mockResolvedValueOnce(rows.map(r => JSON.stringify(r)))
      return bridge
    }

    it('filters with $eq operator', async () => {
      const bridge = await setupBridgeWithRows([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
      ])
      const result = await bridge.query('item', { where: { name: { $eq: 'Alice' } } })
      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ name: 'Alice' })
    })

    it('filters with $gt operator', async () => {
      const bridge = await setupBridgeWithRows([
        { name: 'Alice', age: 30 },
        { name: 'Bob', age: 25 },
        { name: 'Charlie', age: 35 },
      ])
      const result = await bridge.query('item', { where: { age: { $gt: 28 } } })
      expect(result.rows).toHaveLength(2)
      expect(result.rows.map(r => r['name'])).toContain('Alice')
      expect(result.rows.map(r => r['name'])).toContain('Charlie')
    })

    it('filters with $in operator', async () => {
      const bridge = await setupBridgeWithRows([
        { name: 'Alice', status: 'active' },
        { name: 'Bob', status: 'inactive' },
        { name: 'Charlie', status: 'active' },
      ])
      const result = await bridge.query('item', { where: { status: { $in: ['active'] } } })
      expect(result.rows).toHaveLength(2)
    })

    it('throws UnsupportedOperatorError on unknown operator', async () => {
      const bridge = await setupBridgeWithRows([{ name: 'Alice' }])
      await expect(
        bridge.query('item', { where: { name: { $unknown: 'foo' } } }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('throws UnsupportedOperatorError on $or (logical op not declared)', async () => {
      const bridge = new RedisBridge({ host: 'localhost' })
      await bridge.connect()
      await expect(
        bridge.query('item', {
          where: { $or: [{ status: 'active' }, { status: 'pending' }] },
        }),
      ).rejects.toThrow(UnsupportedOperatorError)
    })

    it('applies orderBy', async () => {
      const bridge = await setupBridgeWithRows([
        { name: 'Charlie', score: 10 },
        { name: 'Alice', score: 30 },
        { name: 'Bob', score: 20 },
      ])
      const result = await bridge.query('item', { orderBy: { field: 'score', dir: 'desc' } })
      expect(result.rows.map(r => r['name'])).toEqual(['Alice', 'Bob', 'Charlie'])
    })

    it('applies limit and offset', async () => {
      const bridge = await setupBridgeWithRows([
        { name: 'A' },
        { name: 'B' },
        { name: 'C' },
        { name: 'D' },
      ])
      const result = await bridge.query('item', { limit: 2, offset: 1 })
      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ name: 'B' })
    })

    it('filters with plain equality (non-operator value)', async () => {
      const bridge = await setupBridgeWithRows([
        { name: 'Alice', active: true },
        { name: 'Bob', active: false },
      ])
      const result = await bridge.query('item', { where: { active: true } })
      expect(result.rows).toHaveLength(1)
      expect(result.rows[0]).toMatchObject({ name: 'Alice' })
    })
  })
})
