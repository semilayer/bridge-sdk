import { describe, it, expect, beforeAll } from 'vitest'
import { MockBridge } from './mock-bridge.js'
import { createBridgeTestSuite } from './test-suite.js'
import type { BridgeRow } from '@semilayer/core'

const seedRows: BridgeRow[] = [
  { id: 1, name: 'Alpha', category: 'a' },
  { id: 2, name: 'Beta', category: 'b' },
  { id: 3, name: 'Gamma', category: 'a' },
  { id: 4, name: 'Delta', category: 'b' },
  { id: 5, name: 'Epsilon', category: 'a' },
]

function createSeededBridge(): MockBridge {
  const bridge = new MockBridge()
  bridge.seed('items', seedRows)
  return bridge
}

createBridgeTestSuite({
  factory: () => createSeededBridge(),
  seed: { target: 'items', rows: seedRows, primaryKey: 'id' },
})

describe('MockBridge extras', () => {
  let bridge: MockBridge

  beforeAll(async () => {
    bridge = createSeededBridge()
    await bridge.connect()
  })

  it('throws when not connected', async () => {
    const b = new MockBridge()
    b.seed('items', seedRows)
    await expect(b.read('items')).rejects.toThrow('not connected')
  })

  it('throws for unseeded target', async () => {
    await expect(bridge.read('nonexistent')).rejects.toThrow('not seeded')
  })

  it('read() with fields picks only requested fields', async () => {
    const result = await bridge.read('items', { fields: ['id', 'name'] })
    for (const row of result.rows) {
      expect(Object.keys(row).sort()).toEqual(['id', 'name'])
    }
  })

  it('query() filters with where clause', async () => {
    const result = await bridge.query('items', { where: { category: 'a' } })
    expect(result.rows).toHaveLength(3)
    expect(result.rows.every((r) => r['category'] === 'a')).toBe(true)
  })

  it('query() sorts descending', async () => {
    const result = await bridge.query('items', {
      orderBy: { field: 'id', dir: 'desc' },
    })
    expect(result.rows[0]!['id']).toBe(5)
    expect(result.rows[4]!['id']).toBe(1)
  })

  it('query() with select picks fields', async () => {
    const result = await bridge.query('items', { select: ['name'] })
    for (const row of result.rows) {
      expect(Object.keys(row)).toEqual(['name'])
    }
  })
})
