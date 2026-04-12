import { describe, it, expect, beforeEach } from 'vitest'
import { resolveBridge, registerBridge, clearCustomBridges, listBridges } from './index.js'
import type { Bridge, BridgeConstructor } from '@semilayer/core'

class FakeBridge implements Bridge {
  async connect() {}
  async read() {
    return { rows: [] }
  }
  async count() {
    return 0
  }
  async disconnect() {}
}

beforeEach(() => {
  clearCustomBridges()
})

describe('resolveBridge', () => {
  it('resolves built-in bridge-postgres', () => {
    const Ctor = resolveBridge('@semilayer/bridge-postgres')
    expect(Ctor).toBeDefined()
    expect(typeof Ctor).toBe('function')
  })

  it('throws for unknown bridge', () => {
    expect(() => resolveBridge('@semilayer/bridge-nonexistent')).toThrow('Unknown bridge')
  })
})

describe('registerBridge', () => {
  it('registers and resolves a custom bridge', () => {
    registerBridge('@acme/bridge-oracle', FakeBridge as unknown as BridgeConstructor)
    const Ctor = resolveBridge('@acme/bridge-oracle')
    expect(Ctor).toBe(FakeBridge)
  })

  it('custom bridges take priority over built-in', () => {
    registerBridge('@semilayer/bridge-postgres', FakeBridge as unknown as BridgeConstructor)
    const Ctor = resolveBridge('@semilayer/bridge-postgres')
    expect(Ctor).toBe(FakeBridge)
  })
})

describe('clearCustomBridges', () => {
  it('removes custom bridges', () => {
    registerBridge('@acme/bridge-oracle', FakeBridge as unknown as BridgeConstructor)
    clearCustomBridges()
    expect(() => resolveBridge('@acme/bridge-oracle')).toThrow('Unknown bridge')
  })
})

describe('listBridges', () => {
  it('lists built-in bridges', () => {
    expect(listBridges()).toContain('@semilayer/bridge-postgres')
  })

  it('includes custom bridges', () => {
    registerBridge('@acme/bridge-oracle', FakeBridge as unknown as BridgeConstructor)
    const list = listBridges()
    expect(list).toContain('@semilayer/bridge-postgres')
    expect(list).toContain('@acme/bridge-oracle')
  })
})
