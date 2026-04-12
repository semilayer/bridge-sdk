/**
 * Simulates the exact crash that brought down the service/worker on platforms
 * where the duckdb native binary is missing (Windows, Alpine, older glibc, etc).
 *
 * Before the lazy-load fix, `import '@semilayer/bridge-resolver'` would
 * transitively import '@semilayer/bridge-duckdb' which would run
 * `import duckdb from 'duckdb'` at module scope, triggering
 * `require('./binding/duckdb.node')` which threw MODULE_NOT_FOUND and
 * crashed the entire process at startup.
 *
 * After the fix:
 *   - Importing DuckdbBridge is always safe (no native load at module scope)
 *   - The native load is deferred to connect(), which wraps any failure with
 *     a clear, actionable error message.
 */
import { describe, it, expect, vi } from 'vitest'

// Simulate the native binary being absent. When vi.mock factory throws,
// any `await import('duckdb')` in the module under test will reject with
// this error — exactly what Node.js does when .node binary is missing.
vi.mock('duckdb', () => {
  throw Object.assign(
    new Error("Cannot find module './binding/duckdb.node'"),
    { code: 'MODULE_NOT_FOUND' },
  )
})

describe('DuckdbBridge — missing native binary (simulated crash)', () => {
  it('importing the class is always safe — no crash at module load', async () => {
    // This is the critical invariant: bridge-resolver imports DuckdbBridge at
    // startup on every platform. It must never throw, even when duckdb.node
    // is absent. The native load only happens inside connect().
    const { DuckdbBridge } = await import('./bridge.js')
    expect(() => new DuckdbBridge({ path: ':memory:' })).not.toThrow()
  })

  it('connect() throws a helpful message instead of a raw MODULE_NOT_FOUND', async () => {
    const { DuckdbBridge } = await import('./bridge.js')
    const bridge = new DuckdbBridge({ path: ':memory:' })

    await expect(bridge.connect()).rejects.toThrow(
      'DuckDB native module failed to load',
    )
  })

  it('connect() error includes platform + arch + Node version for diagnosis', async () => {
    const { DuckdbBridge } = await import('./bridge.js')
    const bridge = new DuckdbBridge({ path: ':memory:' })

    await expect(bridge.connect()).rejects.toThrow(process.platform)
    await expect(bridge.connect()).rejects.toThrow(process.arch)
  })

  it('connect() error includes the remediation hint', async () => {
    const { DuckdbBridge } = await import('./bridge.js')
    const bridge = new DuckdbBridge({ path: ':memory:' })

    await expect(bridge.connect()).rejects.toThrow('npm rebuild duckdb')
  })
})
