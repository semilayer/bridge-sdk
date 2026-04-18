<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-sdk</strong>
</p>

---

# @semilayer/bridge-sdk

SemiLayer Bridge SDK — the interface and test harness for building SemiLayer database bridges.

## What's in here

- **Bridge interface** (re-exported from `@semilayer/core`) — the contract every bridge must implement: `connect`, `read`, `count`, `query`, `disconnect`.
- **`MockBridge`** — an in-memory `Bridge` implementation. Use it in your service tests when you need a deterministic bridge without standing up a real database.
- **`createBridgeTestSuite`** — a vitest compliance harness. Drop it into your bridge package's tests and it'll exercise the full Bridge contract against your implementation.

## Install

```bash
npm install --save-dev @semilayer/bridge-sdk vitest
```

`vitest` is a peer dependency — your bridge package brings its own.

## Usage

### MockBridge

```ts
import { MockBridge } from '@semilayer/bridge-sdk'

const bridge = new MockBridge()
bridge.seed('users', [
  { id: 1, name: 'Ada' },
  { id: 2, name: 'Grace' },
])
await bridge.connect()
const result = await bridge.read('users', { limit: 10 })
```

### Compliance test suite

```ts
import { createBridgeTestSuite } from '@semilayer/bridge-sdk'
import { MyBridge } from './bridge.js'

createBridgeTestSuite({
  factory: () => new MyBridge({ /* config */ }),
  seed: {
    target: 'items',
    rows: [/* ... */],
    primaryKey: 'id',
  },
})
```

## Building a new bridge

The canonical guide lives at the [bridge-sdk monorepo CONTRIBUTING.md](https://github.com/semilayer/bridge-sdk/blob/main/CONTRIBUTING.md). Use `pnpm new-bridge <name>` from the monorepo root to scaffold a new bridge package under `packages/`.

## License

MIT
