<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-resolver</strong>
</p>

---

# @semilayer/bridge-resolver

Built-in bridge registry for [SemiLayer](https://semilayer.dev). Ships with first-party database adapters bundled, and provides a community contribution path: submit a PR here to add a new database, and once merged and published, SemiLayer automatically consumes it.

## Installation

```bash
npm install @semilayer/bridge-resolver
# or
pnpm add @semilayer/bridge-resolver
```

## Usage

```typescript
import { resolveBridge } from '@semilayer/bridge-resolver'

const PostgresBridge = resolveBridge('@semilayer/bridge-postgres')
const bridge = new PostgresBridge({ url: process.env.DATABASE_URL })
await bridge.connect()
```

## Adding a New Database (Community Contributions)

**The PR path is the primary way to add a new database to SemiLayer.** Once your PR is merged here and a new version of `@semilayer/bridge-resolver` is published, SemiLayer automatically picks it up on its next release — every SemiLayer user gets your bridge for free, no extra setup required.

The flow:

1. Write your bridge as its own npm package (e.g. `@acme/bridge-mysql`) — see the [Bridge Authoring Guide](https://semilayer.dev/guides/bridges) for the interface and the `@semilayer/bridge-sdk` compliance test suite
2. Publish your package to npm
3. Open a PR to this repo that:
   - Adds your package as a dependency in `package.json`
   - Imports it and registers it in `BUILT_IN_BRIDGES` in `src/index.ts`
   - Adds a row to the "Built-in Bridges" table below
4. CI runs the test suite + your bridge registration test
5. The SemiLayer team reviews: code quality, security audit, license (MIT or Apache 2.0), maintenance commitment
6. Merged → release-please opens a Release PR → new version of `@semilayer/bridge-resolver` published to npm
7. SemiLayer's next release pulls in the new version — your bridge is live

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the detailed PR checklist.

## API

### `resolveBridge(name): BridgeConstructor`

Looks up a bridge by name. Checks custom runtime registrations first, then built-in bridges. Throws if no bridge matches.

### `registerBridge(name, ctor): void`

Registers a bridge constructor at runtime under a name. Used for **private/enterprise bridges** that won't be submitted upstream — custom code specific to your deployment.

```typescript
import { registerBridge } from '@semilayer/bridge-resolver'
import { MyInternalBridge } from './my-bridge.js'

// Call at worker startup, before first ingest job
registerBridge('@internal/bridge-custom', MyInternalBridge)
```

Custom runtime registrations take priority over built-ins with the same name — useful for testing or overriding behavior.

### `listBridges(): string[]`

Returns all registered bridge names (built-in + custom), deduplicated.

### `clearCustomBridges(): void`

Removes all runtime-registered bridges. Built-in bridges remain. Primarily useful in tests.

## Built-in Bridges

| Name | Package | Database |
|------|---------|----------|
| `@semilayer/bridge-postgres` | [`@semilayer/bridge-postgres`](https://github.com/semilayer/bridge-postgres) | PostgreSQL (and wire-compatible variants — Neon, Supabase, CockroachDB, ...) |

New bridges are added via community PRs — see above.

## Requirements

- Node.js 22+
- `@semilayer/core` (peer-compatible version)
- `@semilayer/bridge-postgres` (bundled as a dependency)

## Links

- [SemiLayer documentation](https://semilayer.dev)
- [Bridge authoring guide](https://semilayer.dev/guides/bridges)
- [`@semilayer/bridge-postgres`](https://github.com/semilayer/bridge-postgres) — reference implementation
- [Contributing](./CONTRIBUTING.md)

## License

MIT © SemiLayer
