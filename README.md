<p align="right">
  <img src="assets/logo.png" alt="SemiLayer" width="120" />
  <br />
  <strong>SemiLayer / bridges</strong>
</p>

<p align="right">
The home of every official <a href="https://semilayer.com">SemiLayer</a> database bridge — the SDK that defines the bridge interface, the registry that resolves bridge names at runtime, and every first-party adapter — all in one monorepo.
</p>

---

## Layout

This repo splits cleanly into two top-level directories:

- **`lib/`** — the framework: the SDK that defines the Bridge interface, and the resolver/registry that maps bridge names to constructors. These rarely change.
- **`packages/`** — the bridges themselves: one workspace package per database adapter. This is where the ecosystem grows.

## Packages

| Package | Path | Description |
|---|---|---|
| [`@semilayer/bridge-sdk`](./lib/bridge-sdk) | `lib/bridge-sdk` | Bridge interface, `MockBridge`, and the `createBridgeTestSuite` compliance harness. Bridge authors depend on this. |
| [`@semilayer/bridge-resolver`](./lib/bridge-resolver) | `lib/bridge-resolver` | Built-in registry. `apps/worker` and `apps/service` in the SemiLayer monorepo consume this to resolve bridge names like `@semilayer/bridge-postgres` to constructors. |
| [`@semilayer/bridge-postgres`](./packages/bridge-postgres) | `packages/bridge-postgres` | First-party PostgreSQL adapter. Reference implementation for new bridges. |

More bridges (mysql, sqlite, mongodb, …) live as sibling packages under `packages/` and are released independently via release-please.

## Quick start

```bash
pnpm install
pnpm turbo build
pnpm turbo test
```

## Adding a new bridge

```bash
pnpm new-bridge <name>     # e.g., pnpm new-bridge mysql
```

This scaffolds `packages/bridge-<name>/` from the template under `templates/bridge/`. See [CONTRIBUTING.md](./CONTRIBUTING.md) for the full bridge contribution guide.

## Declaring a bridge manifest

Every bridge should declare a `static manifest` property on its class. The SemiLayer
console and CLI read these manifests from `GET /v1/bridge-manifests` to render a
dynamic config form (or interactive prompts) without any hardcoded per-bridge logic.
Bridges without a manifest fall back to a plain connection-URL input.

### The shape

```ts
import type { BridgeManifest } from '@semilayer/core'

export class MyBridge implements Bridge {
  static manifest: BridgeManifest = {
    packageName:    '@semilayer/bridge-<name>',   // npm package name
    displayName:    'My Database',                // shown in dropdowns
    icon:           '<name>',                     // optional icon slug
    supportsUrl:    true,                         // accept a single connection URL?
    urlPlaceholder: 'mydb://user:pass@host/db',   // example URL for the UI hint
    fields: [
      // Primary fields — shown immediately
      { key: 'host',     label: 'Host',     type: 'string',   required: true,  placeholder: 'localhost' },
      { key: 'port',     label: 'Port',     type: 'number',   required: false, default: 5432 },
      { key: 'database', label: 'Database', type: 'string',   required: true  },
      { key: 'user',     label: 'Username', type: 'string',   required: true  },
      { key: 'password', label: 'Password', type: 'password', required: true  },
      // Advanced fields — collapsed behind a toggle in the console
      { key: 'ssl',      label: 'Use SSL',  type: 'boolean',  required: false, default: false, group: 'advanced' },
    ],
  }

  constructor(config: Record<string, unknown>) { /* ... */ }
  // ... Bridge interface methods
}
```

### Field types

| `type` | Console input | CLI prompt |
|--------|--------------|------------|
| `string` | Text input | `input()` |
| `password` | Masked input with eye toggle | `secret()` |
| `number` | Number input | `input()` — coerced to `Number` |
| `boolean` | Checkbox | `confirm()` |

### `group`

Omit `group` (or set `group: 'primary'`) for fields shown immediately. Set
`group: 'advanced'` for optional tuning knobs (pool sizes, timeouts, TLS options) —
the console collapses them behind a "Show advanced settings" toggle and the CLI
asks "Configure advanced settings? [y/N]" before prompting for them.

### `supportsUrl`

When `true`, the console shows a URL / Individual fields toggle and the CLI offers
"Connection URL" as an alternative to field-by-field entry. The config sent to the
API will be `{ url: "..." }` in URL mode and `{ host: "...", port: 5432, ... }` in
field mode — your constructor should handle both.

### Where it lands

`listManifests()` in `@semilayer/bridge-resolver` iterates every registered bridge and
collects the ones that have a `manifest`. The SemiLayer service exposes this at
`GET /v1/bridge-manifests` (no auth required). Adding `static manifest` to your class
and registering it in `BUILT_IN_BRIDGES` is all that's needed — no other wiring.

## How releases work

Each package versions and publishes independently via [release-please](https://github.com/googleapis/release-please) running in **manifest mode**. Conventional commits in `packages/<name>/` drive version bumps for that package only. The `node-workspace` plugin auto-rewrites `workspace:*` deps to real semver ranges on publish and ripple-bumps dependents (e.g., bumping `bridge-postgres` triggers a `bridge-resolver` patch bump).

## License

MIT
