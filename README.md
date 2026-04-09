# SemiLayer Bridges

The home of every official SemiLayer database bridge — the SDK that defines the bridge interface, the registry that resolves bridge names at runtime, and every first-party adapter — all in one monorepo.

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

## How releases work

Each package versions and publishes independently via [release-please](https://github.com/googleapis/release-please) running in **manifest mode**. Conventional commits in `packages/<name>/` drive version bumps for that package only. The `node-workspace` plugin auto-rewrites `workspace:*` deps to real semver ranges on publish and ripple-bumps dependents (e.g., bumping `bridge-postgres` triggers a `bridge-resolver` patch bump).

## License

MIT
