# Contributing to SemiLayer Bridges

Thanks for your interest in adding a database adapter to SemiLayer. This monorepo is the canonical home for every official bridge — the SDK, the resolver/registry, and every first-party adapter live as workspace siblings under `packages/`.

## Repo layout

```
bridge-sdk/
├── lib/                    # The framework (rarely changes)
│   ├── bridge-sdk/         # Bridge interface, MockBridge, compliance test suite
│   └── bridge-resolver/    # Built-in registry — register new bridges here
├── packages/               # The bridges (this is where the ecosystem grows)
│   ├── bridge-postgres/    # Reference implementation
│   └── bridge-<yours>/     # Your new bridge
├── templates/bridge/       # Template scaffolded by `pnpm new-bridge`
└── scripts/new-bridge.ts   # Scaffolding CLI
```

`lib/` and `packages/` are both pnpm workspace globs — they're treated identically by the tooling. The split is purely organizational: it keeps `packages/` unambiguously "bridge adapters" so contributors aren't visually mixed in with the framework pieces.

## Local development

```bash
pnpm install
pnpm turbo build         # build all packages
pnpm turbo test          # run all tests
pnpm turbo lint
pnpm turbo typecheck
```

Filter to a single package:

```bash
pnpm turbo test --filter @semilayer/bridge-postgres
```

## Adding a new bridge

### 1. Scaffold the package

```bash
pnpm new-bridge mysql
```

This creates `packages/bridge-mysql/` from `templates/bridge/` and registers it in `release-please-config.json` + `.release-please-manifest.json`. Bridge name should be lowercase, the canonical short name of the database (`mysql`, `sqlite`, `cockroachdb`, `dynamodb`, etc.) — don't include the `bridge-` prefix, the script adds it.

### 2. Implement the Bridge interface

Open `packages/bridge-<name>/src/bridge.ts`. The contract is defined in `@semilayer/core`:

| Method | Required | Purpose |
|---|---|---|
| `connect()` | yes | Open the connection / pool. |
| `read(target, opts?)` | yes | Paginated read of rows from a table/collection. Honor `limit`, `cursor`, `fields`, `changedSince`. |
| `count(target)` | yes | Total row count for a target. |
| `query(target, opts)` | yes | Filtered query — `where`, `orderBy`, `limit`, `offset`, `select`. |
| `disconnect()` | yes | Close the connection / pool. |

Look at `packages/bridge-postgres/src/bridge.ts` as the canonical reference — it shows cursor-based keyset pagination, change tracking via `changedSince`, and how to translate `QueryOptions` into native query syntax.

### 3. Wire up the compliance test suite

Open `packages/bridge-<name>/src/bridge.test.ts` and uncomment the `createBridgeTestSuite` block. The suite spins up your bridge against a real seeded fixture and exercises every Bridge contract method (read pagination, cursor advancement, count, query filtering, ordering, etc.).

For databases that need a live fixture (Postgres, MySQL, Mongo, etc.), use Testcontainers or document a `docker compose` requirement in your bridge's README. Tests must be hermetic — running `pnpm turbo test` from a clean clone should pass.

### 4. Register the bridge in the resolver

Edit `lib/bridge-resolver/src/index.ts` and add your bridge to `BUILT_IN_BRIDGES`:

```ts
import { MySQLBridge } from '@semilayer/bridge-mysql'

const BUILT_IN_BRIDGES: Record<string, BridgeConstructor> = {
  '@semilayer/bridge-postgres': PostgresBridge,
  '@semilayer/bridge-mysql': MySQLBridge,
}
```

Also add your bridge to `lib/bridge-resolver/package.json` as a `dependencies` entry with `"workspace:*"`.

### 5. Verify everything passes

```bash
pnpm install
pnpm turbo build test lint typecheck
```

### 6. Open a PR

Use [Conventional Commits](https://www.conventionalcommits.org/) for commit messages — release-please drives version bumps and changelogs from the commit history:

- `feat: initial release of @semilayer/bridge-mysql` → minor bump (or initial 0.1.0)
- `fix(bridge-mysql): handle null in keyset pagination` → patch bump
- `feat(bridge-mysql)!: rename connection option` → major bump (note the `!`)

Scope your commits to the package being changed (`feat(bridge-mysql): ...`). Release-please uses this to figure out which package to bump.

When the PR is merged, release-please opens a release PR with the version bump + changelog. Merging that release PR triggers npm publish.

## Versioning and releases

- Each package versions and publishes **independently** via release-please manifest mode.
- The `node-workspace` plugin auto-rewrites `workspace:*` deps to real semver ranges on publish, and ripple-bumps dependents (e.g., bumping `bridge-postgres` triggers a `bridge-resolver` patch bump).
- The semilayer monorepo (`apps/worker`, `apps/service`) consumes `@semilayer/bridge-resolver` from npm. Once your bridge is published and bridge-resolver is bumped, the next `pnpm install` in the monorepo picks it up automatically.

## Private / enterprise bridges

If your bridge wraps proprietary code (internal company database, vendor adapter you can't open-source), don't PR it here. Instead, build it as a private package and register it at runtime in your worker startup:

```ts
import { registerBridge } from '@semilayer/bridge-resolver'
import { AcmeBridge } from '@acme-corp/semilayer-bridge'

registerBridge('@acme-corp/semilayer-bridge', AcmeBridge)
```

Runtime registration only affects that deployment — the public registry stays clean.

## Code style

- TypeScript strict mode, ESM only.
- Prettier (no semis, single quotes, trailing commas) — `pnpm format` to auto-fix.
- ESLint flat config — `pnpm turbo lint` to check.
- Bridge classes export named (`PostgresBridge`, not `default`).
- Connection config types export alongside the bridge class (`PostgresBridgeConfig`).

## Questions

Open an issue at [github.com/semilayer/bridge-sdk/issues](https://github.com/semilayer/bridge-sdk/issues) or tag a maintainer in your PR.
