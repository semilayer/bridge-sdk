<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-postgres</strong>
</p>

---

# @semilayer/bridge-postgres

First-party PostgreSQL adapter for [SemiLayer](https://semilayer.com) — the intelligence layer for any database.

This package implements the SemiLayer `Bridge` interface for PostgreSQL, enabling ingest, search, similarity, and direct queries against any Postgres database (including hosted flavors like Neon, Supabase, CockroachDB, and others that speak the Postgres wire protocol).

## Installation

```bash
npm install @semilayer/bridge-postgres
# or
pnpm add @semilayer/bridge-postgres
```

## Usage

```typescript
import { PostgresBridge } from '@semilayer/bridge-postgres'

const bridge = new PostgresBridge({
  url: 'postgresql://user:pass@host:5432/db',
  pool: { min: 0, max: 5 }, // optional
})

await bridge.connect()

// Paginated read with keyset cursor
const result = await bridge.read('articles', {
  limit: 100,
  cursor: undefined, // first page
})

console.log(result.rows)        // BridgeRow[]
console.log(result.nextCursor)  // string | undefined
console.log(result.total)       // total row count

// Direct query with filters
const { rows } = await bridge.query('articles', {
  where: { status: 'published', views: { $gt: 100 } },
  orderBy: [{ field: 'created_at', dir: 'desc' }],
  limit: 20,
})

await bridge.disconnect()
```

## Schema Introspection

```typescript
import { introspect, listTables } from '@semilayer/bridge-postgres'
import pg from 'pg'

const pool = new pg.Pool({ connectionString: '...' })

const tables = await listTables(pool)
const info = await introspect(pool, 'articles')
// info.columns: ColumnInfo[] with type, nullable, primaryKey
```

## Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | `string` | **required** | Postgres connection string |
| `pool.min` | `number` | `0` | Minimum pool connections |
| `pool.max` | `number` | `3` | Maximum pool connections |

Accepts `connectionString` as an alias for `url`.

## Query Operators

The `query()` method supports MongoDB-style operators in the `where` clause:

| Operator | SQL | Example |
|----------|-----|---------|
| `$eq` | `=` | `{ status: { $eq: 'active' } }` |
| `$gt` | `>` | `{ age: { $gt: 18 } }` |
| `$gte` | `>=` | `{ score: { $gte: 50 } }` |
| `$lt` | `<` | `{ age: { $lt: 65 } }` |
| `$lte` | `<=` | `{ score: { $lte: 100 } }` |
| `$in` | `= ANY(...)` | `{ status: { $in: ['a', 'b'] } }` |

Bare values default to `$eq`: `{ status: 'active' }` is equivalent to `{ status: { $eq: 'active' } }`.

## Incremental Ingest

`read()` supports `changedSince` for incremental reads, tracking a configurable column (default `updated_at`):

```typescript
await bridge.read('articles', {
  changedSince: new Date('2026-01-01'),
  changeTrackingColumn: 'modified_at', // optional
  limit: 1000,
})
```

If the column does not exist on the table, the `changedSince` filter is silently dropped and a full read is performed.

## Requirements

- Node.js 20+
- PostgreSQL 13+ (any version supported by the `pg` driver)
- `@semilayer/core` (peer-compatible version)

## Compliance Testing

This package passes the SemiLayer bridge compliance suite:

```typescript
import { createBridgeTestSuite } from '@semilayer/bridge-sdk'
import { PostgresBridge } from '@semilayer/bridge-postgres'

createBridgeTestSuite(
  () => new PostgresBridge({ url: process.env.DATABASE_URL! }),
)
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for details on running tests.

## Links

- [SemiLayer documentation](https://semilayer.dev)
- [Bridge authoring guide](https://semilayer.dev/guides/bridges)
- [Bridge resolver & community bridges](https://github.com/semilayer/bridge-resolver)

## License

MIT © SemiLayer
