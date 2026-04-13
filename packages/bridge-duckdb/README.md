# @semilayer/bridge-duckdb

SemiLayer bridge for duckdb.

## Install

```bash
npm install @semilayer/bridge-duckdb
```

## Usage

```ts
import { DuckdbBridge } from '@semilayer/bridge-duckdb'

const bridge = new DuckdbBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `DuckdbBridgeConfig` fields.

## License

MIT
