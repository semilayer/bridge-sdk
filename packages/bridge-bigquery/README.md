# @semilayer/bridge-bigquery

SemiLayer bridge for bigquery.

## Install

```bash
npm install @semilayer/bridge-bigquery
```

## Usage

```ts
import { BigqueryBridge } from '@semilayer/bridge-bigquery'

const bridge = new BigqueryBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `BigqueryBridgeConfig` fields.

## License

MIT
