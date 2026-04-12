# @semilayer/bridge-mssql

SemiLayer bridge for mssql.

## Install

```bash
npm install @semilayer/bridge-mssql
```

## Usage

```ts
import { MssqlBridge } from '@semilayer/bridge-mssql'

const bridge = new MssqlBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `MssqlBridgeConfig` fields.

## License

MIT
