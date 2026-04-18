<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-mssql</strong>
</p>

---

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
