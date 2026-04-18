<p align="right">
  <img src="https://semilayer.com/logo.svg" alt="SemiLayer" width="120" />
  <br />
  <strong>@semilayer/bridge-clickhouse</strong>
</p>

---

# @semilayer/bridge-clickhouse

SemiLayer bridge for clickhouse.

## Install

```bash
npm install @semilayer/bridge-clickhouse
```

## Usage

```ts
import { ClickhouseBridge } from '@semilayer/bridge-clickhouse'

const bridge = new ClickhouseBridge({
  // TODO: connection options
})

await bridge.connect()
const result = await bridge.read('my_table', { limit: 100 })
```

## Configuration

TODO: document `ClickhouseBridgeConfig` fields.

## License

MIT
